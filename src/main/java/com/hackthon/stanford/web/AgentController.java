package com.hackthon.stanford.web;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.hackthon.stanford.chat.StanfordStreamV5Service;
import com.hackthon.stanford.chat.dto.AgentStreamChunk;
import com.hackthon.stanford.chat.dto.AgentStreamChunk.TextDelta;
import com.hackthon.stanford.neo4j.AdsNlGraphPromptAugmentor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 广告归因 Agent 网关：所有 {@code /api/chat/stream/v5} 请求在下游执行前会注入 {@link #ADS_ATTRIBUTION_IDENTITY} 人格（DeepChatBI 风格 / 投放与归因）。
 * <p>
 * 默认 {@code graphRag=true}：将用户 {@code content} 经 {@link com.hackthon.stanford.neo4j.AdsNaturalLanguageGraphPlanner} 转为检索计划（含伪 Cypher），
 * 再经 {@link com.hackthon.stanford.neo4j.Neo4jAdsAttributionRagService} 拉取 Creative 子图事实，写入 {@code skillsPrompt} 的 Step1/Step2 块，最后由 LLM 结合用户问题做总结。
 * 关闭图增强：{@code graphRag=false}（若需兼容旧行为可再设 {@code neoBrain=true} 单独打开）。
 * </p>
 * <p>
 * 每条 SSE 事件只写 {@code data:} 行中的 {@link AgentStreamChunk#getPlainData()} 文本（无外层 chunk JSON）；若 {@code plainData} 为空则用 {@code delta.text} 兜底。
 * 与 EventSource / curl -N 兼容。
 * 使用 {@link StreamingResponseBody} 而非 {@code SseEmitter}+异步 {@code subscribe}，避免部分环境下
 * 响应尚未提交即关闭连接（curl 52 Empty reply、后端看不到已处理请求）。
 * </p>
 */
@Slf4j
@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/api/chat")
@RequiredArgsConstructor
public class AgentController {

    private static final URI DEEPCHATBI_STREAM_V5 = URI.create("http://45.78.204.144:9090/api/chat/stream/v5");

    String myPromtp = """
            🔹 Creative Budget Decision Framework (reference row: cr_adlyze_google_dco — last 30 days shop reporting)
            1. Creative Overview

            Creative ID: cr_adlyze_google_dco
            Channel: Google Ads (Search + PMax overlap)
            Creative Type: DCO / dynamic catalog
            Objective: Purchase / conversion value

            2. Key Performance Metrics (snapshot)

            Spend: $18,420
            Attributed revenue: $91,050
            ROAS: 4.94 (revenue ÷ spend)
            CPA: $38.20 (cost per purchase)
            CTR: 2.35%
            CVR: 4.15%
            Impressions: 1,240,000
            Frequency: 2.28

            3. Benchmark Comparison (vs account baseline week of 2026-04-07)

            Account ROAS baseline: 3.85 | Account CPA target: $41.00 | Account CTR avg: 2.10%

            ROAS vs benchmark: +28.3%
            CPA vs target: -6.8% (better than target)
            CTR vs avg: +11.9%

            Interpretation: Outperforming — ROAS and CTR above baseline; CPA below target cap.

            4. Trend Analysis (Apr 1 → Apr 18, 2026)

            ROAS trend: Increasing (+0.42 vs prior 14d)

            CPA trend: Improving (-$4.10 vs prior 14d)

            Spend efficiency: Scaling well — marginal ROAS holding while daily spend +12%

            Learning phase? No (campaign exited learning on Apr 04)

            Volatility level: Medium (σ ROAS 0.31 across weeks)

            5. Signal Diagnosis

            Positive signals observed for cr_adlyze_google_dco:
            CTR 2.35% vs 2.10% avg → stronger hook / relevance

            CVR 4.15% → PDP + offer alignment holding

            ROAS 4.94 at $18.4k spend → efficiency not collapsing at scale

            Risk signals to monitor on sister asset cr_adlyze_meta_carousel (not this row): high CTR pocket with CPA spike on Apr 14 (one-day anomaly) — watch audience overlap.

            6. Budget Decision Logic

            ✅ Scale Up — applies to cr_adlyze_google_dco given section 3–5

            ROAS 4.94 > target band 3.6–4.2 and rising

            CPA $38.20 < $41.00 cap

            No fatigue pattern (frequency 2.28 < 3.0 guardrail)

            Planned action: raise daily budget +28% for 7 days (within +20–50% band), cap frequency at 3.0 through placement exclusions if exceeded.

            ⚖️ Maintain — pattern for cr_adlyze_google_brand

            ROAS 3.92 vs baseline 3.85 (within ±5%); mixed upper-funnel assist

            Hold spend; swap 2 headline variants (test IDs hb_042a / hb_042b)

            ❌ Reduce / Pause — pattern for cr_adlyze_meta_carousel

            ROAS slipped to 2.05 vs 3.85 baseline; CPA $63.40 vs $41 target

            Cut daily budget -45% for 10 days; rebuild carousel cards C2-C4 before re-scale

            7. Final Recommendation (filled example — cr_adlyze_google_dco)

            Decision: Scale

            Budget change: +28% (from $620/d to $794/d)

            Confidence: High

            Reasoning:
            Spend $18.4k returned $91.1k revenue (ROAS 4.94), beating account ROAS 3.85 by +28% with CPA under the $41 target. Trends are up and frequency is controlled, so expanding budget preserves efficiency.

            8. Cross-creative examples (same framework, real-style numbers)

            Example A — cr_adlyze_google_dco

            Decision: Scale | Budget: +28% | Confidence: High

            Reasoning: DCO personalization driving 4.15% CVR and +12% CTR vs avg; scalable.

            Example B — cr_adlyze_google_brand

            Decision: Maintain | Budget: 0% | Confidence: Medium

            Reasoning: ROAS 3.92 near baseline; upper-funnel support — keep spend, iterate copy.

            Example C — cr_adlyze_meta_carousel

            Decision: Reduce | Budget: -45% | Confidence: High

            Reasoning: ROAS 2.05, CPA $63.4, rising frequency 3.4 — fatigue; cut and refresh.

            9. Structured JSON (machine-readable mirror of Example C)

            {
              "creative_id": "cr_adlyze_meta_carousel",
              "decision": "reduce",
              "budget_change_pct": -45,
              "confidence": "high",
              "reasoning": "ROAS 2.05 vs account 3.85; CPA $63.40 vs $41.00 target; fatigue + overlap with Google DCO winners."
            }

            💡 Pro Tip (production wiring)

            BigQuery fact table: shopify_orders_attributed + ad_platform_spend_daily

            Neo4j graph: (:Creative)-[:PERFORMED_IN]->(:Campaign) with ROAS / budgetSignal edges

            Orchestrator: rules above + LLM narrative in section 7
            """;
    /**
     * Fixed persona: ads attribution / growth analytics (not generic small-talk).
     * Prepended to {@code skillsPrompt} so the LLM carries role, tone, and output shape.
     */
    private static final String ADS_ATTRIBUTION_IDENTITY = """
            === ADS_ATTRIBUTION_AGENT (DeepChatBI-style) ===
            You are an **advertising attribution and profit analytics** copilot (Shopify + ad platforms + GA4 mental model).
            You help operators decide whether to **加钱 (scale budget)** or **减钱 (cut / rebid)** at **Creative** level, using ROAS / POAS / MER, attributed orders, and journey context — not vibes.

            Core stance:
            - **Data-first**: cite retrieved graph facts (spend, attributed revenue, ROAS, budgetSignal, orders) when present; separate “measured in graph” vs assumptions.
            - **Incrementality-aware**: mention first/last click limits when relevant; suggest experiments (geo holdout, budget ladder) when data is thin.
            - **Merchant-safe**: no guaranteed revenue; frame decisions as risk-managed bets.

            How you write:
            - For budget questions: lead with **Recommendation** (加钱 / 减钱 / 保持) mapped from `budgetSignal` / ROAS / POAS logic, then **Evidence**, then **Risks / next checks**.
            - When **Step 1 / Step 2** blocks appear, mirror the retrieval plan briefly, then answer **using those Creative rows**.

            Language: match the user's language when they write in Chinese; otherwise English is fine.
            === END_ADS_ATTRIBUTION_AGENT ===
            """;

    private static final String SRE_PLAYBOOK_INC042 = loadClasspathUtf8("prompts/sre-incident-postmortem-brief.txt");

    private final StanfordStreamV5Service stanfordStreamV5Service;
    private final ObjectProvider<AdsNlGraphPromptAugmentor> nlGraphPromptAugmentor;

    /** 用于确认路由与端口（应先看到 {@link HttpRequestLogFilter} 再打本接口）。 */
    @GetMapping(value = "/health", produces = MediaType.TEXT_PLAIN_VALUE)
    public String health() {
        return "ok";
    }

    @PostMapping(value = "/stream/v5", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> streamV5Post(
            @RequestBody(required = false) AgentStreamChunk req,
            @RequestParam(value = "deepChatBiApi", defaultValue = "false") boolean deepChatBiApi,
            @RequestParam(value = "sreBrain", defaultValue = "false") boolean sreBrain,
            @RequestParam(value = "graphRag", defaultValue = "true") boolean graphRag,
            @RequestParam(value = "neoBrain", defaultValue = "false") boolean neoBrain,
            @RequestParam(value = "graphLimit", required = false) Integer graphLimit) {
        log.info("/stream/v5 POST body: {}", req == null ? "null" : JSON.toJSONString(req));
        if (deepChatBiApi) {
            return streamViaDeepChatBiApi(req);
        }
        AgentStreamChunk effective = applySreLayers(req, sreBrain);
        boolean augmentGraph = graphRag || neoBrain;
        return streamV5Entity(effective, augmentGraph, graphLimit);
    }

    /**
     * 与 {@link #streamV5Post} 相同 SSE 契约；可用查询参数拼请求，或用 {@code payload} 传整条 {@link AgentStreamChunk} 的 JSON（需 URL 编码）。
     */
    /** No {@code produces}: browser GET often sends {@code Accept: text/html}; narrow JSON-only would yield 406. */
    @GetMapping("/stream/v5")
    public ResponseEntity<StreamingResponseBody> streamV5Get(
            @RequestParam(value = "payload", required = false) String payload,
            @RequestParam(value = "content", required = false) String content,
            @RequestParam(value = "sessionId", required = false) String sessionId,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "agentName", required = false) String agentName,
            @RequestParam(value = "nextStepAgent", required = false) String nextStepAgent,
            @RequestParam(value = "stopStream", required = false) Boolean stopStream,
            @RequestParam(value = "skillsPrompt", required = false) String skillsPrompt,
            @RequestParam(value = "deepChatBiApi", defaultValue = "false") boolean deepChatBiApi,
            @RequestParam(value = "sreBrain", defaultValue = "false") boolean sreBrain,
            @RequestParam(value = "graphRag", defaultValue = "true") boolean graphRag,
            @RequestParam(value = "neoBrain", defaultValue = "false") boolean neoBrain,
            @RequestParam(value = "graphLimit", required = false) Integer graphLimit) {
        final AgentStreamChunk req;
        content = content + myPromtp;
        if (StringUtils.hasText(payload)) {
            try {
                req = JSON.parseObject(payload, AgentStreamChunk.class);
            } catch (Exception e) {
                log.warn("/stream/v5 GET invalid payload JSON", e);
                return sseErrorResponse(400, AgentStreamChunk.createErrorEvent("MasterV5Agent",
                        "invalid GET query param payload JSON: " + e.getMessage()));
            }
        } else {
            req = AgentStreamChunk.builder()
                    .content(content)
                    .sessionId(sessionId)
                    .userId(userId)
                    .agentName(agentName)
                    .nextStepAgent(nextStepAgent)
                    .stopStream(stopStream)
                    .skillsPrompt(skillsPrompt)
                    .build();
        }
        AgentStreamChunk effective = applySreLayers(req, sreBrain);
        log.info("/stream/v5 GET -> chunk: {}", JSON.toJSONString(effective));
        return runHardcodedIonRouterCurlViaProcess(content);
    }

    /**
     * Exact {@code curl} the user specified (hardcoded); parses OpenAI-shaped JSON and returns only
     * {@code choices[0].message.content} as plain text for the frontend.
     */
    private static ResponseEntity<StreamingResponseBody> runHardcodedIonRouterCurlViaProcess(String input) {
        StreamingResponseBody body = outputStream -> {
            String userJson = JSON.toJSONString(input == null ? "" : input);
            String jsonBody = "{\"model\":\"qwen3-30b-a3b\",\"messages\":[{\"role\":\"user\",\"content\":"
                    + userJson + "}],\"stream\":false,\"temperature\":0.7}";
            ProcessBuilder pb = new ProcessBuilder(
                    "curl",
                    "-sS",
                    "-N",
                    "https://api.ionrouter.io/v1/chat/completions",
                    "-H",
                    "Authorization: Bearer sk-23a4af7f666f5e97d42c44b1507b0b7e19c1ff758fbb4a54",
                    "-H",
                    "Content-Type: application/json",
                    "-d",
                    jsonBody);
            pb.redirectErrorStream(true);
            try {
                Process p = pb.start();
                String raw;
                try (InputStream in = p.getInputStream()) {
                    raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
                int exit = p.waitFor();
                String assistant = extractChatCompletionAssistantContent(raw);
                if (StringUtils.hasText(assistant)) {
                    outputStream.write(assistant.getBytes(StandardCharsets.UTF_8));
                    return;
                }
                String err = extractOpenAiStyleErrorMessage(raw);
                if (StringUtils.hasText(err)) {
                    outputStream.write(err.getBytes(StandardCharsets.UTF_8));
                    return;
                }
                String fallback = exit != 0 ? ("curl exit " + exit + ": ") : "";
                outputStream.write((fallback + raw).getBytes(StandardCharsets.UTF_8));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outputStream.write("interrupted".getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                String msg = e.getMessage() == null ? e.toString() : e.getMessage();
                outputStream.write(msg.getBytes(StandardCharsets.UTF_8));
            }
        };
        return ResponseEntity.ok()
                .contentType(new MediaType("text", "plain", StandardCharsets.UTF_8))
                .cacheControl(CacheControl.noCache())
                .body(body);
    }

    /** {@code choices[0].message.content} from chat/completions JSON. */
    private static String extractChatCompletionAssistantContent(String raw) {
        if (!StringUtils.hasText(raw)) {
            return "";
        }
        try {
            JSONObject root = JSON.parseObject(raw);
            if (root == null) {
                return "";
            }
            JSONArray choices = root.getJSONArray("choices");
            if (choices == null || choices.isEmpty()) {
                return "";
            }
            JSONObject first = choices.getJSONObject(0);
            if (first == null) {
                return "";
            }
            JSONObject message = first.getJSONObject("message");
            if (message == null) {
                return "";
            }
            String content = message.getString("content");
            return content != null ? content : "";
        } catch (Exception e) {
            return "";
        }
    }

    private static String extractOpenAiStyleErrorMessage(String raw) {
        if (!StringUtils.hasText(raw)) {
            return "";
        }
        try {
            JSONObject root = JSON.parseObject(raw);
            if (root == null) {
                return "";
            }
            JSONObject err = root.getJSONObject("error");
            if (err == null) {
                return "";
            }
            String m = err.getString("message");
            return m != null ? m : err.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static String firstNonBlank(String a, String b) {
        return StringUtils.hasText(a) ? a : b;
    }

    /** Client disconnect or Spring async timeout interrupts the pump thread — not an upstream bug. */
    private static boolean isBenignProxyStreamEnd(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                return true;
            }
        }
        if (e instanceof IOException io) {
            String m = io.getMessage() == null ? "" : io.getMessage();
            if (m.contains("InterruptedException")) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasInterruptedCause(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Passthrough proxy to DeepChatBI API (remote SSE). We send a DeepChatBI-shaped JSON body,
     * then stream the upstream {@code text/event-stream} bytes back to the client unchanged.
     */
    private ResponseEntity<StreamingResponseBody> streamViaDeepChatBiApi(AgentStreamChunk req) {
        if (req == null) {
            return sseErrorResponse(400, AgentStreamChunk.createErrorEvent("DeepChatBIAPI", "empty request body"));
        }

        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("userId", firstNonBlank(req.getUserId(), "test-user"));
        payload.put("sessionId", firstNonBlank(req.getSessionId(), "task-session-001"));
        payload.put("lang", firstNonBlank(req.getLang(), "Chinese"));
        payload.put("datasourceId", req.getDatasourceId() == null ? 1 : req.getDatasourceId());
        payload.put("modelType", firstNonBlank(req.getModelType(), "QWEN"));
        // payload.put("agentName", firstNonBlank(req.getAgentName(), "TaskSystemV5StreamAgent"));
        payload.put("content", firstNonBlank(req.getContent(), ""));

        final String jsonBody = JSON.toJSONString(payload);

        StreamingResponseBody body = outputStream -> {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(DEEPCHATBI_STREAM_V5)
                    .timeout(Duration.ofMinutes(30))
                    .header("Content-Type", "application/json")
                    .header("Accept", "text/event-stream")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            try {
                HttpResponse<InputStream> res = client.send(httpReq, HttpResponse.BodyHandlers.ofInputStream());
                int code = res.statusCode();
                if (code < 200 || code >= 300) {
                    String err = "DeepChatBIAPI upstream error: HTTP " + code;
                    log.warn(err);
                    writeSseEvent(outputStream, AgentStreamChunk.createErrorEvent("DeepChatBIAPI", err));
                    return;
                }

                try (InputStream is = res.body()) {
                    byte[] buf = new byte[8 * 1024];
                    int n;
                    while ((n = is.read(buf)) >= 0) {
                        outputStream.write(buf, 0, n);
                        outputStream.flush();
                    }
                }
            } catch (Exception e) {
                if (isBenignProxyStreamEnd(e)) {
                    if (hasInterruptedCause(e)) {
                        Thread.currentThread().interrupt();
                    }
                    log.debug("DeepChatBIAPI proxy ended early: {}", e.toString());
                    return;
                }
                String msg = e.getMessage() == null ? e.toString() : e.getMessage();
                log.warn("DeepChatBIAPI proxy failed", e);
                try {
                    writeSseEvent(outputStream, AgentStreamChunk.createErrorEvent("DeepChatBIAPI", msg));
                } catch (Exception ignored) {
                    /* response may already be committed / client gone */
                }
            }
        };

        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .cacheControl(CacheControl.noCache())
                .body(body);
    }

    /** Playbook merge only; Neo4j NL→graph augment runs in {@link #streamV5Entity}. */
    private static AgentStreamChunk applySreLayers(AgentStreamChunk req, boolean sreBrain) {
        AgentStreamChunk e = req;
        if (sreBrain) {
            e = mergeSrePlaybook(e);
        }
        return e;
    }

    private static int clampGraphLimit(Integer n) {
        if (n == null) {
            return 3;
        }
        return Math.min(Math.max(n, 1), 10);
    }

    private static AgentStreamChunk applyAdsAgentPersona(AgentStreamChunk req) {
        String tail = req.getSkillsPrompt();
        String head = ADS_ATTRIBUTION_IDENTITY.trim();
        String merged = StringUtils.hasText(tail) ? head + "\n\n" + tail : head;
        return req.toBuilder().skillsPrompt(merged).build();
    }

    private static AgentStreamChunk mergeSrePlaybook(AgentStreamChunk req) {
        if (req == null || !StringUtils.hasText(SRE_PLAYBOOK_INC042)) {
            return req;
        }
        String existing = req.getSkillsPrompt();
        String merged = StringUtils.hasText(existing)
                ? existing + "\n\n---\n" + SRE_PLAYBOOK_INC042
                : SRE_PLAYBOOK_INC042;
        return req.toBuilder().skillsPrompt(merged).build();
    }

    private static String loadClasspathUtf8(String path) {
        ClassLoader cl = AgentController.class.getClassLoader();
        try (InputStream is = cl.getResourceAsStream(path)) {
            if (is == null) {
                return "";
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private ResponseEntity<StreamingResponseBody> streamV5Entity(AgentStreamChunk req, boolean graphAugment, Integer graphLimit) {
        if (req == null) {
            return sseErrorResponse(400, AgentStreamChunk.createErrorEvent("MasterV5Agent", "empty request body"));
        }

        AgentStreamChunk forModel = req;
        if (graphAugment) {
            AdsNlGraphPromptAugmentor augmentor = nlGraphPromptAugmentor.getIfAvailable();
            if (augmentor != null) {
                forModel = augmentor.augment(req, clampGraphLimit(graphLimit));
            }
        }

        AgentStreamChunk withAdsPersona = applyAdsAgentPersona(forModel);

        StreamingResponseBody body = outputStream -> {
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true)) {
                Flux<ServerSentEvent<AgentStreamChunk>> flux = stanfordStreamV5Service.streamV5(withAdsPersona)
                        .onErrorResume(err -> {
                            log.error("/stream/v5 pipeline error", err);
                            String msg = err.getMessage() == null ? err.toString() : err.getMessage();
                            return Flux.just(ServerSentEvent.builder(
                                    AgentStreamChunk.createErrorEvent("MasterV5Agent", msg)
                            ).build());
                        });
                flux.doOnNext(sse -> {
                    AgentStreamChunk chunk = sse.data();
                    if (chunk == null) {
                        return;
                    }
                    writeSsePlainDataLine(writer, chunk);
                    writer.flush();
                }).blockLast(Duration.ofMinutes(30));
            }
        };

        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .cacheControl(CacheControl.noCache())
                .body(body);
    }

    private static ResponseEntity<StreamingResponseBody> sseErrorResponse(int status, AgentStreamChunk chunk) {
        StreamingResponseBody err = outputStream -> writeSseEvent(outputStream, chunk);
        ResponseEntity.BodyBuilder b = status == 400 ? ResponseEntity.badRequest() : ResponseEntity.status(status);
        return b.contentType(MediaType.TEXT_EVENT_STREAM)
                .cacheControl(CacheControl.noCache())
                .body(err);
    }

    private static void writeSseEvent(java.io.OutputStream outputStream, AgentStreamChunk chunk) {
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true)) {
            writeSsePlainDataLine(writer, chunk);
        }
    }

    /**
     * One SSE event: only {@link AgentStreamChunk#getPlainData()} text (frontend-friendly).
     * If {@code plainData} is blank, falls back to {@link TextDelta#getText()} so complete/error/delta-only chunks still surface.
     */
    private static void writeSsePlainDataLine(PrintWriter writer, AgentStreamChunk chunk) {
        String payload = ssePlainPayload(chunk);
        if (payload.isEmpty()) {
            writer.print("data: \n\n");
            return;
        }
        for (String line : payload.split("\\R", -1)) {
            writer.print("data: ");
            writer.print(line);
            writer.print("\n");
        }
        writer.print("\n");
    }

    private static String ssePlainPayload(AgentStreamChunk chunk) {
        if (chunk == null) {
            return "";
        }
        if (StringUtils.hasText(chunk.getPlainData())) {
            return chunk.getPlainData();
        }
        Object delta = chunk.getDelta();
        if (delta instanceof TextDelta td && td.getText() != null) {
            return String.valueOf(td.getText());
        }
        return "";
    }
}
