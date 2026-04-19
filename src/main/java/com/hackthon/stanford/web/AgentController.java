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

    String myPromtp = "ve Budget Decision Framework Standard Template            1 Creative OverviewCreative ID <creative_id>Channel <Google or Meta or etc>Creative Type <DCO or Brand or Carousel or Video or Static>Objective <Conversion or Traffic or Awareness>  2 Key Performance MetricsDefine a consistent metric schemaSpend $XRevenue $XROAS X.XCPA $XCTR X percentCVR X percentImpressions XFrequency X.X if applicable   3 Benchmark ComparisonCompare against account campaign or category baseline        ROAS vs Benchmark plus X percent or minus X percentCPA vs Target plus X percent or minus X percentCTR vs Avg plus X percent or minus X percent          InterpretationOutperforming or On par or Underperforming         4 Trend Analysis Time basedROAS trend Increasing or Stable or DecliningCPA trend Improving or WorseningSpend efficiency Scaling well or Saturating           OptionalLearning phase Yes or NoVolatility level High or Medium or Low        5 Signal Diagnosis Why it performs this wayPositive SignalsHigh CTR means strong creative hookHigh CVR means strong landing page alignmentStable ROAS at scale means scalable creative      Negative SignalsHigh CTR but low CVR means mismatch intentHigh CPA means poor audience targeting or fatigueDeclining ROAS means saturation or competition       6 Budget Decision Logic       Scale UpConditionsROAS greater than target and stable or improvingCPA within acceptable rangeNo strong fatigue signals        ActionIncrease budget by plus 20 percent to plus 50 percentExpand audience or placements if applicable     MaintainConditionsROAS near targetMixed or uncertain signalsStill in learning phase                       ActionKeep budget stableContinue monitoringRun AB tests                       Reduce or PauseConditionsROAS significantly below targetCPA too highNegative trend over time        ActionDecrease budget by minus 30 percent to minus 70 percent or pauseInvestigate creative or targeting issues          7 Final Recommendation LLM Output Format          Decision Scale or Maintain or ReduceBudget Change plus X percent or 0 percent or minus X percentConfidence Level High or Medium or Low        ReasoningThis creative shows key performance summary Compared to benchmark it is outperforming or underperforming The trend indicates trend suggesting core insight Therefore the recommended action is to decision         8 Example Outputs for Your Creatives           Example 1 cr adlyze google dcoDecision ScaleBudget Change plus 30 percentConfidence High        ReasoningThis DCO creative demonstrates strong ROAS and consistent performance above benchmark CTR and CVR indicate strong personalization effectiveness The upward trend suggests scalability so increasing budget is recommended           Example 2 cr adlyze google brandDecision MaintainBudget Change 0 percentConfidence Medium       ReasoningThis brand creative shows stable performance with ROAS near target but limited conversion efficiency As it supports upper funnel engagement maintaining budget while testing variations is appropriate            Example 3 cr adlyze meta carouselDecision ReduceBudget Change minus 40 percentConfidence High      ReasoningThis carousel creative has declining ROAS and rising CPA indicating fatigue and reduced engagement Benchmark comparison shows underperformance so budget reduction is recommended while iterating on new creatives      9 Optional Structured JSON Output for system use creative_id cr adlyze meta carouseldecision reducebudget_change_pct minus 40confidence highreasoning Declining ROAS and increasing CPA indicate performance deterioration and creative fatigue      Pro Tip for your DeepChatBI system      You can standardize this intoPrompt Template to LLMMetrics to BigQueryGraph Context to Neo4jFinal Output to Decision Engine        If you want I can next help you convert this into a production grade prompt system plus user plus tool format or a Neo4j driven causal reasoning layer";
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
                    "{\"model\":\"qwen3-30b-a3b\",\"messages\":[{\"role\":\"user\",\"content\":\""+input+"\"}],\"stream\":false,\"temperature\":0.7}"

            );
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
