package com.hackthon.stanford.web;

import com.alibaba.fastjson2.JSON;
import com.hackthon.stanford.chat.StanfordStreamV5Service;
import com.hackthon.stanford.chat.dto.AgentStreamChunk;
import com.hackthon.stanford.chat.dto.AgentStreamChunk.TextDelta;
import com.hackthon.stanford.neo4j.SreNlGraphPromptAugmentor;
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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * SRE Agent 网关：所有 {@code /api/chat/stream/v5} 请求在下游执行前会注入 {@link #SRE_AGENT_IDENTITY} 人格（Staff SRE / IncidentBrain）。
 * <p>
 * 默认 {@code graphRag=true}：将用户 {@code content} 经 {@link com.hackthon.stanford.neo4j.SreNaturalLanguageGraphPlanner} 转为检索计划（含伪 Cypher），
 * 再经 {@link com.hackthon.stanford.neo4j.Neo4jSreRagService} 拉取子图事实，写入 {@code skillsPrompt} 的 Step1/Step2 块，最后由 LLM 结合用户问题做总结。
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

    /**
     * Fixed persona: this endpoint is always an SRE agent, not a generic assistant.
     * Prepended to {@code skillsPrompt} so the LLM system message carries role, tone, and output shape.
     */
    private static final String SRE_AGENT_IDENTITY = """
            === SRE_AGENT_IDENTITY (IncidentBrain) ===
            You are **IncidentBrain**, a Staff-level **Site Reliability Engineering (SRE)** agent embedded in an on-call / incident-response workflow.
            You are not a generic chatbot: you reason like production SRE — reliability, SLOs, blast radius, rollback, observability, and post-incident learning.

            Core stance:
            - **Blameless**: assume good intent; focus on systems and process, not individuals.
            - **Impact-first**: clarify user impact, severity, and whether this is customer-facing before deep dives.
            - **Evidence-based**: separate facts (logs, metrics, configs) from hypotheses; say what you need to confirm.
            - **Safety**: prefer reversible changes; warn on risky commands; note namespace/cluster context when unknown.

            How you write:
            - Use clear structure when helpful: **Summary**, **Impact**, **Hypotheses**, **Checks / commands**, **Mitigation**, **Verification**, **Follow-ups**.
            - Be concise under pressure; use bullets and short paragraphs.
            - If the runtime cannot run shell/kubectl, still deliver **actionable** steps for a human operator (copy-paste commands, Grafana/PagerDuty hints).
            - When a playbook or background block is provided below, **treat it as trusted runbook context** and align your answer with it.
            - When a **Step 1 / Step 2** block appears (NL→graph plan + retrieval results), briefly mirror the plan, then **answer the user using those retrieved facts** (or explain if nothing matched).

            Language: match the user's language when they write in Chinese; otherwise English is fine.
            === END_SRE_AGENT_IDENTITY ===
            """;

    private static final String SRE_PLAYBOOK_INC042 = loadClasspathUtf8("prompts/sre-incident-postmortem-brief.txt");

    private final StanfordStreamV5Service stanfordStreamV5Service;
    private final ObjectProvider<SreNlGraphPromptAugmentor> nlGraphPromptAugmentor;

    /** 用于确认路由与端口（应先看到 {@link HttpRequestLogFilter} 再打本接口）。 */
    @GetMapping(value = "/health", produces = MediaType.TEXT_PLAIN_VALUE)
    public String health() {
        return "ok";
    }

    @PostMapping(value = "/stream/v5", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> streamV5Post(
            @RequestBody(required = false) AgentStreamChunk req,
            @RequestParam(value = "sreBrain", defaultValue = "false") boolean sreBrain,
            @RequestParam(value = "graphRag", defaultValue = "true") boolean graphRag,
            @RequestParam(value = "neoBrain", defaultValue = "false") boolean neoBrain,
            @RequestParam(value = "graphLimit", required = false) Integer graphLimit) {
        log.info("/stream/v5 POST body: {}", req == null ? "null" : JSON.toJSONString(req));
        AgentStreamChunk effective = applySreLayers(req, sreBrain);
        boolean augmentGraph = graphRag || neoBrain;
        return streamV5Entity(effective, augmentGraph, graphLimit);
    }

    /**
     * 与 {@link #streamV5Post} 相同 SSE 契约；可用查询参数拼请求，或用 {@code payload} 传整条 {@link AgentStreamChunk} 的 JSON（需 URL 编码）。
     */
    @GetMapping(value = "/stream/v5", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> streamV5Get(
            @RequestParam(value = "payload", required = false) String payload,
            @RequestParam(value = "content", required = false) String content,
            @RequestParam(value = "sessionId", required = false) String sessionId,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "agentName", required = false) String agentName,
            @RequestParam(value = "nextStepAgent", required = false) String nextStepAgent,
            @RequestParam(value = "stopStream", required = false) Boolean stopStream,
            @RequestParam(value = "skillsPrompt", required = false) String skillsPrompt,
            @RequestParam(value = "sreBrain", defaultValue = "false") boolean sreBrain,
            @RequestParam(value = "graphRag", defaultValue = "true") boolean graphRag,
            @RequestParam(value = "neoBrain", defaultValue = "false") boolean neoBrain,
            @RequestParam(value = "graphLimit", required = false) Integer graphLimit) {
        final AgentStreamChunk req;
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
        boolean augmentGraph = graphRag || neoBrain;
        return streamV5Entity(effective, augmentGraph, graphLimit);
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

    private static AgentStreamChunk applySreAgentPersona(AgentStreamChunk req) {
        String tail = req.getSkillsPrompt();
        String head = SRE_AGENT_IDENTITY.trim();
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
            SreNlGraphPromptAugmentor augmentor = nlGraphPromptAugmentor.getIfAvailable();
            if (augmentor != null) {
                forModel = augmentor.augment(req, clampGraphLimit(graphLimit));
            }
        }

        AgentStreamChunk withSrePersona = applySreAgentPersona(forModel);

        StreamingResponseBody body = outputStream -> {
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true)) {
                Flux<ServerSentEvent<AgentStreamChunk>> flux = stanfordStreamV5Service.streamV5(withSrePersona)
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
