package com.hackthon.stanford.chat;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.hackthon.stanford.agent.ClaudeAgentRunRequest;
import com.hackthon.stanford.agent.TaskSystemClaudeAgent;
import com.hackthon.stanford.chat.dto.AgentStreamChunk;
import com.hackthon.stanford.chat.enums.AgentAttributes;
import com.hackthon.stanford.chat.enums.AgentStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stanford 版 {@code ChatController#masterAgentStreamV5}：同路径契约与过滤逻辑，底层为 {@link TaskSystemClaudeAgent}（Claude + 工具循环）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StanfordStreamV5Service {

    private final TaskSystemClaudeAgent taskSystemClaudeAgent;

    public Flux<ServerSentEvent<AgentStreamChunk>> streamV5(AgentStreamChunk req) {
        if (req == null) {
            return Flux.just(ServerSentEvent.builder(
                    AgentStreamChunk.createErrorEvent("MasterV5Agent", "empty request")
            ).build());
        }

        if (Boolean.TRUE.equals(req.getStopStream())) {
            try {
                String c = req.getContent();
                if (c != null && c.trim().startsWith("/new")) {
                    log.info("stopStream /new: Stanford has no MasterV5 transcript snapshot (no-op).");
                }
            } catch (Exception e) {
                log.warn("stopStream /new snapshot skipped (non-fatal): {}", e.toString());
            }
            log.info("检测到停止流请求，sessionId: {}, userId: {}", req.getSessionId(), req.getUserId());
            return Flux.just(ServerSentEvent.builder(
                    AgentStreamChunk.createInterruptEvent("MasterV5Agent", "用户请求停止流式响应")
            ).build()).doOnComplete(() -> log.info("流已停止，sessionId: {}", req.getSessionId()));
        }

        if ("true".equalsIgnoreCase(req.getIsChatReport())) {
            log.debug("isChatReport=true (Stanford: no DataModelSqlFetchService; extend here if needed)");
        }

        String directAgentName = req.getAgentName() == null ? "" : req.getAgentName().trim();
        if (AgentAttributes.TASK_SYSTEM_V5_STREAM_AGENT.getAgentName().equals(directAgentName)) {
            log.info("agentName 直调单 Agent: {}", directAgentName);
            return taskToolFlux(req)
                    .filter(event -> streamV5ChunkFilter(event, directAgentName, req.getSessionId()))
                    .doOnCancel(() -> log.info("Direct agent 流被客户端取消，agentName: {}, sessionId: {}", directAgentName, req.getSessionId()))
                    .doFinally(signalType -> log.info("Direct agent 流终止，signalType: {}, agentName: {}, sessionId: {}",
                            signalType, directAgentName, req.getSessionId()));
        }

        String nextStepAgent = (req.getNextStepAgent() != null && !req.getNextStepAgent().trim().isEmpty())
                ? req.getNextStepAgent().trim()
                : parseNextStepAgentFromContent(req.getContent());
        if (nextStepAgent != null && !nextStepAgent.isEmpty()) {
            return runSingleAgentStream(nextStepAgent, req)
                    .filter(event -> streamV5ChunkFilter(event, nextStepAgent, req.getSessionId()))
                    .doOnCancel(() -> log.info("NextStepAgent 流被客户端取消，sessionId: {}", req.getSessionId()))
                    .doFinally(signalType -> log.info("NextStepAgent 流终止，signalType: {}, sessionId: {}", signalType, req.getSessionId()));
        }

        AtomicBoolean nextStepEmitted = new AtomicBoolean(false);
        return taskToolFlux(req)
                .filter(event -> masterStreamV5ChunkFilter(event, nextStepEmitted, req.getSessionId()))
                .doOnCancel(() -> log.info("MasterV5Agent 流被客户端取消，sessionId: {}", req.getSessionId()))
                .doFinally(signalType -> log.info("MasterV5Agent 流终止，signalType: {}, sessionId: {}", signalType, req.getSessionId()));
    }

    private Flux<ServerSentEvent<AgentStreamChunk>> runSingleAgentStream(String nextStepAgent, AgentStreamChunk req) {
        if (AgentAttributes.TASK_SYSTEM_V5_STREAM_AGENT.getAgentName().equals(nextStepAgent)) {
            return taskToolFlux(req);
        }
        return Flux.just(ServerSentEvent.builder(
                AgentStreamChunk.createErrorEvent("MasterV5Agent",
                        "Stanford: unsupported nextStepAgent / single agent: " + nextStepAgent)
        ).build());
    }

    private Flux<ServerSentEvent<AgentStreamChunk>> taskToolFlux(AgentStreamChunk req) {
        return Flux.<ServerSentEvent<AgentStreamChunk>>create(sink -> {
                    try {
                        String agentName = AgentAttributes.TASK_SYSTEM_V5_STREAM_AGENT.getAgentName();
                        sink.next(ServerSentEvent.builder(
                                AgentStreamChunk.createV5ProcessingEvent(agentName,
                                        JSON.toJSONString(Map.of("phase", "tool_loop_start", "sessionId", str(req.getSessionId()))),
                                        "true")
                        ).build());

                        ClaudeAgentRunRequest runReq = new ClaudeAgentRunRequest(
                                req.getContent() == null ? "" : req.getContent(),
                                req.getSessionId(),
                                req.getUserId(),
                                StringUtils.hasText(req.getSkillsPrompt()) ? req.getSkillsPrompt() : null);

                        taskSystemClaudeAgent.run(runReq, evt -> {
                            if (sink.isCancelled()) {
                                return;
                            }
                            sink.next(ServerSentEvent.builder(
                                    AgentStreamChunk.createV5ProcessingEvent(agentName, evt.toJSONString(), "true")
                            ).build());
                        });

                        if (!sink.isCancelled()) {
                            sink.next(ServerSentEvent.builder(
                                    AgentStreamChunk.createCompleteEvent(agentName, "TaskSystemV5StreamAgent (Claude) completed")
                            ).build());
                            sink.complete();
                        }
                    } catch (Exception e) {
                        log.error("taskToolFlux failed", e);
                        sink.error(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private boolean streamV5ChunkFilter(ServerSentEvent<AgentStreamChunk> event, String agentLabel, String sessionId) {
        AgentStreamChunk chunk = event.data();
        if (chunk == null) {
            return false;
        }
        if (chunk.getStatus() == AgentStatus.ERROR || chunk.getStatus() == AgentStatus.INTERRUPT) {
            return true;
        }
        log.info("chunk.getPlainData()是{}", chunk.getPlainData());
        return chunk.getPlainData() != null;
    }

    private boolean masterStreamV5ChunkFilter(ServerSentEvent<AgentStreamChunk> event,
                                              AtomicBoolean nextStepEmitted,
                                              String sessionId) {
        AgentStreamChunk chunk = event.data();
        if (chunk == null) {
            return false;
        }
        if (chunk.getStatus() == AgentStatus.ERROR || chunk.getStatus() == AgentStatus.INTERRUPT) {
            log.info("允许错误/中断事件通过: status={}, agentName={}", chunk.getStatus(), chunk.getAgentName());
            return true;
        }
        if ("NextStepV5StreamAgent".equals(chunk.getAgentName()) && chunk.getPlainData() != null) {
            if (nextStepEmitted.getAndSet(true)) {
                log.debug("跳过重复的 NextStepV5StreamAgent 事件，只保留第一条");
                return false;
            }
        }
        log.info("chunk.getPlainData()是{}", chunk.getPlainData());
        return chunk.getPlainData() != null;
    }

    private static String str(String s) {
        return s == null ? "" : s;
    }

    private static String parseNextStepAgentFromContent(String content) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        String prefix = "nextStepAgent:";
        int idx = content.indexOf(prefix);
        if (idx < 0) {
            return null;
        }
        String after = content.substring(idx + prefix.length()).trim();
        int end = 0;
        while (end < after.length()
                && !Character.isWhitespace(after.charAt(end))
                && after.charAt(end) != ',' && after.charAt(end) != '，') {
            end++;
        }
        String name = after.substring(0, end).trim();
        return name.isEmpty() ? null : name;
    }
}
