package com.hackthon.stanford.agent;

import com.alibaba.fastjson2.JSONObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.function.Consumer;

/**
 * Stanford 侧对 DeepChatBI {@code TaskSystemV5StreamAgent} 的抽象：同一套工具面（bash / 文件 / task_* / chatbi stub），
 * 多轮 tool loop，但底层 LLM 固定为 Anthropic Messages API（{@link ClaudeToolLoopService}），而非 Qwen + Reactor SSE。
 * <p>
 * DeepChatBI 原版还包含：AgentStreamChunk SSE、MasterV5Agent 嵌入 ChatBI、jsonl 会话落盘等；本类只保留「自主工具循环 + Claude」核心，
 * 便于在 Hackthon_Stanford 中独立运行与演示。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskSystemClaudeAgent {

    private final ClaudeToolLoopService claudeToolLoopService;

    /**
     * 跑完整工具循环；事件通过 {@code eventSink} 流出（与 {@code /api/agent/tool-loop} 相同 shape：
     * {@code round}, {@code assistant_text}, {@code tool_call}, {@code tool_result}, {@code stop_reason}, {@code done}, {@code error}）。
     */
    public void run(ClaudeAgentRunRequest request, Consumer<JSONObject> eventSink) {
        if (request == null) {
            eventSink.accept(errorEvt("empty request"));
            return;
        }
        String msg = request.message();
        if (!StringUtils.hasText(msg)) {
            eventSink.accept(errorEvt("message must not be blank"));
            return;
        }
        log.info("[TaskSystemClaudeAgent] sessionId={}, userId={}, messageChars={}",
                request.sessionId(), request.userId(), msg.length());
        claudeToolLoopService.runToolLoop(
                msg.trim(),
                request.sessionId(),
                request.userId(),
                request.systemPrompt(),
                eventSink);
    }

    private static JSONObject errorEvt(String message) {
        JSONObject o = new JSONObject();
        o.put("type", "error");
        o.put("data", message);
        o.put("ts", System.currentTimeMillis());
        return o;
    }
}
