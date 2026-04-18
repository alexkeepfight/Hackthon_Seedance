package com.hackthon.stanford.agent;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.hackthon.stanford.config.AnthropicProperties;
import com.hackthon.stanford.llm.AnthropicMessagesClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.function.Consumer;

/**
 * Multi-round Claude tool loop (same pattern as Qwen {@code queryV5LLMAgentStreamToolLoop} + TaskSystem tools).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClaudeToolLoopService {

    /**
     * If this substring appears in the effective system prompt, the Messages API is called with no tools:
     * the model must answer from background/playbook only (no bash/kubectl in this JVM).
     */
    public static final String ADVISORY_MODE_NO_TOOLS_MARKER = "<<<ADVISORY_MODE_NO_TOOLS>>>";

    private final AnthropicMessagesClient anthropicMessagesClient;
    private final AnthropicProperties anthropicProperties;

    private static final String DEFAULT_SYSTEM = """
            You are an autonomous agent with tools: bash, read_file, write_file, edit_file, task_*, chatbi_master_pipeline (stub).
            For multi-step goals, maintain a task board (task_create / task_update with blockedBy).
            For file inspection use read_file; for shell use bash. Workspace root is the JVM working directory.
            chatbi_master_pipeline is a demo stub — prefer bash/read_file for local proofs.
            When finished, respond with a concise plain-text summary for the user.
            If bash or another tool returns "command not found" or shows the environment cannot run cluster commands (e.g. kubectl), do not retry the same class of command. Summarize what the operator should run on their machine/bastion and use any playbook/background already provided in the system instructions.
            """;

    public void runToolLoop(String userMessage, String sessionId, String userId, Consumer<JSONObject> eventSink) {
        runToolLoop(userMessage, sessionId, userId, null, eventSink);
    }

    /**
     * @param systemPrompt if null or blank, {@link #DEFAULT_SYSTEM} is used
     */
    public void runToolLoop(String userMessage, String sessionId, String userId,
                            String systemPrompt, Consumer<JSONObject> eventSink) {
        String sid = StringUtils.hasText(sessionId) ? sessionId : "demo-session";
        String uid = StringUtils.hasText(userId) ? userId : "demo-user";
        String system = StringUtils.hasText(systemPrompt) ? systemPrompt.trim() : DEFAULT_SYSTEM;

        boolean advisoryNoTools = system.contains(ADVISORY_MODE_NO_TOOLS_MARKER);
        JSONArray tools = advisoryNoTools ? new JSONArray() : StanfordTools.claudeTools();
        if (advisoryNoTools) {
            log.debug("Advisory mode: tools disabled (system contains {})", ADVISORY_MODE_NO_TOOLS_MARKER);
        }
        JSONArray messages = new JSONArray();
        messages.add(userTextMessage(userMessage == null ? "" : userMessage));

        int max = Math.max(1, Math.min(32, anthropicProperties.getMaxToolRounds()));

        for (int round = 0; round < max; round++) {
            eventSink.accept(evt("round", round + 1));
            JSONObject resp = anthropicMessagesClient.createMessage(system, messages, tools);

            if (resp.getBooleanValue("_http_error")) {
                eventSink.accept(evt("error", "Anthropic HTTP " + resp.getIntValue("status") + ": " + resp.getString("body")));
                return;
            }

            String stopReason = resp.getString("stop_reason");
            JSONArray content = resp.getJSONArray("content");
            if (content == null) {
                content = new JSONArray();
            }

            emitAssistantVisibleText(content, eventSink);

            JSONObject assistantTurn = new JSONObject();
            assistantTurn.put("role", "assistant");
            assistantTurn.put("content", content);
            messages.add(assistantTurn);

            if ("tool_use".equals(stopReason)) {
                JSONArray toolResultBlocks = new JSONArray();
                for (int i = 0; i < content.size(); i++) {
                    JSONObject block = content.getJSONObject(i);
                    if (block == null || !"tool_use".equals(block.getString("type"))) {
                        continue;
                    }
                    String toolUseId = block.getString("id");
                    String name = block.getString("name");
                    JSONObject input = block.getJSONObject("input");
                    if (input == null) {
                        input = new JSONObject();
                    }

                    JSONObject callEvt = new JSONObject();
                    callEvt.put("id", toolUseId);
                    callEvt.put("name", name);
                    callEvt.put("input", input);
                    eventSink.accept(evt("tool_call", callEvt));

                    JSONObject exec = TaskToolRuntime.executeTool(sid, uid, name, input);
                    String resultText;
                    if (exec.containsKey("error")) {
                        resultText = "error: " + exec.getString("error");
                    } else {
                        resultText = exec.getString("content");
                        if (resultText == null) {
                            resultText = exec.toJSONString();
                        }
                    }

                    JSONObject resEvt = new JSONObject();
                    resEvt.put("name", name);
                    resEvt.put("tool_use_id", toolUseId);
                    resEvt.put("content", truncate(resultText, 100_000));
                    eventSink.accept(evt("tool_result", resEvt));

                    JSONObject tr = new JSONObject();
                    tr.put("type", "tool_result");
                    tr.put("tool_use_id", toolUseId);
                    tr.put("content", truncate(resultText, 100_000));
                    toolResultBlocks.add(tr);
                }

                if (toolResultBlocks.isEmpty()) {
                    eventSink.accept(evt("error", "stop_reason=tool_use but no tool_use blocks in content"));
                    return;
                }

                JSONObject userToolTurn = new JSONObject();
                userToolTurn.put("role", "user");
                userToolTurn.put("content", toolResultBlocks);
                messages.add(userToolTurn);
                continue;
            }

            eventSink.accept(evt("stop_reason", stopReason));
            break;
        }

        eventSink.accept(evt("done", true));
    }

    private static void emitAssistantVisibleText(JSONArray content, Consumer<JSONObject> eventSink) {
        for (int i = 0; i < content.size(); i++) {
            JSONObject block = content.getJSONObject(i);
            if (block == null) {
                continue;
            }
            if ("text".equals(block.getString("type"))) {
                String t = block.getString("text");
                if (StringUtils.hasText(t)) {
                    eventSink.accept(evt("assistant_text", t));
                }
            }
        }
    }

    private static JSONObject userTextMessage(String text) {
        JSONObject m = new JSONObject();
        m.put("role", "user");
        m.put("content", text);
        return m;
    }

    private static JSONObject evt(String type, Object data) {
        JSONObject o = new JSONObject();
        o.put("type", type);
        o.put("data", data);
        o.put("ts", System.currentTimeMillis());
        return o;
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "\n... (truncated)";
    }
}
