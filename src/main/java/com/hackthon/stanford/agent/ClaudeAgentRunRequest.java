package com.hackthon.stanford.agent;

/**
 * One user turn for {@link TaskSystemClaudeAgent} (maps from HTTP {@code /queryClaude} body).
 */
public record ClaudeAgentRunRequest(
        String message,
        String sessionId,
        String userId,
        /** Optional override; null/blank → {@link ClaudeToolLoopService} default system prompt. */
        String systemPrompt
) {
    public static ClaudeAgentRunRequest of(String message, String sessionId, String userId) {
        return new ClaudeAgentRunRequest(message, sessionId, userId, null);
    }
}
