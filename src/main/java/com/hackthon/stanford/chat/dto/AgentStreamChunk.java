package com.hackthon.stanford.chat.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hackthon.stanford.chat.enums.ActionType;
import com.hackthon.stanford.chat.enums.AgentStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 与 DeepChatBI {@code com.startup.ai.dto.stream.AgentStreamChunk} 字段子集对齐，供 {@code /api/chat/stream/v5} 请求/响应复用。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentStreamChunk {

    private String agentName;
    private AgentStatus status;
    private String actionType;
    private Object delta;

    @JsonProperty("IsJson")
    private String isJson;

    @JsonProperty("IsChatReport")
    private String isChatReport;

    @JsonProperty("stopStream")
    private Boolean stopStream;

    @JsonProperty("nextStepAgent")
    private String nextStepAgent;

    private Map<String, Object> queryData;

    private String plainData;

    private String content;

    private String sessionId;
    private String userId;
    private String lang;
    private String modelType;

    private Long datasourceId;

    private String skillsPrompt;

    public enum DeltaStatus {
        START, CONTINUE, END, SINGLE
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TextDelta {
        @Builder.Default
        private String type = "TEXT";
        @Builder.Default
        private DeltaStatus status = DeltaStatus.CONTINUE;
        @Builder.Default
        private Object text = "";
    }

    public static AgentStreamChunk createStartEvent(String agentName) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .actionType(ActionType.STARTING.name())
                .status(AgentStatus.STARTED)
                .delta(TextDelta.builder().status(DeltaStatus.START).build())
                .build();
    }

    public static AgentStreamChunk createProcessingEvent(String agentName, String content) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .actionType(ActionType.THINKING.name())
                .status(AgentStatus.PROCESSING)
                .delta(TextDelta.builder().text(content).status(DeltaStatus.CONTINUE).build())
                .build();
    }

    public static AgentStreamChunk createV5ProcessingEvent(String agentName, String content, String isJson) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .plainData(content)
                .isJson(isJson)
                .build();
    }

    public static AgentStreamChunk createCompleteEvent(String agentName, String text) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .status(AgentStatus.COMPLETED)
                .delta(TextDelta.builder().text(text).status(DeltaStatus.END).build())
                .build();
    }

    public static AgentStreamChunk createErrorEvent(String agentName, String errorMessage) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .status(AgentStatus.ERROR)
                .delta(TextDelta.builder()
                        .text("Execute error: " + errorMessage)
                        .status(DeltaStatus.END)
                        .build())
                .build();
    }

    public static AgentStreamChunk createInterruptEvent(String agentName, Object reason) {
        return AgentStreamChunk.builder()
                .agentName(agentName)
                .status(AgentStatus.INTERRUPT)
                .delta(TextDelta.builder().text(reason).status(DeltaStatus.END).build())
                .build();
    }
}
