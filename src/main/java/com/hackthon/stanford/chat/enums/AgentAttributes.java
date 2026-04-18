package com.hackthon.stanford.chat.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 仅包含 Stanford 演示所需项；名称与 DeepChatBI {@code AgentAttributes} 一致以便直调。
 */
@Getter
@RequiredArgsConstructor
public enum AgentAttributes {
    MASTER_AGENT("MasterAgent", "MasterAgent"),
    TASK_SYSTEM_V5_STREAM_AGENT("TaskSystemV5StreamAgent", "TaskSystemV5StreamAgent"),
    NEXT_STEP_V5_Stream_AGENT("NextStepV5StreamAgent", "NextStepV5StreamAgent");

    private final String agentName;
    private final String description;
}
