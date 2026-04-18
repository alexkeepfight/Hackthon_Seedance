package com.hackthon.stanford.chat.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 与 DeepChatBI {@code com.startup.ai.enums.AgentStatus} 对齐（供 {@code /api/chat/stream/v5} JSON 一致）。
 */
@Getter
@RequiredArgsConstructor
public enum AgentStatus {
    STARTED("started", "已开始"),
    PROCESSING("processing", "处理中"),
    COMPLETED("completed", "已完成"),
    ERROR("error", "错误"),
    CANCELLED("cancelled", "已取消"),
    INTERRUPT("interrupt", "中断");

    private final String code;
    private final String description;
}
