package com.hackthon.stanford.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Alibaba Cloud DashScope OpenAI-compatible chat completions ({@code /v1/chat/completions}). */
@Data
@ConfigurationProperties(prefix = "dashscope")
public class DashScopeProperties {

    private String apiKey = "";

    /** Full URL including path, e.g. {@code .../compatible-mode/v1/chat/completions} */
    private String baseUrl = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions";

    private String model = "qwen-max";

    private int maxTokens = 30000;

    private double topP = 0.1;

    private int connectTimeoutSeconds = 60;

    /** Total request timeout for non-streaming completion */
    private int readTimeoutSeconds = 300;
}
