package com.hackthon.stanford.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "anthropic")
public class AnthropicProperties {

    private String apiKey = "";

    private String baseUrl = "https://api.anthropic.com";

    private String version = "2023-06-01";

    private String model = "claude-sonnet-4-20250514";

    private int maxTokens = 8192;

    private int connectTimeoutSeconds = 60;

    private int readTimeoutSeconds = 300;

    private int maxToolRounds = 24;
}
