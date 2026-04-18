package com.hackthon.stanford.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Logs whether an Anthropic API key is loaded (masked). Helps debug 401 without printing the secret.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnthropicStartupDiagnostic implements ApplicationRunner {

    private final AnthropicProperties props;

    @Override
    public void run(ApplicationArguments args) {
        String k = props.getApiKey();
        if (!StringUtils.hasText(k)) {
            log.warn("ANTHROPIC: no api key — set environment ANTHROPIC_API_KEY or add anthropic.api-key to "
                    + "application-local.properties (see spring.config.import in application.properties). "
                    + "Requests will fail or return synthetic errors.");
            return;
        }
        String trimmed = k.trim();
        String mask = trimmed.length() <= 20
                ? "(short)"
                : trimmed.substring(0, 12) + "…" + trimmed.substring(trimmed.length() - 4);
        log.info("ANTHROPIC: api key present (length={}, {}) — if calls still return 401, rotate key at console.anthropic.com",
                trimmed.length(), mask);
    }
}
