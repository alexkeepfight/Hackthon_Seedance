package com.hackthon.stanford.llm;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.hackthon.stanford.config.AnthropicProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * Minimal non-streaming Anthropic Messages API client (extracted from DeepChatBI_Al ClaudeClient patterns).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnthropicMessagesClient {

    private final WebClient anthropicWebClient;
    private final AnthropicProperties props;

    /**
     * POST /v1/messages — full JSON response (supports tool_use / end_turn).
     */
    public JSONObject createMessage(String systemPrompt, JSONArray messages, JSONArray tools) {
        JSONObject body = new JSONObject();
        body.put("model", props.getModel());
        body.put("max_tokens", props.getMaxTokens());
        body.put("stream", false);
        if (systemPrompt != null && !systemPrompt.isBlank()) {
            body.put("system", systemPrompt);
        }
        body.put("messages", messages);
        if (tools != null && !tools.isEmpty()) {
            body.put("tools", tools);
        }

        log.debug("Anthropic request model={}, messages.size={}", props.getModel(), messages.size());

        String apiKey = StringUtils.hasText(props.getApiKey()) ? props.getApiKey().trim() : "";
        if (!StringUtils.hasText(apiKey)) {
            JSONObject err = new JSONObject();
            err.put("_http_error", true);
            err.put("status", 401);
            err.put("body", "missing anthropic.api-key (env ANTHROPIC_API_KEY or application-local.properties)");
            return err;
        }

        try {
            String raw = anthropicWebClient.post()
                    .uri("/v1/messages")
                    .header("x-api-key", apiKey)
                    .header("anthropic-version", props.getVersion())
                    .bodyValue(body.toJSONString())
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(props.getReadTimeoutSeconds()))
                    .retryWhen(Retry.backoff(1, Duration.ofMillis(400))
                            .filter(t -> t instanceof WebClientResponseException.TooManyRequests))
                    .block();
            return raw == null ? new JSONObject() : JSONObject.parseObject(raw);
        } catch (WebClientResponseException e) {
            String msg = e.getResponseBodyAsString();
            log.error("Anthropic API error: status={} body={}", e.getStatusCode().value(), msg);
            JSONObject err = new JSONObject();
            err.put("_http_error", true);
            err.put("status", e.getStatusCode().value());
            err.put("body", msg);
            return err;
        }
    }
}
