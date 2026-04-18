package com.hackthon.stanford.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloRootController {

    @GetMapping("/hello")
    public String hello() {
        return "GET /api/chat/health (should return ok). POST /api/chat/stream/v5 with AgentStreamChunk JSON.";
    }

    /** 说明如何用 GET 触发1与 v5 相同的 SSE 流（见 {@link AgentController}）。 */
    @GetMapping("/queryClaude")
    public String queryClaude() {
        return "Claude SSE: GET /api/chat/stream/v5?content=... (optional: sessionId, userId, agentName, nextStepAgent, stopStream, skillsPrompt) "
                + "or ?payload=<url-encoded AgentStreamChunk JSON>. POST /api/chat/stream/v5 still accepts full JSON body.";
    }
}
