package com.hackthon.stanford.neo4j;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Shared limit clamp for {@code GET /api/neo4j/sre-context-for-llm?limit=}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SreContextRequest(String query, Integer limit) {

    public static int clampLimit(Integer limit) {
        int l = limit == null ? 3 : limit;
        return Math.min(Math.max(l, 1), 10);
    }

    public int effectiveLimit() {
        return clampLimit(limit);
    }
}
