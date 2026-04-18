package com.hackthon.stanford.neo4j;

import java.util.List;

/**
 * Result of mapping user natural language → Neo4j-oriented retrieval (for prompt transparency + execution).
 */
public record GraphRetrievalPlan(
        Type type,
        List<String> searchTokens,
        /** Used when {@code searchTokens} is empty: whole normalized user text (truncated). */
        String needleFallback,
        /** Human-readable pseudo-Cypher shown to the LLM. */
        String pseudoCypher,
        String plannerNote
) {
    public enum Type {
        /** User asked what the agent can do, help, etc. — no graph query. */
        META_HELP,
        /** Search incidents by extracted keywords / fallback needle. */
        KEYWORD_SEARCH
    }
}
