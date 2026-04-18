package com.hackthon.stanford.neo4j;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * {@code POST /api/neo4j/query-cypher} 请求体。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryCypherRequest(
        String cypher,
        Map<String, Object> parameters,
        Integer maxRecords
) {
    public QueryCypherRequest {
        parameters = parameters == null ? Map.of() : parameters;
    }

    public int effectiveMaxRecords() {
        int m = maxRecords == null ? 500 : maxRecords;
        return Math.min(Math.max(m, 1), 2000);
    }
}
