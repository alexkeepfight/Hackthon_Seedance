package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Retrieves ad-attribution subgraphs (Creative → Campaign → Platform, attributed orders) for LLM injection.
 */
@Service
@RequiredArgsConstructor
@Conditional(Neo4jEnabledCondition.class)
public class Neo4jAdsAttributionRagService {

    private static final String MATCH_CYPHER = """
            MATCH (c:Creative)
            WHERE $needle <> '' AND (
              toLower(c.searchBlob) CONTAINS $needle
              OR toLower(c.name) CONTAINS $needle
              OR toLower(coalesce(c.rationale, '')) CONTAINS $needle
              OR toLower(coalesce(c.budgetSignalZh, '')) CONTAINS $needle
            )
            WITH DISTINCT c
            ORDER BY coalesce(c.roas, 0) DESC, c.id ASC
            LIMIT $limit
            OPTIONAL MATCH (c)-[:IN_CAMPAIGN]->(camp:Campaign)-[:ON_PLATFORM]->(p:AdPlatform)
            OPTIONAL MATCH (c)-[:ATTRIBUTED_TO]->(o:ShopOrder)
            RETURN c, camp, p, collect(DISTINCT o) AS orders
            """;

    private static final String MATCH_TOKENS_CYPHER = """
            MATCH (c:Creative)
            WHERE size($tokens) > 0 AND ANY(t IN $tokens WHERE t <> '' AND size(t) >= 2 AND (
              toLower(c.searchBlob) CONTAINS t
              OR toLower(c.name) CONTAINS t
              OR toLower(coalesce(c.rationale, '')) CONTAINS t
              OR toLower(coalesce(c.budgetSignalZh, '')) CONTAINS t
            ))
            WITH DISTINCT c
            ORDER BY coalesce(c.roas, 0) DESC, c.id ASC
            LIMIT $limit
            OPTIONAL MATCH (c)-[:IN_CAMPAIGN]->(camp:Campaign)-[:ON_PLATFORM]->(p:AdPlatform)
            OPTIONAL MATCH (c)-[:ATTRIBUTED_TO]->(o:ShopOrder)
            RETURN c, camp, p, collect(DISTINCT o) AS orders
            """;

    private final Driver driver;

    private Session openSession() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        if (!StringUtils.hasText(db)) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(db.trim()));
    }

    public String buildContextBlockForPlan(GraphRetrievalPlan plan, int limit) {
        if (plan == null || plan.type() == GraphRetrievalPlan.Type.META_HELP) {
            return "";
        }
        int lim = Math.min(Math.max(limit, 1), 10);
        try {
            if (!plan.searchTokens().isEmpty()) {
                Optional<String> byTokens = runAndFormat(MATCH_TOKENS_CYPHER,
                        Map.of("tokens", plan.searchTokens(), "limit", lim),
                        "Retrieved from Neo4j (ANY token CONTAINS match on Creative searchBlob/name/rationale/budgetSignalZh):\n\n");
                if (byTokens.isPresent() && !isNoCreativeHitMessage(byTokens.get())) {
                    return byTokens.get();
                }
            }
            String needle = plan.needleFallback() == null ? "" : plan.needleFallback();
            if (needle.length() > 200) {
                needle = needle.substring(0, 200);
            }
            return buildContextForLlm(needle, lim).orElse("No graph context.");
        } catch (Exception e) {
            return "Neo4j context lookup failed: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private static boolean isNoCreativeHitMessage(String s) {
        return s.contains("No matching Creative");
    }

    private Optional<String> runAndFormat(String cypher, Map<String, Object> params, String intro) {
        try (Session session = openSession()) {
            Result result = session.run(cypher, params);
            if (!result.hasNext()) {
                return Optional.empty();
            }
            StringBuilder sb = new StringBuilder(intro);
            while (result.hasNext()) {
                appendCreativeBlock(sb, result.next());
            }
            return Optional.of(sb.toString().trim());
        }
    }

    public Optional<String> buildContextForLlm(String userQuery, int limit) {
        if (!StringUtils.hasText(userQuery)) {
            return Optional.empty();
        }
        String needle = userQuery.trim().toLowerCase(Locale.ROOT);
        if (needle.length() > 200) {
            needle = needle.substring(0, 200);
        }
        int lim = Math.min(Math.max(limit, 1), 10);
        try {
            Optional<String> formatted = runAndFormat(MATCH_CYPHER, Map.of("needle", needle, "limit", lim),
                    "Retrieved from Neo4j ads-attribution graph (substring match on Creative):\n\n");
            if (formatted.isPresent()) {
                return formatted;
            }
            return Optional.of("No matching Creative nodes in Neo4j for this query substring. "
                    + "Run GET /api/neo4j/bootstrap-ads to rebuild the graph, or try keywords: 加钱, 减钱, meta, google, roas, 创意.");
        } catch (Exception e) {
            return Optional.of("Neo4j context lookup failed: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    public Optional<String> buildContextForLlm(String userQuery) {
        return buildContextForLlm(userQuery, 3);
    }

    private static void appendCreativeBlock(StringBuilder sb, Record rec) {
        Node c = rec.get("c").asNode();
        sb.append("## Creative ").append(propStr(c, "id", "?")).append(" — ").append(propStr(c, "name", "")).append("\n");
        sb.append("- spendUsd: ").append(propNum(c, "spendUsd")).append(", attributedRevenueUsd: ")
                .append(propNum(c, "attributedRevenueUsd")).append(", roas: ").append(propNum(c, "roas")).append("\n");
        sb.append("- budgetSignal: ").append(propStr(c, "budgetSignal", ""))
                .append(" (").append(propStr(c, "budgetSignalZh", "")).append(")\n");
        sb.append("- rationale: ").append(propStr(c, "rationale", "")).append("\n");

        if (!rec.get("camp").isNull()) {
            Node camp = rec.get("camp").asNode();
            sb.append("- campaign: ").append(propStr(camp, "name", ""))
                    .append(" [").append(propStr(camp, "externalLabel", "")).append("]\n");
        }
        if (!rec.get("p").isNull()) {
            Node p = rec.get("p").asNode();
            sb.append("- platform: ").append(propStr(p, "name", "")).append("\n");
        }

        List<Node> orders = rec.get("orders").asList(v -> v.isNull() ? null : v.asNode());
        for (Node o : dedupeNodes(orders)) {
            if (o != null) {
                sb.append("- attributed_order: gid=").append(propStr(o, "orderGid", ""))
                        .append(" revenueUsd=").append(propNum(o, "revenueUsd"))
                        .append(" path=").append(propStr(o, "eventPathSummary", "")).append("\n");
            }
        }
        sb.append("\n");
    }

    private static List<Node> dedupeNodes(List<Node> raw) {
        Map<String, Node> byId = new LinkedHashMap<>();
        for (Node n : raw) {
            if (n != null && n.elementId() != null) {
                byId.putIfAbsent(n.elementId(), n);
            }
        }
        return new ArrayList<>(byId.values());
    }

    private static String propStr(Node n, String key, String def) {
        if (n == null || !n.containsKey(key)) {
            return def;
        }
        Value v = n.get(key);
        if (v.isNull()) {
            return def;
        }
        return v.asString();
    }

    private static String propNum(Node n, String key) {
        if (n == null || !n.containsKey(key)) {
            return "";
        }
        Value v = n.get(key);
        if (v.isNull()) {
            return "";
        }
        Object o = v.asObject();
        return o == null ? "" : o.toString();
    }

    public Map<String, Object> buildContextPayload(String userQuery, int limit) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> text = buildContextForLlm(userQuery, limit);
        out.put("ok", true);
        out.put("contextText", text.orElse(""));
        out.put("query", userQuery == null ? "" : userQuery.trim());
        return out;
    }
}
