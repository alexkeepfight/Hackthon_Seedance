package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Node;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Retrieves SRE incident subgraphs from Neo4j and formats plain text for LLM {@code skillsPrompt} injection.
 */
@Service
@RequiredArgsConstructor
@ConditionalOnBean(Driver.class)
public class Neo4jSreRagService {

    private static final String MATCH_CYPHER = """
            MATCH (i:Incident)
            WHERE $needle <> '' AND (
              toLower(i.searchBlob) CONTAINS $needle
              OR toLower(i.title) CONTAINS $needle
              OR toLower(i.summary) CONTAINS $needle
            )
            WITH DISTINCT i
            ORDER BY i.severity ASC, i.id ASC
            LIMIT $limit
            OPTIONAL MATCH (i)-[:EXHIBITED]->(sym:Symptom)
            OPTIONAL MATCH (i)-[:CAUSED_BY]->(rc:RootCause)
            OPTIONAL MATCH (i)-[:RESOLVED_BY]->(f:Fix)
            OPTIONAL MATCH (i)-[:AFFECTED|DEPENDS_ON]->(svc:Service)
            RETURN i,
                   collect(DISTINCT sym) AS symptoms,
                   collect(DISTINCT rc) AS rootCauses,
                   collect(DISTINCT f) AS fixes,
                   collect(DISTINCT svc) AS services
            """;

    private static final String MATCH_TOKENS_CYPHER = """
            MATCH (i:Incident)
            WHERE size($tokens) > 0 AND ANY(t IN $tokens WHERE t <> '' AND size(t) >= 2 AND (
              toLower(i.searchBlob) CONTAINS t
              OR toLower(i.title) CONTAINS t
              OR toLower(i.summary) CONTAINS t
            ))
            WITH DISTINCT i
            ORDER BY i.severity ASC, i.id ASC
            LIMIT $limit
            OPTIONAL MATCH (i)-[:EXHIBITED]->(sym:Symptom)
            OPTIONAL MATCH (i)-[:CAUSED_BY]->(rc:RootCause)
            OPTIONAL MATCH (i)-[:RESOLVED_BY]->(f:Fix)
            OPTIONAL MATCH (i)-[:AFFECTED|DEPENDS_ON]->(svc:Service)
            RETURN i,
                   collect(DISTINCT sym) AS symptoms,
                   collect(DISTINCT rc) AS rootCauses,
                   collect(DISTINCT f) AS fixes,
                   collect(DISTINCT svc) AS services
            """;

    private final Driver driver;

    private Session openSession() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        if (!StringUtils.hasText(db)) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(db.trim()));
    }

    /**
     * NL planner output → formatted facts for {@link SreNlGraphPromptAugmentor}. Not used for META_HELP.
     */
    public String buildContextBlockForPlan(GraphRetrievalPlan plan, int limit) {
        if (plan == null || plan.type() == GraphRetrievalPlan.Type.META_HELP) {
            return "";
        }
        int lim = Math.min(Math.max(limit, 1), 10);
        try {
            if (!plan.searchTokens().isEmpty()) {
                Optional<String> byTokens = runAndFormat(MATCH_TOKENS_CYPHER,
                        Map.of("tokens", plan.searchTokens(), "limit", lim),
                        "Retrieved from Neo4j (ANY token CONTAINS match on Incident searchBlob/title/summary):\n\n");
                if (byTokens.isPresent() && !isNoIncidentHitMessage(byTokens.get())) {
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

    private static boolean isNoIncidentHitMessage(String s) {
        return s.contains("No matching Incident nodes");
    }

    private Optional<String> runAndFormat(String cypher, Map<String, Object> params, String intro) {
        try (Session session = openSession()) {
            Result result = session.run(cypher, params);
            if (!result.hasNext()) {
                return Optional.empty();
            }
            StringBuilder sb = new StringBuilder(intro);
            while (result.hasNext()) {
                appendIncidentBlock(sb, result.next());
            }
            return Optional.of(sb.toString().trim());
        }
    }

    /**
     * @param userQuery raw user question (e.g. {@code AgentStreamChunk.content})
     * @param limit     max incidents to include (default 3)
     */
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
                    "Retrieved from Neo4j SRE knowledge graph (substring match on Incident searchBlob/title/summary):\n\n");
            if (formatted.isPresent()) {
                return formatted;
            }
            return Optional.of("No matching Incident nodes in Neo4j for this query substring. "
                    + "Run GET /api/neo4j/bootstrap-sre to seed the graph, or broaden keywords (e.g. pool, 503, hikari, dns).");
        } catch (Exception e) {
            return Optional.of("Neo4j context lookup failed: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    public Optional<String> buildContextForLlm(String userQuery) {
        return buildContextForLlm(userQuery, 3);
    }

    private static void appendIncidentBlock(StringBuilder sb, Record rec) {
        Node i = rec.get("i").asNode();
        sb.append("## ").append(prop(i, "id", "?")).append(" — ").append(prop(i, "title", "")).append("\n");
        sb.append("- severity: ").append(prop(i, "severity", "")).append("\n");
        sb.append("- summary: ").append(prop(i, "summary", "")).append("\n");

        List<Node> symptoms = dedupeNodes(rec.get("symptoms").asList(v -> v.asNode()));
        for (Node sym : symptoms) {
            if (sym != null && StringUtils.hasText(sym.elementId())) {
                sb.append("- symptom: ").append(prop(sym, "description", "")).append("\n");
            }
        }

        List<Node> rcs = dedupeNodes(rec.get("rootCauses").asList(v -> v.asNode()));
        for (Node rc : rcs) {
            if (rc != null && !rc.elementId().isEmpty()) {
                sb.append("- root_cause [").append(prop(rc, "category", "")).append("]: ")
                        .append(prop(rc, "description", ""))
                        .append(" (config: ").append(prop(rc, "config_key", ""))
                        .append(" bad=").append(prop(rc, "config_value_bad", ""))
                        .append(" good=").append(prop(rc, "config_value_good", "")).append(")\n");
            }
        }

        List<Node> fixes = dedupeNodes(rec.get("fixes").asList(v -> v.asNode()));
        for (Node f : fixes) {
            if (f != null && !f.elementId().isEmpty()) {
                sb.append("- fix: ").append(prop(f, "description", "")).append("\n  command: `")
                        .append(prop(f, "command", "")).append("`\n");
            }
        }

        List<Node> services = dedupeNodes(rec.get("services").asList(v -> v.asNode()));
        for (Node svc : services) {
            if (svc != null && !svc.elementId().isEmpty()) {
                sb.append("- service: ").append(prop(svc, "name", ""))
                        .append(" ns=").append(prop(svc, "namespace", ""))
                        .append(" team=").append(prop(svc, "team", "")).append("\n");
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

    private static String prop(Node n, String key, String def) {
        if (n == null || !n.containsKey(key)) {
            return def;
        }
        var v = n.get(key);
        if (v.isNull()) {
            return def;
        }
        return v.asString();
    }

    /**
     * JSON-friendly preview for {@code /api/neo4j/sre-context-for-llm}.
     */
    public Map<String, Object> buildContextPayload(String userQuery, int limit) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> text = buildContextForLlm(userQuery, limit);
        out.put("ok", true);
        out.put("contextText", text.orElse(""));
        out.put("query", userQuery == null ? "" : userQuery.trim());
        return out;
    }
}
