package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 执行简单 Cypher 以验证与 Aura 的连通性（等价于在 Browser 里跑一条查询）。
 */
@Service
@RequiredArgsConstructor
@Conditional(Neo4jEnabledCondition.class)
public class Neo4jProbeService {

    private final Driver driver;

    private Session openSession() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        if (!StringUtils.hasText(db)) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(db.trim()));
    }

    private static String databaseLabel() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        return StringUtils.hasText(db) ? db.trim() : "(server default)";
    }

    /**
     * 最小探活：与 Browser 中 {@code RETURN 1} 类似。
     */
    public Map<String, Object> ping() {
        Map<String, Object> out = new LinkedHashMap<>();
        try (Session session = openSession()) {
            Result result = session.run("RETURN 1 AS ok, datetime() AS serverTime");
            if (!result.hasNext()) {
                out.put("ok", false);
                out.put("error", "empty result");
                return out;
            }
            Record rec = result.next();
            out.put("ok", true);
            out.put("check", rec.get("ok").asInt());
            out.put("serverTime", rec.get("serverTime").toString());
            out.put("database", databaseLabel());
            return out;
        } catch (Exception e) {
            return authFailureBody(e);
        }
    }

    /**
     * 可选：节点数量（需有读权限；大数据库可能较慢，仅 demo）。
     */
    public Map<String, Object> nodeCount() {
        Map<String, Object> out = new LinkedHashMap<>();
        try (Session session = openSession()) {
            Result result = session.run("MATCH (n) RETURN count(n) AS total LIMIT 1");
            if (!result.hasNext()) {
                out.put("ok", false);
                return out;
            }
            out.put("ok", true);
            out.put("nodeCount", result.next().get("total").asLong());
            return out;
        } catch (Exception e) {
            return authFailureBody(e);
        }
    }

    /**
     * 执行只读 Cypher（会做关键字校验；结果最多 {@code maxRecords} 行，超出部分丢弃并标记 truncated）。
     */
    public Map<String, Object> queryCypher(String cypher, Map<String, Object> parameters, int maxRecords) {
        CypherReadOnlyGuard.validateOrThrow(cypher);
        Map<String, Object> params = parameters == null ? Map.of() : parameters;
        Map<String, Object> out = new LinkedHashMap<>();
        try (Session session = openSession()) {
            Result result = session.run(cypher, params);
            List<String> columns = new ArrayList<>();
            result.keys().forEach(columns::add);
            List<Map<String, Object>> rows = new ArrayList<>();
            while (result.hasNext() && rows.size() < maxRecords) {
                Record rec = result.next();
                Map<String, Object> row = new LinkedHashMap<>();
                for (String key : rec.keys()) {
                    row.put(key, valueToJson(rec.get(key)));
                }
                rows.add(row);
            }
            boolean truncated = result.hasNext();
            result.consume();
            out.put("ok", true);
            out.put("columns", columns);
            out.put("rows", rows);
            out.put("rowCount", rows.size());
            out.put("truncated", truncated);
            out.put("maxRecords", maxRecords);
            out.put("database", databaseLabel());
            return out;
        } catch (Exception e) {
            return authFailureBody(e);
        }
    }

    private static Object valueToJson(Value v) {
        if (v == null || v.isNull()) {
            return null;
        }
        String tn = v.type().name();
        if ("NODE".equals(tn)) {
            return nodeToJson(v.asNode());
        }
        if ("RELATIONSHIP".equals(tn)) {
            return relationshipToJson(v.asRelationship());
        }
        if ("PATH".equals(tn)) {
            return pathToJson(v.asPath());
        }
        if ("LIST".equals(tn)) {
            return v.asList(Neo4jProbeService::valueToJson);
        }
        if ("MAP".equals(tn)) {
            return new LinkedHashMap<>(v.asMap(Neo4jProbeService::valueToJson));
        }
        if ("STRING".equals(tn)) {
            return v.asString();
        }
        if ("INTEGER".equals(tn)) {
            return v.asLong();
        }
        if ("FLOAT".equals(tn)) {
            return v.asDouble();
        }
        if ("BOOLEAN".equals(tn)) {
            return v.asBoolean();
        }
        try {
            return v.asObject();
        } catch (Exception e) {
            return v.toString();
        }
    }

    private static Map<String, Object> nodeToJson(Node n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("_kind", "node");
        m.put("elementId", n.elementId());
        List<String> labels = new ArrayList<>();
        n.labels().forEach(labels::add);
        m.put("labels", labels);
        Map<String, Object> props = new LinkedHashMap<>();
        for (String k : n.keys()) {
            props.put(k, valueToJson(n.get(k)));
        }
        m.put("properties", props);
        return m;
    }

    private static Map<String, Object> relationshipToJson(Relationship r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("_kind", "relationship");
        m.put("elementId", r.elementId());
        m.put("type", r.type());
        m.put("startNodeElementId", r.startNodeElementId());
        m.put("endNodeElementId", r.endNodeElementId());
        Map<String, Object> props = new LinkedHashMap<>();
        for (String k : r.keys()) {
            props.put(k, valueToJson(r.get(k)));
        }
        m.put("properties", props);
        return m;
    }

    private static Map<String, Object> pathToJson(Path p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("_kind", "path");
        m.put("length", p.length());
        List<Object> nodes = new ArrayList<>();
        for (Node n : p.nodes()) {
            nodes.add(nodeToJson(n));
        }
        m.put("nodes", nodes);
        List<Object> rels = new ArrayList<>();
        for (Relationship r : p.relationships()) {
            rels.add(relationshipToJson(r));
        }
        m.put("relationships", rels);
        return m;
    }

    private static Map<String, Object> authFailureBody(Exception e) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", false);
        String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        out.put("error", msg);
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (lower.contains("unauthorized") || lower.contains("authentication")) {
                out.put("hint", "打开 GET /api/neo4j/config-check；若 passwordLooksLikePlaceholder=true，请在 Neo4jHardcodedCredentials.PASSWORD 填入与 Browser 一致的 Aura Database password 后重新编译运行。");
            }
            if (lower.contains("database") && lower.contains("does not exist")) {
                out.put("hint", "将 Neo4jHardcodedCredentials.DATABASE 设为 \"\" 使用默认库，或在 Aura / Browser 中确认实际逻辑库名后再填入 DATABASE。");
            }
        }
        return out;
    }
}
