package com.hackthon.stanford.neo4j;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * HTTP 探活 Neo4j（与在 Browser 里 Connect 后执行 Cypher 等价，便于本地/部署校验）。
 */
@RestController
@RequestMapping("/api/neo4j")
public class Neo4jController {

    private final ObjectProvider<Neo4jProbeService> probeService;
    private final ObjectProvider<Neo4jAdsAttributionGraphBootstrap> adsBootstrap;
    private final ObjectProvider<Neo4jAdsAttributionRagService> adsRagService;

    public Neo4jController(ObjectProvider<Neo4jProbeService> probeService,
                          ObjectProvider<Neo4jAdsAttributionGraphBootstrap> adsBootstrap,
                          ObjectProvider<Neo4jAdsAttributionRagService> adsRagService) {
        this.probeService = probeService;
        this.adsBootstrap = adsBootstrap;
        this.adsRagService = adsRagService;
    }

    /**
     * 不返回密码内容；用于确认是否仍在用占位符、用户名是否正确等。
     */
    @GetMapping("/config-check")
    public Map<String, Object> configCheck() {
        Map<String, Object> m = new LinkedHashMap<>();
        String uri = Neo4jHardcodedCredentials.URI;
        String user = Neo4jHardcodedCredentials.USERNAME;
        String pass = Neo4jHardcodedCredentials.PASSWORD;
        m.put("uriConfigured", StringUtils.hasText(uri));
        m.put("uriHost", safeHost(uri));
        m.put("username", user == null ? "" : user.trim());
        m.put("passwordLength", pass == null ? 0 : pass.length());
        boolean placeholder = passwordLooksUnset(pass);
        m.put("passwordLooksLikePlaceholder", placeholder);
        m.put("driverBeanCreated", probeService.getIfAvailable() != null);
        if (placeholder) {
            if (pass == null || pass.isBlank()) {
                m.put("fix", "在 Neo4jHardcodedCredentials.java 里把 PASSWORD 常量设为 Aura 的 Database password（与 Browser 一致），保存后重新编译并启动。");
            } else {
                m.put("fix", "PASSWORD 仍是占位符或明显无效值；请改为 Aura 控制台 Copy 的真实 Database password 后重新编译运行。");
            }
        }
        return m;
    }

    private static String safeHost(String uri) {
        if (!StringUtils.hasText(uri)) {
            return "";
        }
        try {
            java.net.URI u = java.net.URI.create(uri.trim());
            return u.getHost() == null ? "" : u.getHost();
        } catch (Exception e) {
            return "(invalid-uri)";
        }
    }

    /** 仅匹配明确占位符；不用 contains("paste")，否则会误伤含子串 "paste" 的真密码。 */
    private static final Set<String> NEO4J_PASSWORD_PLACEHOLDERS = Set.of(
            "replace_with_your_aura_password",
            "paste_aura_database_password_here",
            "your-password-here",
            "your-aura-database-password"
    );

    private static boolean passwordLooksUnset(String password) {
        if (password == null || password.isBlank()) {
            return true;
        }
        String t = password.trim();
        if (t.startsWith("REPLACE_")) {
            return true;
        }
        return NEO4J_PASSWORD_PLACEHOLDERS.contains(t.toLowerCase(Locale.ROOT));
    }

    @GetMapping("/ping")
    public ResponseEntity<Map<String, Object>> ping() {
        Neo4jProbeService svc = probeService.getIfAvailable();
        if (svc == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", false);
            body.put("reason", "Neo4j Driver bean was not created (Neo4jHardcodedCredentials.URI is blank).");
            body.put("steps", List.of(
                    "在 Neo4jHardcodedCredentials.java 中设置非空的 URI（一般为 neo4j+s://….databases.neo4j.io）。",
                    "填写 USERNAME（多为 neo4j）与 PASSWORD（Aura Database password），重新编译并启动。"
            ));
            return ResponseEntity.status(503).body(body);
        }
        Map<String, Object> body = svc.ping();
        boolean ok = Boolean.TRUE.equals(body.get("ok"));
        return ok ? ResponseEntity.ok(body) : ResponseEntity.status(502).body(body);
    }

    @GetMapping("/stats/node-count")
    public ResponseEntity<Map<String, Object>> nodeCount() {
        Neo4jProbeService svc = probeService.getIfAvailable();
        if (svc == null) {
            return ResponseEntity.status(503).build();
        }
        Map<String, Object> body = svc.nodeCount();
        boolean ok = Boolean.TRUE.equals(body.get("ok"));
        return ok ? ResponseEntity.ok(body) : ResponseEntity.status(502).body(body);
    }

    /**
     * 执行只读 Cypher。Body: {@code {"cypher":"RETURN 1 AS n","parameters":{},"maxRecords":100}}。
     * 写操作关键字会被拒绝；默认最多 500 行，最大 2000。
     */
    @PostMapping("/query-cypher")
    public ResponseEntity<Map<String, Object>> queryCypher(@RequestBody QueryCypherRequest request) {
        Neo4jProbeService svc = probeService.getIfAvailable();
        if (svc == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", false);
            body.put("reason", "Neo4j Driver bean was not created.");
            return ResponseEntity.status(503).body(body);
        }
        try {
            Map<String, Object> body = svc.queryCypher(
                    request.cypher(),
                    request.parameters(),
                    request.effectiveMaxRecords());
            boolean ok = Boolean.TRUE.equals(body.get("ok"));
            return ok ? ResponseEntity.ok(body) : ResponseEntity.status(502).body(body);
        } catch (IllegalArgumentException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("ok", false);
            err.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        }
    }

    /**
     * Full wipe of all nodes + rebuild ads-attribution schema and demo seed (DeepChatBI / Shopify style).
     * {@code GET /api/neo4j/bootstrap-ads}
     */
    @GetMapping("/bootstrap-ads")
    public ResponseEntity<Map<String, Object>> bootstrapAds() {
        return runAdsBootstrap(false);
    }

    /**
     * @deprecated Alias for {@link #bootstrapAds()}; SRE demo graph has been replaced by ads attribution.
     */
    @Deprecated
    @GetMapping("/bootstrap-sre")
    public ResponseEntity<Map<String, Object>> bootstrapSre() {
        return runAdsBootstrap(true);
    }

    private ResponseEntity<Map<String, Object>> runAdsBootstrap(boolean deprecatedAlias) {
        Neo4jAdsAttributionGraphBootstrap boot = adsBootstrap.getIfAvailable();
        if (boot == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", false);
            body.put("reason", "Neo4j Driver or Neo4jAdsAttributionGraphBootstrap not available.");
            return ResponseEntity.status(503).body(body);
        }
        Map<String, Object> body = boot.seedAll();
        if (deprecatedAlias) {
            body.put("deprecatedEndpoint", "/api/neo4j/bootstrap-sre");
            body.put("useInstead", "/api/neo4j/bootstrap-ads");
        }
        boolean ok = Boolean.TRUE.equals(body.get("ok"));
        return ok ? ResponseEntity.ok(body) : ResponseEntity.status(502).body(body);
    }

    /**
     * Plain-text context from the ads graph for LLM injection (substring / token match on {@code :Creative}).
     * {@code GET /api/neo4j/ads-context-for-llm?query=...&limit=3}
     */
    @GetMapping("/ads-context-for-llm")
    public ResponseEntity<Map<String, Object>> adsContextForLlm(
            @RequestParam("query") String query,
            @RequestParam(value = "limit", required = false) Integer limit) {
        return contextForLlm(query, limit, false);
    }

    /**
     * @deprecated Alias for {@link #adsContextForLlm(String, Integer)}.
     */
    @Deprecated
    @GetMapping("/sre-context-for-llm")
    public ResponseEntity<Map<String, Object>> sreContextForLlm(
            @RequestParam("query") String query,
            @RequestParam(value = "limit", required = false) Integer limit) {
        return contextForLlm(query, limit, true);
    }

    private ResponseEntity<Map<String, Object>> contextForLlm(String query, Integer limit, boolean deprecatedAlias) {
        Neo4jAdsAttributionRagService rag = adsRagService.getIfAvailable();
        if (rag == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", false);
            body.put("reason", "Neo4jAdsAttributionRagService not available.");
            return ResponseEntity.status(503).body(body);
        }
        if (query == null || query.isBlank()) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("ok", false);
            err.put("error", "query must not be blank");
            return ResponseEntity.badRequest().body(err);
        }
        Map<String, Object> body = rag.buildContextPayload(query.trim(), SreContextRequest.clampLimit(limit));
        if (deprecatedAlias) {
            body.put("deprecatedEndpoint", "/api/neo4j/sre-context-for-llm");
            body.put("useInstead", "/api/neo4j/ads-context-for-llm");
        }
        return ResponseEntity.ok(body);
    }
}
