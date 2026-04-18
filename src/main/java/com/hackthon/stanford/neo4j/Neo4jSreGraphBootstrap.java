package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Idempotent schema + SRE demo knowledge graph (INC-042 class incidents) for LLM RAG-style matching.
 * Uses write transactions; not exposed through {@link CypherReadOnlyGuard}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnBean(Driver.class)
public class Neo4jSreGraphBootstrap {

    private final Driver driver;

    private Session openSession() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        if (!StringUtils.hasText(db)) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(db.trim()));
    }

    /**
     * Creates constraints/indexes (best-effort) and MERGEs SRE nodes/relationships.
     *
     * @return summary for HTTP / logs
     */
    public Map<String, Object> seedAll() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", false);
        try (Session session = openSession()) {
            // Neo4j forbids data writes in the same transaction after schema DDL (CREATE CONSTRAINT).
            session.executeWrite(tx -> {
                for (String cypher : schemaStatements()) {
                    tx.run(cypher);
                }
                return null;
            });
            session.executeWrite(tx -> {
                for (String cypher : seedStatements()) {
                    tx.run(cypher);
                }
                return null;
            });
            out.put("ok", true);
            out.put("message", "SRE graph schema + seed MERGE completed (idempotent, two transactions)");
            log.info("Neo4j SRE graph bootstrap OK");
        } catch (Exception e) {
            log.error("Neo4j SRE graph bootstrap failed", e);
            out.put("error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        return out;
    }

    private static String[] schemaStatements() {
        return new String[] {
                "CREATE CONSTRAINT incident_id IF NOT EXISTS FOR (n:Incident) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT service_id IF NOT EXISTS FOR (n:Service) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT symptom_id IF NOT EXISTS FOR (n:Symptom) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT rootcause_id IF NOT EXISTS FOR (n:RootCause) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT fix_id IF NOT EXISTS FOR (n:Fix) REQUIRE n.id IS UNIQUE",
        };
    }

    private static String[] seedStatements() {
        return new String[] {
                // INC-042 — connection pool exhaustion
                """
                MERGE (i:Incident {id: 'INC-042'})
                SET i.title = 'API Server Connection Pool Exhaustion',
                    i.severity = 'P1',
                    i.date = '2026-03-20T02:30:00Z',
                    i.duration_min = 45,
                    i.summary = 'api-server returned 503 under normal traffic; Hikari pool timeouts; DB healthy; DB_POOL_SIZE=1 in api-config ConfigMap.',
                    i.searchBlob = 'inc-042 api-server connection pool exhaustion hikari 503 timeout postgresql healthy configmap db_pool_size cost optimization platform kubernetes production'
                """,
                """
                MERGE (s:Service {id: 'svc-api-server-prod'})
                SET s.name = 'api-server',
                    s.namespace = 'production',
                    s.team = 'backend',
                    s.searchBlob = 'api-server production backend kubernetes'
                """,
                """
                MERGE (p:Service {id: 'svc-postgres-prod'})
                SET p.name = 'postgres',
                    p.namespace = 'production',
                    p.team = 'data',
                    p.searchBlob = 'postgres production database'
                """,
                """
                MERGE (sym:Symptom {id: 'sym-inc042-pool-timeout'})
                SET sym.description = '503 errors; p99 latency high; app logs: Cannot acquire connection from pool; HikariPool-1 timeout 30000ms',
                    sym.kubectl_evidence = 'kubectl logs api-server pods show pool timeout',
                    sym.searchBlob = '503 latency pool timeout hikari connection not available acquire'
                """,
                """
                MERGE (rc:RootCause {id: 'rc-inc042-pool-size'})
                SET rc.category = 'configuration-drift',
                    rc.description = 'DB_POOL_SIZE reduced from 20 to 1 during cost optimization; app pool exhausted while Postgres had spare capacity',
                    rc.config_key = 'DB_POOL_SIZE',
                    rc.config_value_bad = '1',
                    rc.config_value_good = '20',
                    rc.configmap = 'api-config',
                    rc.searchBlob = 'db_pool_size configmap api-config pool size one cost optimization platform'
                """,
                """
                MERGE (f:Fix {id: 'fix-inc042-patch-restart'})
                SET f.description = 'Patch ConfigMap DB_POOL_SIZE to 20 and rollout restart api-server',
                    f.command = "kubectl patch configmap api-config -n production -p '{\\"data\\":{\\"DB_POOL_SIZE\\":\\"20\\"}}' && kubectl rollout restart deployment/api-server -n production",
                    f.searchBlob = 'kubectl patch configmap rollout restart db_pool_size verify env'
                """,
                "MATCH (i:Incident {id: 'INC-042'}), (s:Service {id: 'svc-api-server-prod'}) MERGE (i)-[:AFFECTED]->(s)",
                "MATCH (i:Incident {id: 'INC-042'}), (p:Service {id: 'svc-postgres-prod'}) MERGE (i)-[:DEPENDS_ON]->(p)",
                "MATCH (i:Incident {id: 'INC-042'}), (sym:Symptom {id: 'sym-inc042-pool-timeout'}) MERGE (i)-[:EXHIBITED]->(sym)",
                "MATCH (i:Incident {id: 'INC-042'}), (rc:RootCause {id: 'rc-inc042-pool-size'}) MERGE (i)-[:CAUSED_BY]->(rc)",
                "MATCH (i:Incident {id: 'INC-042'}), (f:Fix {id: 'fix-inc042-patch-restart'}) MERGE (i)-[:RESOLVED_BY]->(f)",

                // Second pattern: DNS / external dependency (short, for multi-match demos)
                """
                MERGE (i2:Incident {id: 'INC-051'})
                SET i2.title = 'Checkout failures after DNS TTL change',
                    i2.severity = 'P2',
                    i2.date = '2026-04-01T10:00:00Z',
                    i2.duration_min = 20,
                    i2.summary = 'Intermittent checkout 502; upstream HTTP client cached stale DNS; fixed by reducing TTL + client refresh.',
                    i2.searchBlob = 'inc-051 dns ttl checkout 502 upstream http client stale resolver'
                """,
                """
                MERGE (sym2:Symptom {id: 'sym-inc051-502'})
                SET sym2.description = '502 from payment-gateway calls; spike correlated with DNS provider change',
                    sym2.kubectl_evidence = 'ingress logs show upstream reset',
                    sym2.searchBlob = '502 payment gateway dns upstream intermittent'
                """,
                """
                MERGE (rc2:RootCause {id: 'rc-inc051-dns'})
                SET rc2.category = 'external-dependency',
                    rc2.description = 'Java DNS cache + long TTL caused stale A records after cutover',
                    rc2.config_key = 'networkaddress.cache.ttl',
                    rc2.config_value_bad = '-1',
                    rc2.config_value_good = '30',
                    rc2.searchBlob = 'dns cache ttl java networkaddress resolver stale'
                """,
                "MATCH (i2:Incident {id: 'INC-051'}), (sym2:Symptom {id: 'sym-inc051-502'}) MERGE (i2)-[:EXHIBITED]->(sym2)",
                "MATCH (i2:Incident {id: 'INC-051'}), (rc2:RootCause {id: 'rc-inc051-dns'}) MERGE (i2)-[:CAUSED_BY]->(rc2)",
        };
    }
}
