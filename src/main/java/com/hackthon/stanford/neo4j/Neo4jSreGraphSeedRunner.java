package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Optionally seeds SRE graph on startup (controlled by {@code neo4j.sre.auto-seed}).
 */
@Slf4j
@Component
@Order(20)
@RequiredArgsConstructor
@ConditionalOnBean({Driver.class, Neo4jSreGraphBootstrap.class})
public class Neo4jSreGraphSeedRunner implements ApplicationRunner {

    @Value("${neo4j.sre.auto-seed:false}")
    private boolean autoSeed;

    private final Neo4jSreGraphBootstrap bootstrap;

    @Override
    public void run(ApplicationArguments args) {
        if (!autoSeed) {
            return;
        }
        log.info("neo4j.sre.auto-seed=true: running SRE graph bootstrap");
        var out = bootstrap.seedAll();
        if (!Boolean.TRUE.equals(out.get("ok"))) {
            log.warn("SRE graph auto-seed did not complete OK: {}", out.get("error"));
        }
    }
}
