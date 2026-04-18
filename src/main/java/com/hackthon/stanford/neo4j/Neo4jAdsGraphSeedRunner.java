package com.hackthon.stanford.neo4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Optionally wipes + seeds the ads-attribution graph on startup ({@code neo4j.ads.auto-seed}).
 */
@Slf4j
@Component
@Order(20)
@RequiredArgsConstructor
@Conditional(Neo4jEnabledCondition.class)
public class Neo4jAdsGraphSeedRunner implements ApplicationRunner {

    @Value("${neo4j.ads.auto-seed:false}")
    private boolean autoSeed;

    private final Neo4jAdsAttributionGraphBootstrap bootstrap;

    @Override
    public void run(ApplicationArguments args) {
        if (!autoSeed) {
            return;
        }
        if (bootstrap.hasCreativeNodes()) {
            log.info("neo4j.ads.auto-seed=true: skip (Creative nodes already present). Use GET /api/neo4j/bootstrap-ads to wipe + rebuild.");
            return;
        }
        log.info("neo4j.ads.auto-seed=true: no Creative nodes — running full wipe + ads attribution seed");
        Map<String, Object> out = bootstrap.seedAll();
        if (!Boolean.TRUE.equals(out.get("ok"))) {
            log.warn("Ads graph auto-seed did not complete OK: {}", out.get("error"));
        }
    }
}
