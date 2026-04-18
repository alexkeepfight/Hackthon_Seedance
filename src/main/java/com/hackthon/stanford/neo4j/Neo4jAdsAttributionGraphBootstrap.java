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
 * Wipes prior demo graphs, creates constraints for the ads-attribution model, and MERGEs
 * a sample Shopify / DeepChatBI–style graph (creatives, campaigns, orders, journeys, budget signals).
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnBean(Driver.class)
public class Neo4jAdsAttributionGraphBootstrap {

    private final Driver driver;

    private Session openSession() {
        String db = Neo4jHardcodedCredentials.DATABASE;
        if (!StringUtils.hasText(db)) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(db.trim()));
    }

    /**
     * Drops legacy SRE + any prior ads constraints, deletes all nodes, then schema + seed (three write txs).
     */
    public Map<String, Object> seedAll() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", false);
        try (Session session = openSession()) {
            session.executeWrite(tx -> {
                for (String cypher : wipeStatements()) {
                    tx.run(cypher);
                }
                return null;
            });
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
            out.put("message", "Ads attribution graph: wiped DB, constraints, MERGE seed (DeepChatBI-style demo)");
            log.info("Neo4j ads attribution graph bootstrap OK");
        } catch (Exception e) {
            log.error("Neo4j ads attribution graph bootstrap failed", e);
            out.put("error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        return out;
    }

    private static String[] wipeStatements() {
        return new String[] {
                "DROP CONSTRAINT incident_id IF EXISTS",
                "DROP CONSTRAINT service_id IF EXISTS",
                "DROP CONSTRAINT symptom_id IF EXISTS",
                "DROP CONSTRAINT rootcause_id IF EXISTS",
                "DROP CONSTRAINT fix_id IF EXISTS",
                "DROP CONSTRAINT creative_id IF EXISTS",
                "DROP CONSTRAINT campaign_id IF EXISTS",
                "DROP CONSTRAINT ad_platform_id IF EXISTS",
                "DROP CONSTRAINT shop_order_id IF EXISTS",
                "DROP CONSTRAINT customer_session_id IF EXISTS",
                "DROP CONSTRAINT touch_channel_id IF EXISTS",
                "DROP CONSTRAINT report_period_id IF EXISTS",
                "MATCH (n) DETACH DELETE n",
        };
    }

    private static String[] schemaStatements() {
        return new String[] {
                "CREATE CONSTRAINT creative_id IF NOT EXISTS FOR (n:Creative) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT campaign_id IF NOT EXISTS FOR (n:Campaign) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT ad_platform_id IF NOT EXISTS FOR (n:AdPlatform) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT shop_order_id IF NOT EXISTS FOR (n:ShopOrder) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT customer_session_id IF NOT EXISTS FOR (n:CustomerSession) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT touch_channel_id IF NOT EXISTS FOR (n:TouchChannel) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT report_period_id IF NOT EXISTS FOR (n:ReportPeriod) REQUIRE n.id IS UNIQUE",
        };
    }

    private static String[] seedStatements() {
        return new String[] {
                """
                MERGE (p1:AdPlatform {id: 'platform-google'})
                SET p1.name = 'Google Ads',
                    p1.searchBlob = 'google ads sem pmax search ctr cpm roas spend'
                """,
                """
                MERGE (p2:AdPlatform {id: 'platform-meta'})
                SET p2.name = 'Meta Ads',
                    p2.searchBlob = 'meta facebook instagram ads creative carousel reel spend'
                """,
                """
                MERGE (tcO:TouchChannel {id: 'ch-organic'})
                SET tcO.name = 'organic',
                    tcO.searchBlob = 'organic direct seo email owned'
                """,
                """
                MERGE (tcG:TouchChannel {id: 'ch-google'})
                SET tcG.name = 'google',
                    tcG.searchBlob = 'google paid search pmax campaign utm'
                """,
                """
                MERGE (tcM:TouchChannel {id: 'ch-meta'})
                SET tcM.name = 'meta',
                    tcM.searchBlob = 'meta meta_ads facebook paid social utm'
                """,
                """
                MERGE (campG:Campaign {id: 'camp-google-23588093409'})
                SET campG.name = 'Google · meal packages (23588093409)',
                    campG.externalLabel = 'Google Campaign:23588093409',
                    campG.searchBlob = 'google campaign 23588093409 two week one month complete meal package'
                """,
                """
                MERGE (campM:Campaign {id: 'camp-meta-prospecting'})
                SET campM.name = 'Meta · prospecting + gifting',
                    campM.externalLabel = 'meta_ads',
                    campM.searchBlob = 'meta prospecting gifting one week package collection'
                """,
                "MATCH (campG:Campaign {id: 'camp-google-23588093409'}), (pg:AdPlatform {id: 'platform-google'}) MERGE (campG)-[:ON_PLATFORM]->(pg)",
                "MATCH (campM:Campaign {id: 'camp-meta-prospecting'}), (pm:AdPlatform {id: 'platform-meta'}) MERGE (campM)-[:ON_PLATFORM]->(pm)",
                """
                MERGE (rp:ReportPeriod {id: 'rp-2026-04-11_17'})
                SET rp.label = 'Apr 11, 2026 – Apr 17, 2026',
                    rp.totalAdsSpend = 1329.07,
                    rp.blendedRoas = 23.522,
                    rp.totalOrderRevenue = 31723.27,
                    rp.totalOrders = 118,
                    rp.googleSpend = 814.07,
                    rp.metaSpend = 515.00,
                    rp.searchBlob = 'deepchatbi shopify summary blended roas mer profit sku cohort attribution'
                """,
                // Creatives with budgetSignal: SCALE_UP = 加钱, SCALE_DOWN = 减钱, HOLD = 保持
                """
                MERGE (cr1:Creative {id: 'cr-google-dco-two-week'})
                SET cr1.name = 'Google DCO · Two-week bundle',
                    cr1.spendUsd = 295.0,
                    cr1.attributedRevenueUsd = 1080.0,
                    cr1.roas = 3.66,
                    cr1.budgetSignal = 'SCALE_UP',
                    cr1.budgetSignalZh = '加钱',
                    cr1.rationale = 'ROAS 3.66 & attributed revenue clearly above spend; MER/POAS-friendly scale candidate for similar SKU bundles.',
                    cr1.searchBlob = 'google dco two week complete meal package roas scale 加钱 creative 创意 放量'
                """,
                """
                MERGE (cr2:Creative {id: 'cr-google-search-brand'})
                SET cr2.name = 'Google Search · brand defense',
                    cr2.spendUsd = 519.0,
                    cr2.attributedRevenueUsd = 478.0,
                    cr2.roas = 0.92,
                    cr2.budgetSignal = 'SCALE_DOWN',
                    cr2.budgetSignalZh = '减钱',
                    cr2.rationale = 'ROAS below 1.0 on attributed sales; trim bids or reallocate to higher-POAS creatives after Creative Catalog review.',
                    cr2.searchBlob = 'google search brand low roas waste 减钱 收缩 预算 创意'
                """,
                """
                MERGE (cr3:Creative {id: 'cr-meta-one-week-gifting'})
                SET cr3.name = 'Meta carousel · one-week + gifting path',
                    cr3.spendUsd = 265.0,
                    cr3.attributedRevenueUsd = 912.0,
                    cr3.roas = 3.44,
                    cr3.budgetSignal = 'SCALE_UP',
                    cr3.budgetSignalZh = '加钱',
                    cr3.rationale = 'Strong last-click/meta journey orders (e.g. multi-touch meta→meta); good incrementality vs spend in demo slice.',
                    cr3.searchBlob = 'meta carousel one week gifting journey multi touch 加钱 roas creative 创意'
                """,
                """
                MERGE (cr4:Creative {id: 'cr-meta-broad-lowintent'})
                SET cr4.name = 'Meta broad · low-intent placements',
                    cr4.spendUsd = 250.0,
                    cr4.attributedRevenueUsd = 195.0,
                    cr4.roas = 0.78,
                    cr4.budgetSignal = 'SCALE_DOWN',
                    cr4.budgetSignalZh = '减钱',
                    cr4.rationale = 'POAS weak; high impressions/clicks vs low attributed checkout value — reduce CPM bids or tighten audience.',
                    cr4.searchBlob = 'meta broad placement low intent cpm high spend 减钱 降预算 创意'
                """,
                "MATCH (cr1:Creative {id: 'cr-google-dco-two-week'}), (cg:Campaign {id: 'camp-google-23588093409'}) MERGE (cr1)-[:IN_CAMPAIGN]->(cg)",
                "MATCH (cr2:Creative {id: 'cr-google-search-brand'}), (cg:Campaign {id: 'camp-google-23588093409'}) MERGE (cr2)-[:IN_CAMPAIGN]->(cg)",
                "MATCH (cr3:Creative {id: 'cr-meta-one-week-gifting'}), (cm:Campaign {id: 'camp-meta-prospecting'}) MERGE (cr3)-[:IN_CAMPAIGN]->(cm)",
                "MATCH (cr4:Creative {id: 'cr-meta-broad-lowintent'}), (cm:Campaign {id: 'camp-meta-prospecting'}) MERGE (cr4)-[:IN_CAMPAIGN]->(cm)",
                """
                MERGE (o1:ShopOrder {id: 'ord-6754557853888'})
                SET o1.orderGid = '6754557853888',
                    o1.revenueUsd = 186.4,
                    o1.eventPathSummary = 'page_viewed → product_viewed → … → checkout_completed',
                    o1.searchBlob = '6754557853888 google campaign 23588093409 two week package'
                """,
                """
                MERGE (o2:ShopOrder {id: 'ord-6754220048576'})
                SET o2.orderGid = '6754220048576',
                    o2.revenueUsd = 142.0,
                    o2.eventPathSummary = 'product_viewed → cart dwell → checkout_completed',
                    o2.searchBlob = '6754220048576 google one month package'
                """,
                """
                MERGE (o3:ShopOrder {id: 'ord-6753993261248'})
                SET o3.orderGid = '6753993261248',
                    o3.revenueUsd = 228.5,
                    o3.eventPathSummary = 'meta touchpoints ×2 → checkout_completed',
                    o3.searchBlob = '6753993261248 meta meta_ads gifting one week'
                """,
                """
                MERGE (o4:ShopOrder {id: 'ord-6754570207424'})
                SET o4.orderGid = '6754570207424',
                    o4.revenueUsd = 96.0,
                    o4.eventPathSummary = 'organic storefront journey',
                    o4.searchBlob = '6754570207424 organic restorative roots'
                """,
                """
                MERGE (s1:CustomerSession {id: 'sess-43cf3f91'})
                SET s1.clientUuid = '43cf3f91-cb15-421b-a776-805a947205fc',
                    s1.searchBlob = '43cf3f91 google session'
                """,
                """
                MERGE (s2:CustomerSession {id: 'sess-a7a5cb63'})
                SET s2.clientUuid = 'a7a5cb63-d0d3-48dc-bf7e-c6d807db7f6d',
                    s2.searchBlob = 'a7a5cb63 google session'
                """,
                """
                MERGE (s3:CustomerSession {id: 'sess-7dfa1552'})
                SET s3.clientUuid = '7dfa1552-89c0-4137-a8fe-755ffbccad21',
                    s3.searchBlob = '7dfa1552 meta multi touch'
                """,
                """
                MERGE (s4:CustomerSession {id: 'sess-28bfdce0'})
                SET s4.clientUuid = '28bfdce0-7c0d-4d4e-9295-ba2c6acda758',
                    s4.searchBlob = '28bfdce0 organic'
                """,
                "MATCH (o1:ShopOrder {id: 'ord-6754557853888'}), (rp:ReportPeriod {id: 'rp-2026-04-11_17'}) MERGE (o1)-[:IN_PERIOD]->(rp)",
                "MATCH (o2:ShopOrder {id: 'ord-6754220048576'}), (rp:ReportPeriod {id: 'rp-2026-04-11_17'}) MERGE (o2)-[:IN_PERIOD]->(rp)",
                "MATCH (o3:ShopOrder {id: 'ord-6753993261248'}), (rp:ReportPeriod {id: 'rp-2026-04-11_17'}) MERGE (o3)-[:IN_PERIOD]->(rp)",
                "MATCH (o4:ShopOrder {id: 'ord-6754570207424'}), (rp:ReportPeriod {id: 'rp-2026-04-11_17'}) MERGE (o4)-[:IN_PERIOD]->(rp)",
                "MATCH (o1:ShopOrder {id: 'ord-6754557853888'}), (tc:TouchChannel {id: 'ch-google'}) MERGE (o1)-[:FIRST_TOUCH]->(tc) MERGE (o1)-[:LAST_TOUCH]->(tc)",
                "MATCH (o2:ShopOrder {id: 'ord-6754220048576'}), (tc:TouchChannel {id: 'ch-google'}) MERGE (o2)-[:FIRST_TOUCH]->(tc) MERGE (o2)-[:LAST_TOUCH]->(tc)",
                "MATCH (o3:ShopOrder {id: 'ord-6753993261248'}), (tc:TouchChannel {id: 'ch-meta'}) MERGE (o3)-[:FIRST_TOUCH]->(tc) MERGE (o3)-[:LAST_TOUCH]->(tc)",
                "MATCH (o4:ShopOrder {id: 'ord-6754570207424'}), (tc:TouchChannel {id: 'ch-organic'}) MERGE (o4)-[:FIRST_TOUCH]->(tc) MERGE (o4)-[:LAST_TOUCH]->(tc)",
                "MATCH (o1:ShopOrder {id: 'ord-6754557853888'}), (s:CustomerSession {id: 'sess-43cf3f91'}) MERGE (o1)-[:BY_SESSION]->(s)",
                "MATCH (o2:ShopOrder {id: 'ord-6754220048576'}), (s:CustomerSession {id: 'sess-a7a5cb63'}) MERGE (o2)-[:BY_SESSION]->(s)",
                "MATCH (o3:ShopOrder {id: 'ord-6753993261248'}), (s:CustomerSession {id: 'sess-7dfa1552'}) MERGE (o3)-[:BY_SESSION]->(s)",
                "MATCH (o4:ShopOrder {id: 'ord-6754570207424'}), (s:CustomerSession {id: 'sess-28bfdce0'}) MERGE (o4)-[:BY_SESSION]->(s)",
                """
                MATCH (c:Creative {id: 'cr-google-dco-two-week'}), (o:ShopOrder {id: 'ord-6754557853888'})
                MERGE (c)-[r:ATTRIBUTED_TO]->(o)
                SET r.model = 'last_click', r.share = 1.0, r.attributedRevenueUsd = 186.4
                """,
                """
                MATCH (c:Creative {id: 'cr-google-dco-two-week'}), (o:ShopOrder {id: 'ord-6754220048576'})
                MERGE (c)-[r:ATTRIBUTED_TO]->(o)
                SET r.model = 'last_click', r.share = 1.0, r.attributedRevenueUsd = 142.0
                """,
                """
                MATCH (c:Creative {id: 'cr-google-search-brand'}), (o:ShopOrder {id: 'ord-6754557853888'})
                MERGE (c)-[r:ATTRIBUTED_TO]->(o)
                SET r.model = 'assist', r.share = 0.35, r.attributedRevenueUsd = 65.2
                """,
                """
                MATCH (c:Creative {id: 'cr-meta-one-week-gifting'}), (o:ShopOrder {id: 'ord-6753993261248'})
                MERGE (c)-[r:ATTRIBUTED_TO]->(o)
                SET r.model = 'last_click', r.share = 1.0, r.attributedRevenueUsd = 228.5
                """,
                """
                MATCH (c:Creative {id: 'cr-meta-broad-lowintent'}), (o:ShopOrder {id: 'ord-6753993261248'})
                MERGE (c)-[r:ATTRIBUTED_TO]->(o)
                SET r.model = 'assist', r.share = 0.25, r.attributedRevenueUsd = 57.1
                """,
        };
    }
}
