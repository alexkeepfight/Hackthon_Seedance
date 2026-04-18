package com.hackthon.stanford.neo4j;

import com.hackthon.stanford.chat.dto.AgentStreamChunk;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * NL → {@link GraphRetrievalPlan}, Neo4j retrieval on {@code :Creative} nodes, prepended to {@code skillsPrompt}.
 */
@Service
@RequiredArgsConstructor
@Conditional(Neo4jEnabledCondition.class)
public class AdsNlGraphPromptAugmentor {

    private static final String META_HINT = """
            The user is asking what you can do or for general help (no Neo4j graph query was run).
            Reply briefly as an ads-attribution analyst: Shopify / DeepChatBI-style MER, ROAS, POAS, first/last click,
            Customer Journey & Order Attribution, and that seeded Creatives expose budgetSignal 加钱 vs 减钱 with rationale.
            Offer 2–3 example questions (e.g. “哪个 Meta 创意该加钱？”, “Google ROAS 低的创意减钱依据？”).
            """;

    private final AdsNaturalLanguageGraphPlanner planner;
    private final Neo4jAdsAttributionRagService rag;

    public AgentStreamChunk augment(AgentStreamChunk req, int limit) {
        if (req == null || !StringUtils.hasText(req.getContent())) {
            return req;
        }
        GraphRetrievalPlan plan = planner.plan(req.getContent().trim());

        StringBuilder sb = new StringBuilder();
        sb.append("### Step 1 — Natural language → graph retrieval plan\n");
        sb.append("- ").append(plan.plannerNote()).append("\n");
        sb.append("- Illustrative Cypher (app uses parameterized driver; shown for transparency):\n```cypher\n");
        sb.append(plan.pseudoCypher().trim()).append("\n```\n");
        if (!plan.searchTokens().isEmpty()) {
            sb.append("- Extracted keyword tokens: ").append(plan.searchTokens()).append("\n");
        } else if (plan.type() == GraphRetrievalPlan.Type.KEYWORD_SEARCH) {
            sb.append("- Fallback: substring match with normalized full user text as `$needle`\n");
        }
        sb.append("\n### Step 2 — Retrieval results (use these facts in your summary)\n");
        if (plan.type() == GraphRetrievalPlan.Type.META_HELP) {
            sb.append(META_HINT.trim()).append("\n");
        } else {
            sb.append(rag.buildContextBlockForPlan(plan, limit).trim());
        }

        String injected = sb.toString().trim();
        String skills = req.getSkillsPrompt();
        String merged = StringUtils.hasText(skills) ? injected + "\n\n" + skills : injected;
        return req.toBuilder().skillsPrompt(merged).build();
    }
}
