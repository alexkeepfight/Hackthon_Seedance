package com.hackthon.stanford.neo4j;

import com.hackthon.stanford.chat.dto.AgentStreamChunk;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Turns user {@code content} into a {@link GraphRetrievalPlan}, runs Neo4j RAG (when applicable),
 * and prepends a structured block to {@code skillsPrompt} so the LLM can summarize with cited graph facts.
 */
@Service
@RequiredArgsConstructor
@ConditionalOnBean(Neo4jSreRagService.class)
public class SreNlGraphPromptAugmentor {

    private static final String META_HINT = """
            The user is asking what you can do or for general help (no Neo4j graph query was run).
            Reply briefly as IncidentBrain: SRE incident triage, postmortem-style reasoning, kubectl/Grafana-style checks,
            connection pool / ConfigMap / DNS-class patterns, and that past incidents may live in Neo4j when seeded.
            Offer 2–3 example questions they can ask next (e.g. connection pool 503, INC-042, db pool size).
            """;

    private final SreNaturalLanguageGraphPlanner planner;
    private final Neo4jSreRagService rag;

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
