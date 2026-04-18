package com.hackthon.stanford.neo4j;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Maps free-text user {@code content} to a {@link GraphRetrievalPlan} (tokens + pseudo-Cypher description).
 */
@Component
public class SreNaturalLanguageGraphPlanner {

    private static final Pattern SPLIT = Pattern.compile("[^a-zA-Z0-9\\p{IsHan}]+");

    private static final Set<String> STOP_EN = Set.of(
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "what", "which", "who", "whom", "whose", "where", "when", "why", "how",
            "can", "could", "would", "should", "may", "might", "must", "shall", "will",
            "do", "does", "did", "done", "doing", "have", "has", "had", "having",
            "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
            "my", "your", "his", "its", "our", "their", "mine", "yours", "ours", "theirs",
            "this", "that", "these", "those", "here", "there", "then", "than", "into", "onto",
            "to", "of", "in", "on", "for", "with", "as", "by", "at", "from", "or", "and", "but",
            "not", "no", "yes", "please", "tell", "about", "help", "thanks", "hello", "hi", "hey",
            "something", "anything", "everything", "nothing", "someone", "anyone", "like", "just", "only"
    );

    public GraphRetrievalPlan plan(String userMessage) {
        if (!StringUtils.hasText(userMessage)) {
            return new GraphRetrievalPlan(GraphRetrievalPlan.Type.KEYWORD_SEARCH, List.of(), "",
                    "MATCH (i:Incident) WHERE false /* empty user message */ RETURN i",
                    "Empty message — no retrieval.");
        }
        String trimmed = userMessage.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);

        if (isMetaHelpQuestion(lower)) {
            String pseudo = """
                    /* No graph scan — meta / capabilities question */
                    RETURN "IncidentBrain SRE agent + optional Neo4j Incident/Symptom/RootCause/Fix subgraph" AS note
                    """;
            return new GraphRetrievalPlan(GraphRetrievalPlan.Type.META_HELP, List.of(), "", pseudo.strip(),
                    "User asked about capabilities or help; answer from agent role without requiring graph hits.");
        }

        LinkedHashSet<String> tokens = new LinkedHashSet<>();
        for (String part : SPLIT.split(lower)) {
            if (!StringUtils.hasText(part)) {
                continue;
            }
            if (part.length() >= 2 && !STOP_EN.contains(part)) {
                tokens.add(part);
            }
            if (tokens.size() >= 10) {
                break;
            }
        }

        List<String> list = new ArrayList<>(tokens);
        String needle = trimmed.toLowerCase(Locale.ROOT);
        if (needle.length() > 200) {
            needle = needle.substring(0, 200);
        }

        String pseudoTokens = list.isEmpty() ? "[]" : list.toString();
        String pseudoCypher = """
                MATCH (i:Incident)
                WHERE ANY(t IN %s WHERE toLower(i.searchBlob) CONTAINS t
                   OR toLower(i.title) CONTAINS t
                   OR toLower(i.summary) CONTAINS t)
                OPTIONAL MATCH (i)-[:EXHIBITED|CAUSED_BY|RESOLVED_BY|AFFECTED|DEPENDS_ON]->(n)
                RETURN i, collect(n) AS related
                """.formatted(pseudoTokens);

        if (list.isEmpty()) {
            pseudoCypher = """
                    MATCH (i:Incident)
                    WHERE toLower(i.searchBlob) CONTAINS $needle
                       OR toLower(i.title) CONTAINS $needle
                       OR toLower(i.summary) CONTAINS $needle
                    OPTIONAL MATCH (i)-[:EXHIBITED|CAUSED_BY|RESOLVED_BY|AFFECTED|DEPENDS_ON]->(n)
                    RETURN i, collect(n) AS related
                    /* $needle = normalized full user text (no keywords extracted) */
                    """.stripIndent();
        }

        return new GraphRetrievalPlan(GraphRetrievalPlan.Type.KEYWORD_SEARCH, list, needle, pseudoCypher.strip(),
                "Extracted keywords from user message for substring match on Incident nodes.");
    }

    private static boolean isMetaHelpQuestion(String lower) {
        if (lower.equals("help") || lower.equals("hi") || lower.equals("hello") || lower.equals("hey")) {
            return true;
        }
        if (lower.length() < 8) {
            return false;
        }
        return lower.contains("what can you do")
                || lower.contains("what do you do")
                || lower.contains("who are you")
                || lower.contains("help me")
                || lower.equals("help")
                || lower.startsWith("help ")
                || lower.contains("your capabilities")
                || lower.contains("what are you")
                || (lower.contains("how") && lower.contains("you") && lower.contains("help"));
    }
}
