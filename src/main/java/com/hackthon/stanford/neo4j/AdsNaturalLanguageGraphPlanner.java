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
 * Maps user {@code content} to a {@link GraphRetrievalPlan} for Creative-centric retrieval (加钱/减钱, ROAS, channels).
 */
@Component
public class AdsNaturalLanguageGraphPlanner {

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

    private static final Set<String> STOP_ZH = Set.of("的", "了", "吗", "呢", "和", "与", "在", "是", "我", "你", "就", "都", "要", "有");

    public GraphRetrievalPlan plan(String userMessage) {
        if (!StringUtils.hasText(userMessage)) {
            return new GraphRetrievalPlan(GraphRetrievalPlan.Type.KEYWORD_SEARCH, List.of(), "",
                    "MATCH (c:Creative) WHERE false RETURN c",
                    "Empty message — no retrieval.");
        }
        String trimmed = userMessage.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);

        if (isMetaHelpQuestion(lower)) {
            String pseudo = """
                    /* No graph scan — meta / capabilities question */
                    RETURN "DeepChatBI-style ads attribution: Creative nodes with budgetSignal (加钱/减钱), ROAS, campaigns, orders" AS note
                    """;
            return new GraphRetrievalPlan(GraphRetrievalPlan.Type.META_HELP, List.of(), "", pseudo.strip(),
                    "User asked about capabilities; answer from ads-attribution analyst role without requiring graph hits.");
        }

        LinkedHashSet<String> tokens = new LinkedHashSet<>();
        for (String part : SPLIT.split(lower)) {
            if (!StringUtils.hasText(part)) {
                continue;
            }
            if (part.length() >= 2 && !STOP_EN.contains(part) && !STOP_ZH.contains(part)) {
                tokens.add(part);
            }
            if (tokens.size() >= 12) {
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
                MATCH (c:Creative)
                WHERE ANY(t IN %s WHERE toLower(c.searchBlob) CONTAINS t
                   OR toLower(c.name) CONTAINS t
                   OR toLower(coalesce(c.rationale,'')) CONTAINS t
                   OR toLower(coalesce(c.budgetSignalZh,'')) CONTAINS t)
                OPTIONAL MATCH (c)-[:IN_CAMPAIGN]->(camp:Campaign)-[:ON_PLATFORM]->(p:AdPlatform)
                OPTIONAL MATCH (c)-[:ATTRIBUTED_TO]->(o:ShopOrder)
                RETURN c, camp, p, collect(DISTINCT o) AS orders
                """.formatted(pseudoTokens);

        if (list.isEmpty()) {
            pseudoCypher = """
                    MATCH (c:Creative)
                    WHERE toLower(c.searchBlob) CONTAINS $needle
                       OR toLower(c.name) CONTAINS $needle
                       OR toLower(coalesce(c.rationale,'')) CONTAINS $needle
                    OPTIONAL MATCH (c)-[:IN_CAMPAIGN]->(camp:Campaign)-[:ON_PLATFORM]->(p:AdPlatform)
                    OPTIONAL MATCH (c)-[:ATTRIBUTED_TO]->(o:ShopOrder)
                    RETURN c, camp, p, collect(DISTINCT o) AS orders
                    /* $needle = normalized full user text */
                    """.stripIndent();
        }

        return new GraphRetrievalPlan(GraphRetrievalPlan.Type.KEYWORD_SEARCH, list, needle, pseudoCypher.strip(),
                "Extracted keywords for substring match on Creative (归因 / ROAS / 加钱 / 减钱 / channel).");
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
