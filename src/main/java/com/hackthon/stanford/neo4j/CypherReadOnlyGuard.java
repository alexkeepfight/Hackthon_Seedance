package com.hackthon.stanford.neo4j;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 粗粒度只读校验：首条语句须为读风格，并拦截常见写关键字。
 * {@code (?<![.])} 避免误伤 {@code apoc.create} 等过程名中的 {@code create} 子串。
 * 生产环境请改用认证 + 固定查询白名单。
 */
final class CypherReadOnlyGuard {

    private static final Pattern WRITE_KEYWORDS = Pattern.compile(
            "(?is)(?<![.])\\b(CREATE|MERGE|DELETE|SET|REMOVE|DROP|FOREACH|INSERT)\\b"
                    + "|\\bDETACH\\s+DELETE\\b"
                    + "|\\bLOAD\\s+CSV\\b"
                    + "|\\bGRANT\\b"
                    + "|\\bDENY\\b"
                    + "|\\bALTER\\b");

    private CypherReadOnlyGuard() {}

    static String validateOrThrow(String cypher) {
        if (cypher == null || cypher.isBlank()) {
            throw new IllegalArgumentException("cypher must not be blank");
        }
        String trimmed = cypher.trim();
        if (!looksLikeReadQuery(trimmed)) {
            throw new IllegalArgumentException(
                    "query must start (after optional EXPLAIN/PROFILE and // comments) with "
                            + "MATCH, OPTIONAL MATCH, RETURN, CALL, WITH, UNWIND, or SHOW");
        }
        if (WRITE_KEYWORDS.matcher(trimmed).find()) {
            throw new IllegalArgumentException(
                    "only read-style Cypher is allowed (blocked write keywords such as CREATE/MERGE/DELETE/SET/…)");
        }
        return trimmed;
    }

    /** 首条有效语句前允许空行与 // 单行注释。 */
    static String stripLeadingNoise(String cypher) {
        String[] lines = cypher.split("\\R", -1);
        StringBuilder rest = new StringBuilder();
        boolean started = false;
        for (String line : lines) {
            String t = line.stripLeading();
            if (!started) {
                if (t.isEmpty() || t.startsWith("//")) {
                    continue;
                }
                started = true;
            }
            if (started) {
                if (rest.length() > 0) {
                    rest.append('\n');
                }
                rest.append(line);
            }
        }
        String s = started ? rest.toString() : cypher.trim();
        return s.isBlank() ? cypher.trim() : s;
    }

    static boolean looksLikeReadQuery(String cypher) {
        String s = stripLeadingNoise(cypher).stripLeading();
        if (s.isEmpty()) {
            return false;
        }
        while (true) {
            String u = s.toUpperCase(Locale.ROOT);
            if (u.startsWith("EXPLAIN ")) {
                s = s.substring(8).stripLeading();
                continue;
            }
            if (u.startsWith("PROFILE ")) {
                s = s.substring(8).stripLeading();
                continue;
            }
            break;
        }
        String u = s.toUpperCase(Locale.ROOT);
        return u.startsWith("MATCH ")
                || u.startsWith("OPTIONAL MATCH ")
                || u.startsWith("RETURN ")
                || u.startsWith("CALL ")
                || u.startsWith("WITH ")
                || u.startsWith("UNWIND ")
                || u.startsWith("SHOW ");
    }
}
