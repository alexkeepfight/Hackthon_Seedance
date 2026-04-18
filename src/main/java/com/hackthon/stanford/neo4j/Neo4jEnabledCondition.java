package com.hackthon.stanford.neo4j;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

/**
 * Only enable Neo4j {@link org.neo4j.driver.Driver} when {@link Neo4jHardcodedCredentials#URI} is non-blank.
 */
public class Neo4jEnabledCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String uri = Neo4jHardcodedCredentials.URI;
        return StringUtils.hasText(uri == null ? null : uri.trim());
    }
}
