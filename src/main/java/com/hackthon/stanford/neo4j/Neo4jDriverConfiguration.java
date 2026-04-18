package com.hackthon.stanford.neo4j;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
@Conditional(Neo4jEnabledCondition.class)
public class Neo4jDriverConfiguration {

    @Bean(destroyMethod = "close")
    public Driver neo4jDriver() {
        String uri = Neo4jHardcodedCredentials.URI == null ? "" : Neo4jHardcodedCredentials.URI.trim();
        if (!StringUtils.hasText(uri)) {
            throw new IllegalStateException("Neo4jHardcodedCredentials.URI must be non-blank");
        }
        String user = Neo4jHardcodedCredentials.USERNAME == null ? "" : Neo4jHardcodedCredentials.USERNAME.trim();
        String pass = Neo4jHardcodedCredentials.PASSWORD == null ? "" : Neo4jHardcodedCredentials.PASSWORD.trim();
        return GraphDatabase.driver(
                uri,
                AuthTokens.basic(user, pass),
                Config.defaultConfig()
        );
    }
}
