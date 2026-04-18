package com.hackthon.stanford.neo4j;

/**
 * Neo4j Aura 连接信息（与 Browser 中 Connection URL / Database password 一致）。
 * <p>
 * 按你的要求从配置文件改为源码常量：把下面 {@link #PASSWORD} 换成 Aura 控制台里的真实密码后重新编译运行。
 * 注意：提交到 Git 会泄露凭据，演示后请改密码或勿推送此文件。
 * </p>
 */
public final class Neo4jHardcodedCredentials {

    public static final String URI = "neo4j+s://ebe3519d.databases.neo4j.io";

    public static final String USERNAME = "ebe3519d";

    /** 粘贴 Aura「Database password」；留空则无法认证。 */
    public static final String PASSWORD = "jdAA9789lPbvTL9nhxR4vQhimPWfhcgI6qgES2nJ4aE";

    /**
     * 逻辑库名。Aura 上若报 “database 'neo4j' does not exist”，请留空 {@code ""}，
     * 使用服务器默认库（与多数 Browser 连接行为一致）。
     */
    public static final String DATABASE = "";

    private Neo4jHardcodedCredentials() {}
}
