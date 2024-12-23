package com.starrocks.sql.analyzer;

public class QueryContext {
    private static final ThreadLocal<QueryContext> contextHolder = new ThreadLocal<>();

    private String catalog;
    private String database;
    private String tableName;

    public QueryContext(String catalog, String database, String tableName) {
        this.catalog = catalog;
        this.database = database;
        this.tableName = tableName;
    }

    public static void setContext(QueryContext context) {
        contextHolder.set(context);
    }

    public static QueryContext getContext() {
        return contextHolder.get();
    }

    public static void clearContext() {
        contextHolder.remove();
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }
}
