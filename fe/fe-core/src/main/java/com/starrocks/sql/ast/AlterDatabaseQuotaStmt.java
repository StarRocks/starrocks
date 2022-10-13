// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class AlterDatabaseQuotaStmt extends DdlStmt {
    private String dbName;
    private final QuotaType quotaType;
    private final String quotaValue;
    private long quota;

    public enum QuotaType {
        NONE,
        DATA,
        REPLICA
    }

    public AlterDatabaseQuotaStmt(String dbName, QuotaType quotaType, String quotaValue) {
        this.dbName = dbName;
        this.quotaType = quotaType;
        this.quotaValue = quotaValue;
    }

    public String getDbName() {
        return dbName;
    }

    public long getQuota() {
        return quota;
    }

    public String getQuotaValue() {
        return quotaValue;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setQuota(long quota) {
        this.quota = quota;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterDatabaseQuotaStatement(this, context);
    }
}
