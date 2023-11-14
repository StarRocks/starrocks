// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.Map;

public class SubmitTaskStmt extends DdlStmt {

    private String dbName;

    private String taskName;

    private Map<String, String> properties;

    private int sqlBeginIndex;

    private String sqlText;

    private CreateTableAsSelectStmt createTableAsSelectStmt;
    private InsertStmt insertStmt;

    public SubmitTaskStmt(String dbName, String taskName, Map<String, String> properties, int sqlBeginIndex,
                          CreateTableAsSelectStmt createTableAsSelectStmt) {
        this.dbName = dbName;
        this.taskName = taskName;
        this.properties = properties;
        this.sqlBeginIndex = sqlBeginIndex;
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    public SubmitTaskStmt(String dbName, String taskName, Map<String, String> properties, int sqlBeginIndex,
                          InsertStmt insertStmt) {
        this.dbName = dbName;
        this.taskName = taskName;
        this.properties = properties;
        this.sqlBeginIndex = sqlBeginIndex;
        this.insertStmt = insertStmt;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getSqlBeginIndex() {
        return sqlBeginIndex;
    }

    public void setSqlBeginIndex(int sqlBeginIndex) {
        this.sqlBeginIndex = sqlBeginIndex;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public CreateTableAsSelectStmt getCreateTableAsSelectStmt() {
        return createTableAsSelectStmt;
    }

    public void setCreateTableAsSelectStmt(CreateTableAsSelectStmt createTableAsSelectStmt) {
        this.createTableAsSelectStmt = createTableAsSelectStmt;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public void setInsertStmt(InsertStmt insertStmt) {
        this.insertStmt = insertStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubmitTaskStatement(this, context);
    }
}
