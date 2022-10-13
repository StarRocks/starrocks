// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

import java.util.Map;

public class DropFileStmt extends DdlStmt {
    public static final String PROP_CATALOG = "globalStateMgr";

    private final String fileName;
    private String dbName;
    private final Map<String, String> properties;

    private String catalogName;

    public DropFileStmt(String fileName, String dbName, Map<String, String> properties) {
        this.fileName = fileName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFileStatement(this, context);
    }
}
