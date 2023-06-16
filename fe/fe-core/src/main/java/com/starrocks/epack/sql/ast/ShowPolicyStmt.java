// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowPolicyStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;
    private String catalog;
    private String dbName;
    private final PolicyType policyType;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Catalog", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Database", ScalarType.createVarchar(100)));
        META_DATA = builder.build();
    }

    public ShowPolicyStmt(String catalog, String dbName, PolicyType policyType, NodePosition pos) {
        super(pos);
        this.catalog = catalog;
        this.dbName = dbName;
        this.policyType = policyType;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPolicyStatement(this, context);
    }
}
