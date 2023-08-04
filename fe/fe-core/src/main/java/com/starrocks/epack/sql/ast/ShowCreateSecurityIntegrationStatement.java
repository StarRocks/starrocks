// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowCreateSecurityIntegrationStatement extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Security Integration", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Create Security Integration", ScalarType.createVarchar(500)))
                    .build();

    private String name;

    public ShowCreateSecurityIntegrationStatement(String name) {
        this(name, NodePosition.ZERO);
    }

    public ShowCreateSecurityIntegrationStatement(String name, NodePosition pos) {
        super(pos);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateSecurityIntegrationStatement(this, context);
    }
}
