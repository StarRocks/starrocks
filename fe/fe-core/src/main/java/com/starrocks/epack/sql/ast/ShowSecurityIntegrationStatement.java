// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowSecurityIntegrationStatement extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Comment", ScalarType.createVarchar(300)));

        META_DATA = builder.build();
    }

    public ShowSecurityIntegrationStatement() {
        this(NodePosition.ZERO);
    }

    public ShowSecurityIntegrationStatement(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSecurityIntegrationStatement(this, context);
    }

}
