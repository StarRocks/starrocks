// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowRoleMappingStatement extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ScalarType.createVarchar(50)));
        builder.addColumn(new Column("IntegrationName", ScalarType.createVarchar(50)));
        builder.addColumn(new Column("Role", ScalarType.createVarchar(50)));
        builder.addColumn(new Column("LdapGroupList", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("LastRefreshCompleteTime", ScalarType.createVarchar(50)));


        META_DATA = builder.build();
    }

    public ShowRoleMappingStatement() {
        this(NodePosition.ZERO);
    }

    public ShowRoleMappingStatement(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRoleMappingStatement(this, context);
    }

}
