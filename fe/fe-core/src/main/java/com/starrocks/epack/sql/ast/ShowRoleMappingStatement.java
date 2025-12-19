// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.TypeFactory;

public class ShowRoleMappingStatement extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", TypeFactory.createVarcharType(50)));
        builder.addColumn(new Column("IntegrationName", TypeFactory.createVarcharType(50)));
        builder.addColumn(new Column("Role", TypeFactory.createVarcharType(50)));
        builder.addColumn(new Column("LdapGroupList", TypeFactory.createVarcharType(300)));
        builder.addColumn(new Column("LastRefreshCompleteTime", TypeFactory.createVarcharType(50)));


        META_DATA = builder.build();
    }

    public ShowRoleMappingStatement() {
        this(NodePosition.ZERO);
    }

    public ShowRoleMappingStatement(NodePosition pos) {
        super(pos);
    }



    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitShowRoleMappingStatement(this, context);
        } else {
            return null;
        }
    }
}
