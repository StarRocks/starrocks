// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowAuthenticationStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Password", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("AuthPlugin", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("UserForAuthPlugin", ScalarType.createVarchar(100)));
        META_DATA = builder.build();
    }

    private final boolean isAll;
    private UserIdentity userIdent;

    // SHOW AUTHENTICATION -> (null, false)
    // SHOW ALL AUTHENTICATION -> (null, true)
    // SHOW AUTHENTICATION FOR xx -> (xx, false)
    public ShowAuthenticationStmt(UserIdentity userIdent, boolean isAll) {
        this.userIdent = userIdent;
        this.isAll = isAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public boolean isAll() {
        return isAll;
    }

    public void setUserIdent(UserIdentity userIdent) {
        this.userIdent = userIdent;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowAuthenticationStatement(this, context);
    }
}
