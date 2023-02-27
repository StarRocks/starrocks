// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

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
        this(userIdent, isAll, NodePosition.ZERO);
    }

    public ShowAuthenticationStmt(UserIdentity userIdent, boolean isAll, NodePosition pos) {
        super(pos);
        this.userIdent = userIdent;
        this.isAll = isAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
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
