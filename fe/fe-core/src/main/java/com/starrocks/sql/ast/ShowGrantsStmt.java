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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;

/*
 *  SHOW ALL GRANTS;
 *      show all grants.
 *
 *  SHOW GRANTS:
 *      show grants of current user
 *
 *  SHOW GRANTS FOR user@'xxx';
 *      show grants for specified user identity
 */
//
// SHOW GRANTS;
// SHOW GRANTS FOR user@'xxx'
public class ShowGrantsStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Grants", ScalarType.createVarchar(400)));
        META_DATA = builder.build();
    }

    private static final ShowResultSetMetaData META_DATA_V2;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Catalog", ScalarType.createVarchar(400)));
        builder.addColumn(new Column("Grants", ScalarType.createVarchar(400)));

        META_DATA_V2 = builder.build();
    }

    private UserIdentity userIdent;
    private final String role;

    public ShowGrantsStmt(UserIdentity userIdent) {
        this(userIdent, null, NodePosition.ZERO);
    }

    public ShowGrantsStmt(String role) {
        this(null, role, NodePosition.ZERO);
    }

    public ShowGrantsStmt(UserIdentity userIdent, String role, NodePosition pos) {
        super(pos);
        this.userIdent = userIdent;
        this.role = role;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getRole() {
        return role;
    }

    public void setUserIdent(UserIdentity userIdent) {
        this.userIdent = userIdent;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            return META_DATA_V2;
        } else {
            return META_DATA;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowGrantsStatement(this, context);
    }
}
