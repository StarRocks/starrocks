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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

// ADMIN SHOW REPLICA STATUS FROM example_db.example_table;
public class AdminShowReplicaStatusStmt extends ShowStmt {
    private final TableRef tblRef;
    private final Expr where;

    public AdminShowReplicaStatusStmt(TableRef tblRef, Expr where) {
        this(tblRef, where, NodePosition.ZERO);
    }

    public AdminShowReplicaStatusStmt(TableRef tblRef, Expr where, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
        this.where = where;
    }

    public String getDbName() {
        return tblRef.getDbName();
    }

    public String getTblName() {
        return tblRef.getTableName();
    }

    public Expr getWhere() {
        return where;
    }

    public PartitionRef getPartitionRef() {
        return tblRef.getPartitionDef();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAdminShowReplicaStatusStatement(this, context);
    }
}
