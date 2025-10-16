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

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TableRefPersist;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// ADMIN SHOW REPLICA STATUS FROM example_db.example_table;
public class AdminShowReplicaStatusStmt extends ShowStmt {
    private final TableRefPersist tblRef;
    private final Expr where;
    private List<String> partitions = Lists.newArrayList();

    public AdminShowReplicaStatusStmt(TableRefPersist tblRef, Expr where) {
        this(tblRef, where, NodePosition.ZERO);
    }

    public AdminShowReplicaStatusStmt(TableRefPersist tblRef, Expr where, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
        this.where = where;
    }

    public TableRefPersist getTblRef() {
        return tblRef;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAdminShowReplicaStatusStatement(this, context);
    }
}
