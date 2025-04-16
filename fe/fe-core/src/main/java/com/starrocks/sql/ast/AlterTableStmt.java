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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// Alter table statement.
public class AlterTableStmt extends DdlStmt {
    private final TableName tbl;
    private final List<AlterClause> alterClauseList;

    public AlterTableStmt(TableName tbl, List<AlterClause> ops) {
        this(tbl, ops, NodePosition.ZERO);
    }

    public AlterTableStmt(TableName tbl, List<AlterClause> ops, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.alterClauseList = ops;
    }

    public TableName getTbl() {
        return tbl;
    }

    public List<AlterClause> getAlterClauseList() {
        return alterClauseList;
    }

    public String getCatalogName() {
        return tbl.getCatalog();
    }

    public String getDbName() {
        return tbl.getDb();
    }

    public String getTableName() {
        return tbl.getTbl();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableStatement(this, context);
    }
}
