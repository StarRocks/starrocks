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

// Alter view statement
public class AlterViewStmt extends DdlStmt {
    private final TableName tableName;
    private final AlterClause alterClause;

    public AlterViewStmt(TableName tableName, AlterClause alterClause, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.alterClause = alterClause;
    }

    public static AlterViewStmt fromReplaceStmt(CreateViewStmt stmt) {
        AlterViewClause alterViewClause = new AlterViewClause(
                stmt.getColWithComments(), stmt.getQueryStatement(), NodePosition.ZERO);
        alterViewClause.setInlineViewDef(stmt.getInlineViewDef());
        alterViewClause.setColumns(stmt.getColumns());
        alterViewClause.setComment(stmt.getComment());
        return new AlterViewStmt(stmt.getTableName(), alterViewClause, NodePosition.ZERO);
    }

    public TableName getTableName() {
        return tableName;
    }

    public AlterClause getAlterClause() {
        return alterClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterViewStatement(this, context);
    }
}
