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

package com.starrocks.sql.automv.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.parser.NodePosition;

public class AlterTunespaceStmt extends DmlStmt {
    private TableName tableName;
    private AlterTunespaceClause alterClause;

    public AlterTunespaceStmt(TableName tableName, AlterTunespaceClause alterClause) {
        super(NodePosition.ZERO);
        this.tableName = tableName;
        this.alterClause = alterClause;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public AlterTunespaceClause getAlterClause() {
        return alterClause;
    }

    public void setAlterClause(AlterTunespaceClause alterClause) {
        this.alterClause = alterClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTunespaceStmt(this, context);
    }
}
