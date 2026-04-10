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

public class ReplacePartitionColumnClause extends AlterTableClause {
    private final Expr oldPartitionExpr;
    private final Expr newPartitionExpr;

    public ReplacePartitionColumnClause(Expr oldPartitionExpr, Expr newPartitionExpr, NodePosition pos) {
        super(pos);
        this.oldPartitionExpr = oldPartitionExpr;
        this.newPartitionExpr = newPartitionExpr;
    }

    public Expr getOldPartitionExpr() {
        return oldPartitionExpr;
    }

    public Expr getNewPartitionExpr() {
        return newPartitionExpr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitReplacePartitionColumnClause(this, context);
    }
}
