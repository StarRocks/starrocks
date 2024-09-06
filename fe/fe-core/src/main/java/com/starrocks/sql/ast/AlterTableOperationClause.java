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

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class AlterTableOperationClause extends AlterTableClause {
    private final String tableOperationName;
    private final List<Expr> exprs;
    private List<ConstantOperator> args;

    public AlterTableOperationClause(NodePosition pos, String tableOperationName, List<Expr> exprs) {
        super(AlterOpType.ALTER_TABLE_OPERATION, pos);
        this.tableOperationName = tableOperationName;
        this.exprs = exprs;
    }

    public String getTableOperationName() {
        return tableOperationName;
    }

    public List<Expr> getExprs() {
        return exprs;
    }

    public List<ConstantOperator> getArgs() {
        return args;
    }

    public void setArgs(List<ConstantOperator> args) {
        this.args = args;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableOperationClause(this, context);
    }
}
