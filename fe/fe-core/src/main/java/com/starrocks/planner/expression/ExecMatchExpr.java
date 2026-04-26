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

package com.starrocks.planner.expression;

import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.expression.ExprOpcodeRegistry;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.BooleanType;

import java.util.List;

/**
 * Execution-plan MATCH expression (full-text search).
 */
public class ExecMatchExpr extends ExecExpr {
    private final MatchExpr.MatchOperator matchOp;

    public ExecMatchExpr(MatchExpr.MatchOperator matchOp, List<ExecExpr> children) {
        super(BooleanType.BOOLEAN, children);
        this.matchOp = matchOp;
    }

    private ExecMatchExpr(ExecMatchExpr other) {
        super(other.type, other.cloneChildren());
        this.matchOp = other.matchOp;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public MatchExpr.MatchOperator getMatchOp() {
        return matchOp;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.MATCH_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOpcode(ExprOpcodeRegistry.getMatchOpcode(matchOp));
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecMatchExpr(this, context);
    }

    @Override
    public ExecMatchExpr clone() {
        return new ExecMatchExpr(this);
    }
}
