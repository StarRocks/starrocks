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

import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.expression.ExprOpcodeRegistry;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan arithmetic expression (+, -, *, /, %, etc.).
 */
public class ExecArithmetic extends ExecExpr {
    private final ArithmeticExpr.Operator op;

    public ExecArithmetic(Type type, ArithmeticExpr.Operator op, List<ExecExpr> children) {
        super(type, children);
        this.op = op;
    }

    private ExecArithmetic(ExecArithmetic other) {
        super(other.type, other.cloneChildren());
        this.op = other.op;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public ArithmeticExpr.Operator getOp() {
        return op;
    }

    @Override
    public boolean isNullable() {
        if (op == ArithmeticExpr.Operator.DIVIDE || op == ArithmeticExpr.Operator.INT_DIVIDE
                || op == ArithmeticExpr.Operator.MOD) {
            return true;
        }
        for (ExecExpr child : children) {
            if (child.isNullable() || child.getType().isDecimalV3()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.ARITHMETIC_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOpcode(ExprOpcodeRegistry.getArithmeticOpcode(op));
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecArithmetic(this, context);
    }

    @Override
    public ExecArithmetic clone() {
        return new ExecArithmetic(this);
    }
}
