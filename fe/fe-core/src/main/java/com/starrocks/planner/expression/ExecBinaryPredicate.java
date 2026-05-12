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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.expression.ExprOpcodeRegistry;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.type.BooleanType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/**
 * Execution-plan binary predicate (e.g., =, !=, <, >, <=, >=, <=>).
 */
public class ExecBinaryPredicate extends ExecExpr {
    private final BinaryType op;

    public ExecBinaryPredicate(BinaryType op, ExecExpr left, ExecExpr right) {
        super(BooleanType.BOOLEAN, List.of(left, right));
        this.op = op;
    }

    private ExecBinaryPredicate(ExecBinaryPredicate other) {
        super(other.type, other.cloneChildren());
        this.op = other.op;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public BinaryType getOp() {
        return op;
    }

    @Override
    public boolean isNullable() {
        if (op == BinaryType.EQ_FOR_NULL) {
            return false;
        }
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.BINARY_PRED;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOpcode(ExprOpcodeRegistry.getBinaryOpcode(op));
        node.setVector_opcode(TExprOpcode.INVALID_OPCODE);
        Type childType = children.get(0).getType();
        if (childType.isComplexType()) {
            node.setChild_type_desc(TypeSerializer.toThrift(childType));
        } else {
            node.setChild_type(TypeSerializer.toThrift(childType.getPrimitiveType()));
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecBinaryPredicate(this, context);
    }

    @Override
    public ExecBinaryPredicate clone() {
        return new ExecBinaryPredicate(this);
    }
}
