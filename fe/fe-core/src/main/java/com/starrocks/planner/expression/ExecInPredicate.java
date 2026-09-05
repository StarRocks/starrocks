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

import com.starrocks.sql.expression.ExprOpcodeRegistry;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TInPredicate;
import com.starrocks.type.BooleanType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/**
 * Execution-plan IN predicate (e.g., col IN (1, 2, 3)).
 */
public class ExecInPredicate extends ExecExpr {
    private final boolean isNotIn;

    public ExecInPredicate(boolean isNotIn, List<ExecExpr> children) {
        super(BooleanType.BOOLEAN, children);
        this.isNotIn = isNotIn;
    }

    private ExecInPredicate(ExecInPredicate other) {
        super(other.type, other.cloneChildren());
        this.isNotIn = other.isNotIn;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.IN_PRED;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.in_predicate = new TInPredicate(isNotIn);
        node.setOpcode(ExprOpcodeRegistry.getInPredicateOpcode(isNotIn));
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
        return visitor.visitExecInPredicate(this, context);
    }

    @Override
    public ExecInPredicate clone() {
        return new ExecInPredicate(this);
    }
}
