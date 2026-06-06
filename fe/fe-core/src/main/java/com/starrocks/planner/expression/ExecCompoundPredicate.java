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

import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.BooleanType;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution-plan compound predicate (AND, OR, NOT).
 */
public class ExecCompoundPredicate extends ExecExpr {
    private final CompoundPredicate.Operator compoundType;

    public ExecCompoundPredicate(CompoundPredicate.Operator compoundType, List<ExecExpr> children) {
        super(BooleanType.BOOLEAN, children);
        this.compoundType = compoundType;
    }

    public ExecCompoundPredicate(CompoundPredicate.Operator compoundType, ExecExpr left, ExecExpr right) {
        super(BooleanType.BOOLEAN, new ArrayList<>(List.of(left, right)));
        this.compoundType = compoundType;
    }

    private ExecCompoundPredicate(ExecCompoundPredicate other) {
        super(other.type, other.cloneChildren());
        this.compoundType = other.compoundType;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public CompoundPredicate.Operator getCompoundType() {
        return compoundType;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.COMPOUND_PRED;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOpcode(ThriftEnumConverter.compoundPredicateOperatorToThrift(compoundType));
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecCompoundPredicate(this, context);
    }

    @Override
    public ExecCompoundPredicate clone() {
        return new ExecCompoundPredicate(this);
    }
}
