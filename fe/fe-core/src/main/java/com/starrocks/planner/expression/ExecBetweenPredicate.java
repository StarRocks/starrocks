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

import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.BooleanType;

import java.util.List;

/**
 * Execution-plan BETWEEN predicate.
 * Note: BetweenPredicate should normally be rewritten into a CompoundPredicate
 * before reaching the execution plan. This class exists for completeness.
 */
public class ExecBetweenPredicate extends ExecExpr {
    private final boolean isNotBetween;

    public ExecBetweenPredicate(boolean isNotBetween, List<ExecExpr> children) {
        super(BooleanType.BOOLEAN, children);
        this.isNotBetween = isNotBetween;
    }

    private ExecBetweenPredicate(ExecBetweenPredicate other) {
        super(other.type, other.cloneChildren());
        this.isNotBetween = other.isNotBetween;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean isNotBetween() {
        return isNotBetween;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public void toThrift(TExprNode node) {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecBetweenPredicate(this, context);
    }

    @Override
    public ExecBetweenPredicate clone() {
        return new ExecBetweenPredicate(this);
    }
}
