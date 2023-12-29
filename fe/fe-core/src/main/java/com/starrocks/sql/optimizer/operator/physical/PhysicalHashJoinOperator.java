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

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class PhysicalHashJoinOperator extends PhysicalJoinOperator {
    public PhysicalHashJoinOperator(JoinOperator joinType,
                                    ScalarOperator onPredicate,
                                    String joinHint,
                                    long limit,
                                    ScalarOperator predicate,
                                    Projection projection) {
        super(OperatorType.PHYSICAL_HASH_JOIN, joinType, onPredicate, joinHint, limit, predicate, projection);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalHashJoin(optExpression, context);
    }

    @Override
    public String toString() {
        return "PhysicalHashJoinOperator{" +
                "joinType=" + joinType +
                ", joinPredicate=" + onPredicate +
                ", limit=" + limit +
                ", predicate=" + predicate +
                '}';
    }

    @Override
    public String getJoinAlgo() {
        return "HASH";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalHashJoinOperator that = (PhysicalHashJoinOperator) o;
        return joinType == that.joinType && Objects.equals(onPredicate, that.onPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, onPredicate);
    }

}
