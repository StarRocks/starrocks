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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;

public class PhysicalRepeatOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> outputGrouping;
    private final List<List<ColumnRefOperator>> repeatColumnRef;
    private final List<List<Long>> groupingIds;

    public PhysicalRepeatOperator(List<ColumnRefOperator> outputGrouping, List<List<ColumnRefOperator>> repeatColumnRef,
                                  List<List<Long>> groupingIds, long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_REPEAT);
        this.outputGrouping = outputGrouping;
        this.repeatColumnRef = repeatColumnRef;
        this.groupingIds = groupingIds;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRepeat(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalRepeat(optExpression, context);
    }

    public List<List<ColumnRefOperator>> getRepeatColumnRef() {
        return repeatColumnRef;
    }

    public List<ColumnRefOperator> getOutputGrouping() {
        return outputGrouping;
    }

    public List<List<Long>> getGroupingIds() {
        return groupingIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalRepeatOperator that = (PhysicalRepeatOperator) o;
        return Objects.equals(outputGrouping, that.outputGrouping) &&
                Objects.equals(repeatColumnRef, that.repeatColumnRef) &&
                Objects.equals(groupingIds, that.groupingIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputGrouping, repeatColumnRef);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        outputGrouping.forEach(set::union);
        repeatColumnRef.forEach(s -> s.forEach(set::union));
        return set;
    }
}
