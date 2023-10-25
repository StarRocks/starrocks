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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class PhysicalJoinOperator extends PhysicalOperator {
    protected final JoinOperator joinType;
    protected final ScalarOperator onPredicate;
    protected final String joinHint;
    protected boolean canShuffleOutput = false;

    protected PhysicalJoinOperator(OperatorType operatorType, JoinOperator joinType,
                                   ScalarOperator onPredicate,
                                   String joinHint,
                                   long limit,
                                   ScalarOperator predicate,
                                   Projection projection) {
        super(operatorType);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        this.joinHint = joinHint;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public String getJoinAlgo() {
        return "PhysicalJoin";
    }

    public ScalarOperator getOnPredicate() {
        return onPredicate;
    }

    public String getJoinHint() {
        return joinHint;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        if (onPredicate != null) {
            refs.union(onPredicate.getUsedColumns());
        }
        return refs;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (OptExpression input : inputs) {
            for (ColumnOutputInfo entry : input.getRowOutputInfo().getColumnOutputInfo()) {
                entryList.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
            }
        }
        return new RowOutputInfo(entryList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalJoinOperator that = (PhysicalJoinOperator) o;
        return joinType == that.joinType && Objects.equals(onPredicate, that.onPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, onPredicate);
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = ColumnRefSet.createByIds(childDictColumns);

        if (predicate != null && predicate.getUsedColumns().isIntersect(dictSet)) {
            return false;
        }

        if (onPredicate != null && onPredicate.getUsedColumns().isIntersect(dictSet)) {
            return false;
        }

        return true;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet) {
        if (predicate != null) {
            columnRefSet.union(predicate.getUsedColumns());
        }

        if (onPredicate != null) {
            columnRefSet.union(onPredicate.getUsedColumns());
        }
    }

    public boolean getCanShuffleOutput() {
        return canShuffleOutput;
    }

    public void setCanShuffleOutput(boolean v) {
        canShuffleOutput = v;
    }
}
