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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;

import java.util.Objects;

public class TableScanDesc {
    private final Table table;
    // there may have multi same tables in the query. so assign it an index to distinguish them
    private final int index;
    private final LogicalScanOperator scanOperator;
    // join type of LogicalJoinOperator above scan operator
<<<<<<< HEAD
    private final OptExpression joinOptExpression;
    private final boolean isLeft;

    public TableScanDesc(Table table, int index,
                         LogicalScanOperator scanOperator, OptExpression joinOptExpression,
=======
    private final JoinOperator parentJoinType;
    private final boolean isLeft;

    public TableScanDesc(Table table, int index,
                         LogicalScanOperator scanOperator, JoinOperator parentJoinType,
>>>>>>> branch-2.5-mrs
                         boolean isLeft) {
        this.table = table;
        this.index = index;
        this.scanOperator = scanOperator;
<<<<<<< HEAD
        this.joinOptExpression = joinOptExpression;
=======
        this.parentJoinType = parentJoinType;
>>>>>>> branch-2.5-mrs
        this.isLeft = isLeft;
    }

    public Table getTable() {
        return table;
    }

    public int getIndex() {
        return index;
    }

    public OptExpression getJoinOptExpression() {
        return joinOptExpression;
    }

    public String getName() {
        return table.getName();
    }

    public LogicalScanOperator getScanOperator() {
        return scanOperator;
    }

<<<<<<< HEAD
    public JoinOperator getJoinType() {
        if (joinOptExpression == null) {
            return null;
        }
        LogicalJoinOperator joinOperator = joinOptExpression.getOp().cast();
        return joinOperator.getJoinType();
    }

=======
>>>>>>> branch-2.5-mrs
    public boolean isMatch(TableScanDesc other) {
        boolean matched =  table.equals(other.table);
        if (!matched) {
            return false;
        }

        // for
        // query: a left join c
        // mv: a inner join b left join c
<<<<<<< HEAD
        JoinOperator joinOperator = getJoinType();
        JoinOperator otherJoinOperator = other.getJoinType();
        if (joinOperator == null && otherJoinOperator == null) {
            return true;
        } else if (joinOperator == null || otherJoinOperator == null) {
            return false;
        }
        if (joinOperator.isInnerJoin()) {
            return otherJoinOperator.isInnerJoin()
                    || (otherJoinOperator.isLeftOuterJoin() && other.isLeft);
=======
        if (parentJoinType.isInnerJoin()) {
            return other.parentJoinType.isInnerJoin() || (other.parentJoinType.isLeftOuterJoin() && other.isLeft);
>>>>>>> branch-2.5-mrs
        }

        // for
        // query: a inner join c
        // mv: a left outer join b inner join c
<<<<<<< HEAD
        if (joinOperator.isLeftOuterJoin()) {
            return (isLeft && otherJoinOperator.isInnerJoin())
                    || (otherJoinOperator.isLeftOuterJoin() && isLeft == other.isLeft);
=======
        if (parentJoinType.isLeftOuterJoin()) {
            return (isLeft && other.parentJoinType.isInnerJoin())
                    || (other.parentJoinType.isLeftOuterJoin() && isLeft == other.isLeft);
>>>>>>> branch-2.5-mrs
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableScanDesc that = (TableScanDesc) o;
        return Objects.equals(table, that.table) && index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, index);
    }
}
