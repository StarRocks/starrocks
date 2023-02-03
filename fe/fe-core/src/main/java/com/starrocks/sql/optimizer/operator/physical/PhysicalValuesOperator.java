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

import com.google.common.base.Objects;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalValuesOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> columnRefSet;
    private final List<List<ScalarOperator>> rows;

    public PhysicalValuesOperator(List<ColumnRefOperator> columnRefSet, List<List<ScalarOperator>> rows,
                                  long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_VALUES);
        this.columnRefSet = columnRefSet;
        this.rows = rows;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public List<List<ScalarOperator>> getRows() {
        return rows;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalValues(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalValues(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalValuesOperator that = (PhysicalValuesOperator) o;
        return Objects.equal(columnRefSet, that.columnRefSet) && Objects.equal(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columnRefSet);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        rows.forEach(r -> r.forEach(s -> set.union(s.getUsedColumns())));
        return set;
    }
}
