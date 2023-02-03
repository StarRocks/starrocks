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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalSetOperation extends PhysicalOperator {
    protected List<ColumnRefOperator> outputColumnRefOp;
    protected List<List<ColumnRefOperator>> childOutputColumns;

    public PhysicalSetOperation(OperatorType type, List<ColumnRefOperator> outputColumnRefOp,
                                List<List<ColumnRefOperator>> childOutputColumns,
                                long limit,
                                ScalarOperator predicate,
                                Projection projection) {
        super(type);
        this.outputColumnRefOp = outputColumnRefOp;
        this.childOutputColumns = childOutputColumns;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getOutputColumnRefOp() {
        return outputColumnRefOp;
    }

    public List<List<ColumnRefOperator>> getChildOutputColumns() {
        return childOutputColumns;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        childOutputColumns.forEach(l -> l.forEach(set::union));
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSetOperation that = (PhysicalSetOperation) o;
        return Objects.equal(outputColumnRefOp, that.outputColumnRefOp) &&
                Objects.equal(childOutputColumns, that.childOutputColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), outputColumnRefOp);
    }
}
