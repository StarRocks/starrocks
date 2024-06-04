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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalMergeOperator extends PhysicalSetOperation {
    public enum LocalExchangeType {
        PASS_THROUGH,
        DIRECT
    }

    private final LocalExchangeType localExchangeType;

    public PhysicalMergeOperator(List<ColumnRefOperator> columnRef, List<List<ColumnRefOperator>> childOutputColumns,
                                 LocalExchangeType localExchangeType, long limit) {
        super(OperatorType.PHYSICAL_MERGE, columnRef, childOutputColumns, limit, null, null);
        this.localExchangeType = localExchangeType;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalMergeOperator that = (PhysicalMergeOperator) o;
        return localExchangeType == that.localExchangeType;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalMerge(optExpression, context);
    }
}

