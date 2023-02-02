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

import java.util.Map;
import java.util.Objects;

public class PhysicalCTEConsumeOperator extends PhysicalOperator {
    private final int cteId;

    private final Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap;

    public PhysicalCTEConsumeOperator(int cteId, Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap,
                                      long limit, ScalarOperator predicate, Projection projection) {
        super(OperatorType.PHYSICAL_CTE_CONSUME);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public int getCteId() {
        return cteId;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getCteOutputColumnRefMap() {
        return cteOutputColumnRefMap;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCTEConsume(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEConsume(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalCTEConsumeOperator that = (PhysicalCTEConsumeOperator) o;
        return Objects.equals(cteId, that.cteId) &&
                Objects.equals(cteOutputColumnRefMap, that.cteOutputColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId, cteOutputColumnRefMap);
    }

    @Override
    public String toString() {
        return "PhysicalCTEConsumeOperator{" +
                "cteId='" + cteId + '\'' +
                ", limit=" + limit +
                ", predicate=" + predicate +
                '}';
    }
}
