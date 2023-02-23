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

import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.starrocks.sql.optimizer.operator.OperatorType.PHYSICAL_WINDOW;

public class PhysicalWindowOperator extends PhysicalOperator {
    private final Map<ColumnRefOperator, CallOperator> analyticCall;
    private final List<ScalarOperator> partitionExpressions;
    private final List<Ordering> orderByElements;
    private final AnalyticWindow analyticWindow;
    private final List<Ordering> enforceOrderBy;
    private final boolean useHashBasedPartition;

    public PhysicalWindowOperator(Map<ColumnRefOperator, CallOperator> analyticCall,
                                  List<ScalarOperator> partitionExpressions,
                                  List<Ordering> orderByElements,
                                  AnalyticWindow analyticWindow,
                                  List<Ordering> enforceOrderBy,
                                  boolean useHashBasedPartition,
                                  long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(PHYSICAL_WINDOW);
        this.analyticCall = analyticCall;
        this.partitionExpressions = partitionExpressions;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.enforceOrderBy = enforceOrderBy;
        this.useHashBasedPartition = useHashBasedPartition;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public Map<ColumnRefOperator, CallOperator> getAnalyticCall() {
        return analyticCall;
    }

    public List<ScalarOperator> getPartitionExpressions() {
        return partitionExpressions;
    }

    public List<Ordering> getOrderByElements() {
        return orderByElements;
    }

    public AnalyticWindow getAnalyticWindow() {
        return analyticWindow;
    }

    public List<Ordering> getEnforceOrderBy() {
        return enforceOrderBy;
    }

    public boolean isUseHashBasedPartition() {
        return useHashBasedPartition;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAnalytic(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalAnalytic(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalWindowOperator that = (PhysicalWindowOperator) o;
        return Objects.equals(analyticCall, that.analyticCall) &&
                Objects.equals(partitionExpressions, that.partitionExpressions) &&
                Objects.equals(orderByElements, that.orderByElements) &&
                Objects.equals(analyticWindow, that.analyticWindow) &&
                Objects.equals(useHashBasedPartition, that.useHashBasedPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), analyticCall, partitionExpressions, orderByElements, analyticWindow,
                useHashBasedPartition);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        analyticCall.values().forEach(d -> set.union(d.getUsedColumns()));
        partitionExpressions.forEach(d -> set.union(d.getUsedColumns()));
        orderByElements.forEach(o -> set.union(o.getColumnRef()));
        return set;
    }
}
