// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.planner.stream.IMTInfo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;

public class PhysicalStreamAggOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> groupBys;
    private final Map<ColumnRefOperator, CallOperator> aggregations;

    // Map from AggExpr and GroupingExpr Index to IMT column Index
    private Map<Integer, Integer> aggExprImtMap;
    private Map<Integer, Integer> groupExprImtMap;

    // IMT information
    private IMTInfo aggImt;

    // TODO: support more IMT
    private IMTInfo detailImt;

    public PhysicalStreamAggOperator(List<ColumnRefOperator> groupBys,
            Map<ColumnRefOperator, CallOperator> aggregations, ScalarOperator predicate, Projection projection) {
        super(OperatorType.PHYSICAL_STREAM_AGG);
        this.aggregations = aggregations;
        this.groupBys = groupBys;
    }

    public List<ColumnRefOperator> getGroupBys() { return groupBys; }

    public Map<ColumnRefOperator, CallOperator> getAggregations() { return aggregations; }

    public IMTInfo getAggImt() { return this.aggImt; }

    public Map<Integer, Integer> getAggExprImtMap() { return this.aggExprImtMap; }

    public Map<Integer, Integer> getGroupExprImtMap() { return this.groupExprImtMap; }

    public void setAggImt(IMTInfo imt) {
        this.aggImt = imt;

        // TODO: assign it when creating IMT
        this.aggExprImtMap = new HashMap<>();
        this.groupExprImtMap = new HashMap<>();
        int columnIdx = 0;
        for (int i = 0; i < groupBys.size(); i++) {
            this.groupExprImtMap.put(i, columnIdx++);
        }
        for (int i = 0; i < aggregations.size(); i++) {
            this.aggExprImtMap.put(i, columnIdx++);
        }
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStreamAgg(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalStreamAgg(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet columns = super.getUsedColumns();
        groupBys.forEach(columns::union);
        aggregations.values().forEach(d -> columns.union(d.getUsedColumns()));
        return columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(aggregations.values().stream().map(CallOperator::toString).collect(Collectors.joining(", ")));
        if (CollectionUtils.isNotEmpty(groupBys)) {
            sb.append(" group by ");
            sb.append(groupBys.stream().map(ColumnRefOperator::getName).collect(Collectors.joining(", ")));
        }
        return "PhysicalStreamAgg " + sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupBys, aggregations.keySet());
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
        PhysicalStreamAggOperator that = (PhysicalStreamAggOperator) o;
        return Objects.equals(aggregations, that.aggregations) && Objects.equals(groupBys, that.groupBys);
    }
}
