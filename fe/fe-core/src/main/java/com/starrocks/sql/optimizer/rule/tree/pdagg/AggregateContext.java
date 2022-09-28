// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

class AggregateContext {
    public static final AggregateContext EMPTY = new AggregateContext();

    public LogicalAggregationOperator origAggregator;
    public final Map<ColumnRefOperator, CallOperator> aggregations;
    public final Map<ColumnRefOperator, ScalarOperator> groupBys;

    // record push down path
    // key: the tree level of multi-child node
    // value: the index of children which should push down
    public final List<Integer> pushPaths;

    public AggregateContext() {
        origAggregator = null;
        aggregations = Maps.newHashMap();
        groupBys = Maps.newHashMap();
        pushPaths = Lists.newArrayList();
    }

    public void setAggregator(LogicalAggregationOperator aggregator) {
        this.origAggregator = aggregator;
        this.aggregations.putAll(aggregator.getAggregations());
        aggregator.getGroupingKeys().forEach(c -> groupBys.put(c, c));
        this.pushPaths.clear();
    }

    public boolean isEmpty() {
        return origAggregator == null;
    }
}
