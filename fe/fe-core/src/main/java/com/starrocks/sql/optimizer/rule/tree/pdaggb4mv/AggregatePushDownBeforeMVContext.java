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


package com.starrocks.sql.optimizer.rule.tree.pdaggb4mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

class AggregatePushDownBeforeMVContext {
    public static final AggregatePushDownBeforeMVContext EMPTY = new AggregatePushDownBeforeMVContext();

    public LogicalAggregationOperator origAggregator;
    public final Map<ColumnRefOperator, CallOperator> aggregations;
    public final Map<ColumnRefOperator, ScalarOperator> groupBys;

    public boolean hasWindow = false;

    // record push down path
    // the index of children which should push down
    public final List<Integer> pushPaths;

    public AggregatePushDownBeforeMVContext() {
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
