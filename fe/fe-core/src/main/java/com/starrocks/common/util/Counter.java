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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/Counter.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TCounterMergeType;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TUnit;

import java.util.List;
import java.util.Objects;

// Counter means indicators field. The counter's name is key, the counter itself is value.  
public class Counter {
    private volatile int type;
    private volatile TCounterStrategy strategy;
    private volatile long value;

    public long getValue() {
        return value;
    }

    public void setValue(long newValue) {
        value = newValue;
    }

    public void update(long increment) {
        value += increment;
    }

    public TUnit getType() {
        return TUnit.findByValue(type);
    }

    public void setType(TUnit type) {
        this.type = type.getValue();
    }

    public boolean isSum() {
        return Objects.equals(strategy.aggregate_type, TCounterAggregateType.SUM);
    }

    public boolean isAvg() {
        return Objects.equals(strategy.aggregate_type, TCounterAggregateType.AVG);
    }

    public boolean isSkipMerge() {
        return Objects.equals(strategy.merge_type, TCounterMergeType.SKIP_ALL)
                || Objects.equals(strategy.merge_type, TCounterMergeType.SKIP_SECOND_MERGE);
    }

    public void setStrategy(TCounterStrategy strategy) {
        this.strategy = strategy;
    }

    public TCounterStrategy getStrategy() {
        return this.strategy;
    }

    public Counter(TUnit type, TCounterStrategy strategy, long value) {
        this.type = type.getValue();
        if (strategy == null || strategy.aggregate_type == null || strategy.merge_type == null) {
            this.strategy = Counter.createStrategy(type);
        } else {
            this.strategy = strategy;
        }
        this.value = value;
    }

    public static boolean isTimeType(TUnit type) {
        return TUnit.CPU_TICKS == type
                || TUnit.TIME_NS == type
                || TUnit.TIME_MS == type
                || TUnit.TIME_S == type;
    }

    public static TCounterStrategy createStrategy(TUnit type) {
        TCounterStrategy strategy = new TCounterStrategy();
        TCounterAggregateType aggregateType = isTimeType(type) ? TCounterAggregateType.AVG : TCounterAggregateType.SUM;
        TCounterMergeType mergeType = TCounterMergeType.MERGE_ALL;
        strategy.aggregate_type = aggregateType;
        strategy.merge_type = mergeType;
        return strategy;
    }

    /**
     * Merge all the isomorphic counters
     * The exact semantics of merge depends on TUnit
     */
    public static MergedInfo mergeIsomorphicCounters(List<Counter> counters) {
        long mergedValue = 0;
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;

        for (Counter counter : counters) {
            if (counter.getValue() < minValue) {
                minValue = counter.getValue();
            }

            if (counter.getValue() > maxValue) {
                maxValue = counter.getValue();
            }

            mergedValue += counter.getValue();
        }

        if (counters.get(0).isAvg()) {
            mergedValue /= counters.size();
        }

        return new MergedInfo(mergedValue, minValue, maxValue);
    }

    public static final class MergedInfo {
        public final long mergedValue;
        public final long minValue;
        public final long maxValue;

        public MergedInfo(long mergedValue, long minValue, long maxValue) {
            this.mergedValue = mergedValue;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }
    }

    @Override
    public String toString() {
        return "Counter{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }
}
