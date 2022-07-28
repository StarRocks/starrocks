// This file is made available under Elastic License 2.0.
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

import com.starrocks.thrift.TUnit;

import java.util.List;

// Counter means indicators field. The counter's name is key, the counter itself is value.  
public class Counter {
    private volatile long value;
    private volatile int type;

    public long getValue() {
        return value;
    }

    public void setValue(long newValue) {
        value = newValue;
    }

    public TUnit getType() {
        return TUnit.findByValue(type);
    }

    public void setType(TUnit type) {
        this.type = type.getValue();
    }

    public Counter(TUnit type, long value) {
        this.value = value;
        this.type = type.getValue();
    }

    public static boolean isAverageType(TUnit type) {
        return TUnit.CPU_TICKS == type
                || TUnit.TIME_NS == type
                || TUnit.TIME_MS == type
                || TUnit.TIME_S == type;
    }

    /**
     * Merge all the isomorphic counters
     * The exact semantics of merge depends on TUnit
     */
    public static MergedInfo mergeIsomorphicCounters(TUnit type, List<Counter> counters) {
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

        if (isAverageType(type)) {
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
}
