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

package com.starrocks.metric;

import java.util.concurrent.atomic.LongAdder;

/**
 * LeaderAwareCounterMetricLong represents a counter metric that is aware of the current fe's role (leader or follower)
 * and internally maintains a long counter value.
 */
public class LeaderAwareCounterMetricLong extends LeaderAwareCounterMetric<Long> {

    public LeaderAwareCounterMetricLong(String name, MetricUnit unit, String description) {
        super(name, unit, description);
    }

    // LongAdder is used for purposes such as collecting statistics, not for fine-grained synchronization control.
    // Under high contention, expected throughput of LongAdder is significantly higher than AtomicLong
    private LongAdder value = new LongAdder();

    @Override
    public void increase(Long delta) {
        value.add(delta);
    }

    @Override
    public Long getValueLeader() {
        return value.longValue();
    }

    @Override
    public Long getValueNonLeader() {
        return 0L;
    }
}
