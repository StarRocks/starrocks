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

import com.starrocks.sql.common.UnsupportedException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * LeaderAwareCounterMetricExternalLong represents a counter metric that is aware of the current fe's role (leader or follower)
 * and whose value is maintained by an external `AtomicLong`.
 */
public class LeaderAwareCounterMetricExternalLong extends LeaderAwareCounterMetric<Long> {
    private final AtomicLong counter;

    public LeaderAwareCounterMetricExternalLong(String name, MetricUnit unit, String description, AtomicLong counter) {
        super(name, unit, description);
        this.counter = counter;
    }

    @Override
    public void increase(Long delta) {
        throw UnsupportedException.unsupportedException("Not support increase() because it uses external counter");
    }

    @Override
    public Long getValueLeader() {
        return counter.longValue();
    }

    @Override
    public Long getValueNonLeader() {
        return 0L;
    }
}
