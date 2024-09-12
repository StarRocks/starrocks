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

package com.starrocks.common.profile;

import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TUnit;

/**
 * Summarize MIN/MAX/SUM/AVG while merging
 */
public final class SummarizationCounter {

    public final TUnit unit;
    public final TCounterStrategy strategy;
    public long min = Long.MAX_VALUE;
    public long max = Long.MIN_VALUE;
    public long sum = 0L;
    public long cnt = 0L;

    public SummarizationCounter(TUnit unit, TCounterStrategy strategy) {
        this.unit = unit;
        this.strategy = strategy;
    }

    public void merge(TCounter counter, boolean updateSum) {
        if (updateSum) {
            if (Counter.isSkipMerge(strategy)) {
                sum = counter.value;
                cnt = 1;
            } else {
                sum += counter.value;
                cnt++;
            }
        }
        min = Long.min(min, counter.getValue());
        max = Long.max(max, counter.getValue());
    }

    public void finalized(Counter minCounter, Counter maxCounter, Counter mergedCounter) {
        if (cnt == 0) {
            min = 0L;
            max = 0L;
            return;
        }
        long mergedValue = Counter.isAvg(strategy) ? sum / cnt : sum;
        minCounter.setValue(min);
        maxCounter.setValue(max);
        mergedCounter.setValue(mergedValue);
    }

}
