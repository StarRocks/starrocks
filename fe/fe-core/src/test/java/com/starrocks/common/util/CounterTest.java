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

package com.starrocks.common.util;

import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CounterTest {
    private static Counter.MergedInfo merge(TCounterStrategy strategy, long first, long second) {
        return Counter.mergeIsomorphicCounters(List.of(
                new Counter(TUnit.UNIT, strategy, first), new Counter(TUnit.UNIT, strategy, second)));
    }

    @Test
    void testSaturatingSumBounds() {
        TCounterStrategy strategy = Counter.createStrategy(TCounterAggregateType.SUM).setSaturating_sum(true);
        Counter.MergedInfo positive = merge(strategy, Long.MAX_VALUE, 1);
        Assertions.assertEquals(Long.MAX_VALUE, positive.mergedValue);
        Assertions.assertEquals(1, positive.minValue);
        Assertions.assertEquals(Long.MAX_VALUE, positive.maxValue);
        Counter.MergedInfo negative = merge(strategy, Long.MIN_VALUE, -1);
        Assertions.assertEquals(Long.MIN_VALUE, negative.mergedValue);
        Assertions.assertEquals(Long.MIN_VALUE, negative.minValue);
        Assertions.assertEquals(-1, negative.maxValue);
        Assertions.assertEquals(-1, merge(strategy, Long.MAX_VALUE, Long.MIN_VALUE).mergedValue);
        Assertions.assertEquals(30, merge(strategy, 10, 20).mergedValue);
    }

    @Test
    void testLegacyAndAveragePhasesAreUnchanged() {
        TCounterStrategy legacy = Counter.createStrategy(TCounterAggregateType.SUM);
        Assertions.assertFalse(legacy.isSetSaturating_sum());
        Assertions.assertEquals(-2, merge(legacy, Long.MAX_VALUE, Long.MAX_VALUE).mergedValue);
        Assertions.assertEquals(-2, merge(legacy.setSaturating_sum(false),
                Long.MAX_VALUE, Long.MAX_VALUE).mergedValue);
        for (TCounterAggregateType type : TCounterAggregateType.values()) {
            TCounterStrategy strategy = Counter.createStrategy(type).setSaturating_sum(true);
            // FE sums SUM/AVG_SUM and averages AVG/SUM_AVG.
            boolean sum = type == TCounterAggregateType.SUM || type == TCounterAggregateType.AVG_SUM;
            Assertions.assertEquals(sum ? Long.MAX_VALUE : -1,
                    merge(strategy, Long.MAX_VALUE, Long.MAX_VALUE).mergedValue);
            Assertions.assertEquals(sum ? 30 : 15, merge(strategy, 10, 20).mergedValue);
        }
    }

    @Test
    void testSaturatingStrategySurvivesProfileCopyWireAndMerge() throws Exception {
        RuntimeProfile first = new RuntimeProfile("profile");
        TCounterStrategy strategy = Counter.createStrategy(TCounterAggregateType.SUM).setSaturating_sum(true);
        first.addCounter("count", TUnit.UNIT, strategy).setValue(Long.MAX_VALUE);
        RuntimeProfile copied = new RuntimeProfile("profile");
        copied.copyAllCountersFrom(first);
        copied.addInfoString("source", "test");
        Assertions.assertTrue(copied.getCounter("count").getStrategy().isSaturating_sum());

        TRuntimeProfileTree wire = new TRuntimeProfileTree();
        new TDeserializer().deserialize(wire, new TSerializer().serialize(copied.toThrift()));
        RuntimeProfile received = new RuntimeProfile("profile");
        received.update(wire);
        Assertions.assertTrue(received.getCounter("count").getStrategy().isSetSaturating_sum());
        Assertions.assertTrue(received.getCounter("count").getStrategy().isSaturating_sum());
        RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(List.of(first, received), null);
        Assertions.assertEquals(Long.MAX_VALUE, merged.getCounter("count").getValue());
        Assertions.assertTrue(merged.getCounter("count").getStrategy().isSaturating_sum());
        RuntimeProfile next = RuntimeProfile.mergeIsomorphicProfiles(List.of(merged, copied), null);
        Assertions.assertEquals(Long.MAX_VALUE, next.getCounter("count").getValue());
    }
}
