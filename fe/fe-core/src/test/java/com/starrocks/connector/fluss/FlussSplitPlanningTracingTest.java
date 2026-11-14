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

package com.starrocks.connector.fluss;

import com.starrocks.common.profile.Tracers;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.predicate.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FlussSplitPlanningTracingTest {
    private static final String PREFIX = "orders.";

    @BeforeEach
    public void setUp() {
        Tracers.register();
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.EXTERNAL, true, false);
    }

    @AfterEach
    public void tearDown() {
        Tracers.close();
    }

    @Test
    public void testLakePlanIsTracedAndDelegated() throws Exception {
        StubLakeSource stub = new StubLakeSource();
        LakeSource<LakeSplit> traced = FlussSplitPlanningTracing.traceLakePlanning(stub, PREFIX);

        Assertions.assertSame(stub.plannedSplits, traced.createPlanner(null).plan());
        Assertions.assertEquals(1, stub.createPlannerCalls);
        String timers = Tracers.printScopeTimer();
        Assertions.assertTrue(timers.contains(PREFIX + "lakePlan"), timers);
    }

    @Test
    public void testLakeSourceConfigurationIsDelegated() throws Exception {
        StubLakeSource stub = new StubLakeSource();
        LakeSource<LakeSplit> traced = FlussSplitPlanningTracing.traceLakePlanning(stub, PREFIX);

        traced.withProject(new int[][] { {0} });
        traced.withLimit(5);
        traced.withFilters(Collections.emptyList());
        traced.createRecordReader(null);
        traced.getSplitSerializer();

        Assertions.assertEquals(List.of("withProject", "withLimit", "withFilters", "createRecordReader",
                "getSplitSerializer"), stub.calls);
    }

    @Test
    public void testCopyKeepsTracing() throws Exception {
        StubLakeSource stub = new StubLakeSource();
        LakeSource<LakeSplit> copy = FlussSplitPlanningTracing.traceLakePlanning(stub, PREFIX).copy();

        copy.createPlanner(null).plan();
        Assertions.assertTrue(stub.copied);
        Assertions.assertTrue(Tracers.printScopeTimer().contains(PREFIX + "lakePlan"));
    }

    @Test
    public void testNullLakeSourceStaysNull() {
        Assertions.assertNull(FlussSplitPlanningTracing.traceLakePlanning(null, PREFIX));
    }

    @Test
    public void testBucketOffsetsAreTracedAndDelegated() {
        Map<Integer, Long> latest = Map.of(0, 10L);
        Map<Integer, Long> earliest = Map.of(0, 1L);
        Map<Integer, Long> fromTimestamp = Map.of(0, 5L);
        List<String> calls = new ArrayList<>();
        OffsetsInitializer.BucketOffsetsRetriever traced = FlussSplitPlanningTracing.traceBucketOffsets(
                new OffsetsInitializer.BucketOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
                        calls.add("latest:" + partitionName + ":" + buckets);
                        return latest;
                    }

                    @Override
                    public Map<Integer, Long> earliestOffsets(String partitionName, Collection<Integer> buckets) {
                        calls.add("earliest");
                        return earliest;
                    }

                    @Override
                    public Map<Integer, Long> offsetsFromTimestamp(String partitionName, Collection<Integer> buckets,
                                                                   long timestamp) {
                        calls.add("timestamp:" + timestamp);
                        return fromTimestamp;
                    }
                }, PREFIX);

        Assertions.assertSame(latest, traced.latestOffsets(null, List.of(0, 1)));
        Assertions.assertSame(earliest, traced.earliestOffsets(null, List.of(0)));
        Assertions.assertSame(fromTimestamp, traced.offsetsFromTimestamp(null, List.of(0), 42L));
        Assertions.assertEquals(List.of("latest:null:[0, 1]", "earliest", "timestamp:42"), calls);
        String timers = Tracers.printScopeTimer();
        Assertions.assertTrue(timers.contains(PREFIX + "bucketOffsets"), timers);
    }

    private static final class StubLakeSource implements LakeSource<LakeSplit> {
        private final List<LakeSplit> plannedSplits = new ArrayList<>();
        private final List<String> calls = new ArrayList<>();
        private int createPlannerCalls;
        private boolean copied;

        @Override
        public void withProject(int[][] project) {
            calls.add("withProject");
        }

        @Override
        public void withLimit(int limit) {
            calls.add("withLimit");
        }

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            calls.add("withFilters");
            return null;
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            createPlannerCalls++;
            return () -> plannedSplits;
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            calls.add("createRecordReader");
            return null;
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            calls.add("getSplitSerializer");
            return null;
        }

        @Override
        public LakeSource<LakeSplit> copy() {
            copied = true;
            return this;
        }
    }
}
