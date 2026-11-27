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

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.predicate.Predicate;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * Wraps the collaborators handed to Fluss {@code LakeSplitGenerator} so its remote work shows up as Planner
 * scopes: the Paimon scan plan and the per-bucket offset lookups. The generator's remaining remote call,
 * {@code Admin.getReadableLakeSnapshot}, goes through the shared admin and is the residual of the
 * enclosing generateHybridLakeFlussSplits scope.
 */
final class FlussSplitPlanningTracing {

    private FlussSplitPlanningTracing() {
    }

    static <S extends LakeSplit> LakeSource<S> traceLakePlanning(LakeSource<S> delegate, String tracePrefix) {
        return delegate == null ? null : new TracingLakeSource<>(delegate, tracePrefix);
    }

    static OffsetsInitializer.BucketOffsetsRetriever traceBucketOffsets(
            OffsetsInitializer.BucketOffsetsRetriever delegate, String tracePrefix) {
        return new TracingBucketOffsetsRetriever(delegate, tracePrefix);
    }

    private static final class TracingLakeSource<S extends LakeSplit> implements LakeSource<S> {
        private static final long serialVersionUID = 1L;

        private final LakeSource<S> delegate;
        private final String tracePrefix;

        private TracingLakeSource(LakeSource<S> delegate, String tracePrefix) {
            this.delegate = delegate;
            this.tracePrefix = tracePrefix;
        }

        @Override
        public void withProject(int[][] project) {
            delegate.withProject(project);
        }

        @Override
        public void withLimit(int limit) {
            delegate.withLimit(limit);
        }

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            return delegate.withFilters(predicates);
        }

        @Override
        public Planner<S> createPlanner(PlannerContext context) throws IOException {
            Planner<S> planner = delegate.createPlanner(context);
            return () -> {
                try (Timer ignored = Tracers.watchScope(EXTERNAL, tracePrefix + "lakePlan")) {
                    return planner.plan();
                }
            };
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<S> context) throws IOException {
            return delegate.createRecordReader(context);
        }

        @Override
        public SimpleVersionedSerializer<S> getSplitSerializer() {
            return delegate.getSplitSerializer();
        }

        @Override
        public LakeSource<S> copy() {
            return new TracingLakeSource<>(delegate.copy(), tracePrefix);
        }
    }

    private static final class TracingBucketOffsetsRetriever implements OffsetsInitializer.BucketOffsetsRetriever {
        private final OffsetsInitializer.BucketOffsetsRetriever delegate;
        private final String tracePrefix;

        private TracingBucketOffsetsRetriever(OffsetsInitializer.BucketOffsetsRetriever delegate, String tracePrefix) {
            this.delegate = delegate;
            this.tracePrefix = tracePrefix;
        }

        @Override
        public Map<Integer, Long> latestOffsets(String partitionName, Collection<Integer> buckets) {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, tracePrefix + "bucketOffsets")) {
                return delegate.latestOffsets(partitionName, buckets);
            }
        }

        @Override
        public Map<Integer, Long> earliestOffsets(String partitionName, Collection<Integer> buckets) {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, tracePrefix + "bucketOffsets")) {
                return delegate.earliestOffsets(partitionName, buckets);
            }
        }

        @Override
        public Map<Integer, Long> offsetsFromTimestamp(String partitionName, Collection<Integer> buckets, long timestamp) {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, tracePrefix + "bucketOffsets")) {
                return delegate.offsetsFromTimestamp(partitionName, buckets, timestamp);
            }
        }
    }
}
