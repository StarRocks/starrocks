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

package com.starrocks.planner;

import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TRuntimeFilterLayout;
import com.starrocks.thrift.TRuntimeFilterLayoutMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class RuntimeFilterLayoutTest {
    @Test
    public void testLayout() {
        SessionVariable sv = new SessionVariable();
        sv.setEnablePipelineEngine(true);
        sv.setEnablePipelineLevelMultiPartitionedRf(true);
        RuntimeFilterDescription desc = new RuntimeFilterDescription(sv);
        desc.setFilterId(1);
        // BROADCAST JOIN
        for (JoinNode.DistributionMode mode : new JoinNode.DistributionMode[] {JoinNode.DistributionMode.BROADCAST,
                JoinNode.DistributionMode.REPLICATED}) {
            desc.setJoinMode(mode);
            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            TRuntimeFilterLayout layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.SINGLETON);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);

            sv.setEnablePipelineLevelMultiPartitionedRf(true);
            layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.SINGLETON);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }

        // SHUFFLE JOIN
        for (JoinNode.DistributionMode mode : new JoinNode.DistributionMode[] {
                JoinNode.DistributionMode.PARTITIONED,
                JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET}) {

            desc.setJoinMode(mode);
            sv.setEnablePipelineLevelMultiPartitionedRf(true);
            TRuntimeFilterLayout layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_2L);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_SHUFFLE);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_1L);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }

        // BUCKET JOIN, assign bucket sequences among drivers
        for (JoinNode.DistributionMode mode : new JoinNode.DistributionMode[] {
                JoinNode.DistributionMode.LOCAL_HASH_BUCKET,
                JoinNode.DistributionMode.COLOCATE}) {

            desc.setJoinMode(mode);
            sv.setEnablePipelineLevelMultiPartitionedRf(true);

            desc.setBucketSeqToInstance(Arrays.asList(1, 1, 1, 2, 2, 2, 2, 2, 2));
            desc.setBucketSeqToDriverSeq(Arrays.asList(0, 1, 2, 0, 1, 2, 0, 1, 2));
            desc.setBucketSeqToPartition(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8));

            TRuntimeFilterLayout layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_BUCKET);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }

        // BUCKET JOIN, NOT assign bucket sequences among drivers, with local exchange interpolated
        for (JoinNode.DistributionMode mode : new JoinNode.DistributionMode[] {
                JoinNode.DistributionMode.LOCAL_HASH_BUCKET,
                JoinNode.DistributionMode.COLOCATE}) {

            desc.setJoinMode(mode);
            sv.setEnablePipelineLevelMultiPartitionedRf(true);

            desc.setBucketSeqToInstance(Arrays.asList(1, 1, 1, 2, 2, 2, 2, 2, 2));
            desc.setBucketSeqToDriverSeq(Collections.emptyList());
            desc.setBucketSeqToPartition(Collections.emptyList());

            TRuntimeFilterLayout layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L_LX);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_BUCKET_LX);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assertions.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L);
            Assertions.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }
    }

    @Test
    public void testLayoutForRangeColocateJoin() {
        SessionVariable sv = new SessionVariable();
        sv.setEnablePipelineEngine(true);
        sv.setEnablePipelineLevelMultiPartitionedRf(true);
        RuntimeFilterDescription desc = new RuntimeFilterDescription(sv);
        desc.setFilterId(1);
        desc.setJoinMode(JoinNode.DistributionMode.COLOCATE);
        desc.setBucketSeqToInstance(Arrays.asList(1, 1, 1, 2, 2, 2, 2, 2, 2));
        desc.setBucketSeqToDriverSeq(Arrays.asList(0, 1, 2, 0, 1, 2, 0, 1, 2));
        desc.setBucketSeqToPartition(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8));

        // Control: a hash-colocate join keeps the partitioned bucket layout and
        // may push its runtime filter across an exchange.
        TRuntimeFilterLayout hashColocate = desc.toLayout();
        Assertions.assertEquals(TRuntimeFilterLayoutMode.PIPELINE_BUCKET, hashColocate.local_layout);
        Assertions.assertEquals(TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L, hashColocate.global_layout);
        Assertions.assertTrue(hashColocate.pipeline_level_multi_partitioned);
        Assertions.assertTrue(desc.canPushAcrossExchangeNode());

        // Range colocate: data is bucketed by range containment, which no
        // hash-partitioned runtime-filter layout can match. The filter must stay
        // local and singleton: SINGLETON local layout, not multi-partitioned, and
        // never pushed across an exchange (a global RF would be merged as disjoint
        // per-instance partitions that a singleton layout cannot probe correctly).
        desc.setColocateWithRangeDistribution(true);
        TRuntimeFilterLayout rangeColocate = desc.toLayout();
        Assertions.assertEquals(TRuntimeFilterLayoutMode.SINGLETON, rangeColocate.local_layout);
        Assertions.assertFalse(rangeColocate.pipeline_level_multi_partitioned);
        Assertions.assertFalse(desc.canPushAcrossExchangeNode());
    }
}
