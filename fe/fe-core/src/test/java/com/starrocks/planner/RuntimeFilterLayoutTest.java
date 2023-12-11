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
import org.junit.Assert;
import org.junit.Test;

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
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.SINGLETON);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);

            sv.setEnablePipelineLevelMultiPartitionedRf(true);
            layout = desc.toLayout();
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.SINGLETON);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }

        // SHUFFLE JOIN
        for (JoinNode.DistributionMode mode : new JoinNode.DistributionMode[] {
                JoinNode.DistributionMode.PARTITIONED,
                JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET}) {

            desc.setJoinMode(mode);
            sv.setEnablePipelineLevelMultiPartitionedRf(true);
            TRuntimeFilterLayout layout = desc.toLayout();
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_2L);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_SHUFFLE);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_1L);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
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
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_BUCKET);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
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
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L_LX);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.PIPELINE_BUCKET_LX);

            sv.setEnablePipelineLevelMultiPartitionedRf(false);
            layout = desc.toLayout();
            Assert.assertEquals(layout.global_layout, TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L);
            Assert.assertEquals(layout.local_layout, TRuntimeFilterLayoutMode.SINGLETON);
        }
    }
}
