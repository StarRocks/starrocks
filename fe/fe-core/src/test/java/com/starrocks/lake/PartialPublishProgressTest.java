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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Tablet;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.PartitionCommitInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartialPublishProgressTest {

    @Test
    public void testRecordPendingAndRestore() {
        ComputeNode node1 = new ComputeNode(1001L, "127.0.0.1", 9040);
        ComputeNode node2 = new ComputeNode(1002L, "127.0.0.2", 9040);
        List<Tablet> all = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(2L), new LakeTablet(3L),
                new LakeTablet(4L));

        // Attempt 1: node1 published 1 and 2, node2 published 4 but tablet 3 was still busy.
        Map<Long, Double> scores = new HashMap<>();
        scores.put(1L, 1.0);
        scores.put(2L, 2.0);
        scores.put(4L, 4.0);
        Map<Long, TabletStatPB> stats = new HashMap<>();
        stats.put(1L, new TabletStatPB());
        stats.put(4L, new TabletStatPB());
        List<VectorIndexBuildInfoPB> buildInfos = Lists.newArrayList(new VectorIndexBuildInfoPB());
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        nodeToTablets.put(node1, Lists.newArrayList(1L, 2L));
        nodeToTablets.put(node2, Lists.newArrayList(4L));

        PartialPublishProgress progress = new PartialPublishProgress(5L, 7L);
        progress.recordAttempt(all, Sets.newHashSet(3L), scores, stats, buildInfos, nodeToTablets);
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 4L), progress.getPublishedTabletIds());

        // Attempt 2 only has to send tablet 3 ...
        List<Tablet> pending = progress.pendingTablets(all);
        Assertions.assertEquals(1, pending.size());
        Assertions.assertEquals(3L, pending.get(0).getId());

        // ... and starts from what attempt 1 collected, so the final result covers every tablet.
        Map<Long, Double> scores2 = new HashMap<>();
        Map<Long, TabletStatPB> stats2 = new HashMap<>();
        List<VectorIndexBuildInfoPB> buildInfos2 = new ArrayList<>();
        Map<ComputeNode, List<Long>> nodeToTablets2 = new HashMap<>();
        progress.restoreInto(scores2, stats2, buildInfos2, nodeToTablets2);
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 4L), scores2.keySet());
        Assertions.assertEquals(Sets.newHashSet(1L, 4L), stats2.keySet());
        Assertions.assertEquals(1, buildInfos2.size());
        Assertions.assertEquals(Lists.newArrayList(1L, 2L), nodeToTablets2.get(node1));
        Assertions.assertEquals(Lists.newArrayList(4L), nodeToTablets2.get(node2));

        // Restoring twice does not duplicate txn-log owners or build infos.
        progress.restoreInto(scores2, stats2, buildInfos2, nodeToTablets2);
        Assertions.assertEquals(Lists.newArrayList(1L, 2L), nodeToTablets2.get(node1));
        Assertions.assertEquals(1, buildInfos2.size());

        // Null result holders are allowed: callers that do not track a kind of result pass null.
        progress.restoreInto(null, null, null, null);
    }

    @Test
    public void testScoresAndOwnersOfFailedTabletsAreDropped() {
        ComputeNode node = new ComputeNode(1001L, "127.0.0.1", 9040);
        Map<Long, Double> scores = new HashMap<>();
        scores.put(1L, 1.0);
        scores.put(2L, 2.0);
        Map<Long, TabletStatPB> stats = new HashMap<>();
        stats.put(2L, new TabletStatPB());
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        nodeToTablets.put(node, Lists.newArrayList(1L, 2L));

        PartialPublishProgress progress = new PartialPublishProgress(1L, 2L);
        progress.recordAttempt(Lists.newArrayList(new LakeTablet(1L), new LakeTablet(2L)), Sets.newHashSet(2L),
                scores, stats, null, nodeToTablets);

        Map<Long, Double> restoredScores = new HashMap<>();
        Map<Long, TabletStatPB> restoredStats = new HashMap<>();
        Map<ComputeNode, List<Long>> restoredOwners = new HashMap<>();
        progress.restoreInto(restoredScores, restoredStats, null, restoredOwners);
        Assertions.assertEquals(Sets.newHashSet(1L), restoredScores.keySet());
        Assertions.assertTrue(restoredStats.isEmpty());
        Assertions.assertEquals(Lists.newArrayList(1L), restoredOwners.get(node));
    }

    @Test
    public void testFindIsKeyedByVersionRange() {
        PartitionCommitInfo a = new PartitionCommitInfo(100L, 6, 0);
        PartitionCommitInfo b = new PartitionCommitInfo(100L, 7, 0);
        List<PartitionCommitInfo> commitInfos = Lists.newArrayList(a, b);
        Assertions.assertNull(PartialPublishProgress.find(commitInfos, 5L, 7L));

        PartialPublishProgress progress = new PartialPublishProgress(5L, 7L);
        PartialPublishProgress.attach(commitInfos, progress);
        Assertions.assertSame(progress, PartialPublishProgress.find(commitInfos, 5L, 7L));
        // A batch that now ends at a different version must republish everything.
        Assertions.assertNull(PartialPublishProgress.find(commitInfos, 5L, 8L));
        Assertions.assertNull(PartialPublishProgress.find(commitInfos, 4L, 7L));
        // Any commit info of the batch is enough to find it.
        Assertions.assertSame(progress, PartialPublishProgress.find(Lists.newArrayList(b), 5L, 7L));

        PartialPublishProgress.clear(commitInfos);
        Assertions.assertNull(PartialPublishProgress.find(commitInfos, 5L, 7L));
        Assertions.assertNull(a.getPartialPublishProgress());
    }
}
