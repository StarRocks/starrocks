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

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionMgrTest {

    @Test
    public void testChoosePartitionsToCompact() {
        Config.lake_compaction_selector = "SimpleSelector";
        Config.lake_compaction_sorter = "RandomSorter";
        CompactionMgr compactionManager = new CompactionMgr();

        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        Set<Long> excludeTables = new HashSet<>();
        for (int i = 1; i <= Config.lake_compaction_simple_selector_threshold_versions - 1; i++) {
            compactionManager.handleLoadingFinished(partition1, i, System.currentTimeMillis(),
                                                    Quantiles.compute(Lists.newArrayList(1d)));
            compactionManager.handleLoadingFinished(partition2, i, System.currentTimeMillis(),
                                                    Quantiles.compute(Lists.newArrayList(1d)));
            Assert.assertEquals(0, compactionManager.choosePartitionsToCompact(excludeTables).size());
        }
        compactionManager.handleLoadingFinished(partition1, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)));
        List<PartitionIdentifier> compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition1, compactionList.get(0));

        Assert.assertEquals(compactionList, compactionManager.choosePartitionsToCompact(excludeTables));

        compactionManager.handleLoadingFinished(partition2, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)));

        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(2, compactionList.size());
        Assert.assertTrue(compactionList.contains(partition1));
        Assert.assertTrue(compactionList.contains(partition2));

        compactionList = compactionManager.choosePartitionsToCompact(Collections.singleton(partition1), excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0));

        compactionManager.enableCompactionAfter(partition1, 5000);
        compactionManager.enableCompactionAfter(partition2, 5000);
        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(0, compactionList.size());

        compactionManager.enableCompactionAfter(partition1, 0);
        compactionManager.enableCompactionAfter(partition2, 0);
        compactionManager.removePartition(partition1);
        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0));
    }

    @Test
    public void testGetMaxCompactionScore() {
        double delta = 0.001;

        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        Assert.assertEquals(0, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.handleLoadingFinished(partition1, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));
        Assert.assertEquals(1, compactionMgr.getMaxCompactionScore(), delta);
        compactionMgr.handleCompactionFinished(partition1, 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(2d)));
        Assert.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.handleLoadingFinished(partition2, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(3d)));
        Assert.assertEquals(3, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.removePartition(partition2);
        Assert.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);
    }

    @Test
    public void testTriggerManualCompaction() {
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);

        PartitionStatistics statistics = compactionManager.triggerManualCompaction(partition);
        Assert.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT, statistics.getPriority());

        Collection<PartitionStatistics> allStatistics = compactionManager.getAllStatistics();
        Assert.assertEquals(1, allStatistics.size());
        Assert.assertTrue(allStatistics.contains(statistics));
    }

    @Test
    public void testExistCompaction() {
        long txnId = 11111;
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(), "");
        compactionManager.setCompactionScheduler(compactionScheduler);
        new MockUp<CompactionScheduler>() {
            @Mock
            public ConcurrentHashMap<PartitionIdentifier, CompactionJob> getRunningCompactions() {
                ConcurrentHashMap<PartitionIdentifier, CompactionJob> r = new ConcurrentHashMap<>();
                PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 3);
                Database db = new Database();
                Table table = new LakeTable();
                PhysicalPartition partition = new Partition(123, "aaa", null, null);
                CompactionJob job = new CompactionJob(db, table, partition, txnId, false);
                r.put(partitionIdentifier, job);
                return r;
            }
        };
        Assert.assertEquals(true, compactionManager.existCompaction(txnId));
    }

    @Test
    public void testSaveAndLoad() throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        PartitionIdentifier partition3 = new PartitionIdentifier(1, 2, 5);

        compactionMgr.handleLoadingFinished(partition1, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));
        compactionMgr.handleLoadingFinished(partition2, 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(2d)));
        compactionMgr.handleLoadingFinished(partition3, 4, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(3d)));

        Assert.assertEquals(3, compactionMgr.getPartitionStatsCount());

        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
                if (partitionId == 3) {
                    return true;
                }
                if (partitionId == 4) {
                    return false;
                }
                if (partitionId == 5) {
                    return false;
                }
                return true;
            }
        };

        UtFrameUtils.PseudoImage.setUpImageVersion();
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        compactionMgr.save(image.getImageWriter());
        CompactionMgr compactionMgr2 = new CompactionMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        compactionMgr2.load(reader);
        Assert.assertEquals(1, compactionMgr2.getPartitionStatsCount());
    }
}
