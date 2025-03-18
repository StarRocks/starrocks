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
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionMgrTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    @Mocked
    private Database db;

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
        List<PartitionStatisticsSnapshot> compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition1, compactionList.get(0).getPartition());

        compactionManager.handleLoadingFinished(partition2, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)));

        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(2, compactionList.size());

        compactionList = compactionManager.choosePartitionsToCompact(Collections.singleton(partition1), excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0).getPartition());

        compactionManager.enableCompactionAfter(partition1, 5000);
        compactionManager.enableCompactionAfter(partition2, 5000);
        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(0, compactionList.size());

        compactionManager.enableCompactionAfter(partition1, 0);
        compactionManager.enableCompactionAfter(partition2, 0);
        compactionManager.removePartition(partition1);
        compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0).getPartition());
    }

    @Test
    public void testChoosePartitionsToCompactWithActiveTxnFilter() {
        long dbId = 10001L;
        long tableId1 = 10002L;
        long tableId2 = 10003L;
        long partitionId10 = 20001L;
        long partitionId11 = 20003L;
        long partitionId20 = 20002L;

        PartitionIdentifier partition10 = new PartitionIdentifier(dbId, tableId1, partitionId10);
        PartitionIdentifier partition11 = new PartitionIdentifier(dbId, tableId1, partitionId11);
        PartitionIdentifier partition20 = new PartitionIdentifier(dbId, tableId2, partitionId20);

        CompactionMgr compactionManager = new CompactionMgr();
        compactionManager.handleLoadingFinished(partition10, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(100d)));
        compactionManager.handleLoadingFinished(partition11, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(100d)));
        compactionManager.handleLoadingFinished(partition20, 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(100d)));

        // build active txn on table1
        long txnId = 10001L;
        Map<Long, Long> txnIdToTableIdMap = new HashMap<>();
        txnIdToTableIdMap.put(txnId, tableId1);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;

                globalTransactionMgr.getLakeCompactionActiveTxnStats();
                result = txnIdToTableIdMap;

            }
        };
        compactionManager.buildActiveCompactionTransactionMap();

        Set<PartitionIdentifier> allPartitions = compactionManager.getAllPartitions();
        Assert.assertEquals(3, allPartitions.size());
        Assert.assertTrue(allPartitions.contains(partition10));
        Assert.assertTrue(allPartitions.contains(partition11));
        Assert.assertTrue(allPartitions.contains(partition20));

        List<PartitionStatisticsSnapshot> compactionList =
                compactionManager.choosePartitionsToCompact(new HashSet<>(), new HashSet<>());
        // both partition10 and partition11 are filtered because table1 has active txn
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition20, compactionList.get(0).getPartition());

        Set<Long> excludeTables = new HashSet<>();
        excludeTables.add(tableId2);
        compactionList = compactionManager.choosePartitionsToCompact(new HashSet<>(), excludeTables);
        // tableId2 is filtered by excludeTables
        Assert.assertEquals(0, compactionList.size());
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
                Quantiles.compute(Lists.newArrayList(2d)), 1234);
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
        compactionManager.handleLoadingFinished(partition, 1, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(1d)));

        PartitionStatistics statistics = compactionManager.triggerManualCompaction(partition);
        Assert.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT, statistics.getPriority());

        Set<Long> excludeTables = new HashSet<>();
        List<PartitionStatisticsSnapshot> compactionList = compactionManager.choosePartitionsToCompact(excludeTables);
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition, compactionList.get(0).getPartition());
        Assert.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT, compactionList.get(0).getPriority());
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
                PhysicalPartition partition = new PhysicalPartition(123, "aaa", 123,  null);
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

            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
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

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        compactionMgr.save(image.getImageWriter());
        CompactionMgr compactionMgr2 = new CompactionMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        compactionMgr2.load(reader);
        Assert.assertEquals(1, compactionMgr2.getPartitionStatsCount());
    }

    @Test
    public void testActiveCompactionTransactionMapOnRestart() {
        long txnId = 10001L;
        long tableId = 10002L;
        Map<Long, Long> txnIdToTableIdMap = new HashMap<>();
        txnIdToTableIdMap.put(txnId, tableId);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;

                globalTransactionMgr.getLakeCompactionActiveTxnStats();
                result = txnIdToTableIdMap;

            }
        };

        CompactionMgr compactionMgr = new CompactionMgr();
        compactionMgr.buildActiveCompactionTransactionMap();
        ConcurrentHashMap<Long, Long> activeCompactionTransactionMap =
                compactionMgr.getRemainedActiveCompactionTxnWhenStart();
        Assert.assertEquals(1, activeCompactionTransactionMap.size());
        Assert.assertTrue(activeCompactionTransactionMap.containsValue(tableId));

        // test for removeFromStartupActiveCompactionTransactionMap
        long nonExistedTxnId = 10003L;
        compactionMgr.removeFromStartupActiveCompactionTransactionMap(nonExistedTxnId);
        Assert.assertEquals(1, activeCompactionTransactionMap.size());

        compactionMgr.removeFromStartupActiveCompactionTransactionMap(txnId);
        Assert.assertEquals(0, activeCompactionTransactionMap.size());
    }
}
