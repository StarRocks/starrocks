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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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

        Set<Long> excludeIds = new HashSet<>();
        for (int i = 1; i <= Config.lake_compaction_simple_selector_threshold_versions - 1; i++) {
            compactionManager.handleLoadingFinished(partition1, i, System.currentTimeMillis(),
                                                    Quantiles.compute(Lists.newArrayList(1d)));
            compactionManager.handleLoadingFinished(partition2, i, System.currentTimeMillis(),
                                                    Quantiles.compute(Lists.newArrayList(1d)));
            Assertions.assertEquals(0, compactionManager.choosePartitionsToCompact(excludeIds).size());
        }
        compactionManager.handleLoadingFinished(partition1, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)));
        List<PartitionStatisticsSnapshot> compactionList = compactionManager.choosePartitionsToCompact(excludeIds);
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertSame(partition1, compactionList.get(0).getPartition());

        compactionManager.handleLoadingFinished(partition2, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)));

        compactionList = compactionManager.choosePartitionsToCompact(excludeIds);
        Assertions.assertEquals(2, compactionList.size());

        compactionList = compactionManager.choosePartitionsToCompact(Collections.singleton(partition1), excludeIds);
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertSame(partition2, compactionList.get(0).getPartition());

        compactionManager.enableCompactionAfter(partition1, 5000);
        compactionManager.enableCompactionAfter(partition2, 5000);
        compactionList = compactionManager.choosePartitionsToCompact(excludeIds);
        Assertions.assertEquals(0, compactionList.size());

        compactionManager.enableCompactionAfter(partition1, 0);
        compactionManager.enableCompactionAfter(partition2, 0);
        compactionManager.removePartition(partition1);
        compactionList = compactionManager.choosePartitionsToCompact(excludeIds);
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertSame(partition2, compactionList.get(0).getPartition());
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
        Assertions.assertEquals(3, allPartitions.size());
        Assertions.assertTrue(allPartitions.contains(partition10));
        Assertions.assertTrue(allPartitions.contains(partition11));
        Assertions.assertTrue(allPartitions.contains(partition20));

        List<PartitionStatisticsSnapshot> compactionList =
                compactionManager.choosePartitionsToCompact(new HashSet<>(), new HashSet<>());
        // both partition10 and partition11 are filtered because table1 has active txn
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertSame(partition20, compactionList.get(0).getPartition());

        Set<Long> excludeIds = new HashSet<>();
        excludeIds.add(tableId2);
        compactionList = compactionManager.choosePartitionsToCompact(new HashSet<>(), excludeIds);
        // tableId2 is filtered by excludeIds
        Assertions.assertEquals(0, compactionList.size());
    }

    @Test
    public void testGetMaxCompactionScore() {
        double delta = 0.001;

        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        Assertions.assertEquals(0, compactionMgr.getMaxCompactionScore(), delta);

        // load and compact partition 1
        compactionMgr.handleLoadingFinished(partition1, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));
        Assertions.assertEquals(1, compactionMgr.getMaxCompactionScore(), delta);
        compactionMgr.handleCompactionFinished(partition1, 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(2d)), 1234, false);
        Assertions.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);

        // load partition 2
        compactionMgr.handleLoadingFinished(partition2, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(3d)));
        Assertions.assertEquals(3, compactionMgr.getMaxCompactionScore(), delta);

        // set partition 2 compaction score to null
        PartitionStatistics statistics2 = compactionMgr.getStatistics(partition2);
        statistics2.setCompactionScore(null);
        Assertions.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);

        // remove partition 2
        compactionMgr.removePartition(partition2);
        Assertions.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);
    }

    @Test
    public void testVisiblePartialSuccessIncrementsCount() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionMgr.handleLoadingFinished(partition, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));

        compactionMgr.handleCompactionFinished(partition,
                2, System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)), 100L, true);

        Assertions.assertEquals(1, compactionMgr.getStatistics(partition).getConsecutiveAbnormalCount());
    }

    @Test
    public void testVisibleFullSuccessResetsNonZeroCount() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionMgr.handleLoadingFinished(partition, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));
        compactionMgr.getStatistics(partition).incrementConsecutiveAbnormalCount();
        compactionMgr.getStatistics(partition).incrementConsecutiveAbnormalCount();

        compactionMgr.handleCompactionFinished(partition,
                2, System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)), 101L, false);

        Assertions.assertEquals(0, compactionMgr.getStatistics(partition).getConsecutiveAbnormalCount());
    }

    @Test
    public void testVisibleFullSuccessKeepsZeroCount() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionMgr.handleLoadingFinished(partition, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));

        compactionMgr.handleCompactionFinished(partition,
                2, System.currentTimeMillis(), Quantiles.compute(Lists.newArrayList(1d)), 102L, false);

        Assertions.assertEquals(0, compactionMgr.getStatistics(partition).getConsecutiveAbnormalCount());
    }

    @Test
    public void testVisibleFullSuccessCannotResetBeforeEarlierPartialCountIsApplied() throws Exception {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        BlockingCountPartitionStatistics statistics = new BlockingCountPartitionStatistics(partition);
        FullComputeAttemptSignalingMap statisticsMap = new FullComputeAttemptSignalingMap("visible-full-success");
        statisticsMap.put(partition, statistics);
        Deencapsulation.setField(compactionMgr, "partitionStatisticsHashMap", statisticsMap);

        Thread partial = new Thread(() -> compactionMgr.handleCompactionFinished(partition, 1, 1,
                Quantiles.compute(Lists.newArrayList(1d)), 100L, true));
        Thread full = new Thread(() -> compactionMgr.handleCompactionFinished(partition, 2, 2,
                Quantiles.compute(Lists.newArrayList(1d)), 101L, false), "visible-full-success");

        try {
            partial.start();
            Assertions.assertTrue(statistics.incrementEntered.await(5, TimeUnit.SECONDS));

            full.start();
            Assertions.assertTrue(statisticsMap.fullComputeAttempted.await(5, TimeUnit.SECONDS),
                    "the full worker must attempt the statistics-map operation before reset is checked");
            Assertions.assertFalse(statistics.resetEntered.await(200, TimeUnit.MILLISECONDS),
                    "a visible full success must wait for the earlier partial count to be applied");
        } finally {
            statistics.releaseIncrement.countDown();
            partial.join(5_000);
            full.join(5_000);
        }

        Assertions.assertFalse(partial.isAlive());
        Assertions.assertFalse(full.isAlive());
        Assertions.assertEquals(0, compactionMgr.getStatistics(partition).getConsecutiveAbnormalCount());
    }

    @Test
    public void testVisiblePartialSuccessCreatesMissingStatisticsThroughCompute() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);

        compactionMgr.handleCompactionFinished(partition, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)), 103L, true);

        Assertions.assertNotNull(compactionMgr.getStatistics(partition));
        Assertions.assertEquals(1, compactionMgr.getStatistics(partition).getConsecutiveAbnormalCount());
    }

    @Test
    public void testCollectCompactionMetricsReturnsBothMaximaAfterOneMapTraversal() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        PartitionIdentifier partition3 = new PartitionIdentifier(1, 2, 5);
        Assertions.assertEquals(new CompactionMgr.CompactionMetrics(0, 0),
                compactionMgr.collectCompactionMetrics());

        compactionMgr.handleCompactionFinished(partition1, 1, System.currentTimeMillis(), null, 1L, true);
        compactionMgr.handleLoadingFinished(partition2, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(2d)));
        compactionMgr.handleLoadingFinished(partition3, 1, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(9d)));
        compactionMgr.getStatistics(partition2).incrementConsecutiveAbnormalCount();
        compactionMgr.getStatistics(partition2).incrementConsecutiveAbnormalCount();
        for (int i = 1; i < 5; i++) {
            compactionMgr.getStatistics(partition1).incrementConsecutiveAbnormalCount();
        }

        CountingValuesMap statisticsMap = new CountingValuesMap();
        statisticsMap.put(partition1, compactionMgr.getStatistics(partition1));
        statisticsMap.put(partition2, compactionMgr.getStatistics(partition2));
        statisticsMap.put(partition3, compactionMgr.getStatistics(partition3));
        Deencapsulation.setField(compactionMgr, "partitionStatisticsHashMap", statisticsMap);

        Assertions.assertEquals(new CompactionMgr.CompactionMetrics(9, 5),
                compactionMgr.collectCompactionMetrics());
        Assertions.assertEquals(1, statisticsMap.valuesCallCount);
    }

    @Test
    public void testConsecutiveAbnormalCountIsNotPersisted() throws IOException, SRMetaBlockException,
            SRMetaBlockEOFException {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionMgr.handleLoadingFinished(partition, 2, 1234L, Quantiles.compute(Lists.newArrayList(1d)));
        compactionMgr.enableCompactionAfter(partition, 1000L);
        compactionMgr.triggerManualCompaction(partition);
        compactionMgr.getStatistics(partition).incrementConsecutiveAbnormalCount();
        compactionMgr.getStatistics(partition).incrementConsecutiveAbnormalCount();
        PartitionStatistics beforeSave = compactionMgr.getStatistics(partition);

        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
                return true;
            }
        };

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        compactionMgr.save(image.getImageWriter());
        CompactionMgr loadedMgr = new CompactionMgr();
        loadedMgr.load(new SRMetaBlockReaderV2(image.getJsonReader()));
        PartitionStatistics loaded = loadedMgr.getStatistics(partition);

        Assertions.assertNotNull(loaded);
        Assertions.assertEquals(beforeSave.getCurrentVersion().getVersion(), loaded.getCurrentVersion().getVersion());
        Assertions.assertEquals(beforeSave.getCurrentVersion().getCreateTime(), loaded.getCurrentVersion().getCreateTime());
        Assertions.assertEquals(beforeSave.getCompactionVersion().getVersion(), loaded.getCompactionVersion().getVersion());
        Assertions.assertEquals(beforeSave.getCompactionVersion().getCreateTime(),
                loaded.getCompactionVersion().getCreateTime());
        Assertions.assertEquals(beforeSave.getNextCompactionTime(), loaded.getNextCompactionTime());
        Assertions.assertEquals(beforeSave.getCompactionScore().getAvg(), loaded.getCompactionScore().getAvg());
        Assertions.assertEquals(beforeSave.getCompactionScore().getP50(), loaded.getCompactionScore().getP50());
        Assertions.assertEquals(beforeSave.getCompactionScore().getMax(), loaded.getCompactionScore().getMax());
        Assertions.assertEquals(beforeSave.getPriority(), loaded.getPriority());
        Assertions.assertEquals(0, loaded.getConsecutiveAbnormalCount());
    }


    @Test
    public void testTriggerManualCompaction() {
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionManager.handleLoadingFinished(partition, 1, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(1d)));

        PartitionStatistics statistics = compactionManager.triggerManualCompaction(partition);
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT, statistics.getPriority());

        Set<Long> excludeIds = new HashSet<>();
        List<PartitionStatisticsSnapshot> compactionList = compactionManager.choosePartitionsToCompact(excludeIds);
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertSame(partition, compactionList.get(0).getPartition());
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT, compactionList.get(0).getPriority());
    }

    @Test
    public void testTriggerUnshareCompactionHasHighestPriority() {
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = compactionManager.triggerUnshareCompaction(partition);

        Assertions.assertEquals(PartitionStatistics.CompactionPriority.UNSHARE, statistics.getPriority());
        List<PartitionStatisticsSnapshot> compactionList = compactionManager.choosePartitionsToCompact(new HashSet<>());
        Assertions.assertEquals(1, compactionList.size());
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.UNSHARE, compactionList.get(0).getPriority());
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
                PhysicalPartition partition = new PhysicalPartition(123, 123,  new MaterializedIndex());
                CompactionJob job = new CompactionJob(db, table, partition, txnId, false, null, "", null);
                r.put(partitionIdentifier, job);
                return r;
            }
        };
        Assertions.assertEquals(true, compactionManager.existCompaction(txnId));
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

        Assertions.assertEquals(3, compactionMgr.getPartitionStatsCount());

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
        Assertions.assertEquals(1, compactionMgr2.getPartitionStatsCount());
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
        Assertions.assertEquals(1, activeCompactionTransactionMap.size());
        Assertions.assertTrue(activeCompactionTransactionMap.containsValue(tableId));

        // test for removeFromStartupActiveCompactionTransactionMap
        long nonExistedTxnId = 10003L;
        compactionMgr.removeFromStartupActiveCompactionTransactionMap(nonExistedTxnId);
        Assertions.assertEquals(1, activeCompactionTransactionMap.size());

        compactionMgr.removeFromStartupActiveCompactionTransactionMap(txnId);
        Assertions.assertEquals(0, activeCompactionTransactionMap.size());
    }

    /**
     * A priority-carrying request that is abandoned before it starts must not leave the marker behind.
     * The scheduler's own reset only covers partitions that reached runningCompactions, and ScoreSelector
     * admits any non-DEFAULT priority regardless of score or cooldown -- so a marker left on a partition
     * that never starts gets it reselected and refused every cycle, and its ordinary compaction never
     * resumes.
     */
    @Test
    public void testResetPriorityClearsAnAbandonedMarker() {
        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        compactionMgr.handleLoadingFinished(partition, 1, System.currentTimeMillis(), new Quantiles(0, 0, 0));

        compactionMgr.triggerUnshareCompaction(partition);
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.UNSHARE,
                compactionMgr.getStatistics(partition).getPriority());

        compactionMgr.resetPriority(partition);
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.DEFAULT,
                compactionMgr.getStatistics(partition).getPriority(),
                "an abandoned UNSHARE request must not keep the partition at a non-default priority");

        // Unknown partitions are a no-op rather than an error: the scheduler may drop a request for a
        // partition the manager has already forgotten.
        compactionMgr.resetPriority(new PartitionIdentifier(9, 9, 9));
    }

    private static class BlockingCountPartitionStatistics extends PartitionStatistics {
        private final CountDownLatch incrementEntered = new CountDownLatch(1);
        private final CountDownLatch releaseIncrement = new CountDownLatch(1);
        private final CountDownLatch resetEntered = new CountDownLatch(1);

        BlockingCountPartitionStatistics(PartitionIdentifier partition) {
            super(partition);
        }

        @Override
        void incrementConsecutiveAbnormalCount() {
            incrementEntered.countDown();
            try {
                releaseIncrement.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            super.incrementConsecutiveAbnormalCount();
        }

        @Override
        void resetConsecutiveAbnormalCount() {
            resetEntered.countDown();
            super.resetConsecutiveAbnormalCount();
        }
    }

    private static class FullComputeAttemptSignalingMap
            extends ConcurrentHashMap<PartitionIdentifier, PartitionStatistics> {
        private final CountDownLatch fullComputeAttempted = new CountDownLatch(1);
        private final String fullThreadName;

        FullComputeAttemptSignalingMap(String fullThreadName) {
            this.fullThreadName = fullThreadName;
        }

        @Override
        public PartitionStatistics compute(PartitionIdentifier key,
                                           BiFunction<? super PartitionIdentifier, ? super PartitionStatistics,
                                                   ? extends PartitionStatistics> remappingFunction) {
            if (Thread.currentThread().getName().equals(fullThreadName)) {
                fullComputeAttempted.countDown();
            }
            return super.compute(key, remappingFunction);
        }
    }

    private static class CountingValuesMap extends ConcurrentHashMap<PartitionIdentifier, PartitionStatistics> {
        private int valuesCallCount;

        @Override
        public Collection<PartitionStatistics> values() {
            valuesCallCount++;
            return super.values();
        }
    }
}
