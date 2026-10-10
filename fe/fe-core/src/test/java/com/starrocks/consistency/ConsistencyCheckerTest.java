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


package com.starrocks.consistency;

import com.google.common.collect.Lists;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.journal.JournalTask;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsistencyCheckerTest {

    @Test
    public void testTimeoutPreservesLastCompletedCheckAndAllowsRetry(@Mocked GlobalStateMgr globalStateMgr,
                                                                     @Mocked EditLog editLog,
                                                                     @Mocked JournalTask journalTask) throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long replicaId = 6L;
        long backendId = 7L;
        long physicalPartitionId = 8L;
        long visibleVersion = 2L;
        TStorageMedium medium = TStorageMedium.HDD;

        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Replica replica = new Replica(replicaId, backendId, visibleVersion, 1111,
                10, 1000, Replica.ReplicaState.NORMAL, -1, visibleVersion);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, medium);
        LocalTablet tablet = new LocalTablet(tabletId, Lists.newArrayList(replica));
        tablet.setCheckedVersion(visibleVersion - 1);
        tablet.setIsConsistent(false);
        materializedIndex.addTablet(tablet, tabletMeta, false);
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.addPartition(partitionId, new DataProperty(medium), (short) 3, null);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList());
        Partition partition = new Partition(partitionId, physicalPartitionId, "partition", materializedIndex,
                distributionInfo);
        partition.getDefaultPhysicalPartition().setVisibleVersion(visibleVersion, System.currentTimeMillis());
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.registerTableUnlocked(table);
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        invertedIndex.addTablet(tabletId, tabletMeta);

        List<ConsistencyCheckInfo> loggedChecks = new ArrayList<>();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getTabletInvertedIndex();
                result = invertedIndex;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
                result = table;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDbIds();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTables(dbId);
                result = Lists.newArrayList(table);
                minTimes = 0;

                globalStateMgr.getEditLog();
                result = editLog;

                editLog.logFinishConsistencyCheck((ConsistencyCheckInfo) any);
                result = journalTask;
                maxTimes = 3;
            }
        };

        CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
        job.setState(CheckConsistencyJob.JobState.RUNNING);
        FieldUtils.writeField(job, "checkedVersion", visibleVersion, true);
        FieldUtils.writeField(job, "checkedSchemaHash", -1, true);
        FieldUtils.writeField(job, "createTime", System.currentTimeMillis() - 1000, true);
        FieldUtils.writeField(job, "timeoutMs", 1L, true);
        FieldUtils.writeField(job, "checksumMap", Map.of(backendId, -1L), true);

        Assertions.assertEquals(1, job.tryFinishJob());
        Assertions.assertFalse(tablet.isConsistent(), "an incomplete check must preserve the prior result");
        Assertions.assertEquals(visibleVersion - 1, tablet.getCheckedVersion(),
                "an incomplete check must not mark its visible version as checked");
        Assertions.assertTrue(tablet.getLastCheckTime() > 0, "the attempt should still enforce cooldown");

        new Verifications() {
            {
                editLog.logFinishConsistencyCheck(withCapture(loggedChecks));
                times = 1;
            }
        };
        Assertions.assertEquals(1, loggedChecks.size());
        Assertions.assertEquals(visibleVersion - 1, loggedChecks.get(0).getCheckedVersion());
        Assertions.assertFalse(loggedChecks.get(0).isConsistent());

        ConsistencyChecker checker = new ConsistencyChecker();
        checker.replayFinishConsistencyCheck(loggedChecks.get(0), globalStateMgr);
        Assertions.assertEquals(visibleVersion - 1, tablet.getCheckedVersion(),
                "edit-log replay must retain the last completed version");
        Assertions.assertFalse(tablet.isConsistent(), "edit-log replay must retain the last completed result");

        tablet.setLastCheckTime(0);
        Assertions.assertEquals(Lists.newArrayList(tabletId), checker.chooseTablets(),
                "the unchanged visible version should be eligible again after cooldown");

        long secondBackendId = 9L;
        tablet.addReplica(new Replica(10L, secondBackendId, visibleVersion, 1111,
                10, 1000, Replica.ReplicaState.NORMAL, -1, visibleVersion), false);
        CheckConsistencyJob matchingJob = new CheckConsistencyJob(tabletId);
        matchingJob.setState(CheckConsistencyJob.JobState.RUNNING);
        FieldUtils.writeField(matchingJob, "checkedVersion", visibleVersion, true);
        FieldUtils.writeField(matchingJob, "checkedSchemaHash", -1, true);
        FieldUtils.writeField(matchingJob, "createTime", System.currentTimeMillis(), true);
        FieldUtils.writeField(matchingJob, "timeoutMs", 60000L, true);
        FieldUtils.writeField(matchingJob, "checksumMap", Map.of(backendId, 100L, secondBackendId, 100L), true);
        Assertions.assertEquals(1, matchingJob.tryFinishJob());
        Assertions.assertTrue(tablet.isConsistent(), "matching completed checks should still mark the tablet consistent");
        Assertions.assertEquals(visibleVersion, tablet.getCheckedVersion());

        CheckConsistencyJob mismatchingJob = new CheckConsistencyJob(tabletId);
        mismatchingJob.setState(CheckConsistencyJob.JobState.RUNNING);
        FieldUtils.writeField(mismatchingJob, "checkedVersion", visibleVersion, true);
        FieldUtils.writeField(mismatchingJob, "checkedSchemaHash", -1, true);
        FieldUtils.writeField(mismatchingJob, "createTime", System.currentTimeMillis(), true);
        FieldUtils.writeField(mismatchingJob, "timeoutMs", 60000L, true);
        FieldUtils.writeField(mismatchingJob, "checksumMap", Map.of(backendId, 100L, secondBackendId, 200L), true);
        Assertions.assertEquals(1, mismatchingJob.tryFinishJob());
        Assertions.assertFalse(tablet.isConsistent(), "different completed checksums should still mark the tablet inconsistent");
        Assertions.assertEquals(visibleVersion, tablet.getCheckedVersion());
    }

    @Test
    public void testInsufficientReplicasPreservesLastCompletedCheck(@Mocked GlobalStateMgr globalStateMgr,
                                                                    @Mocked EditLog editLog,
                                                                    @Mocked JournalTask journalTask) {
        long dbId = 11L;
        long tableId = 12L;
        long partitionId = 13L;
        long indexId = 14L;
        long tabletId = 15L;
        long replicaId = 16L;
        long backendId = 17L;
        long physicalPartitionId = 18L;
        long visibleVersion = 2L;
        TStorageMedium medium = TStorageMedium.HDD;

        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Replica replica = new Replica(replicaId, backendId, visibleVersion, 1111,
                10, 1000, Replica.ReplicaState.NORMAL, -1, visibleVersion);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, medium);
        LocalTablet tablet = new LocalTablet(tabletId, Lists.newArrayList(replica));
        tablet.setCheckedVersion(visibleVersion - 1);
        tablet.setIsConsistent(false);
        materializedIndex.addTablet(tablet, tabletMeta, false);
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.addPartition(partitionId, new DataProperty(medium), (short) 3, null);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList());
        Partition partition = new Partition(partitionId, physicalPartitionId, "partition", materializedIndex,
                distributionInfo);
        partition.getDefaultPhysicalPartition().setVisibleVersion(visibleVersion, System.currentTimeMillis());
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.registerTableUnlocked(table);
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        invertedIndex.addTablet(tabletId, tabletMeta);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getTabletInvertedIndex();
                result = invertedIndex;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
                result = table;
                minTimes = 0;

                globalStateMgr.getEditLog();
                result = editLog;

                editLog.logFinishConsistencyCheck((ConsistencyCheckInfo) any);
                result = journalTask;
            }
        };

        CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
        Assertions.assertFalse(job.sendTasks(), "one replica is below the configured quorum of two");
        Assertions.assertEquals(visibleVersion - 1, tablet.getCheckedVersion(),
                "a check that did not start must preserve the last completed version");
        Assertions.assertFalse(tablet.isConsistent(), "a check that did not start must preserve the last result");
        Assertions.assertTrue(tablet.getLastCheckTime() > 0, "the failed attempt should still enforce cooldown");
    }

    @Test
    public void testChooseTablets(@Mocked GlobalStateMgr globalStateMgr) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long replicaId = 6L;
        long backendId = 7L;
        long physicalPartitionId = 8L;
        TStorageMedium medium = TStorageMedium.HDD;

        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Replica replica = new Replica(replicaId, backendId, 2L, 1111,
                10, 1000, Replica.ReplicaState.NORMAL, -1, 2);

        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, medium);
        LocalTablet tablet = new LocalTablet(tabletId, Lists.newArrayList(replica));
        materializedIndex.addTablet(tablet, tabletMeta, false);
        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 3, null);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList());
        Partition partition = new Partition(partitionId, physicalPartitionId, "partition", materializedIndex, distributionInfo);
        partition.getDefaultPhysicalPartition().setVisibleVersion(2L, System.currentTimeMillis());
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.registerTableUnlocked(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDbIds();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTables(dbId);
                result = database.getTables();
                minTimes = 0;

                // chooseTabletsFromTable re-fetches the table under the per-table lock to
                // detect tables dropped after the snapshot. The RESTORE-state branch
                // pre-filters before the helper is invoked, so this mock is permissive
                // (minTimes = 0) and shared across both assertions in this test.
                globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
                result = table;
                minTimes = 0;
            }
        };

        Assertions.assertEquals(1, new ConsistencyChecker().chooseTablets().size());

        // set table state to RESTORE, we will make sure checker will not choose its tablets.
        table.setState(OlapTable.OlapTableState.RESTORE);
        Assertions.assertEquals(0, new ConsistencyChecker().chooseTablets().size());
    }

    @Test
    public void testChooseTabletsSkipsAlreadyCheckedTablet(@Mocked GlobalStateMgr globalStateMgr) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long replicaId = 6L;
        long backendId = 7L;
        long physicalPartitionId = 8L;
        long visibleVersion = 2L;
        TStorageMedium medium = TStorageMedium.HDD;

        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Replica replica = new Replica(replicaId, backendId, visibleVersion, 1111,
                10, 1000, Replica.ReplicaState.NORMAL, -1, visibleVersion);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, medium);
        LocalTablet tablet = new LocalTablet(tabletId, Lists.newArrayList(replica));
        materializedIndex.addTablet(tablet, tabletMeta, false);

        // Pre-mark the tablet as already verified at the current visible version. This is the
        // state replayFinishConsistencyCheck would have left the tablet in after a successful
        // earlier run, and the checker should skip it on the next pass.
        tablet.setCheckedVersion(visibleVersion);
        tablet.setIsConsistent(true);

        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 3, null);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList());
        Partition partition = new Partition(partitionId, physicalPartitionId, "partition", materializedIndex, distributionInfo);
        partition.getDefaultPhysicalPartition().setVisibleVersion(visibleVersion, System.currentTimeMillis());
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.registerTableUnlocked(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDbIds();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getDb(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTables(dbId);
                result = database.getTables();
                minTimes = 0;

                globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
                result = table;
                minTimes = 0;
            }
        };

        // Tablet has already been verified at visibleVersion and is consistent; the checker
        // must not pick it for a redundant re-check.
        Assertions.assertEquals(0, new ConsistencyChecker().chooseTablets().size());
    }

    @Test
    public void testResetToBeCleanedTime() {
        TabletMeta tabletMeta = new TabletMeta(1, 2, 3,
                4, TStorageMedium.HDD);
        tabletMeta.setToBeCleanedTime(123L);
        tabletMeta.resetToBeCleanedTime();
        Assertions.assertNull(tabletMeta.getToBeCleanedTime());
    }

    @Test
    public void testOnStoppedClearsLeaderSessionState() throws Exception {
        ConsistencyChecker checker = new ConsistencyChecker();

        @SuppressWarnings("unchecked")
        Map<Long, CheckConsistencyJob> jobs =
                (Map<Long, CheckConsistencyJob>) FieldUtils.readField(checker, "jobs", true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<Long, Integer> creatingTableCounters =
                (ConcurrentHashMap<Long, Integer>) FieldUtils.readField(checker, "creatingTableCounters", true);

        jobs.put(101L, new CheckConsistencyJob(101L));
        jobs.put(102L, new CheckConsistencyJob(102L));
        creatingTableCounters.put(7L, 1);
        FieldUtils.writeField(checker, "lastTabletMetaCheckTime", 12345L, true);

        Assertions.assertEquals(2, jobs.size(), "precondition: jobs populated");
        Assertions.assertEquals(1, creatingTableCounters.size(), "precondition: counter populated");
        Assertions.assertEquals(12345L, FieldUtils.readField(checker, "lastTabletMetaCheckTime", true));

        // CREATE TABLE flows that bumped creatingTableCounters fail when the editlog is sealed,
        // so leftover counters would otherwise leak across leader sessions and incorrectly mask
        // tablets from future consistency checks. lastTabletMetaCheckTime is reset for the same
        // reason - the next leader should re-issue a tablet-meta scan instead of honoring a
        // watermark from a different session.
        MethodUtils.invokeMethod(checker, true, "onStopped");

        Assertions.assertTrue(jobs.isEmpty(), "jobs cleared on demotion");
        Assertions.assertTrue(creatingTableCounters.isEmpty(), "creatingTableCounters cleared on demotion");
        Assertions.assertEquals(0L, FieldUtils.readField(checker, "lastTabletMetaCheckTime", true),
                "lastTabletMetaCheckTime watermark reset on demotion");
    }

    @Test
    public void testOnStoppedSwallowsClearException() throws Exception {
        // A misbehaving CheckConsistencyJob.clear() must not abort the demotion drain - the
        // remaining jobs still need to be removed and the watermark/counters still need to be
        // reset. Exercises the per-job catch around job.clear().
        ConsistencyChecker checker = new ConsistencyChecker();

        @SuppressWarnings("unchecked")
        Map<Long, CheckConsistencyJob> jobs =
                (Map<Long, CheckConsistencyJob>) FieldUtils.readField(checker, "jobs", true);

        CheckConsistencyJob throwingJob = new CheckConsistencyJob(201L) {
            @Override
            public synchronized void clear() {
                throw new RuntimeException("simulated AgentTaskQueue failure");
            }
        };
        jobs.put(201L, throwingJob);
        jobs.put(202L, new CheckConsistencyJob(202L));

        MethodUtils.invokeMethod(checker, true, "onStopped");

        Assertions.assertTrue(jobs.isEmpty(),
                "jobs map must be cleared even when an individual job's clear() throws");
    }
}
