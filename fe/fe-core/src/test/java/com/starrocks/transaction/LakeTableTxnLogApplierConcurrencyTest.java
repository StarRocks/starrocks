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

package com.starrocks.transaction;

import com.starrocks.alter.reshard.ReshardingPhysicalPartition;
import com.starrocks.alter.reshard.SplitTabletJob;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class LakeTableTxnLogApplierConcurrencyTest extends LakeTableTestHelper {
    private static boolean replayCheckpoint;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPublishScopesSharedFileProofAndScansJobsOncePerLockScope(boolean batch) {
        LakeTable table = buildLakeTable();
        long freePartitionId = physicalPartitionId + 1;
        MaterializedIndex freeIndex = new MaterializedIndex(indexId + 1);
        for (long id : new long[] {91001L, 91002L}) {
            freeIndex.addTablet(new LakeTablet(id), new TabletMeta(dbId, tableId, freePartitionId,
                    indexId + 1, TStorageMedium.HDD, true));
        }
        table.addPartition(new Partition(partitionId + 1, freePartitionId, "p1", freeIndex, null));
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        TabletReshardJobMgr manager = companionJobManager(table, physicalPartitionId);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return false;
            }
        };
        List<TransactionState> states = new java.util.ArrayList<>();
        for (long version : new long[] {2L, 3L}) {
            TransactionState state = newTransactionState();
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            TableCommitInfo commit = new TableCommitInfo(tableId);
            for (long id : new long[] {physicalPartitionId, freePartitionId}) {
                PartitionCommitInfo partitionCommit = new PartitionCommitInfo(id, version, 100L);
                for (com.starrocks.catalog.Tablet tablet : table.getPhysicalPartition(id).getLatestBaseIndex().getTablets()) {
                    TabletStatPB stat = new TabletStatPB();
                    stat.numRows = 5L;
                    stat.dataSize = 100L;
                    stat.hasSharedFiles = false;
                    partitionCommit.getTabletStats().put(tablet.getId(), stat);
                }
                commit.addPartitionCommitInfo(partitionCommit);
            }
            state.putIdToTableCommitInfo(tableId, commit);
            states.add(state);
        }
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        Database db = new Database(dbId, "db");
        if (batch) {
            applier.applyVisibleLogBatch(new TransactionStateBatch(states), db);
        } else {
            for (TransactionState state : states) {
                applier.applyVisibleLog(state, state.getTableCommitInfo(tableId), db);
            }
        }
        for (com.starrocks.catalog.Tablet tablet : freeIndex.getTablets()) {
            Assertions.assertFalse(((LakeTablet) tablet).hasSharedFiles());
            Assertions.assertEquals(5L, ((LakeTablet) tablet).getRowCount(-1));
        }
        for (com.starrocks.catalog.Tablet tablet : table.getPhysicalPartition(physicalPartitionId)
                .getLatestBaseIndex().getTablets()) {
            Assertions.assertTrue(((LakeTablet) tablet).hasSharedFiles());
            Assertions.assertEquals(5L, ((LakeTablet) tablet).getRowCount(-1));
        }
        Mockito.verify(manager, Mockito.times(batch ? 1 : 2)).getReshardingPhysicalPartitionIdsLocked(table);
    }

    @ParameterizedTest
    @EnumSource(value = OlapTable.OlapTableState.class, names = {"NORMAL", "TABLET_RESHARD"},
            mode = EnumSource.Mode.EXCLUDE)
    public void testOtherAlterStatesKeepPublishSharedFileProofConservative(OlapTable.OlapTableState state) {
        LakeTable table = buildLakeTable();
        table.setState(state);
        companionJobManager(table, physicalPartitionId - 1);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return false;
            }
        };
        TransactionState txn = newTransactionState();
        txn.setTransactionStatus(TransactionStatus.VISIBLE);
        TableCommitInfo commit = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommit = new PartitionCommitInfo(physicalPartitionId, 2L, 100L);
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 5L;
        stat.dataSize = 100L;
        stat.hasSharedFiles = false;
        partitionCommit.getTabletStats().put(tabletId[0], stat);
        commit.addPartitionCommitInfo(partitionCommit);
        new LakeTableTxnLogApplier(table).applyVisibleLog(txn, commit, new Database(dbId, "db"));
        LakeTablet tablet = (LakeTablet) table.getPhysicalPartition(physicalPartitionId).getLatestBaseIndex()
                .getTablet(tabletId[0]);
        Assertions.assertTrue(tablet.hasSharedFiles());
        Assertions.assertEquals(5L, tablet.getRowCount(-1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplaySkipsOccupancyQueryWithoutPublishMetadataLock(boolean checkpoint) {
        replayCheckpoint = checkpoint;
        LakeTable table = buildLakeTable();
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        TabletReshardJobMgr manager = companionJobManager(table, physicalPartitionId);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return checkpoint;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return replayCheckpoint;
            }
        };
        // Checkpoint takes the leader=true branch above, but it must still skip transient stat work.
        // Follower replay has no table metadata lock, so it must never call the locked occupancy API.
        TransactionState txn = newTransactionState();
        txn.setTransactionStatus(TransactionStatus.VISIBLE);
        TableCommitInfo commit = new TableCommitInfo(tableId);
        commit.addPartitionCommitInfo(new PartitionCommitInfo(physicalPartitionId, 2L, 100L));
        new LakeTableTxnLogApplier(table).applyVisibleLog(txn, commit, new Database(dbId, "db"));
        Assertions.assertEquals(2L, table.getPhysicalPartition(physicalPartitionId).getVisibleVersion());
        Mockito.verify(manager, Mockito.never()).getReshardingPhysicalPartitionIdsLocked(table);
    }

    private TabletReshardJobMgr companionJobManager(OlapTable table, long busyPartitionId) {
        TabletReshardJobMgr manager = Mockito.spy(new TabletReshardJobMgr());
        Map<Long, TabletReshardJob> jobs = manager.getTabletReshardJobs();
        jobs.put(1L, new SplitTabletJob(1L, 1L, table.getId(),
                Map.of(busyPartitionId, new ReshardingPhysicalPartition(busyPartitionId, Map.of()))));
        for (long id = 2; id <= 20; id++) {
            SplitTabletJob historical = new SplitTabletJob(id, 1L, table.getId(),
                    Map.of(busyPartitionId + 1, new ReshardingPhysicalPartition(busyPartitionId + 1, Map.of())));
            Deencapsulation.setField(historical, "jobState", TabletReshardJob.JobState.FINISHED);
            jobs.put(id, historical);
        }
        new MockUp<GlobalStateMgr>() {
            @Mock
            public TabletReshardJobMgr getTabletReshardJobMgr() {
                return manager;
            }
        };
        return manager;
    }
}
