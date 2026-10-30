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

import com.google.common.collect.Lists;
<<<<<<< HEAD
=======
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
>>>>>>> 4d43676 ([BugFix] Prevent incorrect global dictionary renewal across FEs (#79511))
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
<<<<<<< HEAD
=======
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.TStorageMedium;
>>>>>>> 4d43676 ([BugFix] Prevent incorrect global dictionary renewal across FEs (#79511))
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

<<<<<<< HEAD
=======
import java.util.Arrays;
import java.util.HashMap;
>>>>>>> 4d43676 ([BugFix] Prevent incorrect global dictionary renewal across FEs (#79511))
import java.util.List;

public class LakeTableTxnLogApplierTest extends LakeTableTestHelper {
    @Test
    public void testCommitAndApply() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testDictionaryIdentityFallbackForWholeList() {
        for (List<Long> identities : Arrays.<List<Long>>asList(null, List.of(), List.of(0L, -7L))) {
            LakeTable table = buildLakeTable();
            TransactionState state = newTransactionState();
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            ColumnId a = ColumnId.create("a");
            ColumnId b = ColumnId.create("b");
            PartitionCommitInfo partition = new PartitionCommitInfo(physicalPartitionId, 2, 200,
                    List.of(), List.of(a, b), List.of(100L, 101L));
            if (identities != null) {
                partition.setDictContentIdentities(identities);
            }
            TableCommitInfo commit = new TableCommitInfo(tableId);
            commit.addPartitionCommitInfo(partition);
            IDictManager manager = Mockito.mock(IDictManager.class);
            try (MockedStatic<IDictManager> mocked = Mockito.mockStatic(IDictManager.class)) {
                mocked.when(IDictManager::getInstance).thenReturn(manager);
                new LakeTableTxnLogApplier(table).applyVisibleLog(state, commit, null);
                boolean hasIdentity = identities != null && !identities.isEmpty();
                Mockito.verify(manager).updateGlobalDict(table, a, 100L, 200L, hasIdentity ? 0L : null);
                Mockito.verify(manager).updateGlobalDict(table, b, 101L, 200L, hasIdentity ? -7L : null);
                Mockito.verifyNoMoreInteractions(manager);
            }
        }
    }

    @Test
    public void testCommitAndApplyCompaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newCompactionTransactionState();
        CompactionTxnCommitAttachment attachment = new CompactionTxnCommitAttachment(true);
        state.setTxnCommitAttachment(attachment);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testApplyCommitLogWithDroppedPartition() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId - 1, 2, 0);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }

    @Test
    public void testApplyVisibleLogBatchPublishesOnlyTheFinalVersion() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);

        // Record every version the partition is ever made visible at, in order.
        List<Long> publishedVersions = Lists.newArrayList();
        new MockUp<PhysicalPartition>() {
            @Mock
            public void setVisibleVersion(Invocation invocation, long visibleVersion, long visibleVersionTime) {
                publishedVersions.add(visibleVersion);
                invocation.proceed(visibleVersion, visibleVersionTime);
            }
        };

        // Three batched load transactions taking the partition from version 1 to version 4.
        long baseVersionTime = System.currentTimeMillis();
        List<TransactionState> states = Lists.newArrayList();
        for (long version = 2; version <= 4; version++) {
            TransactionState state = newTransactionState();
            state.setTransactionStatus(TransactionStatus.VISIBLE);
            PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, version, 0);
            partitionCommitInfo.setVersionTime(baseVersionTime + version);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            state.putIdToTableCommitInfo(tableId, tableCommitInfo);
            states.add(state);
        }

        applier.applyVisibleLogBatch(new TransactionStateBatch(states), /*unused*/null);

        // Versions 2 and 3 get no tablet metadata object of their own, so they must never become
        // visible: the partition jumps straight from 1 to the batch's final version.
        Assertions.assertEquals(Lists.newArrayList(4L), publishedVersions);
        PhysicalPartition partition = table.getPartition(partitionId).getDefaultPhysicalPartition();
        Assertions.assertEquals(4, partition.getVisibleVersion());
        Assertions.assertEquals(baseVersionTime + 4, partition.getVisibleVersionTime());
    }
}
