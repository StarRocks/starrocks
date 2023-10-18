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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LakeTableTxnStateListenerTest {
    long dbId = 9;
    long tableId = 10;
    long partitionId = 11;
    long indexId = 12;
    long[] tabletId = {13, 14};

    @Test
    public void testHasUnfinishedTablet() {
        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        TransactionCommitFailedException exception = Assert.assertThrows(TransactionCommitFailedException.class, () -> {
            listener.preCommit(newTransactionState(), buildPartialTabletCommitInfo(), Collections.emptyList());
        });
        Assert.assertTrue(exception.getMessage().contains("has unfinished tablets"));
    }

    @Test
    public void testCommitRateExceeded() {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return true;
            }
        };

        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        makeCompactionScoreExceedSlowdownThreshold();
        Assert.assertThrows(CommitRateExceededException.class, () -> {
            listener.preCommit(newTransactionState(), buildFullTabletCommitInfo(), Collections.emptyList());
        });
    }

    @Test
    public void testCommitRateLimiterDisabled() throws TransactionException {
        new MockUp<LakeTableTxnStateListener>() {
            @Mock
            boolean enableIngestSlowdown() {
                return false;
            }
        };

        LakeTable table = buildLakeTable();
        DatabaseTransactionMgr databaseTransactionMgr = addDatabaseTransactionMgr();
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(databaseTransactionMgr, table);
        makeCompactionScoreExceedSlowdownThreshold();
        listener.preCommit(newTransactionState(), buildFullTabletCommitInfo(), Collections.emptyList());
    }

    private LakeTable buildLakeTable() {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (long id : tabletId) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, 0, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(id, tabletMeta);
            index.addTablet(new LakeTablet(id), tabletMeta);
        }
        Partition partition = new Partition(partitionId, "p0", index, null);
        LakeTable table = new LakeTable(
                tableId, "t0",
                Lists.newArrayList(new Column("c0", Type.BIGINT)),
                KeysType.DUP_KEYS, null, null);
        table.addPartition(partition);
        return table;
    }

    private DatabaseTransactionMgr addDatabaseTransactionMgr() {
        GlobalStateMgr.getCurrentGlobalTransactionMgr().addDatabaseTransactionMgr(dbId);
        try {
            return GlobalStateMgr.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    private List<TabletCommitInfo> buildFullTabletCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<>();
        for (long id : tabletId) {
            tabletCommitInfos.add(new TabletCommitInfo(id, 1));
        }
        return tabletCommitInfos;
    }

    private List<TabletCommitInfo> buildPartialTabletCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<>();
        tabletCommitInfos.add(new TabletCommitInfo(tabletId[0], 1));
        return tabletCommitInfos;
    }

    private TransactionState newTransactionState() {
        long currentTimeMs = System.currentTimeMillis();
        TransactionState transactionState =
                new TransactionState(dbId, Lists.newArrayList(tableId), 123456L, "label", null,
                        TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, null, 0, 60_000);
        transactionState.setPrepareTime(currentTimeMs - 10_000);
        transactionState.setWriteEndTimeMs(currentTimeMs);
        return transactionState;
    }

    private void makeCompactionScoreExceedSlowdownThreshold() {
        long currentTimeMs = System.currentTimeMillis();
        CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(Config.lake_ingest_slowdown_threshold + 10.0)));
    }
}
