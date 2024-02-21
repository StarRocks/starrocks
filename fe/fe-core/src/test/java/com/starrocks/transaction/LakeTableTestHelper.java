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
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;

import java.util.ArrayList;
import java.util.List;

public class LakeTableTestHelper {
    long dbId = 9000;
    long tableId = 9001;
    long partitionId = 9002;
    long indexId = 9003;
    long[] tabletId = {9004, 90005};
    long nextTxnId = 10000;

    LakeTable buildLakeTable() {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
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

    DatabaseTransactionMgr addDatabaseTransactionMgr() {
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(dbId);
        try {
            return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    List<TabletCommitInfo> buildFullTabletCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<>();
        for (long id : tabletId) {
            tabletCommitInfos.add(new TabletCommitInfo(id, 1));
        }
        return tabletCommitInfos;
    }

    List<TabletCommitInfo> buildPartialTabletCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<>();
        tabletCommitInfos.add(new TabletCommitInfo(tabletId[0], 1));
        return tabletCommitInfos;
    }

    TransactionState newTransactionState() {
        long txnId = nextTxnId++;
        long currentTimeMs = System.currentTimeMillis();
        TransactionState transactionState =
                new TransactionState(dbId, Lists.newArrayList(tableId), txnId, "label", null,
                        TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, null, 0, 60_000);
        transactionState.setPrepareTime(currentTimeMs - 10_000);
        transactionState.setWriteEndTimeMs(currentTimeMs);
        return transactionState;
    }
}
