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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PublishVersionDaemon;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import mockit.Mock;
import mockit.MockUp;

import java.util.ArrayList;
import java.util.List;

/**
 * A load that commits on a table while an alter or reshard job on that table is under way, as a stream load
 * would. It writes every tablet of every index, so it also writes a shadow index the job has already exposed.
 */
public class ConcurrentLoad {
    /**
     * Keeps the publish daemon from publishing loads for the rest of the test, so that a committed load
     * becomes visible exactly when the test calls {@link #makeVisible}.
     */
    public static void holdPublishDaemon() {
        new MockUp<PublishVersionDaemon>() {
            @Mock
            public void runAfterLeaseValid() {
            }
        };
    }

    /**
     * Begins a load and commits it on every tablet of the table, holding the table WRITE lock as a load does.
     */
    public static TransactionState commit(Database db, OlapTable table) throws Exception {
        GlobalTransactionMgr txnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long txnId = txnMgr.beginTransaction(db.getId(), List.of(table.getId()), "load_" + UUIDUtil.genUUID(),
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe"),
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        long backendId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().get(0);
        List<TabletCommitInfo> commitInfos = new ArrayList<>();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index : partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    commitInfos.add(new TabletCommitInfo(tablet.getId(), backendId));
                }
            }
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE);
        try {
            txnMgr.commitTransaction(db.getId(), txnId, commitInfos, new ArrayList<>(), null);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE);
        }
        return txnMgr.getTransactionState(db.getId(), txnId);
    }

    /**
     * Finishes a committed load at its own version, as its successful publish would.
     */
    public static void makeVisible(Database db, TransactionState load) throws Exception {
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .finishTransaction(db.getId(), load.getTransactionId(), null);
    }

    /**
     * The version the load committed in the given physical partition.
     */
    public static long versionIn(TransactionState load, OlapTable table, long physicalPartitionId) {
        return load.getTableCommitInfo(table.getId()).getPartitionCommitInfo(physicalPartitionId).getVersion();
    }
}
