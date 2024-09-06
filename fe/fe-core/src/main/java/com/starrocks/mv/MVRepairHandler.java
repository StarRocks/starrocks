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

package com.starrocks.mv;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;

import java.util.List;
import java.util.Map;

public interface MVRepairHandler {

    class PartitionRepairInfo {
        private final long partitionId; // partition id
        private final String partitionName; // partition name
        private final long lastVersion; // last commit partition visible version
        private final long newVersion; // new commit partition visible version
        private final long newVersionTime; // new commit partition visible version time

        public PartitionRepairInfo(long partitionId,
                                   String partitionName,
                                   long lastVersion,
                                   long newVersion,
                                   long newVersionTime) {
            this.partitionId = partitionId;
            this.partitionName = partitionName;
            this.newVersion = newVersion;
            this.newVersionTime = newVersionTime;
            this.lastVersion = lastVersion;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public long getNewVersion() {
            return newVersion;
        }

        public long getNewVersionTime() {
            return newVersionTime;
        }

        public long getLastVersion() {
            return lastVersion;
        }
    }

    // Only called in fe leader
    // Caller should ensure that this interface is not called in the db lock
    void handleMVRepair(Database db, Table table, List<PartitionRepairInfo> partitionRepairInfos);

    // Only called in fe leader
    // Caller should ensure that this interface is not called in the db lock
    default void handleMVRepair(TransactionState transactionState) {
        if (transactionState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            return;
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(transactionState.getDbId());
        if (db == null) {
            return;
        }

        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableCommitInfo.getTableId());
            if (table == null || !(table instanceof OlapTable) || table.getRelatedMaterializedViews().isEmpty()) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            Map<Long, PartitionCommitInfo> partitionCommitInfos = tableCommitInfo.getIdToPartitionCommitInfo();
            List<PartitionRepairInfo> partitionRepairInfos = Lists.newArrayListWithCapacity(partitionCommitInfos.size());

            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
            try {
                for (PartitionCommitInfo partitionCommitInfo : partitionCommitInfos.values()) {
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null || olapTable.isTempPartition(partitionId)) {
                        continue;
                    }
                    // TODO(fixme): last version/version time is not kept in transaction state, use version - 1 for last commit
                    //  version.
                    // TODO: we may add last version time to check mv's version map with base table's version time.
                    PartitionRepairInfo partitionRepairInfo = new PartitionRepairInfo(partition.getId(),
                            partition.getName(), partitionCommitInfo.getVersion()  - 1,
                            partitionCommitInfo.getVersion(), partitionCommitInfo.getVersionTime());
                    partitionRepairInfos.add(partitionRepairInfo);
                }
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
            }

            if (partitionRepairInfos.isEmpty()) {
                continue;
            }

            handleMVRepair(db, table, partitionRepairInfos);
        }
    }
}
