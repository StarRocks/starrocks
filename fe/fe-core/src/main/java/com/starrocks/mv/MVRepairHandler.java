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
        private final long curVersion; // new commit partition visible version
        private final long curVersionTime; // new commit partition visible version time
        private final long newVersion; // new commit partition visible version
        private final long newVersionTime; // new commit partition visible version time

        public PartitionRepairInfo(Partition partition, long newVersion, long newVersionTime) {
            this.partitionId = partition.getId();
            this.partitionName = partition.getName();
            this.curVersion = partition.getVisibleVersion();
            this.curVersionTime = partition.getVisibleVersionTime();
            this.newVersion = newVersion;
            this.newVersionTime = newVersionTime;
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

        public long getCurVersion() {
            return curVersion;
        }

        public long getCurVersionTime() {
            return curVersionTime;
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
            Table table = db.getTable(tableCommitInfo.getTableId());
            if (table == null || !(table instanceof OlapTable) || table.getRelatedMaterializedViews().isEmpty()) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            Map<Long, PartitionCommitInfo> partitionCommitInfos = tableCommitInfo.getIdToPartitionCommitInfo();
            List<PartitionRepairInfo> partitionRepairInfos = Lists.newArrayListWithCapacity(partitionCommitInfos.size());

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                for (PartitionCommitInfo partitionCommitInfo : partitionCommitInfos.values()) {
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null || olapTable.isTempPartition(partitionId)) {
                        continue;
                    }
                    PartitionRepairInfo partitionRepairInfo = new PartitionRepairInfo(partition,
                            partitionCommitInfo.getVersion(), partitionCommitInfo.getVersionTime());
                    partitionRepairInfos.add(partitionRepairInfo);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }

            if (partitionRepairInfos.isEmpty()) {
                continue;
            }

            handleMVRepair(db, table, partitionRepairInfos);
        }
    }
}
