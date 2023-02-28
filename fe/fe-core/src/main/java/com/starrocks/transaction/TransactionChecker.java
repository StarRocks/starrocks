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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;

import java.util.ArrayList;
import java.util.List;

/**
 * Used by the {@link PublishVersionDaemon} to check that the transaction is finished/visible.
 * It will check that all involved tablets are reaching desired version in quorum.
 * It will also record all the involved replica's version, so they can be persisted when
 * the transaction's publish state is persisted.
 * <p>
 * Note: originally, the check logic is done in DatabaseTransactionManager and every check require db lock held
 * This class implement an optimization that doesn't require db lock to be held.
 */
public class TransactionChecker {
    private List<PartitionChecker> partitions;

    public TransactionChecker(List<PartitionChecker> partitions) {
        this.partitions = partitions;
    }

    public boolean finished(TxnFinishState finishState) {
        for (PartitionChecker p : partitions) {
            if (!p.finished(finishState)) {
                return false;
            }
        }
        return true;
    }

    // return abnormal tablets/replicas which is causing this txn unfinished
    public String debugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("errors:");
        int totalTablet = 0;
        for (PartitionChecker p : partitions) {
            p.debugInfo(sb);
            totalTablet += p.tablets.size();
        }
        sb.append(String.format(" #partition:%d #tablet:%d", partitions.size(), totalTablet));
        return sb.toString();
    }

    static class PartitionChecker {
        long partitionId;
        long version;
        long quorum;
        List<LocalTablet> tablets = new ArrayList<>();

        PartitionChecker(long partitionId, long version, long quorum) {
            this.partitionId = partitionId;
            this.version = version;
            this.quorum = quorum;
        }

        boolean finished(TxnFinishState finishState) {
            for (LocalTablet t : tablets) {
                if (!t.quorumReachVersion(version, quorum, finishState)) {
                    return false;
                }
            }
            return true;
        }

        void debugInfo(StringBuilder sb) {
            for (LocalTablet t : tablets) {
                t.getAbnormalReplicaInfos(version, quorum, sb);
            }
        }
    }

    // Note: caller should hold db lock
    public static TransactionChecker create(TransactionState txn, Database db) {
        List<PartitionChecker> partitions = new ArrayList<>();
        for (TableCommitInfo tableCommitInfo : txn.getIdToTableCommitInfos().values()) {
            OlapTable table = (OlapTable) db.getTable(tableCommitInfo.getTableId());
            if (table == null || table.isCloudNativeTable()) {
                continue;
            }
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                PartitionChecker partitionChecker = new PartitionChecker(partitionId, partitionCommitInfo.getVersion(),
                        table.getPartitionInfo().getQuorumNum(partitionId, table.writeQuorum()));
                List<MaterializedIndex> allIndices = txn.getPartitionLoadedTblIndexes(tableCommitInfo.getTableId(), partition);
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        partitionChecker.tablets.add((LocalTablet) tablet);
                    }
                }
                partitions.add(partitionChecker);
            }
        }
        return new TransactionChecker(partitions);
    }
}
