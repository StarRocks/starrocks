// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.lake.CompactionManager;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.PartitionIdentifier;
import com.starrocks.server.GlobalStateMgr;

public class LakeTableTxnLogApplier implements TransactionLogApplier {
    private final Database db;
    private final LakeTable table;

    LakeTableTxnLogApplier(Database db, LakeTable table) {
        this.db = db;
        this.table = table;
    }

    @Override
    public void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo) {
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            Partition partition = table.getPartition(partitionId);
            partition.setNextVersion(partition.getNextVersion() + 1);
        }
    }

    @Override
    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo) {
        CompactionManager compactionManager = GlobalStateMgr.getCurrentState().getCompactionManager();
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            Partition partition = table.getPartition(partitionCommitInfo.getPartitionId());
            long version = partitionCommitInfo.getVersion();
            long versionTime = partitionCommitInfo.getVersionTime();
            partition.updateVisibleVersion(version, versionTime);

            PartitionIdentifier partitionIdentifier = new PartitionIdentifier(db.getId(), table.getId(), partition.getId());
            compactionManager.handlePartitionVersionUpdated(partitionIdentifier);
        }
    }
}
