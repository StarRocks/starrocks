// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.starrocks.catalog.Partition;
import com.starrocks.lake.LakeTable;

public class LakeTableTxnLogApplier implements TransactionLogApplier {
    private final LakeTable table;

    LakeTableTxnLogApplier(LakeTable table) {
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
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            Partition partition = table.getPartition(partitionCommitInfo.getPartitionId());
            long version = partitionCommitInfo.getVersion();
            long versionTime = partitionCommitInfo.getVersionTime();
            partition.updateVisibleVersion(version, versionTime);
        }
    }
}
