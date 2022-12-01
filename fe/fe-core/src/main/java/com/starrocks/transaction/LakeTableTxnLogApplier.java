// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.compaction.CompactionManager;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;

import java.util.List;

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

    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db) {
        List<String> validDictCacheColumns = Lists.newArrayList();
        long maxPartitionVersionTime = -1;
        long tableId = table.getId();
        CompactionManager compactionManager = GlobalStateMgr.getCurrentState().getCompactionManager();
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            Partition partition = table.getPartition(partitionCommitInfo.getPartitionId());
            long version = partitionCommitInfo.getVersion();
            long versionTime = partitionCommitInfo.getVersionTime();
            Quantiles compactionScore = partitionCommitInfo.getCompactionScore();
            Preconditions.checkState(version == partition.getVisibleVersion() + 1);
            partition.updateVisibleVersion(version, versionTime);

            PartitionIdentifier partitionIdentifier =
                    new PartitionIdentifier(txnState.getDbId(), table.getId(), partition.getId());
            if (txnState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                compactionManager.handleCompactionFinished(partitionIdentifier, version, versionTime, compactionScore);
            } else {
                compactionManager.handleLoadingFinished(partitionIdentifier, version, versionTime, compactionScore);
            }

            if (!partitionCommitInfo.getInvalidDictCacheColumns().isEmpty()) {
                for (String column : partitionCommitInfo.getInvalidDictCacheColumns()) {
                    IDictManager.getInstance().removeGlobalDict(tableId, column);
                }
            }
            if (!partitionCommitInfo.getValidDictCacheColumns().isEmpty()) {
                validDictCacheColumns = partitionCommitInfo.getValidDictCacheColumns();
            }
            maxPartitionVersionTime = Math.max(maxPartitionVersionTime, versionTime);
        }
        for (String column : validDictCacheColumns) {
            IDictManager.getInstance().updateGlobalDict(tableId, column, maxPartitionVersionTime);
        }
    }
}
