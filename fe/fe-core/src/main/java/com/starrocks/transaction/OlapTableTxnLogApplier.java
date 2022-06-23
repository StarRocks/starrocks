// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.sql.optimizer.statistics.IDictManager;

import java.util.List;
import java.util.Set;

public class OlapTableTxnLogApplier implements TransactionLogApplier {
    private final OlapTable table;

    public OlapTableTxnLogApplier(OlapTable table) {
        this.table = table;
    }

    @Override
    public void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo) {
        Set<Long> errorReplicaIds = txnState.getErrorReplicas();
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            Partition partition = table.getPartition(partitionId);
            List<MaterializedIndex> allIndices =
                    partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                        if (errorReplicaIds.contains(replica.getId())) {
                            // should get from transaction state
                            replica.updateLastFailedVersion(partitionCommitInfo.getVersion());
                        }
                    }
                }
            }
            partition.setNextVersion(partition.getNextVersion() + 1);
        }
    }

    @Override
    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo) {
        Set<Long> errorReplicaIds = txnState.getErrorReplicas();
        long tableId = table.getId();
        List<String> validDictCacheColumns = Lists.newArrayList();
        long maxPartitionVersionTime = -1;
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            long newCommitVersion = partitionCommitInfo.getVersion();
            Partition partition = table.getPartition(partitionId);
            List<MaterializedIndex> allIndices =
                    partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                        long lastFailedVersion = replica.getLastFailedVersion();
                        long newVersion = newCommitVersion;
                        long lastSucessVersion = replica.getLastSuccessVersion();
                        if (!errorReplicaIds.contains(replica.getId())) {
                            if (replica.getLastFailedVersion() > 0) {
                                // if the replica is a failed replica, then not changing version
                                newVersion = replica.getVersion();
                            } else if (!replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
                                // this means the replica has error in the past, but we did not observe it
                                // during upgrade, one job maybe in quorum finished state, for example, A,B,C 3 replica
                                // A,B 's version is 10, C's version is 10 but C' 10 is abnormal should be rollback
                                // then we will detect this and set C's last failed version to 10 and last success version to 11
                                // this logic has to be replayed in checkpoint thread
                                lastFailedVersion = partition.getVisibleVersion();
                                newVersion = replica.getVersion();
                            }

                            // success version always move forward
                            lastSucessVersion = newCommitVersion;
                        } else {
                            // for example, A,B,C 3 replicas, B,C failed during publish version, then B C will be set abnormal
                            // all loading will failed, B,C will have to recovery by clone, it is very inefficient and maybe lost data
                            // Using this method, B,C will publish failed, and fe will publish again, not update their last failed version
                            // if B is publish successfully in next turn, then B is normal and C will be set abnormal so that quorum is maintained
                            // and loading will go on.
                            newVersion = replica.getVersion();
                            if (newCommitVersion > lastFailedVersion) {
                                lastFailedVersion = newCommitVersion;
                            }
                        }
                        replica.updateVersionInfo(newVersion, lastFailedVersion, lastSucessVersion);
                    }
                }
            } // end for indices
            long version = partitionCommitInfo.getVersion();
            long versionTime = partitionCommitInfo.getVersionTime();
            partition.updateVisibleVersion(version, versionTime);
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
