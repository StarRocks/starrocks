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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class OlapTableTxnLogApplier implements TransactionLogApplier {
    private static final Logger LOG = LogManager.getLogger(OlapTableTxnLogApplier.class);
    // olap table or olap materialized view
    private final OlapTable table;

    public OlapTableTxnLogApplier(OlapTable table) {
        this.table = table;
    }

    @Override
    public void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo) {
        Set<Long> errorReplicaIds = txnState.getErrorReplicas();
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.warn("partition {} is dropped, ignore", partitionId);
                continue;
            }
            List<MaterializedIndex> allIndices =
                    partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
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
    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db) {
        Set<Long> errorReplicaIds = txnState.getErrorReplicas();
        long tableId = table.getId();
        OlapTable table = (OlapTable) db.getTable(tableId);
        if (table == null) {
            LOG.warn("table {} is dropped, ignore", tableId);
            return;
        }
        List<String> validDictCacheColumns = Lists.newArrayList();
        List<Long> dictCollectedVersions = Lists.newArrayList();

        long maxPartitionVersionTime = -1;

        table.lastVersionUpdateStartTime.set(System.currentTimeMillis());

        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.warn("partition {} is dropped, ignore", partitionId);
                continue;
            }
            short replicationNum = table.getPartitionInfo().getReplicationNum(partitionId);
            long version = partitionCommitInfo.getVersion();
            List<MaterializedIndex> allIndices =
                    partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    boolean hasFailedVersion = false;
                    List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                    for (Replica replica : replicas) {
                        if (txnState.isNewFinish()) {
                            updateReplicaVersion(version, replica, txnState.getFinishState());
                            continue;
                        }
                        long lastFailedVersion = replica.getLastFailedVersion();
                        long newVersion = version;
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
                                hasFailedVersion = true;
                                newVersion = replica.getVersion();
                            }

                            // success version always move forward
                            lastSucessVersion = version;
                        } else {
                            // for example, A,B,C 3 replicas, B,C failed during publish version,
                            // then B C will be set abnormal and all loadings will be failed, B,C will have to recover
                            // by clone, it is very inefficient and may lose data.
                            // Using this method, B,C will publish failed, and fe will publish again,
                            // not update their last failed version.
                            // if B is published successfully in next turn, then B is normal and C will be set
                            // abnormal so that quorum is maintained and loading will go on.
                            newVersion = replica.getVersion();
                            if (version > lastFailedVersion) {
                                lastFailedVersion = version;
                                hasFailedVersion = true;
                            }
                        }
                        replica.updateVersionInfo(newVersion, lastFailedVersion, lastSucessVersion);
                    } // end for replicas

                    if (hasFailedVersion && replicationNum == 1) {
                        TabletScheduler.resetDecommStatForSingleReplicaTabletUnlocked(tablet.getId(), replicas);
                    }
                } // end for tablets
            } // end for indices
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
            if (!partitionCommitInfo.getDictCollectedVersions().isEmpty()) {
                dictCollectedVersions = partitionCommitInfo.getDictCollectedVersions();
            }
            maxPartitionVersionTime = Math.max(maxPartitionVersionTime, versionTime);
        }

        table.lastVersionUpdateEndTime.set(System.currentTimeMillis());
        if (!GlobalStateMgr.isCheckpointThread() && dictCollectedVersions.size() == validDictCacheColumns.size()) {
            for (int i = 0; i < validDictCacheColumns.size(); i++) {
                String columnName = validDictCacheColumns.get(i);
                long collectedVersion = dictCollectedVersions.get(i);
                IDictManager.getInstance()
                        .updateGlobalDict(tableId, columnName, collectedVersion, maxPartitionVersionTime);
            }
        }
    }

    private void updateReplicaVersion(long version, Replica replica, TxnFinishState finishState) {
        if (finishState.normalReplicas.contains(replica.getId())) {
            replica.updateVersion(version);
        } else {
            Long v = finishState.abnormalReplicasWithVersion.get(replica.getId());
            if (v != null) {
                replica.updateVersion(v);
            }
            if (replica.getVersion() < version && replica.getState() != Replica.ReplicaState.ALTER) {
                // update replica's last failed version, to be compatible with existing code
                replica.updateVersionInfo(replica.getVersion(), version, replica.getLastSuccessVersion());
            }
        }
    }
}
