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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Util;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class OlapTableRollupJobBuilder extends AlterJobV2Builder {
    private static final Logger LOG = LogManager.getLogger(OlapTableRollupJobBuilder.class);
    private OlapTable olapTable;

    public OlapTableRollupJobBuilder(OlapTable table) {
        this.olapTable = table;
    }

    @Override
    public AlterJobV2 build() throws StarRocksException {
        int baseSchemaHash = olapTable.getSchemaHashByIndexId(baseIndexId);
        // mvSchemaVersion will keep same with the src MaterializedIndex
        int mvSchemaVersion = olapTable.getIndexMetaByIndexId(baseIndexId).getSchemaVersion();
        int mvSchemaHash = Util.schemaHash(0 /* init schema version */, rollupColumns, olapTable.getBfColumnNames(),
                olapTable.getBfFpp());

        AlterJobV2 mvJob = new RollupJobV2(jobId, dbId, olapTable.getId(), olapTable.getName(), timeoutMs,
                baseIndexId, rollupIndexId, baseIndexName, rollupIndexName, mvSchemaVersion,
                rollupColumns, whereClause, baseSchemaHash, mvSchemaHash,
                rollupKeysType, rollupShortKeyColumnCount, origStmt, viewDefineSql, isColocateMVIndex);

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Tablet> addedTablets = Lists.newArrayList();
        for (Partition partition : olapTable.getPartitions()) {
            long partitionId = partition.getId();
            TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partitionId);

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                // index state is SHADOW
                MaterializedIndex mvIndex = new MaterializedIndex(rollupIndexId, MaterializedIndex.IndexState.SHADOW);
                MaterializedIndex baseIndex = physicalPartition.getIndex(baseIndexId);
                TabletMeta mvTabletMeta = new TabletMeta(dbId, olapTable.getId(),
                        physicalPartitionId, rollupIndexId, mvSchemaHash, medium);
                for (Tablet baseTablet : baseIndex.getTablets()) {
                    long baseTabletId = baseTablet.getId();
                    long mvTabletId = globalStateMgr.getNextId();

                    Tablet newTablet = new LocalTablet(mvTabletId);

                    mvIndex.addTablet(newTablet, mvTabletMeta);
                    addedTablets.add(newTablet);

                    mvJob.addTabletIdMap(physicalPartitionId, mvTabletId, baseTabletId);

                    List<Replica> baseReplicas = ((LocalTablet) baseTablet).getImmutableReplicas();

                    int healthyReplicaNum = 0;
                    for (Replica baseReplica : baseReplicas) {
                        long mvReplicaId = globalStateMgr.getNextId();
                        long backendId = baseReplica.getBackendId();
                        if (baseReplica.getState() == Replica.ReplicaState.CLONE
                                || baseReplica.getState() == Replica.ReplicaState.DECOMMISSION
                                || baseReplica.getLastFailedVersion() > 0) {
                            LOG.info(
                                    "base replica {} of tablet {} state is {}, and last failed version is {}, " +
                                            "skip creating rollup replica",
                                    baseReplica.getId(), baseTabletId, baseReplica.getState(),
                                    baseReplica.getLastFailedVersion());
                            continue;
                        }
                        Preconditions
                                .checkState(baseReplica.getState() == Replica.ReplicaState.NORMAL, baseReplica.getState());
                        // replica's init state is ALTER, so that tablet report process will ignore its report
                        Replica mvReplica = new Replica(mvReplicaId, backendId, Replica.ReplicaState.ALTER,
                                Partition.PARTITION_INIT_VERSION,
                                mvSchemaHash);
                        ((LocalTablet) (newTablet)).addReplica(mvReplica);
                        healthyReplicaNum++;
                    } // end for baseReplica

                    if (healthyReplicaNum < replicationNum / 2 + 1) {
                        /*
                         * TODO(cmy): This is a bad design.
                         * Because in the rollup job, we will only send tasks to the rollup replicas that have been created,
                         * without checking whether the quorum of replica number are satisfied.
                         * This will cause the job to fail until we find that the quorum of replica number
                         * is not satisfied until the entire job is done.
                         * So here we check the replica number strictly and do not allow to submit the job
                         * if the quorum of replica number is not satisfied.
                         */
                        for (Tablet tablet : addedTablets) {
                            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tablet.getId());
                        }
                        throw new DdlException("tablet " + baseTabletId + " has few healthy replica: " + healthyReplicaNum);
                    }
                } // end for baseTablets

                mvJob.addMVIndex(physicalPartitionId, mvIndex);

                LOG.debug("create materialized view index {} based on index {} in partition {}:{}",
                        rollupIndexId, baseIndexId, partitionId, physicalPartitionId);
            }
        } // end for partitions
        return mvJob;
    }
}
