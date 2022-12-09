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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Util;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class OlapTableAlterJobV2Builder extends AlterJobV2Builder {
    private static final Logger LOG = LogManager.getLogger(OlapTableAlterJobV2Builder.class);

    private final OlapTable table;

    public OlapTableAlterJobV2Builder(OlapTable table) {
        this.table = table;
    }

    @Override
    public AlterJobV2 build() throws UserException {
        if (newIndexSchema.isEmpty() && !hasIndexChanged) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        long tableId = table.getId();
        SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2(jobId, dbId, tableId, table.getName(), timeoutMs);
        schemaChangeJob.setBloomFilterInfo(bloomFilterColumnsChanged, bloomFilterColumns, bloomFilterFpp);
        schemaChangeJob.setAlterIndexInfo(hasIndexChanged, indexes);
        schemaChangeJob.setStartTime(startTime);
        schemaChangeJob.setStorageFormat(newStorageFormat);
        schemaChangeJob.setSortKeyIdxes(sortKeyIdxes);
        /*
         * Create schema change job
         * 1. For each index which has been changed, create a SHADOW index, and save the mapping of origin index to SHADOW index.
         * 2. Create all tablets and replicas of all SHADOW index, add them to tablet inverted index.
         * 3. Change table's state as SCHEMA_CHANGE
         */
        for (Map.Entry<Long, List<Column>> entry : newIndexSchema.entrySet()) {
            long originIndexId = entry.getKey();
            MaterializedIndexMeta currentIndexMeta = table.getIndexMetaByIndexId(originIndexId);
            // 1. get new schema version/schema version hash, short key column count
            int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
            int newSchemaVersion = currentSchemaVersion + 1;
            // generate schema hash for new index has to generate a new schema hash not equal to current schema hash
            int currentSchemaHash = currentIndexMeta.getSchemaHash();
            int newSchemaHash = Util.generateSchemaHash();
            while (currentSchemaHash == newSchemaHash) {
                newSchemaHash = Util.generateSchemaHash();
            }
            String newIndexName = SchemaChangeHandler.SHADOW_NAME_PRFIX + table.getIndexNameById(originIndexId);
            short newShortKeyColumnCount = newIndexShortKeyCount.get(originIndexId);
            long shadowIndexId = globalStateMgr.getNextId();

            // create SHADOW index for each partition
            List<Tablet> addedTablets = Lists.newArrayList();
            for (Partition partition : table.getPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                // index state is SHADOW
                MaterializedIndex shadowIndex = new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW);
                MaterializedIndex originIndex = partition.getIndex(originIndexId);
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId, newSchemaHash, medium);
                short replicationNum = table.getPartitionInfo().getReplicationNum(partitionId);
                for (Tablet originTablet : originIndex.getTablets()) {
                    long originTabletId = originTablet.getId();
                    long shadowTabletId = globalStateMgr.getNextId();

                    LocalTablet shadowTablet = new LocalTablet(shadowTabletId);
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    addedTablets.add(shadowTablet);

                    schemaChangeJob.addTabletIdMap(partitionId, shadowIndexId, shadowTabletId, originTabletId);
                    List<Replica> originReplicas = ((LocalTablet) originTablet).getImmutableReplicas();

                    int healthyReplicaNum = 0;
                    for (Replica originReplica : originReplicas) {
                        long shadowReplicaId = globalStateMgr.getNextId();
                        long backendId = originReplica.getBackendId();

                        if (originReplica.getState() == Replica.ReplicaState.CLONE
                                || originReplica.getState() == Replica.ReplicaState.DECOMMISSION
                                || originReplica.getLastFailedVersion() > 0) {
                            LOG.info(
                                    "origin replica {} of tablet {} state is {}, and last failed version is {}, " +
                                            "skip creating shadow replica",
                                    originReplica.getId(), originReplica, originReplica.getState(),
                                    originReplica.getLastFailedVersion());
                            continue;
                        }
                        Preconditions
                                .checkState(originReplica.getState() == Replica.ReplicaState.NORMAL, originReplica.getState());
                        // replica's init state is ALTER, so that tablet report process will ignore its report
                        Replica shadowReplica = new Replica(shadowReplicaId, backendId, Replica.ReplicaState.ALTER,
                                Partition.PARTITION_INIT_VERSION,
                                newSchemaHash);
                        shadowTablet.addReplica(shadowReplica);
                        healthyReplicaNum++;
                    }

                    if (healthyReplicaNum < replicationNum / 2 + 1) {
                        /*
                         * TODO(cmy): This is a bad design.
                         * Because in the schema change job, we will only send tasks to the shadow replicas that
                         * have been created, without checking whether the quorum of replica number are satisfied.
                         * This will cause the job to fail until we find that the quorum of replica number
                         * is not satisfied until the entire job is done.
                         * So here we check the replica number strictly and do not allow to submit the job
                         * if the quorum of replica number is not satisfied.
                         */
                        for (Tablet tablet : addedTablets) {
                            GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                        }
                        throw new DdlException(
                                "tablet " + originTabletId + " has few healthy replica: " + healthyReplicaNum);
                    }
                }

                schemaChangeJob.addPartitionShadowIndex(partitionId, shadowIndexId, shadowIndex);
            } // end for partition
            schemaChangeJob.addIndexSchema(shadowIndexId, originIndexId, newIndexName, newSchemaVersion, newSchemaHash,
                    newShortKeyColumnCount, entry.getValue());
        } // end for index
        return schemaChangeJob;
    }
}
