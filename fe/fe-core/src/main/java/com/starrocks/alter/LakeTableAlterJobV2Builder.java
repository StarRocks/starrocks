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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeTableAlterJobV2Builder extends AlterJobV2Builder {
    private final LakeTable table;

    public LakeTableAlterJobV2Builder(LakeTable table) {
        this.table = table;
    }

    @Override
    public AlterJobV2 build() throws UserException {
        if (newIndexSchema.isEmpty() && !hasIndexChanged) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        long tableId = table.getId();
        LakeTableSchemaChangeJob schemaChangeJob =
                new LakeTableSchemaChangeJob(jobId, dbId, tableId, table.getName(), timeoutMs);
        schemaChangeJob.setBloomFilterInfo(bloomFilterColumnsChanged, bloomFilterColumns, bloomFilterFpp);
        schemaChangeJob.setAlterIndexInfo(hasIndexChanged, indexes);
        schemaChangeJob.setStartTime(startTime);
        schemaChangeJob.setSortKeyIdxes(sortKeyIdxes);
        for (Map.Entry<Long, List<Column>> entry : newIndexSchema.entrySet()) {
            long originIndexId = entry.getKey();
            // 1. get new schema version/schema version hash, short key column count
            String newIndexName = SchemaChangeHandler.SHADOW_NAME_PRFIX + table.getIndexNameById(originIndexId);
            short newShortKeyColumnCount = newIndexShortKeyCount.get(originIndexId);
            long shadowIndexId = globalStateMgr.getNextId();

            // create SHADOW index for each partition
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                long partitionId = partition.getParentId();
                long physicalPartitionId = partition.getId();
                long shardGroupId = partition.getShardGroupId();
                List<Tablet> originTablets = partition.getIndex(originIndexId).getTablets();
                // TODO: It is not good enough to create shards into the same group id, schema change PR needs to
                //  revise the code again.
                List<Long> originTabletIds = originTablets.stream().map(Tablet::getId).collect(Collectors.toList());
                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(shadowIndexId));
                List<Long> shadowTabletIds =
                        createShards(originTablets.size(), table.getPartitionFilePathInfo(partitionId),
                                     table.getPartitionFileCacheInfo(partitionId), shardGroupId,
                                     originTabletIds, properties);
                Preconditions.checkState(originTablets.size() == shadowTabletIds.size());

                TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexId, 0, medium, true);
                MaterializedIndex shadowIndex =
                        new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW);
                for (int i = 0; i < originTablets.size(); i++) {
                    Tablet originTablet = originTablets.get(i);
                    Tablet shadowTablet = new LakeTablet(shadowTabletIds.get(i));
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    schemaChangeJob
                            .addTabletIdMap(physicalPartitionId, shadowIndexId, shadowTablet.getId(), originTablet.getId());
                }
                schemaChangeJob.addPartitionShadowIndex(physicalPartitionId, shadowIndexId, shadowIndex);
            } // end for partition
            schemaChangeJob.addIndexSchema(shadowIndexId, originIndexId, newIndexName, newShortKeyColumnCount,
                    entry.getValue());
        } // end for index
        return schemaChangeJob;
    }

    @VisibleForTesting
    public static List<Long> createShards(int shardCount, FilePathInfo pathInfo, FileCacheInfo cacheInfo,
                                          long groupId, List<Long> matchShardIds, Map<String, String> properties)
        throws DdlException {
        return GlobalStateMgr.getCurrentStarOSAgent().createShards(shardCount, pathInfo, cacheInfo, groupId, matchShardIds,
                properties);
    }

}
