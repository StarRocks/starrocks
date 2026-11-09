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
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeTableAlterJobV2Builder extends AlterJobV2Builder {

    // The table could be either an LakeTable or LakeMaterializedView
    private final OlapTable table;

    public LakeTableAlterJobV2Builder(OlapTable table) {
        Preconditions.checkArgument(table.isCloudNativeTableOrMaterializedView());
        this.table = table;
    }

    @Override
    public AlterJobV2 build() throws StarRocksException {
        if (newIndexMetaIdToSchema.isEmpty() && !hasIndexChanged) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        long tableId = table.getId();
        LakeTableSchemaChangeJob schemaChangeJob =
                new LakeTableSchemaChangeJob(jobId, dbId, tableId, table.getName(), timeoutMs);
        schemaChangeJob.setBloomFilterInfo(bloomFilterColumnsChanged, bloomFilterColumns, bloomFilterFpp);
        schemaChangeJob.setAlterIndexInfo(hasIndexChanged, indexes);
        schemaChangeJob.setStartTime(startTime);
        schemaChangeJob.setComputeResource(computeResource);
        schemaChangeJob.setSortKeyIdxes(sortKeyIdxes);
        schemaChangeJob.setSortKeyUniqueIds(sortKeyUniqueIds);
        ColocateTableIndex colocateTableIndex = globalStateMgr.getColocateTableIndex();
        ColocateTableIndex.GroupId rangeColocateGroupId = colocateTableIndex.getRangeColocateGroupId(tableId);
        List<ColocateRange> colocateRanges = rangeColocateGroupId == null ? null
                : colocateTableIndex.getColocateRanges(rangeColocateGroupId.grpId);
        int colocateColumnCount = rangeColocateGroupId == null ? 0
                : colocateTableIndex.getGroupSchema(rangeColocateGroupId).getColocateColumnCount();
        for (Map.Entry<Long, List<Column>> entry : newIndexMetaIdToSchema.entrySet()) {
            long originIndexMetaId = entry.getKey();
            // 1. get new schema version/schema version hash, short key column count
            String newIndexName = SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getIndexNameByMetaId(originIndexMetaId);
            short newShortKeyColumnCount = newIndexMetaIdToShortKeyCount.get(originIndexMetaId);
            long shadowIndexMetaId = globalStateMgr.getNextId();
            // initially, index id and index meta id are the same
            long shadowIndexId = shadowIndexMetaId;

            // create SHADOW index for each physicalPartition
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                long partitionId = physicalPartition.getParentId();
                long physicalPartitionId = physicalPartition.getId();
                MaterializedIndex originIndex = physicalPartition.getLatestIndex(originIndexMetaId);
                long shardGroupId = originIndex.getShardGroupId();

                List<Tablet> originTablets = originIndex.getTablets();
                // TODO: It is not good enough to create shards into the same group id, schema change PR needs to
                //  revise the code again.
                List<Long> originTabletIds = originTablets.stream().map(Tablet::getId).collect(Collectors.toList());
                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(shadowIndexId));
                List<Long> shadowTabletIds;
                if (colocateRanges == null) {
                    shadowTabletIds = createShards(originTablets.size(),
                            table.getPartitionFilePathInfo(physicalPartitionId),
                            table.getPartitionFileCacheInfo(physicalPartitionId), shardGroupId,
                            originTabletIds, properties, computeResource);
                } else {
                    shadowTabletIds = createRangeColocateShadowShards(originTablets, colocateRanges,
                            colocateColumnCount, physicalPartitionId, shardGroupId, properties);
                }
                Preconditions.checkState(originTablets.size() == shadowTabletIds.size());

                TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexId, medium, true);
                MaterializedIndex shadowIndex =
                        new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW, shardGroupId);
                for (int i = 0; i < originTablets.size(); i++) {
                    Tablet originTablet = originTablets.get(i);
                    Tablet shadowTablet = new LakeTablet(shadowTabletIds.get(i));
                    if (table.isRangeDistribution()) {
                        shadowTablet.setRange(originTablet.getRange());
                    }
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    schemaChangeJob
                            .addTabletIdMap(physicalPartitionId, shadowIndexMetaId, shadowTablet.getId(), originTablet.getId());
                }

                schemaChangeJob.addPartitionShadowIndex(physicalPartitionId, shadowIndexMetaId, shadowIndex);
            } // end for physicalPartition
            schemaChangeJob.addIndexSchema(shadowIndexMetaId, originIndexMetaId, newIndexName, newShortKeyColumnCount,
                    entry.getValue());
        } // end for index
        return schemaChangeJob;
    }

    /**
     * Creates the shadow shards of a table in a range colocate group, one per origin tablet and in the same order.
     *
     * <p>Every tablet of such a table must be in the PACK shard group of the ColocateRange it lies in, as partition
     * creation, split and merge place it. Otherwise, once the origin shard is deleted, nothing keeps the tablet with
     * the other tablets of its range, and nothing puts it back while the group stays stable. A shadow tablet copies its
     * origin tablet's range, so its shard joins the index shard group and the PACK shard group of the origin tablet's
     * ColocateRange. Like the children of a split, it stays pinned to its origin shard to reuse the warm cache. In a
     * stable group the origin shard already sits with that PACK shard group; while the group is unstable they may
     * differ, and the colocate checker settles the placement after the job, as it does after a split.
     */
    private List<Long> createRangeColocateShadowShards(List<Tablet> originTablets, List<ColocateRange> colocateRanges,
                                                       int colocateColumnCount, long physicalPartitionId,
                                                       long shardGroupId, Map<String, String> properties)
            throws DdlException {
        Map<Long, Long> newToOldShardId = new LinkedHashMap<>();
        Map<Long, List<Long>> newShardIdToGroupIds = new LinkedHashMap<>();
        for (Tablet originTablet : originTablets) {
            // Reads only the colocate prefix of the range, so it also holds for a rollup with a sort key of its own.
            long packShardGroupId = ColocateRangeUtils.lookupPackShardGroupId(originTablet.getRange().getRange(),
                    colocateRanges, colocateColumnCount);
            // The ColocateRanges of a group cover the whole key space.
            Preconditions.checkState(packShardGroupId != PhysicalPartition.INVALID_SHARD_GROUP_ID,
                    "tablet %s %s of table %s lies in no ColocateRange", originTablet.getId(),
                    originTablet.getRange().getRange(), table.getName());
            long shadowShardId = GlobalStateMgr.getCurrentState().getNextId();
            newToOldShardId.put(shadowShardId, originTablet.getId());
            newShardIdToGroupIds.put(shadowShardId, List.of(shardGroupId, packShardGroupId));
        }
        GlobalStateMgr.getCurrentState().getStarOSAgent().createShardsForSplit(newToOldShardId, newShardIdToGroupIds,
                table.getPartitionFilePathInfo(physicalPartitionId), table.getPartitionFileCacheInfo(physicalPartitionId),
                properties, computeResource, false);
        return new ArrayList<>(newToOldShardId.keySet());
    }

    @VisibleForTesting
    public static List<Long> createShards(int shardCount, FilePathInfo pathInfo, FileCacheInfo cacheInfo,
                                          long groupId, List<Long> matchShardIds, Map<String, String> properties,
                                          ComputeResource computeResource)
            throws DdlException {
        return GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShards(shardCount, pathInfo, cacheInfo, groupId, matchShardIds, properties,
                        computeResource);
    }
}
