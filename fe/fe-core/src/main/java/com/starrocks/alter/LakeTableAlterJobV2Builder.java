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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.HashMap;
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
        // True when the post-alter table has a VECTOR index. Any shadow-tablet rewrite
        // (DirectSchemaChange or SortedSchemaChange in be/src/storage/lake/schema_change.cpp)
        // force-inline-builds the vector index over ALL existing data, so its shadow tablets are
        // stamped with vibv=V_snap (below) to tell the async build scheduler that data is already
        // built (no redundant rebuild). `indexes` is the post-alter index set: column/index changes
        // supply it via withAlterIndexInfo, and the sort-key path now propagates it too
        // (createJobForProcessModifySortKeyColumn), so it reliably reflects whether the table has a
        // vector index. NOT gated on hasIndexChanged: a pure column/sort-key rewrite on a table that
        // already has a vector index still inline-builds it during conversion — leaving vibv=0 there
        // would make the scheduler redundantly rebuild the entire existing dataset after the ALTER.
        boolean inlineBuildsVectorIndex = indexes != null &&
                indexes.stream().anyMatch(i -> i.getIndexType() == IndexDef.IndexType.VECTOR);
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
                List<Long> shadowTabletIds = createShards(originTablets.size(),
                        table.getPartitionFilePathInfo(physicalPartitionId),
                        table.getPartitionFileCacheInfo(physicalPartitionId), shardGroupId,
                        originTabletIds, properties, computeResource);
                Preconditions.checkState(originTablets.size() == shadowTabletIds.size());

                TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexId, medium, true);
                MaterializedIndex shadowIndex =
                        new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW, shardGroupId);
                // For vector-index ALTER jobs, stamp V_snap = partition visible version on every
                // shadow tablet at creation time.  The async build scheduler uses this watermark
                // to skip existing rowsets (version <= V_snap) that were force-inline-built by
                // the conversion, avoiding redundant rebuild.  Setting it here — before the job
                // is persisted via logAlterJob — makes it durable through GSON round-trips
                // (LakeTablet.vibv is @SerializedName("vibv"); the enclosing
                // physicalPartitionIndexMap is @SerializedName("partitionIndexMap")).
                long vSnap = inlineBuildsVectorIndex ? physicalPartition.getVisibleVersion() : 0L;
                for (int i = 0; i < originTablets.size(); i++) {
                    Tablet originTablet = originTablets.get(i);
                    LakeTablet shadowTablet = new LakeTablet(shadowTabletIds.get(i));
                    if (table.isRangeDistribution()) {
                        shadowTablet.setRange(originTablet.getRange());
                    }
                    if (inlineBuildsVectorIndex) {
                        shadowTablet.setVectorIndexBuiltVersion(vSnap);
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
