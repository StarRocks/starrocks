// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;

import java.util.List;
import java.util.Map;

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
        schemaChangeJob.setStorageFormat(newStorageFormat);
        for (Map.Entry<Long, List<Column>> entry : newIndexSchema.entrySet()) {
            long originIndexId = entry.getKey();
            // 1. get new schema version/schema version hash, short key column count
            String newIndexName = SchemaChangeHandler.SHADOW_NAME_PRFIX + table.getIndexNameById(originIndexId);
            short newShortKeyColumnCount = newIndexShortKeyCount.get(originIndexId);
            long shadowIndexId = globalStateMgr.getNextId();

            // create SHADOW index for each partition
            for (Partition partition : table.getPartitions()) {
                long partitionId = partition.getId();
                List<Tablet> originTablets = partition.getIndex(originIndexId).getTablets();
                List<Long> shadowTabletIds = createShards(originTablets.size(),
                                table.getPartitionShardStorageInfo(partitionId), partitionId);
                Preconditions.checkState(originTablets.size() == shadowTabletIds.size());

                TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, partitionId, shadowIndexId, 0, medium, true);
                MaterializedIndex shadowIndex =
                        new MaterializedIndex(shadowIndexId, MaterializedIndex.IndexState.SHADOW);
                for (int i = 0; i < originTablets.size(); i++) {
                    Tablet originTablet = originTablets.get(i);
                    Tablet shadowTablet = new LakeTablet(shadowTabletIds.get(i));
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    schemaChangeJob
                            .addTabletIdMap(partitionId, shadowIndexId, shadowTablet.getId(), originTablet.getId());
                }
                schemaChangeJob.addPartitionShadowIndex(partitionId, shadowIndexId, shadowIndex);
            } // end for partition
            schemaChangeJob.addIndexSchema(shadowIndexId, originIndexId, newIndexName, newShortKeyColumnCount,
                    entry.getValue());
        } // end for index
        return schemaChangeJob;
    }

    @VisibleForTesting
    public static List<Long> createShards(int shardCount, ShardStorageInfo storageInfo, long groupId) throws DdlException {
        return GlobalStateMgr.getCurrentStarOSAgent().createShards(shardCount, storageInfo, groupId);
    }

}
