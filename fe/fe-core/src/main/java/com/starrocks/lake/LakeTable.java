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

package com.starrocks.lake;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.backup.Status;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for StarRocks lake table
 * todo: Rename to CloudNativeTable
 */
public class LakeTable extends OlapTable {

    private static final Logger LOG = LogManager.getLogger(LakeTable.class);

    public LakeTable() {
        super(TableType.CLOUD_NATIVE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        super(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, indexes, TableType.CLOUD_NATIVE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    @Override
    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        FileCacheInfo cacheInfo = null;
        DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        if (dataCacheInfo == null) {
            cacheInfo = tableProperty.getStorageInfo().getCacheInfo();
        } else {
            cacheInfo = dataCacheInfo.getCacheInfo();
        }
        return cacheInfo;
    }

    @Override
    public void setStorageInfo(FilePathInfo pathInfo, DataCacheInfo dataCacheInfo) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setStorageInfo(new StorageInfo(pathInfo, dataCacheInfo.getCacheInfo()));
    }

    @Override
    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState,
                                   MaterializedIndex.IndexExtState extState) {
        LakeTable copied = DeepCopy.copyWithGson(this, LakeTable.class);
        if (copied == null) {
            LOG.warn("failed to copy lake table: {}", getName());
            return null;
        }
        return selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    public static LakeTable read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTable.class);
    }

    @Override
    public boolean isDeleteRetryable() {
        return true;
    }

    @Override
    public boolean delete(long dbId, boolean replay) {
        return LakeTableHelper.deleteTable(dbId, this, replay);
    }

    @Override
    public boolean deleteFromRecycleBin(long dbId, boolean replay) {
        return LakeTableHelper.deleteTableFromRecycleBin(dbId, this, replay);
    }

    @Override
    public AlterJobV2Builder alterTable() {
        return LakeTableHelper.alterTable(this);
    }

    @Override
    public AlterJobV2Builder rollUp() {
        return LakeTableHelper.rollUp(this);
    }

    @Override
    public Map<String, String> getUniqueProperties() {
        Map<String, String> properties = Maps.newHashMap();

        if (tableProperty != null) {
            StorageInfo storageInfo = tableProperty.getStorageInfo();
            if (storageInfo != null) {
                // datacache.enable
                properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE,
                        String.valueOf(storageInfo.isEnableDataCache()));

                // enable_async_write_back
                properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK,
                        String.valueOf(storageInfo.isEnableAsyncWriteBack()));
            }

            // datacache partition duration
            String partitionDuration =
                    tableProperty.getProperties().get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION);
            if (partitionDuration != null) {
                properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, partitionDuration);
            }
        }

        // storage volume
        StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(id));

        // persistent index type
        if (keysType == KeysType.PRIMARY_KEYS && enablePersistentIndex()
                && !Strings.isNullOrEmpty(getPersistentIndexTypeString())) {
            properties.put(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, getPersistentIndexTypeString());
        }

        return properties;
    }

    @Override
    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long physicalPartitionId, Database db) {
        FilePathInfo fsInfo = getPartitionFilePathInfo(physicalPartitionId);
        FileCacheInfo cacheInfo = getPartitionFileCacheInfo(physicalPartitionId);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        List<Long> shardIds = null;
        try {
            // Ignore the parameter replicationNum
            shardIds = globalStateMgr.getStarOSAgent().createShards(tabletNum, fsInfo, cacheInfo, index.getShardGroupId(),
                    null, properties,
                    StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        } catch (DdlException e) {
            LOG.error(e.getMessage(), e);
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, false/* update inverted index */);
        }
        return Status.OK;
    }

    // used in colocate table index, return an empty list for LakeTable
    @Override
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        return Lists.newArrayList();
    }

    public List<Long> getShardGroupIds() {
        List<Long> shardGroupIds = new ArrayList<>();
        for (Partition p : getAllPartitions()) {
            for (MaterializedIndex index : p.getDefaultPhysicalPartition()
                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                shardGroupIds.add(index.getShardGroupId());
            }
        }
        return shardGroupIds;
    }

    @Override
    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return TableType.OLAP.name();
    }

    @Override
    public String getDisplayComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return CatalogUtils.addEscapeCharacter(comment);
        }
        return TableType.OLAP.name();
    }

    @Override
    protected RecyclePartitionInfo buildRecyclePartitionInfo(long dbId, Partition partition) {
        if (partitionInfo.isRangePartition()) {
            Range<PartitionKey> range = ((RangePartitionInfo) partitionInfo).getRange(partition.getId());
            return new RecycleLakeRangePartitionInfo(dbId, id, partition, range,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isListPartition()) {
            return new RecycleLakeListPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isUnPartitioned()) {
            return new RecycleLakeUnPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else {
            throw new RuntimeException("Unknown partition type: " + partitionInfo.getType());
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (getMaxColUniqueId() <= 0) {
            setMaxColUniqueId(LakeTableHelper.restoreColumnUniqueId(this));
        }
    }

    @Override
    public boolean getUseFastSchemaEvolution() {
        return !hasRowStorageType() && Config.enable_fast_schema_evolution_in_share_data_mode;
    }

    @Override
    public void checkStableAndNormal() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(state, getName());
        }
    }
}
