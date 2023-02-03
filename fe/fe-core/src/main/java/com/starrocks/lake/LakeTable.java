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

import com.google.common.collect.Lists;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.backup.Status;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for StarRocks lake table
 */
public class LakeTable extends OlapTable {

    private static final Logger LOG = LogManager.getLogger(LakeTable.class);

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        super(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                GlobalStateMgr.getCurrentState().getClusterId(), indexes, TableType.LAKE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    public static LakeTable read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTable.class);
    }

    public String getStorageGroup() {
        return getDefaultFilePathInfo().getFullPath();
    }

    public FilePathInfo getDefaultFilePathInfo() {
        return tableProperty.getStorageInfo().getFilePathInfo();
    }

    public FilePathInfo getPartitionFilePathInfo() {
        return getDefaultFilePathInfo();
    }

    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        FileCacheInfo cacheInfo = null;
        StorageCacheInfo storageCacheInfo = partitionInfo.getStorageCacheInfo(partitionId);
        if (storageCacheInfo == null) {
            cacheInfo = tableProperty.getStorageInfo().getCacheInfo();
        } else {
            cacheInfo = storageCacheInfo.getCacheInfo();
        }
        return cacheInfo;
    }

    public void setStorageInfo(FilePathInfo pathInfo, boolean enableCache, long cacheTtlS,
                               boolean asyncWriteBack) throws DdlException {
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder().setEnableCache(enableCache).setTtlSeconds(cacheTtlS)
                .setAsyncWriteBack(asyncWriteBack).build();
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setStorageInfo(new StorageInfo(pathInfo, cacheInfo));
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

    @Override
    public void write(DataOutput out) throws IOException {
        // write type first
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        dropAllTempPartitions();
    }

    @Override
    public Runnable delete(boolean replay) {
        GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseTable(this, replay);
        return replay ? null : new DeleteLakeTableTask(this);
    }

    @Override
    public AlterJobV2Builder alterTable() {
        return new LakeTableAlterJobV2Builder(this);
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = super.getProperties();
        if (tableProperty != null) {
            StorageInfo storageInfo = tableProperty.getStorageInfo();
            if (storageInfo != null) {
                // enable_storage_cache
                properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE,
                        String.valueOf(storageInfo.isEnableStorageCache()));

                // storage_cache_ttl
                properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL,
                        String.valueOf(storageInfo.getStorageCacheTtlS()));

                // allow_async_write_back
                properties.put(PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK,
                        String.valueOf(storageInfo.isAllowAsyncWriteBack()));
            }
        }
        return properties;
    }

    @Override
    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long partitionId, long shardGroupId) {
        FilePathInfo fsInfo = getPartitionFilePathInfo();
        FileCacheInfo cacheInfo = getPartitionFileCacheInfo(partitionId);

        List<Long> shardIds = null;
        try {
            shardIds = globalStateMgr.getStarOSAgent().createShards(tabletNum, replicationNum, fsInfo, cacheInfo,
                    shardGroupId);
        } catch (DdlException e) {
            LOG.error(e.getMessage());
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, false/* update inverted index */);
        }
        return Status.OK;
    }

    @Override
    public Short getDefaultReplicationNum() {
        if (tableProperty != null) {
            return tableProperty.getReplicationNum();
        }
        return 1;
    }

    // used in colocate table index, return an empty list for LakeTable
    @Override
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        return Lists.newArrayList();
    }

    public List<Long> getShardGroupIds() {
        List<Long> shardGroupIds = new ArrayList<>();
        for (Partition p : getAllPartitions()) {
            shardGroupIds.add(p.getShardGroupId());
        }
        return shardGroupIds;
    }
}
