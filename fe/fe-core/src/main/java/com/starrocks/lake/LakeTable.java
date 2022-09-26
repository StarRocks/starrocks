// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.staros.proto.CacheInfo;
import com.staros.proto.ObjectStorageInfo;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.backup.Status;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for StarRocks lake table
 * <p>
 * Currently, storage group is table level, which stores all the tablets data and metadata of this table.
 * Format: service storage uri (from StarOS) + table id
 * <p>
 * TODO: support table api like Iceberg
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

    public String getStorageGroup() {
        return getDefaultShardStorageInfo().getObjectStorageInfo().getObjectUri();
    }

    public ShardStorageInfo getDefaultShardStorageInfo() {
        return tableProperty.getStorageInfo().getShardStorageInfo();
    }

    public ShardStorageInfo getPartitionShardStorageInfo(long partitionId) {
        CacheInfo cacheInfo = null;
        StorageCacheInfo storageCacheInfo = partitionInfo.getStorageCacheInfo(partitionId);
        if (storageCacheInfo == null) {
            cacheInfo = getDefaultShardStorageInfo().getCacheInfo();
        } else {
            cacheInfo = storageCacheInfo.getCacheInfo();
        }
        return ShardStorageInfo.newBuilder(getDefaultShardStorageInfo()).setCacheInfo(cacheInfo).build();
    }

    public void setStorageInfo(ShardStorageInfo shardStorageInfo, boolean enableCache, long cacheTtlS,
                               boolean allowAsyncWriteBack) throws DdlException {
        String storageGroup;
        // s3://bucket/serviceId/tableId/
        String path = String.format("%s/%d/", shardStorageInfo.getObjectStorageInfo().getObjectUri(), id);
        try {
            URI uri = new URI(path);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw new DdlException("Invalid storage path [" + path + "]: no scheme");
            }
            storageGroup = uri.normalize().toString();
        } catch (URISyntaxException e) {
            throw new DdlException("Invalid storage path [" + path + "]: " + e.getMessage());
        }

        ObjectStorageInfo objectStorageInfo =
                ObjectStorageInfo.newBuilder(shardStorageInfo.getObjectStorageInfo()).setObjectUri(storageGroup)
                        .build();
        CacheInfo cacheInfo = CacheInfo.newBuilder().setEnableCache(enableCache).setTtlSeconds(cacheTtlS)
                .setAllowAsyncWriteBack(allowAsyncWriteBack).build();
        ShardStorageInfo newShardStorageInfo =
                ShardStorageInfo.newBuilder(shardStorageInfo).setObjectStorageInfo(objectStorageInfo)
                        .setCacheInfo(cacheInfo).build();

        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setStorageInfo(new StorageInfo(newShardStorageInfo));
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

    public static LakeTable read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTable.class);
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        dropAllTempPartitions();
    }

    @Override
    public Runnable delete(boolean replay) {
        GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseTable(this);
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
                                          int replicationNum, long version, int schemaHash, long partitionId) {
        ShardStorageInfo shardStorageInfo = getPartitionShardStorageInfo(partitionId);
        List<Long> shardIds = null;
        try {
            shardIds = globalStateMgr.getStarOSAgent().createShards(tabletNum, shardStorageInfo, partitionId);
        } catch (DdlException e) {
            LOG.error(e.getMessage());
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, true /* is restore */);
        }
        return Status.OK;
    }
}
