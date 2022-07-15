// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.staros.proto.ObjectStorageInfo;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

/**
 * Metadata for StarRocks lake table
 * <p>
 * Currently, storage group is table level, which stores all the tablets data and metadata of this table.
 * Format: service storage uri (from StarOS) + table id
 * <p>
 * TODO: support table api like Iceberg
 */
public class LakeTable extends OlapTable {

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
        return getShardStorageInfo().getObjectStorageInfo().getObjectUri();
    }

    public ShardStorageInfo getShardStorageInfo() {
        return tableProperty.getStorageInfo().getShardStorageInfo();
    }

    public void setStorageInfo(ShardStorageInfo shardStorageInfo, boolean enableCache, long cacheTtlS)
            throws DdlException {
        String storageGroup = null;
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
        ShardStorageInfo newShardStorageInfo =
                ShardStorageInfo.newBuilder(shardStorageInfo).setObjectStorageInfo(objectStorageInfo).build();

        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .setStorageInfo(new StorageInfo(newShardStorageInfo, new StorageCacheInfo(enableCache, cacheTtlS)));
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
        LakeTable table = GsonUtils.GSON.fromJson(json, LakeTable.class);
        return table;
    }
}
