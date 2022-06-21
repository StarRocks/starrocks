// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog.lake;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Metadata for StarRocks lake table
 * <p>
 * TODO: support table api like Iceberg
 */
public class LakeTable extends OlapTable {
    public static final String STORAGE_GROUP = "storageGroup";

    // Currently, storage group is table level, which stores all the tablets data and metadata of this table.
    // Format: service storage uri (from StarOS) + table id
    @SerializedName(value = STORAGE_GROUP)
    private String storageGroup;

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
        return storageGroup;
    }

    public void setStorageGroup(String serviceStorageUri) throws AnalysisException {
        // s3://bucket/serviceId/tableId/
        String path = String.format("%s/%d/", serviceStorageUri, id);
        try {
            URI uri = new URI(path);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw new AnalysisException("Invalid storage path [" + path + "]: no scheme");
            }
            this.storageGroup = uri.normalize().toString();
        } catch (URISyntaxException e) {
            throw new AnalysisException("Invalid storage path [" + path + "]: " + e.getMessage());
        }
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
