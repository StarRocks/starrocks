// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents the cloud olap tablet related metadata.
 * StarOSTablet is based on cloud object storage.
 * Data replicas are managed by object storage and compute replicas are managed by StarOS through Shard.
 */
public class StarOSTablet extends Tablet {
    private static final String JSON_KEY_SHARD_ID = "shardId";
    private static final String JSON_KEY_DATA_SIZE = "dataSize";
    private static final String JSON_KEY_ROW_COUNT = "rowCount";

    @SerializedName(value = JSON_KEY_SHARD_ID)
    private long shardId;
    @SerializedName(value = JSON_KEY_DATA_SIZE)
    private long dataSize = 0L;
    @SerializedName(value = JSON_KEY_ROW_COUNT)
    private long rowCount = 0L;

    public StarOSTablet() {
        this(-1L, -1L);
    }

    public StarOSTablet(long id, long shardId) {
        super(id);
        this.shardId = shardId;
    }

    public long getShardId() {
        return shardId;
    }

    @Override
    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    // Version is not used
    @Override
    public long getRowCount(long version) {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_ID, id);
        jsonObject.addProperty(JSON_KEY_SHARD_ID, shardId);
        jsonObject.addProperty(JSON_KEY_DATA_SIZE, dataSize);
        jsonObject.addProperty(JSON_KEY_ROW_COUNT, rowCount);
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        JsonObject jsonObject = JsonParser.parseString(Text.readString(in)).getAsJsonObject();
        id = jsonObject.getAsJsonPrimitive(JSON_KEY_ID).getAsLong();
        shardId = jsonObject.getAsJsonPrimitive(JSON_KEY_SHARD_ID).getAsLong();
        dataSize = jsonObject.getAsJsonPrimitive(JSON_KEY_DATA_SIZE).getAsLong();
        rowCount = jsonObject.getAsJsonPrimitive(JSON_KEY_ROW_COUNT).getAsLong();
    }

    public static StarOSTablet read(DataInput in) throws IOException {
        StarOSTablet tablet = new StarOSTablet();
        tablet.readFields(in);
        return tablet;
    }
}
