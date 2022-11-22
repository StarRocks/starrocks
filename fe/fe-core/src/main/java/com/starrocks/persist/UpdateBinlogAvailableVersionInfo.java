// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class UpdateBinlogAvailableVersionInfo implements Writable {

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("partitionIdToBinlogAvailableVersionMap")
    private Map<Long, Long> partitionIdToBinlogAvailableVersionMap;

    @SerializedName("enable")
    private boolean isEnable;

    public UpdateBinlogAvailableVersionInfo(
            long dbId, long tableId, boolean isEnable,
            Map<Long, Long> partitionIdToBinlogAvailableVersionMap) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.isEnable = isEnable;
        this.partitionIdToBinlogAvailableVersionMap = partitionIdToBinlogAvailableVersionMap;
    }

    public boolean isEnable() {
        return isEnable;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, Long> getPartitionIdToBinlogAvailableVersionMap() {
        return partitionIdToBinlogAvailableVersionMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UpdateBinlogAvailableVersionInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), UpdateBinlogAvailableVersionInfo.class);
    }
}
