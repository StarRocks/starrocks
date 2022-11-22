// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class UpdateBinlogConfigInfo implements Writable {

    @SerializedName("binlogConfig")
    private BinlogConfig binlogConfig;

    @SerializedName("dbId")
    private Long dbId;

    @SerializedName("tableId")
    private Long tableId;

    @SerializedName("allTableIds")
    private Map<Long, Long> allTableIds;

    public UpdateBinlogConfigInfo(Long dbId, Long tableId, BinlogConfig binlogConfig, Map<Long, Long> allTableIds) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.binlogConfig = binlogConfig;
        this.allTableIds = allTableIds;
    }

    public UpdateBinlogConfigInfo() {

    }

    public BinlogConfig getBinlogConfig() {
        return binlogConfig;
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public Map<Long, Long> getAllTableIds() {
        return allTableIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UpdateBinlogConfigInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), UpdateBinlogConfigInfo.class);
    }
}