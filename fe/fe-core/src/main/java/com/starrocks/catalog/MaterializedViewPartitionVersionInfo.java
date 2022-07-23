// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaterializedViewPartitionVersionInfo implements Writable {

    @SerializedName("dbId")
    private long dbId;
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("tablePartitionName")
    private String tablePartitionName;
    @SerializedName("tablePartitionId")
    private long tablePartitionId;
    @SerializedName("tablePartitionVersion")
    private long tablePartitionVersion;

    public MaterializedViewPartitionVersionInfo(long dbId, long mvId, long tableId, String tablePartitionName,
                                                long tablePartitionId, long tablePartitionVersion) {
        this.dbId = dbId;
        this.mvId = mvId;
        this.tableId = tableId;
        this.tablePartitionName = tablePartitionName;
        this.tablePartitionId = tablePartitionId;
        this.tablePartitionVersion = tablePartitionVersion;
    }

    public MaterializedViewPartitionVersionInfo() {
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getMvId() {
        return mvId;
    }

    public void setMvId(long mvId) {
        this.mvId = mvId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getTablePartitionName() {
        return tablePartitionName;
    }

    public void setTablePartitionName(String tablePartitionName) {
        this.tablePartitionName = tablePartitionName;
    }

    public long getTablePartitionId() {
        return tablePartitionId;
    }

    public void setTablePartitionId(long tablePartitionId) {
        this.tablePartitionId = tablePartitionId;
    }

    public long getTablePartitionVersion() {
        return tablePartitionVersion;
    }

    public void setTablePartitionVersion(long tablePartitionVersion) {
        this.tablePartitionVersion = tablePartitionVersion;
    }

    public static MaterializedViewPartitionVersionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedViewPartitionVersionInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return "MaterializedViewVersionMapInfo{" +
                "dbId='" + dbId + '\'' +
                ", mvId='" + mvId + "\'" +
                ", tableId='" + tableId +
                ",partitionName='" + tablePartitionName + '\'' +
                ", partitionId='" + tablePartitionId + "\'" +
                ", partitionVersion='" + tablePartitionVersion;
    }
}
