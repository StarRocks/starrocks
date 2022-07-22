// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaterializedViewPartitionNameRefInfo implements Writable {

    @SerializedName("dbId")
    private long dbId;
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("mvPartitionName")
    private String mvPartitionName;
    @SerializedName("tablePartitionName")
    private String tablePartitionName;

    public MaterializedViewPartitionNameRefInfo(long dbId, long mvId, String mvPartitionName,
                                                String tablePartitionName) {
        this.dbId = dbId;
        this.mvId = mvId;
        this.mvPartitionName = mvPartitionName;
        this.tablePartitionName = tablePartitionName;
    }

    public MaterializedViewPartitionNameRefInfo() {
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

    public String getMvPartitionName() {
        return mvPartitionName;
    }

    public void setMvPartitionName(String mvPartitionName) {
        this.mvPartitionName = mvPartitionName;
    }

    public String getTablePartitionName() {
        return tablePartitionName;
    }

    public void setTablePartitionName(String tablePartitionName) {
        this.tablePartitionName = tablePartitionName;
    }

    public static MaterializedViewPartitionNameRefInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedViewPartitionNameRefInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return "MaterializedViewPartitionNameStatus{" +
                "dbId='" + dbId + '\'' +
                "mvId='" + mvId + '\'' +
                " mvPartitionName='" + mvPartitionName + '\'' +
                ", baseTablePartitionName='" + tablePartitionName;
    }
}
