// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionPersistInfoV2 implements Writable {

    @SerializedName("dbId")
    private Long dbId;
    @SerializedName("tableId")
    private Long tableId;
    @SerializedName("partition")
    private Partition partition;
    @SerializedName("dataProperty")
    private DataProperty dataProperty;
    @SerializedName("replicationNum")
    private short replicationNum;
    @SerializedName("isInMemory")
    private boolean isInMemory;
    @SerializedName("isTempPartition")
    private boolean isTempPartition;
    @SerializedName("storageCacheInfo")
    private StorageCacheInfo storageCacheInfo;

    public PartitionPersistInfoV2(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition) {
        this(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition, null);
    }

    public PartitionPersistInfoV2(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition,
                                  StorageCacheInfo storageCacheInfo) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;
        this.storageCacheInfo = storageCacheInfo;
    }

    public final boolean isListPartitionPersistInfo() {
        return this.getClass() == ListPartitionPersistInfo.class;
    }

    public final boolean isRangePartitionPersistInfo() {
        return this.getClass() == RangePartitionPersistInfo.class;
    }

    public final ListPartitionPersistInfo asListPartitionPersistInfo() {
        return (ListPartitionPersistInfo) this;
    }

    public final RangePartitionPersistInfo asRangePartitionPersistInfo() {
        return (RangePartitionPersistInfo) this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionPersistInfoV2 read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionPersistInfoV2.class);
    }

    public Long getDbId() {
        return this.dbId;
    }

    public Long getTableId() {
        return this.tableId;
    }

    public Partition getPartition() {
        return this.partition;
    }

    public DataProperty getDataProperty() {
        return this.dataProperty;
    }

    public short getReplicationNum() {
        return this.replicationNum;
    }

    public boolean isInMemory() {
        return this.isInMemory;
    }

    public boolean isTempPartition() {
        return this.isTempPartition;
    }

    public StorageCacheInfo getStorageCacheInfo() {
        return this.storageCacheInfo;
    }

}