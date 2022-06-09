// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class PartitionPersistInfoV2 implements Writable {

    public static final  short UN_PARTITION_TYPE = 0;
    public static final  short LIST_PARTITION_TYPE = 1;
    public static final  short RANGE_PARTITION_TYPE = 2;

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

    public PartitionPersistInfoV2() {
    }

    public PartitionPersistInfoV2(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;
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

    public abstract PartitionPersistInfoV2 readFieldIn(DataInput in) throws IOException;

    @Override
    public void write(DataOutput out) throws IOException {
        if (this.isListPartitionPersistInfo()) {
            out.writeShort(LIST_PARTITION_TYPE);
        } else if (this.isRangePartitionPersistInfo()) {
            out.writeShort(RANGE_PARTITION_TYPE);
        } else {
            out.writeShort(UN_PARTITION_TYPE);
        }
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionPersistInfoV2 read(DataInput in) throws IOException {
        short partitionType = in.readShort();
        PartitionPersistInfoV2 info;
        switch (partitionType) {
            case LIST_PARTITION_TYPE:
                info = new ListPartitionPersistInfo();
                break;
            case RANGE_PARTITION_TYPE:
                info = new RangePartitionPersistInfo();
                break;
            default:
                info = new UnPartitionPersistInfo();
                break;
        }
        return info.readFieldIn(in);
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

}
