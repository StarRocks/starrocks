// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class PartitionPersistInfoV2 implements Writable {

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
    private DataCacheInfo dataCacheInfo;

    public PartitionPersistInfoV2(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition) {
        this(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition, null);
    }

    public PartitionPersistInfoV2(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition,
                                  DataCacheInfo dataCacheInfo) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;
        this.dataCacheInfo = dataCacheInfo;
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

    public DataCacheInfo getDataCacheInfo() {
        return this.dataCacheInfo;
    }

}