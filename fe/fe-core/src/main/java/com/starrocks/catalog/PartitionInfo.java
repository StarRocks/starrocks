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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/PartitionInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo implements Cloneable, Writable, GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(PartitionInfo.class);

    @SerializedName(value = "type")
    protected PartitionType type;
    // partition id -> data property
    @SerializedName(value = "idToDataProperty")
    protected Map<Long, DataProperty> idToDataProperty;
    // partition id -> replication num
    @SerializedName(value = "idToReplicationNum")
    protected Map<Long, Short> idToReplicationNum;
    // true if the partition has multi partition columns
    @SerializedName(value = "isMultiColumnPartition")
    protected boolean isMultiColumnPartition = false;

    @SerializedName(value = "idToInMemory")
    protected Map<Long, Boolean> idToInMemory;

    // partition id -> tablet type
    // Note: currently it's only used for testing, it may change/add more meta field later,
    // so we defer adding meta serialization until memory engine feature is more complete.
    protected Map<Long, TTabletType> idToTabletType;

    // for lake table
    // storage cache, ttl and enable_async_write_back
    @SerializedName(value = "idToStorageCacheInfo")
    protected Map<Long, DataCacheInfo> idToStorageCacheInfo;


    public PartitionInfo() {
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
        this.idToStorageCacheInfo = new HashMap<>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToInMemory = new HashMap<>();
        this.idToTabletType = new HashMap<>();
        this.idToStorageCacheInfo = new HashMap<>();
    }

    public PartitionType getType() {
        return type;
    }

    public boolean isRangePartition() {
        return type == PartitionType.RANGE || type == PartitionType.EXPR_RANGE || type == PartitionType.EXPR_RANGE_V2;
    }

    public boolean isListPartition() {
        return type == PartitionType.LIST;
    }

    public boolean isPartitioned() {
        return type != PartitionType.UNPARTITIONED;
    }

    public DataProperty getDataProperty(long partitionId) {
        return idToDataProperty.get(partitionId);
    }

    public void setDataProperty(long partitionId, DataProperty newDataProperty) {
        idToDataProperty.put(partitionId, newDataProperty);
    }

    public int getQuorumNum(long partitionId, TWriteQuorumType writeQuorum) {
        if (writeQuorum == TWriteQuorumType.ALL) {
            return getReplicationNum(partitionId);
        } else if (writeQuorum == TWriteQuorumType.ONE) {
            return 1;
        } else {
            return getReplicationNum(partitionId) / 2 + 1;
        }
    }

    public short getReplicationNum(long partitionId) {
        if (!idToReplicationNum.containsKey(partitionId)) {
            LOG.debug("failed to get replica num for partition: {}", partitionId);
            return (short) -1;
        }
        return idToReplicationNum.get(partitionId);
    }

    public void setReplicationNum(long partitionId, short replicationNum) {
        idToReplicationNum.put(partitionId, replicationNum);
    }

    public boolean getIsInMemory(long partitionId) {
        return idToInMemory.get(partitionId);
    }

    public void setIsInMemory(long partitionId, boolean isInMemory) {
        idToInMemory.put(partitionId, isInMemory);
    }

    public TTabletType getTabletType(long partitionId) {
        if (idToTabletType == null || !idToTabletType.containsKey(partitionId)) {
            return TTabletType.TABLET_TYPE_DISK;
        }
        return idToTabletType.get(partitionId);
    }

    public void setTabletType(long partitionId, TTabletType tabletType) {
        if (idToTabletType == null) {
            idToTabletType = new HashMap<>();
        }
        idToTabletType.put(partitionId, tabletType);
    }

    public DataCacheInfo getDataCacheInfo(long partitionId) {
        return idToStorageCacheInfo.get(partitionId);
    }

    public void setDataCacheInfo(long partitionId, DataCacheInfo dataCacheInfo) {
        idToStorageCacheInfo.put(partitionId, dataCacheInfo);
    }

    public void dropPartition(long partitionId) {
        idToDataProperty.remove(partitionId);
        idToReplicationNum.remove(partitionId);
        idToInMemory.remove(partitionId);
    }

    public void moveRangeFromTempToFormal(long tempPartitionId) {
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             short replicationNum,
                             boolean isInMemory) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
        idToInMemory.put(partitionId, isInMemory);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             short replicationNum,
                             boolean isInMemory,
                             DataCacheInfo dataCacheInfo) {
        this.addPartition(partitionId, dataProperty, replicationNum, isInMemory);
        if (dataCacheInfo != null) {
            idToStorageCacheInfo.put(partitionId, dataCacheInfo);
        }
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    public boolean isMultiColumnPartition() {
        return isMultiColumnPartition;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    public List<Column> getPartitionColumns() {
        return Collections.emptyList();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        Preconditions.checkState(idToDataProperty.size() == idToReplicationNum.size());
        Preconditions.checkState(idToInMemory.keySet().equals(idToReplicationNum.keySet()));
        out.writeInt(idToDataProperty.size());
        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                entry.getValue().write(out);
            }

            out.writeShort(idToReplicationNum.get(entry.getKey()));
            out.writeBoolean(idToInMemory.get(entry.getKey()));
        }
    }

    public void readFields(DataInput in) throws IOException {
        type = PartitionType.valueOf(Text.readString(in));

        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            long partitionId = in.readLong();
            boolean isDefaultHddDataProperty = in.readBoolean();
            if (isDefaultHddDataProperty) {
                idToDataProperty.put(partitionId, new DataProperty(TStorageMedium.HDD));
            } else {
                idToDataProperty.put(partitionId, DataProperty.read(in));
            }

            short replicationNum = in.readShort();
            idToReplicationNum.put(partitionId, replicationNum);
            idToInMemory.put(partitionId, in.readBoolean());
        }
    }

    @Override
    public void gsonPreProcess() throws IOException {
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("type: ").append(type.typeString).append("; ");

        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            buff.append(entry.getKey()).append(" is HDD: ");
            if (entry.getValue().equals(new DataProperty(TStorageMedium.HDD))) {
                buff.append(true);
            } else {
                buff.append(false);
            }
            buff.append(" data_property: ").append(entry.getValue().toString());
            buff.append(" replica number: ").append(idToReplicationNum.get(entry.getKey()));
            buff.append(" in memory: ").append(idToInMemory.get(entry.getKey()));
        }

        return buff.toString();
    }

    public void createAutomaticShadowPartition(long partitionId, String replicateNum) throws DdlException {
    }

    public boolean isAutomaticPartition() {
        return false;
    }

    protected Object clone()  {
        try {
            // shallow clone on base partition info
            PartitionInfo p = (PartitionInfo) super.clone();
            p.type = this.type;
            p.idToDataProperty = this.idToDataProperty;
            p.idToReplicationNum = this.idToReplicationNum;
            p.isMultiColumnPartition = this.isMultiColumnPartition;
            p.idToInMemory = this.idToInMemory;
            p.idToTabletType = this.idToTabletType;
            p.idToStorageCacheInfo = this.idToStorageCacheInfo;
            return p;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
