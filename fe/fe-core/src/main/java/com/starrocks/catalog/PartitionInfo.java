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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.JsonWriter;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.DataCacheInfoPB;
import com.starrocks.persist.proto.DataPropertyPB;
import com.starrocks.persist.proto.PartitionInfoPB;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;

/*
 * Repository of a partition's related infos
 */
public class PartitionInfo extends JsonWriter implements Cloneable, GsonPreProcessable, GsonPostProcessable {
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

    // for lake table
    // storage cache, ttl and enable_async_write_back
    @SerializedName(value = "idToStorageCacheInfo")
    protected Map<Long, DataCacheInfo> idToStorageCacheInfo;


    public PartitionInfo() {
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToStorageCacheInfo = new HashMap<>();
    }

    public PartitionInfo(PartitionType type) {
        this.type = type;
        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToStorageCacheInfo = new HashMap<>();
    }

    /**
     * Image v3 form: the fields of this base class, then the variant of the concrete class.
     */
    public PartitionInfoPB toProto() throws IOException {
        PartitionInfoPB.Builder builder = PartitionInfoPB.newBuilder()
                .setIsMultiColumnPartition(isMultiColumnPartition);
        if (type != null) {
            builder.setType(ProtoEnums.toProto(type, PartitionInfoPB.PartitionType.class));
        }
        // Like GSON, an entry with a null value is not persisted (shared-nothing tables keep a null
        // DataCacheInfo per partition).
        for (Map.Entry<Long, DataProperty> entry : ProtoMaps.withoutNullValues(idToDataProperty).entrySet()) {
            builder.putIdToDataProperty(entry.getKey(), entry.getValue().toProto());
        }
        for (Map.Entry<Long, Short> entry : ProtoMaps.withoutNullValues(idToReplicationNum).entrySet()) {
            builder.putIdToReplicationNum(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, DataCacheInfo> entry : ProtoMaps.withoutNullValues(idToStorageCacheInfo).entrySet()) {
            builder.putIdToStorageCacheInfo(entry.getKey(), entry.getValue().toProto());
        }
        fillProtoVariant(builder);
        return builder.build();
    }

    /** Sets the kind and the variant message of the concrete class. Only the persistable subclasses override it. */
    protected void fillProtoVariant(PartitionInfoPB.Builder builder) throws IOException {
        throw new IllegalStateException("partition info class " + getClass().getName() + " cannot be persisted");
    }

    public static PartitionInfo fromProto(PartitionInfoPB pb) throws IOException {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        PartitionInfo info;
        switch (pb.getKind()) {
            case KIND_SINGLE:
                info = new SinglePartitionInfo();
                break;
            case KIND_RANGE: {
                RangePartitionInfo range = new RangePartitionInfo();
                range.fillFromVariantProto(pb.getRange());
                info = range;
                break;
            }
            case KIND_LIST: {
                ListPartitionInfo list = new ListPartitionInfo();
                list.fillFromVariantProto(pb.getList());
                info = list;
                break;
            }
            case KIND_EXPRESSION_RANGE: {
                ExpressionRangePartitionInfo expression = new ExpressionRangePartitionInfo();
                expression.fillFromVariantProto(pb.getExpressionRange());
                info = expression;
                break;
            }
            case KIND_EXPRESSION_RANGE_V2: {
                ExpressionRangePartitionInfoV2 expression = new ExpressionRangePartitionInfoV2();
                expression.fillFromVariantProto(pb.getExpressionRangeV2());
                info = expression;
                break;
            }
            default:
                throw new IllegalArgumentException("unknown partition info kind " + pb.getKind());
        }
        info.fillFromCoreProto(pb);
        return info;
    }

    private void fillFromCoreProto(PartitionInfoPB pb) throws IOException {
        if (pb.hasType()) {
            type = ProtoEnums.fromProto(pb.getType(), PartitionType.class);
        }
        idToDataProperty = new HashMap<>();
        for (Map.Entry<Long, DataPropertyPB> entry : pb.getIdToDataPropertyMap().entrySet()) {
            idToDataProperty.put(entry.getKey(), DataProperty.fromProto(entry.getValue()));
        }
        idToReplicationNum = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : pb.getIdToReplicationNumMap().entrySet()) {
            idToReplicationNum.put(entry.getKey(), entry.getValue().shortValue());
        }
        isMultiColumnPartition = pb.getIsMultiColumnPartition();
        idToStorageCacheInfo = new HashMap<>();
        for (Map.Entry<Long, DataCacheInfoPB> entry : pb.getIdToStorageCacheInfoMap().entrySet()) {
            idToStorageCacheInfo.put(entry.getKey(), DataCacheInfo.fromProto(entry.getValue()));
        }
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

    /**
     * Whether it is expr range partitioned which is used in materialized view.
     * TODO: type may not be compatible with PartitionType.EXPR_RANGE!!!
     // return type == PartitionType.EXPR_RANGE;
     * @return ture if it is expr range partitioned
     */
    public boolean isExprRangePartitioned() {
        return this instanceof ExpressionRangePartitionInfo;
    }

    public boolean isUnPartitioned() {
        return type == PartitionType.UNPARTITIONED;
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
        // Perform the op under no lock, the formal containsKey() call can't guarantee the later get() op success.
        Short replicationNum = idToReplicationNum.get(partitionId);
        if (replicationNum == null) {
            LOG.debug("failed to get replica num for partition: {}", partitionId);
            return (short) -1;
        }
        return replicationNum;
    }

    public short getMinReplicationNum() {
        return idToReplicationNum.values().stream().min(Short::compareTo).orElse((short) 1);
    }

    public void setReplicationNum(long partitionId, short replicationNum) {
        idToReplicationNum.put(partitionId, replicationNum);
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
        idToStorageCacheInfo.remove(partitionId);
    }

    public void moveRangeFromTempToFormal(long tempPartitionId) {
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             short replicationNum) {
        idToDataProperty.put(partitionId, dataProperty);
        idToReplicationNum.put(partitionId, replicationNum);
    }

    public void addPartition(long partitionId, DataProperty dataProperty,
                             short replicationNum,
                             DataCacheInfo dataCacheInfo) {
        this.addPartition(partitionId, dataProperty, replicationNum);
        if (dataCacheInfo != null) {
            idToStorageCacheInfo.put(partitionId, dataCacheInfo);
        }
    }

    public boolean isMultiColumnPartition() {
        return isMultiColumnPartition;
    }

    public String toSql(OlapTable table, List<Long> partitionId) {
        return "";
    }

    @NotNull
    public List<Column> getPartitionColumns(Map<ColumnId, Column> idToColumn) {
        return Collections.emptyList();
    }

    public int getPartitionColumnsSize() {
        return 0;
    }

    /**
     * Return the sorted partitions based on partition value
     * 1. RANGE: sorted by the range
     * 2. LIST: sorted by the list value
     * 3. EXPR: sorted by the expression value
     */
    public List<Long> getSortedPartitions(boolean asc) {
        throw new NotImplementedException("not reachable");
    }

    /**
     * Return the partitions that contains NULL partition values
     * e.g. PARTITION p_null VALUES IN (NULL)
     */
    public Set<Long> getNullValuePartitions() {
        throw new NotImplementedException("not reachable");
    }

    @Override
    public void gsonPreProcess() throws IOException {
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // NOTE: clean dirty data in idToStorageCacheInfo due to historic bugs.
        // Taking idToReplicationNum as reference, remove all the items of idToStorageCacheInfo
        // that doesn't have the corresponding key in idToReplicationNum, ASSUMING that all valid
        // partitions should have a record in idToReplicationNum.
        //
        // Can be removed after several major releases.
        if (idToStorageCacheInfo.size() > idToReplicationNum.size()) {
            HashSet<Long> keyToDelete = new HashSet<>(idToStorageCacheInfo.keySet());
            keyToDelete.removeAll(idToReplicationNum.keySet());
            keyToDelete.forEach(idToStorageCacheInfo::remove);
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("type: ").append(type.typeString).append("; ");

        for (Map.Entry<Long, DataProperty> entry : idToDataProperty.entrySet()) {
            buff.append(entry.getKey()).append(" is HDD: ")
                    .append(DataProperty.DATA_PROPERTY_HDD.equals(entry.getValue()));
            buff.append(" data_property: ").append(entry.getValue().toString());
            buff.append(" replica number: ").append(idToReplicationNum.get(entry.getKey()));
        }

        return buff.toString();
    }

    public void createAutomaticShadowPartition(List<Column> schema, long partitionId, String replicateNum) throws DdlException {
    }

    public boolean isAutomaticPartition() {
        return false;
    }

    protected Object clone()  {
        try {
            PartitionInfo p = (PartitionInfo) super.clone();
            p.type = this.type;
            p.idToDataProperty = new HashMap<>(this.idToDataProperty);
            p.idToReplicationNum = new HashMap<>(this.idToReplicationNum);
            p.isMultiColumnPartition = this.isMultiColumnPartition;
            p.idToStorageCacheInfo = new HashMap<>(this.idToStorageCacheInfo);
            return p;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public void setPartitionIdsForRestore(Map<Long, Long> partitionOldIdToNewId) {
        Map<Long, DataProperty> oldIdToDataProperty = this.idToDataProperty;
        Map<Long, Short> oldIdToReplicationNum = this.idToReplicationNum;
        Map<Long, DataCacheInfo> oldIdToStorageCacheInfo = this.idToStorageCacheInfo;

        this.idToDataProperty = new HashMap<>();
        this.idToReplicationNum = new HashMap<>();
        this.idToStorageCacheInfo = new HashMap<>();

        for (Map.Entry<Long, Long> entry : partitionOldIdToNewId.entrySet()) {
            Long oldId = entry.getKey();
            Long newId = entry.getValue();

            DataProperty dataProperty = oldIdToDataProperty.get(oldId);
            if (dataProperty != null) {
                this.idToDataProperty.put(newId, dataProperty);
            }
            Short replicationNum = oldIdToReplicationNum.get(oldId);
            if (replicationNum != null) {
                this.idToReplicationNum.put(newId, replicationNum);
            }
            DataCacheInfo dataCacheInfo = oldIdToStorageCacheInfo.get(oldId);
            if (dataCacheInfo != null) {
                this.idToStorageCacheInfo.put(newId, dataCacheInfo);
            }
        }
    }
}
