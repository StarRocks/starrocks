// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java

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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TWriteQuorumType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TableProperty contains additional information about OlapTable
 * TableProperty includes properties to persistent the additional information
 * Different properties is recognized by prefix such as dynamic_partition
 * If there is different type properties is added.Write a method such as buildDynamicProperty to build it.
 */
public class TableProperty implements Writable, GsonPostProcessable {
    public static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition";
    public static final int NO_TTL = -1;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private transient DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(Maps.newHashMap());
    // table's default replication num
    private Short replicationNum = FeConstants.default_replication_num;

    // partition time to live number, -1 means no ttl
    private int partitionTTLNumber = NO_TTL;

    private boolean isInMemory = false;

    private boolean enablePersistentIndex = false;

    /*
     * the default storage format of this table.
     * DEFAULT: depends on BE's config 'default_rowset_type'
     * V1: alpha rowset
     * V2: beta rowset
     *
     * This property should be set when creating the table, and can only be changed to V2 using Alter Table stmt.
     */
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    // the default compression type of this table.
    private TCompressionType compressionType = TCompressionType.LZ4_FRAME;

    // the default write quorum
    private TWriteQuorumType writeQuorum = TWriteQuorumType.MAJORITY;

    // 1. This table has been deleted. if hasDelete is false, the BE segment must don't have deleteConditions.
    //    If hasDelete is true, the BE segment maybe have deleteConditions because compaction.
    // 2. Before checkpoint, we relay delete job journal log to persist.
    //    After checkpoint, we relay TableProperty::write to persist.
    private boolean hasDelete = false;

    private boolean hasForbitGlobalDict = false;

    @SerializedName(value = "storageInfo")
    private StorageInfo storageInfo;

    public TableProperty(Map<String, String> properties) {
        this.properties = properties;
    }

    public static boolean isSamePrefixProperties(Map<String, String> properties, String prefix) {
        for (String value : properties.keySet()) {
            if (!value.startsWith(prefix)) {
                return false;
            }
        }
        return true;
    }

    public TableProperty buildProperty(short opCode) {
        switch (opCode) {
            case OperationType.OP_DYNAMIC_PARTITION:
                buildDynamicProperty();
                break;
            case OperationType.OP_MODIFY_REPLICATION_NUM:
                buildReplicationNum();
                break;
            case OperationType.OP_MODIFY_IN_MEMORY:
                buildInMemory();
                break;
            case OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX:
                buildEnablePersistentIndex();
                break;
            case OperationType.OP_MODIFY_WRITE_QUORUM:
                buildWriteQuorum();
                break;
            case OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES:
                buildMvProperties();
                break;
            default:
                break;
        }
        return this;
    }

    public TableProperty buildMvProperties() {
        partitionTTLNumber = Integer.parseInt(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER,
                String.valueOf(NO_TTL)));
        return this;
    }

    public TableProperty buildDynamicProperty() {
        HashMap<String, String> dynamicPartitionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                dynamicPartitionProperties.put(entry.getKey(), entry.getValue());
            }
        }
        dynamicPartitionProperty = new DynamicPartitionProperty(dynamicPartitionProperties);
        return this;
    }

    public TableProperty buildReplicationNum() {
        replicationNum = Short.parseShort(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                String.valueOf(FeConstants.default_replication_num)));
        return this;
    }

    public TableProperty buildPartitionTTL() {
        partitionTTLNumber = Integer.parseInt(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER,
                String.valueOf(NO_TTL)));
        return this;
    }

    public TableProperty buildInMemory() {
        isInMemory = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_INMEMORY, "false"));
        return this;
    }

    public TableProperty buildStorageFormat() {
        storageFormat = TStorageFormat.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT,
                TStorageFormat.DEFAULT.name()));
        return this;
    }

    public TableProperty buildCompressionType() {
        compressionType = TCompressionType.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_COMPRESSION,
                TCompressionType.LZ4_FRAME.name()));
        return this;
    }

    public TableProperty buildWriteQuorum() {
        writeQuorum = WriteQuorum
                .findTWriteQuorumByName(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM,
                        WriteQuorum.MAJORITY));
        return this;
    }
    public TableProperty buildEnablePersistentIndex() {
        enablePersistentIndex = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "false"));
        return this;
    }

    public void modifyTableProperties(Map<String, String> modifyProperties) {
        properties.putAll(modifyProperties);
    }

    public void modifyTableProperties(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public DynamicPartitionProperty getDynamicPartitionProperty() {
        return dynamicPartitionProperty;
    }

    public Short getReplicationNum() {
        return replicationNum;
    }

    public void setPartitionTTLNumber(int partitionTTLNumber) {
        this.partitionTTLNumber = partitionTTLNumber;
    }

    public int getPartitionTTLNumber() {
        return partitionTTLNumber;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean enablePersistentIndex() {
        return enablePersistentIndex;
    }

    public TWriteQuorumType writeQuorum() {
        return writeQuorum;
    }

    public TStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public TCompressionType getCompressionType() {
        return compressionType;
    }

    public boolean hasDelete() {
        return hasDelete;
    }

    public void setHasDelete(boolean hasDelete) {
        this.hasDelete = hasDelete;
    }

    public boolean hasForbitGlobalDict() {
        return hasForbitGlobalDict;
    }

    public void setHasForbitGlobalDict(boolean hasForbitGlobalDict) {
        this.hasForbitGlobalDict = hasForbitGlobalDict;
    }

    public void setStorageInfo(StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }

    public StorageInfo getStorageInfo() {
        return storageInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableProperty read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableProperty.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        buildDynamicProperty();
        buildReplicationNum();
        buildInMemory();
        buildStorageFormat();
        buildEnablePersistentIndex();
        buildCompressionType();
        buildWriteQuorum();
        buildPartitionTTL();
    }
}
