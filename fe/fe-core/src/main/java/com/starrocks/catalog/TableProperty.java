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

<<<<<<< HEAD
import com.clearspring.analytics.util.Lists;
=======
import com.google.common.base.Joiner;
>>>>>>> 4277d9435f ([Enhancement] introduce loose query rewrite mode (#27280))
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TableProperty contains additional information about OlapTable
 * TableProperty includes properties to persistent the additional information
 * Different properties is recognized by prefix such as dynamic_partition
 * If there is different type properties is added.Write a method such as buildDynamicProperty to build it.
 */
public class TableProperty implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TableProperty.class);
    public static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition";
    public static final int INVALID = -1;

<<<<<<< HEAD
=======
    public static final String BINLOG_PROPERTY_PREFIX = "binlog";
    public static final String BINLOG_PARTITION = "binlog_partition_";

    public enum QueryRewriteConsistencyMode {
        DISABLE,    // 0: disable query rewrite
        LOOSE,      // 1: enable query rewrite, and skip the partition version check
        CHECKED;    // 2: enable query rewrite, and rewrite only if mv partition version is consistent with table meta

        public static QueryRewriteConsistencyMode defaultForOlapTable() {
            return CHECKED;
        }

        public static QueryRewriteConsistencyMode defaultForExternalTable() {
            return DISABLE;
        }

        public static QueryRewriteConsistencyMode parse(String str) {
            return EnumUtils.getEnumIgnoreCase(QueryRewriteConsistencyMode.class, str);
        }

        public static String valueList() {
            return Joiner.on("/").join(QueryRewriteConsistencyMode.values());
        }
    }

>>>>>>> 4277d9435f ([Enhancement] introduce loose query rewrite mode (#27280))
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private transient DynamicPartitionProperty dynamicPartitionProperty =
            new DynamicPartitionProperty(Maps.newHashMap());
    // table's default replication num
    private Short replicationNum = FeConstants.default_replication_num;

    // partition time to live number, -1 means no ttl
    private int partitionTTLNumber = INVALID;

    // This property only applies to materialized views
    // It represents the maximum number of partitions that will be refreshed by a TaskRun refresh
    private int partitionRefreshNumber = INVALID;

    // This property only applies to materialized views
    // When using the system to automatically refresh, the maximum range of the most recent partitions will be refreshed.
    // By default, all partitions will be refreshed.
    private int autoRefreshPartitionsLimit = INVALID;

    // This property only applies to materialized views,
    // Indicates which tables do not listen to auto refresh events when load
    private List<TableName> excludedTriggerTables;

    // This property only applies to materialized views,
    // Specify the query rewrite behaviour for external table
    private QueryRewriteConsistencyMode forceExternalTableQueryRewrite =
            QueryRewriteConsistencyMode.defaultForExternalTable();

    // This property only applies to materialized views,
    // Specify the query rewrite behaviour for external table
    private QueryRewriteConsistencyMode olapTableQueryRewrite = QueryRewriteConsistencyMode.defaultForOlapTable();

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

    // the default disable replicated storage
    private boolean enableReplicatedStorage = false;

    // 1. This table has been deleted. if hasDelete is false, the BE segment must don't have deleteConditions.
    //    If hasDelete is true, the BE segment maybe have deleteConditions because compaction.
    // 2. Before checkpoint, we relay delete job journal log to persist.
    //    After checkpoint, we relay TableProperty::write to persist.
    @SerializedName(value = "hasDelete")
    private boolean hasDelete = false;
    @SerializedName(value = "hasForbitGlobalDict")
    private boolean hasForbitGlobalDict = false;

    @SerializedName(value = "storageInfo")
    private StorageInfo storageInfo;

    // unique constraints for mv rewrite
    // a table may have multi unique constraints
    private List<UniqueConstraint> uniqueConstraints;

    // foreign key constraint for mv rewrite
    private List<ForeignKeyConstraint> foreignKeyConstraints;
    public TableProperty(Map<String, String> properties) {
        this.properties = properties;
    }

    public TableProperty copy() {
        TableProperty newTableProperty = new TableProperty(Maps.newHashMap(this.properties));
        try {
            newTableProperty.gsonPostProcess();
        } catch (IOException e) {
            Preconditions.checkState(false, "gsonPostProcess shouldn't fail");
        }
        newTableProperty.hasDelete = this.hasDelete;
        newTableProperty.hasForbitGlobalDict = this.hasDelete;
        return newTableProperty;
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
            case OperationType.OP_MODIFY_REPLICATED_STORAGE:
                buildReplicatedStorage();
                break;
            case OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY:
                buildConstraint();
                break;
            default:
                break;
        }
        return this;
    }

    public TableProperty buildMvProperties() {
        buildPartitionTTL();
        buildPartitionRefreshNumber();
        buildAutoRefreshPartitionsLimit();
        buildExcludedTriggerTables();
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

<<<<<<< HEAD
=======
    public TableProperty buildBinlogAvailableVersion() {
        binlogAvailabeVersions = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(BINLOG_PARTITION)) {
                long partitionId = Long.parseLong(entry.getKey().split("_")[2]);
                binlogAvailabeVersions.put(partitionId, Long.parseLong(entry.getValue()));
            }
        }
        return this;
    }

>>>>>>> 4277d9435f ([Enhancement] introduce loose query rewrite mode (#27280))
    public TableProperty buildReplicationNum() {
        replicationNum = Short.parseShort(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                String.valueOf(FeConstants.default_replication_num)));
        return this;
    }

    public TableProperty buildPartitionTTL() {
        partitionTTLNumber = Integer.parseInt(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER,
                String.valueOf(INVALID)));
        return this;
    }

    public TableProperty buildAutoRefreshPartitionsLimit() {
        autoRefreshPartitionsLimit =
                Integer.parseInt(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT,
                        String.valueOf(INVALID)));
        return this;
    }

    public TableProperty buildPartitionRefreshNumber() {
        partitionRefreshNumber =
                Integer.parseInt(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER,
                        String.valueOf(INVALID)));
        return this;
    }

    public TableProperty buildExcludedTriggerTables() {
        String excludedRefreshConf = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, null);
        List<TableName> tables = Lists.newArrayList();
        if (excludedRefreshConf == null) {
            excludedTriggerTables = tables;
        } else {
            List<String> tableList = Splitter.on(",").omitEmptyStrings().trimResults()
                    .splitToList(excludedRefreshConf);
            for (String table : tableList) {
                TableName tableName = AnalyzerUtils.stringToTableName(table);
                tables.add(tableName);
            }
            excludedTriggerTables = tables;
        }
        return this;
    }

    public static QueryRewriteConsistencyMode analyzeQueryRewriteMode(String value) throws AnalysisException {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            // old version use the boolean value
            boolean boolValue = Boolean.parseBoolean(value);
            return boolValue ? QueryRewriteConsistencyMode.CHECKED : QueryRewriteConsistencyMode.DISABLE;
        } else {
            QueryRewriteConsistencyMode res = EnumUtils.getEnumIgnoreCase(QueryRewriteConsistencyMode.class, value);
            if (res == null) {
                String allValues = EnumUtils.getEnumList(QueryRewriteConsistencyMode.class)
                        .stream().map(Enum::name).collect(Collectors.joining(","));
                throw new AnalysisException(
                        "force_external_table_query_rewrite could only be " + allValues + " but got " + value);
            }
            return res;
        }
    }

    public TableProperty buildForceExternalTableQueryRewrite() {
        // NOTE: keep compatible with previous version
        String value = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
        if (value != null) {
            try {
                forceExternalTableQueryRewrite = analyzeQueryRewriteMode(value);
            } catch (AnalysisException e) {
                LOG.error("analyze {} failed", PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, e);
            }
        }

        value = properties.get(PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE);
        if (value != null) {
            try {
                olapTableQueryRewrite = analyzeQueryRewriteMode(value);
            } catch (AnalysisException e) {
                LOG.error("analyze {} failed", PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE, e);
            }
        }

        return this;
    }

    public TableProperty buildInMemory() {
        isInMemory = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_INMEMORY, "false"));
        return this;
    }

<<<<<<< HEAD
    public TableProperty buildStorageFormat() {
        storageFormat = TStorageFormat.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT,
                TStorageFormat.DEFAULT.name()));
=======
    public TableProperty buildStorageVolume() {
        storageVolume = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME,
                RunMode.allowCreateLakeTable() ? "default" : "local");
>>>>>>> 4277d9435f ([Enhancement] introduce loose query rewrite mode (#27280))
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

    public TableProperty buildReplicatedStorage() {
        enableReplicatedStorage = Boolean
                .parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, "false"));
        return this;
    }

    public TableProperty buildEnablePersistentIndex() {
        enablePersistentIndex = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "false"));
        return this;
    }

    public TableProperty buildConstraint() {
        try {
            uniqueConstraints = UniqueConstraint.parse(
                    properties.getOrDefault(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, ""));
        } catch (AnalysisException e) {
            LOG.warn("Failed to parse unique constraint, ignore this unique constraint", e);
        }

        foreignKeyConstraints = ForeignKeyConstraint.parse(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, ""));
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

    public int getAutoRefreshPartitionsLimit() {
        return autoRefreshPartitionsLimit;
    }

    public void setAutoRefreshPartitionsLimit(int autoRefreshPartitionsLimit) {
        this.autoRefreshPartitionsLimit = autoRefreshPartitionsLimit;
    }

    public int getPartitionRefreshNumber() {
        return partitionRefreshNumber;
    }

    public void setPartitionRefreshNumber(int partitionRefreshNumber) {
        this.partitionRefreshNumber = partitionRefreshNumber;
    }

    public List<TableName> getExcludedTriggerTables() {
        return excludedTriggerTables;
    }

    public void setExcludedTriggerTables(List<TableName> excludedTriggerTables) {
        this.excludedTriggerTables = excludedTriggerTables;
    }

    public QueryRewriteConsistencyMode getForceExternalTableQueryRewrite() {
        return this.forceExternalTableQueryRewrite;
    }

    public void setForceExternalTableQueryRewrite(QueryRewriteConsistencyMode externalTableQueryRewrite) {
        this.forceExternalTableQueryRewrite = externalTableQueryRewrite;
    }

    public void setOlapTableQueryRewrite(QueryRewriteConsistencyMode mode) {
        this.olapTableQueryRewrite = mode;
    }

    public QueryRewriteConsistencyMode getOlapTableQueryRewrite() {
        return this.olapTableQueryRewrite;
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

    public boolean enableReplicatedStorage() {
        return enableReplicatedStorage;
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

    public List<UniqueConstraint> getUniqueConstraints() {
        return uniqueConstraints;
    }

    public void setUniqueConstraints(List<UniqueConstraint> uniqueConstraints) {
        this.uniqueConstraints = uniqueConstraints;
    }

    public List<ForeignKeyConstraint> getForeignKeyConstraints() {
        return foreignKeyConstraints;
    }

    public void setForeignKeyConstraints(List<ForeignKeyConstraint> foreignKeyConstraints) {
        this.foreignKeyConstraints = foreignKeyConstraints;
    }

<<<<<<< HEAD
=======
    public Map<Long, Long> getBinlogAvailaberVersions() {
        return binlogAvailabeVersions;
    }

    public PeriodDuration getDataCachePartitionDuration() {
        return dataCachePartitionDuration;
    }

    public void clearBinlogAvailableVersion() {
        binlogAvailabeVersions.clear();
        for (Iterator<Map.Entry<String, String>> it = properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey().startsWith(BINLOG_PARTITION)) {
                it.remove();
            }
        }
    }

>>>>>>> 4277d9435f ([Enhancement] introduce loose query rewrite mode (#27280))
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
        buildAutoRefreshPartitionsLimit();
        buildPartitionRefreshNumber();
        buildExcludedTriggerTables();
        buildReplicatedStorage();
        buildForceExternalTableQueryRewrite();
        buildConstraint();
    }
}
