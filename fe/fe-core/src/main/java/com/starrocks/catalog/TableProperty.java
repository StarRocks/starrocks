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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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

    public static final String BINLOG_PROPERTY_PREFIX = "binlog";
    public static final String BINLOG_PARTITION = "binlog_partition_";

    public enum QueryRewriteConsistencyMode {
        DISABLE,    // 0: disable query rewrite
        LOOSE,      // 1: enable query rewrite, and skip the partition version check, still need to check mv partition exist
        CHECKED,    // 2: enable query rewrite, and rewrite only if mv partition version is consistent with table meta
        NOCHECK;   // 3: enable query rewrite, and rewrite without any check

        public static QueryRewriteConsistencyMode defaultQueryRewriteConsistencyMode() {
            return CHECKED;
        }

        public static QueryRewriteConsistencyMode defaultForExternalTable() {
            return CHECKED;
        }

        public static QueryRewriteConsistencyMode parse(String str) {
            return EnumUtils.getEnumIgnoreCase(QueryRewriteConsistencyMode.class, str);
        }

        public static String valueList() {
            return Joiner.on("/").join(QueryRewriteConsistencyMode.values());
        }
    }

    /**
     * The value for enable_query_rewrite variable
     */
    public enum MVQueryRewriteSwitch {
        DEFAULT,    // default, no check but eligible for query rewrite
        TRUE,       // enabled, check the semantic and is eligible for query rewrite
        FALSE,      // disabled
        ;

        public boolean isEnable() {
            return TRUE == this || DEFAULT == this;
        }

        public static MVQueryRewriteSwitch defaultValue() {
            return DEFAULT;
        }

        public static MVQueryRewriteSwitch parse(String str) {
            if (StringUtils.isEmpty(str)) {
                return DEFAULT;
            }
            return EnumUtils.getEnumIgnoreCase(MVQueryRewriteSwitch.class, str);
        }

        public static String valueList() {
            return Joiner.on(",").join(MVQueryRewriteSwitch.values());
        }
    }

    /**
     * The value for transparent_mv_rewrite_mode variable
     */
    public enum MVTransparentRewriteMode {
        FALSE, // default, mv acts as a normal table, only return the contained data no matter it's fresh or not
        TRUE, // transparent, mv acts as transparent table of its defined query, its result is the same as its
        // defined query.And it will redirect to its defined query if transparent rewrite failed or exceptions occurs.
        TRANSPARENT_OR_ERROR, // try to transparent rewrite, and it will throw exception if transparent rewrite failed or
        // exceptions occurs.
        TRANSPARENT_OR_DEFAULT; // try to transparent rewrite, and it will use the original materialized view without partition
        // compensated if transparent rewrite failed or exceptions occurs.

        public boolean isEnable() {
            return TRUE == this || TRANSPARENT_OR_ERROR == this || TRANSPARENT_OR_DEFAULT == this;
        }

        public static MVTransparentRewriteMode defaultValue() {
            return FALSE;
        }

        public static MVTransparentRewriteMode parse(String str) {
            if (StringUtils.isEmpty(str)) {
                return FALSE;
            }
            return EnumUtils.getEnumIgnoreCase(MVTransparentRewriteMode.class, str);
        }

        public static String valueList() {
            return Joiner.on(",").join(MVTransparentRewriteMode.values());

        }
    }

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private transient DynamicPartitionProperty dynamicPartitionProperty =
            new DynamicPartitionProperty(Maps.newHashMap());
    // table's default replication num
    private Short replicationNum = RunMode.defaultReplicationNum();

    // partition time to live number, -1 means no ttl
    private int partitionTTLNumber = INVALID;

    private PeriodDuration partitionTTL = PeriodDuration.ZERO;

    // This property only applies to materialized views
    // It represents the maximum number of partitions that will be refreshed by a TaskRun refresh
    private int partitionRefreshNumber = Config.default_mv_partition_refresh_number;

    // This property only applies to materialized views
    // When using the system to automatically refresh, the maximum range of the most recent partitions will be refreshed.
    // By default, all partitions will be refreshed.
    private int autoRefreshPartitionsLimit = INVALID;

    // This property only applies to materialized views,
    // Indicates which tables do not listen to auto refresh events when load
    private List<TableName> excludedTriggerTables;

    // This property only applies to materialized views
    private List<String> mvSortKeys;

    // This property only applies to materialized views,
    // Specify the query rewrite behaviour for external table
    private QueryRewriteConsistencyMode forceExternalTableQueryRewrite =
            QueryRewriteConsistencyMode.defaultForExternalTable();

    // This property only applies to materialized views,
    // Specify the query rewrite behaviour for external table
    private QueryRewriteConsistencyMode queryRewriteConsistencyMode =
            QueryRewriteConsistencyMode.defaultQueryRewriteConsistencyMode();

    private MVQueryRewriteSwitch mvQueryRewriteSwitch = MVQueryRewriteSwitch.DEFAULT;
    private MVTransparentRewriteMode mvTransparentRewriteMode = MVTransparentRewriteMode.FALSE;

    private boolean isInMemory = false;

    private boolean enablePersistentIndex = false;

    // Only meaningful when enablePersistentIndex = true.
    TPersistentIndexType persistentIndexType;

    private int primaryIndexCacheExpireSec = 0;

    /*
     * the default storage volume of this table.
     * DEFAULT: depends on FE's config 'run_mode'
     * run_mode == "shared_nothing": "local"
     * run_mode == "shared_data": "default"
     *
     * This property should be set when creating the table, and can not be changed.
     */
    private String storageVolume;

    private PeriodDuration storageCoolDownTTL;

    // This property only applies to materialized views
    private String resourceGroup = null;

    // the default compression type of this table.
    private TCompressionType compressionType = TCompressionType.LZ4_FRAME;

    @SerializedName(value = "compressionLevel")
    // the default compression level of this table, only used for zstd for now.
    private int compressionLevel = -1;

    // the default write quorum
    private TWriteQuorumType writeQuorum = TWriteQuorumType.MAJORITY;

    // the default disable replicated storage
    private boolean enableReplicatedStorage = false;

    private String storageType;

    // the default automatic bucket size
    private long bucketSize = 0;

    // the default mutable bucket number
    private long mutableBucketNum = 0;

    private boolean enableLoadProfile = false;

    // 1. This table has been deleted. if hasDelete is false, the BE segment must don't have deleteConditions.
    //    If hasDelete is true, the BE segment maybe have deleteConditions because compaction.
    // 2. Before checkpoint, we relay delete job journal log to persist.
    //    After checkpoint, we relay TableProperty::write to persist.
    @SerializedName(value = "hasDelete")
    private boolean hasDelete = false;
    @SerializedName(value = "hasForbitGlobalDict")
    private boolean hasForbiddenGlobalDict = false;

    @SerializedName(value = "storageInfo")
    private StorageInfo storageInfo;

    // partitionId -> binlogAvailableVersion
    private Map<Long, Long> binlogAvailableVersions = new HashMap<>();

    private BinlogConfig binlogConfig;

    // unique constraints for mv rewrite
    // a table may have multi unique constraints
    private List<UniqueConstraint> uniqueConstraints;

    // foreign key constraint for mv rewrite
    private List<ForeignKeyConstraint> foreignKeyConstraints;

    private boolean useFastSchemaEvolution;

    private PeriodDuration dataCachePartitionDuration;

    private Multimap<String, String> location;

    public TableProperty() {
        this(Maps.newLinkedHashMap());
    }

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
        newTableProperty.hasForbiddenGlobalDict = this.hasForbiddenGlobalDict;
        if (this.storageInfo != null) {
            newTableProperty.storageInfo =
                    new StorageInfo(this.storageInfo.getFilePathInfo(), this.storageInfo.getCacheInfo());
        }
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
                buildPersistentIndexType();
                break;
            case OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC:
                buildPrimaryIndexCacheExpireSec();
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
            case OperationType.OP_MODIFY_BUCKET_SIZE:
                buildBucketSize();
                break;
            case OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM:
                buildMutableBucketNum();
                break;
            case OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE:
                buildEnableLoadProfile();
                break;
            case OperationType.OP_MODIFY_BINLOG_CONFIG:
                buildBinlogConfig();
                break;
            case OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION:
                buildBinlogAvailableVersion();
                break;
            case OperationType.OP_ALTER_TABLE_PROPERTIES:
                buildPartitionLiveNumber();
                buildDataCachePartitionDuration();
                buildLocation();
                buildStorageCoolDownTTL();
                break;
            case OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY:
                buildConstraint();
                break;
            default:
                break;
        }
        return this;
    }

    // TODO: refactor the postProcessing code into listener-based instead of procedure-oriented
    public TableProperty buildMvProperties() {
        buildPartitionTTL();
        buildPartitionRefreshNumber();
        buildAutoRefreshPartitionsLimit();
        buildExcludedTriggerTables();
        buildResourceGroup();
        buildConstraint();
        buildMvSortKeys();
        buildQueryRewrite();
        buildMVQueryRewriteSwitch();
        buildMVTransparentRewriteMode();
        return this;
    }

    public TableProperty buildBinlogConfig() {
        long binlogVersion = Long.parseLong(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION,
                String.valueOf(INVALID)));
        boolean binlogEnable = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE,
                "false"));
        long binlogTtlSecond = Long.parseLong(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_BINLOG_TTL,
                String.valueOf(Config.binlog_ttl_second)));
        long binlogMaxSize = Long.parseLong(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE,
                String.valueOf(Config.binlog_max_size)));
        binlogConfig = new BinlogConfig(binlogVersion, binlogEnable, binlogTtlSecond, binlogMaxSize);
        return this;
    }

    // just modify binlogConfig, not properties
    public void setBinlogConfig(BinlogConfig binlogConfig) {
        this.binlogConfig = binlogConfig;
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

    public TableProperty buildBinlogAvailableVersion() {
        binlogAvailableVersions = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(BINLOG_PARTITION)) {
                long partitionId = Long.parseLong(entry.getKey().split("_")[2]);
                binlogAvailableVersions.put(partitionId, Long.parseLong(entry.getValue()));
            }
        }
        return this;
    }

    public TableProperty buildReplicationNum() {
        replicationNum = Short.parseShort(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                String.valueOf(RunMode.defaultReplicationNum())));
        return this;
    }

    public TableProperty buildPartitionTTL() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            partitionTTLNumber = Integer.parseInt(properties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER));
        }
        return this;
    }

    public TableProperty buildPartitionLiveNumber() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            partitionTTLNumber = Integer.parseInt(properties.get(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER));
        }
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

    public TableProperty buildResourceGroup() {
        resourceGroup = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP,
                null);
        return this;
    }

    public TableProperty buildExcludedTriggerTables() {
        String excludedRefreshConf = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, null);
        List<TableName> tables = Lists.newArrayList();
        if (excludedRefreshConf != null) {
            List<String> tableList = Splitter.on(",").omitEmptyStrings().trimResults()
                    .splitToList(excludedRefreshConf);
            for (String table : tableList) {
                TableName tableName = AnalyzerUtils.stringToTableName(table);
                tables.add(tableName);
            }
        }
        excludedTriggerTables = tables;
        return this;
    }

    public TableProperty buildMvSortKeys() {
        String sortKeys = properties.get(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS);
        this.mvSortKeys = analyzeMvSortKeys(sortKeys);
        return this;
    }

    public void putMvSortKeys() {
        if (CollectionUtils.isNotEmpty(mvSortKeys)) {
            String value = Joiner.on(",").join(mvSortKeys);
            this.properties.put(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS, value);
        }
    }

    public static List<String> analyzeMvSortKeys(String value) {
        if (StringUtils.isEmpty(value)) {
            return Lists.newArrayList();
        } else {
            return Splitter.on(",").omitEmptyStrings().trimResults().splitToList(value);
        }
    }

    public TableProperty buildMVQueryRewriteSwitch() {
        String value = properties.get(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
        this.mvQueryRewriteSwitch = MVQueryRewriteSwitch.parse(value);
        return this;
    }

    public static MVQueryRewriteSwitch analyzeQueryRewriteSwitch(String value) {
        MVQueryRewriteSwitch res = MVQueryRewriteSwitch.parse(value);
        if (res == null) {
            String valueList = MVQueryRewriteSwitch.valueList();
            throw new SemanticException(
                    PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE
                            + " can only be " + valueList + " but got " + value);
        }
        return res;
    }

    public TableProperty buildMVTransparentRewriteMode() {
        String value = properties.get(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
        this.mvTransparentRewriteMode = MVTransparentRewriteMode.parse(value);
        return this;
    }

    public static MVTransparentRewriteMode analyzeMVTransparentRewrite(String value) {
        MVTransparentRewriteMode res = MVTransparentRewriteMode.parse(value);
        if (res == null) {
            String valueList = MVTransparentRewriteMode.valueList();
            throw new SemanticException(
                    PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE
                            + " can only be " + valueList + " but got " + value);
        }
        return res;
    }

    public static QueryRewriteConsistencyMode analyzeQueryRewriteMode(String value) {
        QueryRewriteConsistencyMode res = EnumUtils.getEnumIgnoreCase(QueryRewriteConsistencyMode.class, value);
        if (res == null) {
            String allValues = EnumUtils.getEnumList(QueryRewriteConsistencyMode.class)
                    .stream().map(Enum::name).collect(Collectors.joining(","));
            throw new SemanticException(
                    PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY + " could only be " + allValues + " but got " + value);
        }
        return res;
    }

    public static QueryRewriteConsistencyMode analyzeExternalTableQueryRewrite(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            // old version use the boolean value
            boolean boolValue = Boolean.parseBoolean(value);
            return boolValue ? QueryRewriteConsistencyMode.CHECKED : QueryRewriteConsistencyMode.DISABLE;
        } else {
            QueryRewriteConsistencyMode res = EnumUtils.getEnumIgnoreCase(QueryRewriteConsistencyMode.class, value);
            if (res == null) {
                String allValues = EnumUtils.getEnumList(QueryRewriteConsistencyMode.class)
                        .stream().map(Enum::name).collect(Collectors.joining(","));
                throw new SemanticException(
                        PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE + " could only be " + allValues + " but " +
                                "got " + value);
            }
            return res;
        }
    }

    public TableProperty buildQueryRewrite() {
        // NOTE: keep compatible with previous version
        String value = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
        forceExternalTableQueryRewrite = QueryRewriteConsistencyMode.defaultForExternalTable();
        if (value != null) {
            try {
                forceExternalTableQueryRewrite = analyzeExternalTableQueryRewrite(value);
            } catch (SemanticException e) {
                LOG.error("analyze {} failed", PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, e);
            }
        }

        queryRewriteConsistencyMode = QueryRewriteConsistencyMode.defaultQueryRewriteConsistencyMode();
        value = properties.get(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
        if (value != null) {
            try {
                queryRewriteConsistencyMode = analyzeQueryRewriteMode(value);
            } catch (SemanticException e) {
                LOG.error("analyze {} failed", PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, e);
            }
        }

        return this;
    }

    public TableProperty buildInMemory() {
        isInMemory = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_INMEMORY, "false"));
        return this;
    }

    public TableProperty buildStorageVolume() {
        storageVolume = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME,
                RunMode.isSharedDataMode() ? "default" : "local");
        return this;
    }

    public TableProperty buildCompressionType() {
        String compression = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_COMPRESSION,
                TCompressionType.LZ4_FRAME.name());
        for (TCompressionType type : TCompressionType.values()) {
            if (type.name().equalsIgnoreCase(compression)) {
                compressionType = type;
                return this;
            }
        }

        compressionType = TCompressionType.LZ4_FRAME;
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

    public TableProperty buildBucketSize() {
        if (properties.get(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE) != null) {
            bucketSize = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE));
        }
        return this;
    }

    public TableProperty buildMutableBucketNum() {
        if (properties.get(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM) != null) {
            mutableBucketNum = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM));
        }
        return this;
    }

    public TableProperty buildEnableLoadProfile() {
        if (properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE) != null) {
            enableLoadProfile = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE));
        }
        return this;
    }

    public TableProperty buildEnablePersistentIndex() {
        enablePersistentIndex = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "false"));
        return this;
    }

    public TableProperty buildPrimaryIndexCacheExpireSec() {
        primaryIndexCacheExpireSec = Integer.parseInt(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC, "0"));
        return this;
    }

    public TableProperty buildPersistentIndexType() {
        String type = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, "LOCAL");
        if (type.equals("LOCAL")) {
            persistentIndexType = TPersistentIndexType.LOCAL;
        } else if (type.equals("CLOUD_NATIVE")) {
            persistentIndexType = TPersistentIndexType.CLOUD_NATIVE;
        }
        return this;
    }

    public static String persistentIndexTypeToString(TPersistentIndexType type) {
        switch (type) {
            case LOCAL:
                return "LOCAL";
            case CLOUD_NATIVE:
                return "CLOUD_NATIVE";
            default:
                // shouldn't happen
                // for it has been checked outside
                LOG.warn("unknown PersistentIndexType");
                return "UNKNOWN";
        }
    }

    public TableProperty buildConstraint() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            try {
                uniqueConstraints = UniqueConstraint.parse(null, null, null,
                        properties.getOrDefault(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, ""));
            } catch (Exception e) {
                LOG.warn("Failed to parse unique constraints, ignore this unique constraint", e);
            }
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            try {
                foreignKeyConstraints = ForeignKeyConstraint.parse(
                        properties.getOrDefault(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, ""));
            } catch (Exception e) {
                LOG.warn("Failed to parse foreign key constraints, ignore this foreign key constraints", e);
            }
        }
        return this;
    }

    public TableProperty buildDataCachePartitionDuration() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
            dataCachePartitionDuration = TimeUtils.parseHumanReadablePeriodOrDuration(
                    properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION));
        }
        return this;
    }

    public TableProperty buildStorageCoolDownTTL() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (Strings.isNullOrEmpty(storageCoolDownTTL)) {
                this.storageCoolDownTTL = null;
            } else {
                this.storageCoolDownTTL = TimeUtils.parseHumanReadablePeriodOrDuration(storageCoolDownTTL);
            }
        }
        return this;
    }

    public TableProperty buildStorageType() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
            storageType = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE);
        }
        return this;
    }

    public TableProperty buildLocation() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            String locationStr = properties.get(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
            if (locationStr.isEmpty()) {
                properties.remove(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
                location = null;
            } else {
                location = PropertyAnalyzer.analyzeLocationStringToMap(
                        properties.get(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION));
            }
        } else {
            location = null;
        }
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

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public void setPartitionTTLNumber(int partitionTTLNumber) {
        this.partitionTTLNumber = partitionTTLNumber;
    }

    public int getPartitionTTLNumber() {
        return partitionTTLNumber;
    }

    public void setPartitionTTL(PeriodDuration ttlDuration) {
        this.partitionTTL = ttlDuration;
    }

    public PeriodDuration getPartitionTTL() {
        return partitionTTL;
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

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public List<TableName> getExcludedTriggerTables() {
        return excludedTriggerTables;
    }

    public void setExcludedTriggerTables(List<TableName> excludedTriggerTables) {
        this.excludedTriggerTables = excludedTriggerTables;
    }

    public List<String> getMvSortKeys() {
        return mvSortKeys;
    }

    public void setMvSortKeys(List<String> sortKeys) {
        this.mvSortKeys = sortKeys;
    }

    public QueryRewriteConsistencyMode getForceExternalTableQueryRewrite() {
        return this.forceExternalTableQueryRewrite;
    }

    public void setForceExternalTableQueryRewrite(QueryRewriteConsistencyMode externalTableQueryRewrite) {
        this.forceExternalTableQueryRewrite = externalTableQueryRewrite;
    }

    public void setQueryRewriteConsistencyMode(QueryRewriteConsistencyMode mode) {
        this.queryRewriteConsistencyMode = mode;
    }

    public QueryRewriteConsistencyMode getQueryRewriteConsistencyMode() {
        return this.queryRewriteConsistencyMode;
    }

    public void setMvQueryRewriteSwitch(MVQueryRewriteSwitch value) {
        this.mvQueryRewriteSwitch = value;
    }

    public MVQueryRewriteSwitch getMvQueryRewriteSwitch() {
        return this.mvQueryRewriteSwitch;
    }

    public void setMvTransparentRewriteMode(MVTransparentRewriteMode value) {
        this.mvTransparentRewriteMode = value;
    }

    public MVTransparentRewriteMode getMvTransparentRewriteMode() {
        return this.mvTransparentRewriteMode;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean enablePersistentIndex() {
        return enablePersistentIndex;
    }

    public int primaryIndexCacheExpireSec() {
        return primaryIndexCacheExpireSec;
    }

    public String getPersistentIndexTypeString() {
        return persistentIndexTypeToString(persistentIndexType);
    }

    public TPersistentIndexType getPersistentIndexType() {
        return persistentIndexType;
    }

    public String storageType() {
        return storageType;
    }

    public Multimap<String, String> getLocation() {
        return location;
    }

    public TWriteQuorumType writeQuorum() {
        return writeQuorum;
    }

    public boolean enableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public long getBucketSize() {
        return bucketSize;
    }

    public long getMutableBucketNum() {
        return mutableBucketNum;
    }

    public boolean enableLoadProfile() {
        return enableLoadProfile;
    }

    public String getStorageVolume() {
        return storageVolume;
    }

    public TCompressionType getCompressionType() {
        return compressionType;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public boolean hasDelete() {
        return hasDelete;
    }

    public void setHasDelete(boolean hasDelete) {
        this.hasDelete = hasDelete;
    }

    public boolean hasForbiddenGlobalDict() {
        return hasForbiddenGlobalDict;
    }

    public void setHasForbiddenGlobalDict(boolean hasForbiddenGlobalDict) {
        this.hasForbiddenGlobalDict = hasForbiddenGlobalDict;
    }

    public void setStorageInfo(StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }

    public StorageInfo getStorageInfo() {
        return storageInfo;
    }

    public BinlogConfig getBinlogConfig() {
        return binlogConfig;
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

    public Map<Long, Long> getBinlogAvailableVersions() {
        return binlogAvailableVersions;
    }

    public PeriodDuration getDataCachePartitionDuration() {
        return dataCachePartitionDuration;
    }

    public PeriodDuration getStorageCoolDownTTL() {
        return storageCoolDownTTL;
    }

    public void clearBinlogAvailableVersion() {
        binlogAvailableVersions.clear();
        for (Iterator<Map.Entry<String, String>> it = properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey().startsWith(BINLOG_PARTITION)) {
                it.remove();
            }
        }
    }

    public TableProperty buildUseFastSchemaEvolution() {
        useFastSchemaEvolution = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_USE_FAST_SCHEMA_EVOLUTION, "false"));
        return this;
    }

    public boolean getUseFastSchemaEvolution() {
        return useFastSchemaEvolution;
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
        try {
            buildDynamicProperty();
        } catch (Exception ex) {
            LOG.warn("build dynamic property failed", ex);
        }
        buildReplicationNum();
        buildInMemory();
        buildStorageVolume();
        buildStorageCoolDownTTL();
        buildEnablePersistentIndex();
        buildPersistentIndexType();
        buildPrimaryIndexCacheExpireSec();
        buildCompressionType();
        buildWriteQuorum();
        buildPartitionLiveNumber();
        buildReplicatedStorage();
        buildBucketSize();
        buildEnableLoadProfile();
        buildBinlogConfig();
        buildBinlogAvailableVersion();
        buildDataCachePartitionDuration();
        buildUseFastSchemaEvolution();
        buildStorageType();
        buildMvProperties();
        buildLocation();
    }
}
