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
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TCompactionStrategy;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

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
    public static final String FLAT_JSON_PROPERTY_PREFIX = "flat_json";
    public static final int INVALID = -1;

    public static final String BINLOG_PROPERTY_PREFIX = "binlog";
    public static final String BINLOG_PARTITION = "binlog_partition_";

    public static final String CLOUD_NATIVE_INDEX_TYPE = "CLOUD_NATIVE";
    public static final String LOCAL_INDEX_TYPE = "LOCAL";

    public static final String DEFAULT_COMPACTION_STRATEGY = "DEFAULT";
    public static final String REAL_TIME_COMPACTION_STRATEGY = "REAL_TIME";

    public enum QueryRewriteConsistencyMode {
        DISABLE,    // 0: disable query rewrite
        LOOSE,      // 1: enable query rewrite, and skip the partition version check, still need to check mv partition exist
        CHECKED,    // 2: enable query rewrite, and rewrite only if mv partition version is consistent with table meta
        NOCHECK,    // 3: enable query rewrite, and rewrite without any check
        FORCE_MV;   // 4: force to use mv, if mv contains no ttl; otherwise, use mv for partitions are in ttl range and
        // use base table for partitions are out of ttl range

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

    private FlatJsonConfig flatJsonConfig;

    private transient DynamicPartitionProperty dynamicPartitionProperty =
            new DynamicPartitionProperty(Maps.newHashMap());

    // table's default replication num
    private Short replicationNum = RunMode.defaultReplicationNum();

    // partition time to live number, -1 means no ttl
    private int partitionTTLNumber = INVALID;

    private PeriodDuration partitionTTL = PeriodDuration.ZERO;

    // This property can be used to specify the retention condition of the partition table or materialized view,
    // it's a SQL expression, and the partition will be deleted if the condition is not true.
    private String partitionRetentionCondition = null;

    private String timeDriftConstraintSpec = null;

    // This property only applies to materialized views
    // It represents the maximum number of partitions that will be refreshed by a TaskRun refresh
    private int partitionRefreshNumber = INVALID;

    // This property only applies to materialized views
    // It represents the mode selected to determine the number of partitions to refresh
    private String partitionRefreshStrategy = "";

    // This property only applies to materialized views/
    // It represents the mode selected to determine how to refresh the materialized view
    private String mvRefreshMode = "";

    // This property only applies to materialized views
    // When using the system to automatically refresh, the maximum range of the most recent partitions will be refreshed.
    // By default, all partitions will be refreshed.
    private int autoRefreshPartitionsLimit = INVALID;

    // This property only applies to materialized views,
    // Indicates which tables do not listen to auto refresh events when load
    private List<TableName> excludedTriggerTables;

    // This property only applies to materialized views,
    // Indicates which tables do not refresh the base table when auto refresh
    private List<TableName> excludedRefreshTables;

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

    private String baseCompactionForbiddenTimeRanges = "";

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

    private boolean fileBundling = false;

    private TCompactionStrategy compactionStrategy = TCompactionStrategy.DEFAULT;

    // Maximum number of parallel compaction subtasks per tablet
    // 0 means disable parallel compaction, positive value enables it
    // Default: 3
    @SerializedName(value = "lakeCompactionMaxParallel")
    private int lakeCompactionMaxParallel = 3;

    @SerializedName(value = "enableStatisticCollectOnFirstLoad")
    private boolean enableStatisticCollectOnFirstLoad = true;

    /**
     * Whether to enable the v2 implementation of fast schema evolution for cloud-native tables.
     * This version is more lightweight, modifying only FE metadata instead of both FE and tablet metadata.
     * It is disabled by default for existing tables to ensure backward compatibility after an upgrade.
     * New tables will have this property explicitly set to true upon creation.
     */
    private boolean cloudNativeFastSchemaEvolutionV2 = false;

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
            case OperationType.OP_MODIFY_REPLICATED_STORAGE:
                buildReplicatedStorage();
                break;
            case OperationType.OP_MODIFY_BUCKET_SIZE:
                buildBucketSize();
                break;
            case OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM:
                buildMutableBucketNum();
                break;
            case OperationType.OP_MODIFY_DEFAULT_BUCKET_NUM:
                break;
            case OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE:
                buildEnableLoadProfile();
                break;
            case OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES:
                buildBaseCompactionForbiddenTimeRanges();
                break;
            case OperationType.OP_MODIFY_BINLOG_CONFIG:
                buildBinlogConfig();
                break;
            case OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION:
                buildBinlogAvailableVersion();
                break;
            case OperationType.OP_MODIFY_FLAT_JSON_CONFIG:
                buildFlatJsonConfig();
                break;
            case OperationType.OP_ALTER_TABLE_PROPERTIES:
                buildPartitionTTL();
                buildPartitionLiveNumber();
                buildPartitionRetentionCondition();
                buildTimeDriftConstraint();
                buildDataCachePartitionDuration();
                buildLocation();
                buildStorageCoolDownTTL();
                buildEnableStatisticCollectOnFirstLoad();
                buildCloudNativeFastSchemaEvolutionV2();
                buildLakeCompactionMaxParallel();
                break;
            case OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY:
                buildConstraint();
                break;
            default:
                break;
        }
        return this;
    }

    public TableProperty buildInMemory() {
        if (properties != null) {
            properties.remove(PropertyAnalyzer.PROPERTIES_INMEMORY);
        }
        return this;
    }

    // TODO: refactor the postProcessing code into listener-based instead of procedure-oriented
    public TableProperty buildMvProperties() {
        buildPartitionTTL();
        buildPartitionRefreshNumber();
        buildMVPartitionRefreshStrategy();
        buildPartitionRetentionCondition();
        buildAutoRefreshPartitionsLimit();
        buildMVRefreshMode();
        buildExcludedTriggerTables();
        buildResourceGroup();
        buildConstraint();
        buildTimeDriftConstraint();
        buildMvSortKeys();
        buildQueryRewrite();
        buildMVQueryRewriteSwitch();
        buildMVTransparentRewriteMode();
        buildLocation();
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

    public TableProperty buildFlatJsonConfig() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX)) {
            boolean enableFlatJson = PropertyAnalyzer.analyzeFlatJsonEnabled(properties);
            
            // In gsonPostProcess, we should be tolerant of existing properties even when flat_json.enable is false.
            // The validation should be done at ALTER TABLE time, not during deserialization/copy.
            // If flat_json.enable is false, ignore other flat JSON properties and use default values.
            try {
                double flatJsonNullFactor = PropertyAnalyzer.analyzerDoubleProp(properties,
                        PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, Config.flat_json_null_factor);
                double flatJsonSparsityFactory = PropertyAnalyzer.analyzerDoubleProp(properties,
                        PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, Config.flat_json_sparsity_factory);
                int flatJsonColumnMax = PropertyAnalyzer.analyzeIntProp(properties,
                        PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, Config.flat_json_column_max);
                flatJsonConfig = new FlatJsonConfig(enableFlatJson, flatJsonNullFactor,
                        flatJsonSparsityFactory, flatJsonColumnMax);
            } catch (AnalysisException e) {
                throw new RuntimeException("Failed to analyze flat JSON properties: " + e.getMessage(), e);
            }
        }
        return this;
    }

    // just modify binlogConfig, not properties
    public void setBinlogConfig(BinlogConfig binlogConfig) {
        this.binlogConfig = binlogConfig;
    }

    public void setFlatJsonConfig(FlatJsonConfig flatJsonConfig) {
        this.flatJsonConfig = flatJsonConfig;
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

    public void setReplicationNum(short replicationNum) {
        this.replicationNum = replicationNum;
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

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            Pair<String, PeriodDuration> ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, false);
            if (ttlDuration != null) {
                partitionTTL = ttlDuration.second;
            }
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

    public TableProperty buildMVPartitionRefreshStrategy() {
        partitionRefreshStrategy = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY,
                Config.default_mv_partition_refresh_strategy);
        return this;
    }

    public TableProperty buildMVRefreshMode() {
        mvRefreshMode = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE,
                Config.default_mv_refresh_mode);
        return this;
    }

    public TableProperty buildPartitionRetentionCondition() {
        partitionRetentionCondition = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, "");
        return this;
    }

    public TableProperty buildTimeDriftConstraint() {
        timeDriftConstraintSpec = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, "");
        return this;
    }

    public TableProperty buildResourceGroup() {
        resourceGroup = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP,
                null);
        return this;
    }

    public TableProperty buildExcludedTriggerTables() {
        excludedTriggerTables = parseExcludedTables(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES);
        excludedRefreshTables = parseExcludedTables(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES);
        return this;
    }

    private List<TableName> parseExcludedTables(String propertiesKey) {
        String excludedTables = properties.getOrDefault(propertiesKey, null);
        List<TableName> tables = Lists.newArrayList();
        if (excludedTables != null) {
            List<String> tableList = Splitter.on(",").omitEmptyStrings().trimResults()
                    .splitToList(excludedTables);
            for (String table : tableList) {
                TableName tableName = AnalyzerUtils.stringToTableName(table);
                tables.add(tableName);
            }
        }
        return tables;
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

    public TableProperty buildBaseCompactionForbiddenTimeRanges() {
        baseCompactionForbiddenTimeRanges = properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES, "");
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
        String defaultType = Config.enable_cloud_native_persistent_index_by_default ? CLOUD_NATIVE_INDEX_TYPE : LOCAL_INDEX_TYPE;
        String type = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, defaultType);
        if (type.equals(LOCAL_INDEX_TYPE)) {
            persistentIndexType = TPersistentIndexType.LOCAL;
        } else if (type.equals(CLOUD_NATIVE_INDEX_TYPE)) {
            persistentIndexType = TPersistentIndexType.CLOUD_NATIVE;
        }
        return this;
    }

    public static String persistentIndexTypeToString(TPersistentIndexType type) {
        switch (type) {
            case LOCAL:
                return LOCAL_INDEX_TYPE;
            case CLOUD_NATIVE:
                return CLOUD_NATIVE_INDEX_TYPE;
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

    public void setDataCachePartitionDuration(PeriodDuration dataCachePartitionDuration) {
        this.dataCachePartitionDuration = dataCachePartitionDuration;
    }

    public TableProperty buildDataCachePartitionDuration() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
            dataCachePartitionDuration = TimeUtils.parseHumanReadablePeriodOrDuration(
                    properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION));
        }
        return this;
    }

    public TableProperty buildFileBundling() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING)) {
            fileBundling = Boolean.parseBoolean(
                    properties.getOrDefault(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING, "false"));
        }
        return this;
    }

    public void setStorageCoolDownTTL(PeriodDuration storageCoolDownTTL) {
        this.storageCoolDownTTL = storageCoolDownTTL;
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

    public TableProperty buildCompactionStrategy() {
        String defaultStrategy = properties.getOrDefault(
                    PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY, DEFAULT_COMPACTION_STRATEGY);
        if (defaultStrategy.equalsIgnoreCase(DEFAULT_COMPACTION_STRATEGY)) {
            compactionStrategy = TCompactionStrategy.DEFAULT;
        } else if (defaultStrategy.equalsIgnoreCase(REAL_TIME_COMPACTION_STRATEGY)) {
            compactionStrategy = TCompactionStrategy.REAL_TIME;
        }
        return this;
    }

    public TableProperty buildLakeCompactionMaxParallel() {
        String value = properties.get(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL);
        if (value != null) {
            try {
                lakeCompactionMaxParallel = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid lake_compaction_max_parallel value: {}", value);
                lakeCompactionMaxParallel = 0;
            }
        }
        return this;
    }

    public static String compactionStrategyToString(TCompactionStrategy strategy) {
        switch (strategy) {
            case DEFAULT:
                return DEFAULT_COMPACTION_STRATEGY;
            case REAL_TIME:
                return REAL_TIME_COMPACTION_STRATEGY;
            default:
                LOG.warn("unknown compactionStrategy");
                return "UNKNOWN";
        }
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

    public String getPartitionRetentionCondition() {
        return partitionRetentionCondition;
    }

    public void setPartitionRetentionCondition(String partitionRetentionCondition) {
        this.partitionRetentionCondition = partitionRetentionCondition;
    }

    public String getTimeDriftConstraintSpec() {
        return timeDriftConstraintSpec;
    }

    public void setTimeDriftConstraintSpec(String spec) {
        this.timeDriftConstraintSpec = spec;
    }

    public int getAutoRefreshPartitionsLimit() {
        return autoRefreshPartitionsLimit;
    }

    public void setAutoRefreshPartitionsLimit(int autoRefreshPartitionsLimit) {
        this.autoRefreshPartitionsLimit = autoRefreshPartitionsLimit;
    }

    public boolean isSetPartitionRefreshNumber() {
        return properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER);
    }

    public int getPartitionRefreshNumber() {
        if (isSetPartitionRefreshNumber()) {
            return partitionRefreshNumber;
        } else {
            return Config.default_mv_partition_refresh_number;
        }
    }

    public boolean isSetPartitionRefreshStrategy() {
        return properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)
                && !Strings.isNullOrEmpty(partitionRefreshStrategy);
    }

    public String getPartitionRefreshStrategy() {
        return !isSetPartitionRefreshStrategy() ? Config.default_mv_partition_refresh_strategy
                : partitionRefreshStrategy;
    }

    public void setPartitionRefreshNumber(int partitionRefreshNumber) {
        this.partitionRefreshNumber = partitionRefreshNumber;
    }

    public void setPartitionRefreshStrategy(String partitionRefreshStrategy) {
        this.partitionRefreshStrategy = partitionRefreshStrategy;
    }

    public void setMvRefreshMode(String mvRefreshMode) {
        this.mvRefreshMode = mvRefreshMode;
    }

    public String getMvRefreshMode() {
        return Strings.isNullOrEmpty(mvRefreshMode) ? Config.default_mv_refresh_mode : mvRefreshMode;
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

    public List<TableName> getExcludedRefreshTables() {
        return excludedRefreshTables;
    }

    public void setExcludedRefreshTables(List<TableName> excludedRefreshTables) {
        this.excludedRefreshTables = excludedRefreshTables;
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

    public boolean enablePersistentIndex() {
        return enablePersistentIndex;
    }

    public boolean isFileBundling() {
        return fileBundling;
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

    public TCompactionStrategy getCompactionStrategy() {
        return compactionStrategy;
    }

    public int getLakeCompactionMaxParallel() {
        return lakeCompactionMaxParallel;
    }

    public Multimap<String, String> getLocation() {
        return location;
    }

    public boolean enableStatisticCollectOnFirstLoad() {
        return enableStatisticCollectOnFirstLoad;
    }

    public void setEnableStatisticCollectOnFirstLoad(boolean enableStatisticCollectOnFirstLoad) {
        this.enableStatisticCollectOnFirstLoad = enableStatisticCollectOnFirstLoad;
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

    public String getBaseCompactionForbiddenTimeRanges() {
        return baseCompactionForbiddenTimeRanges;
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

    public FlatJsonConfig getFlatJsonConfig() {
        return flatJsonConfig;
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

    public TableProperty buildEnableStatisticCollectOnFirstLoad() {
        enableStatisticCollectOnFirstLoad = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_COLLECT_ON_FIRST_LOAD, "true"));
        return this;
    }

    public TableProperty buildCloudNativeFastSchemaEvolutionV2() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2)) {
            cloudNativeFastSchemaEvolutionV2 = Boolean.parseBoolean(
                    properties.get(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2));
        }
        return this;
    }

    public boolean isCloudNativeFastSchemaEvolutionV2() {
        return cloudNativeFastSchemaEvolutionV2;
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
        buildPartitionRetentionCondition();
        buildTimeDriftConstraint();
        buildReplicatedStorage();
        buildBucketSize();
        buildEnableLoadProfile();
        buildBinlogConfig();
        buildFlatJsonConfig();
        buildBinlogAvailableVersion();
        buildDataCachePartitionDuration();
        buildUseFastSchemaEvolution();
        buildCloudNativeFastSchemaEvolutionV2();
        buildStorageType();
        buildMvProperties();
        buildLocation();
        buildBaseCompactionForbiddenTimeRanges();
        buildFileBundling();
        buildMutableBucketNum();
        buildCompactionStrategy();
        buildLakeCompactionMaxParallel();
        buildEnableStatisticCollectOnFirstLoad();
    }
}
