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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/PropertyAnalyzer.java

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

package com.starrocks.common.util;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.Property;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.starrocks.catalog.TableProperty.INVALID;

public class PropertyAnalyzer {
    private static final Logger LOG = LogManager.getLogger(PropertyAnalyzer.class);
    private static final String COMMA_SEPARATOR = ",";

    public static final String PROPERTIES_SHORT_KEY = "short_key";
    public static final String PROPERTIES_REPLICATION_NUM = "replication_num";
    public static final String PROPERTIES_STORAGE_TYPE = "storage_type";
    public static final String PROPERTIES_STORAGE_MEDIUM = "storage_medium";
    public static final String PROPERTIES_STORAGE_COOLDOWN_TIME = "storage_cooldown_time";
    public static final String PROPERTIES_STORAGE_COOLDOWN_TTL = "storage_cooldown_ttl";
    // for 1.x -> 2.x migration
    public static final String PROPERTIES_VERSION_INFO = "version_info";
    // for restore
    public static final String PROPERTIES_SCHEMA_VERSION = "schema_version";

    public static final String PROPERTIES_BF_COLUMNS = "bloom_filter_columns";
    public static final String PROPERTIES_BF_FPP = "bloom_filter_fpp";
    private static final double MAX_FPP = 0.05;
    private static final double MIN_FPP = 0.0001;

    public static final String PROPERTIES_COLUMN_SEPARATOR = "column_separator";
    public static final String PROPERTIES_LINE_DELIMITER = "line_delimiter";

    public static final String PROPERTIES_COLOCATE_WITH = "colocate_with";

    public static final String PROPERTIES_TIMEOUT = "timeout";

    public static final String PROPERTIES_DISTRIBUTION_TYPE = "distribution_type";
    public static final String PROPERTIES_SEND_CLEAR_ALTER_TASK = "send_clear_alter_tasks";

    public static final String PROPERTIES_COMPRESSION = "compression";

    public static final String PROPERTIES_COLOCATE_MV = "colocate_mv";

    public static final String PROPERTIES_INMEMORY = "in_memory";

    public static final String PROPERTIES_ENABLE_PERSISTENT_INDEX = "enable_persistent_index";

    public static final String PROPERTIES_PERSISTENT_INDEX_TYPE = "persistent_index_type";

    public static final String PROPERTIES_BINLOG_VERSION = "binlog_version";

    public static final String PROPERTIES_BINLOG_ENABLE = "binlog_enable";

    public static final String PROPERTIES_BINLOG_TTL = "binlog_ttl_second";

    public static final String PROPERTIES_BINLOG_MAX_SIZE = "binlog_max_size";

    public static final String PROPERTIES_WRITE_QUORUM = "write_quorum";

    public static final String PROPERTIES_REPLICATED_STORAGE = "replicated_storage";

    public static final String PROPERTIES_TABLET_TYPE = "tablet_type";

    public static final String PROPERTIES_STRICT_RANGE = "strict_range";
    public static final String PROPERTIES_USE_TEMP_PARTITION_NAME = "use_temp_partition_name";

    public static final String PROPERTIES_TYPE = "type";

    public static final String ENABLE_LOW_CARD_DICT_TYPE = "enable_low_card_dict";
    public static final String ABLE_LOW_CARD_DICT = "1";
    public static final String DISABLE_LOW_CARD_DICT = "0";

    public static final String PROPERTIES_ENABLE_ASYNC_WRITE_BACK = "enable_async_write_back";
    public static final String PROPERTIES_PARTITION_TTL_NUMBER = "partition_ttl_number";
    public static final String PROPERTIES_PARTITION_TTL = "partition_ttl";
    public static final String PROPERTIES_PARTITION_LIVE_NUMBER = "partition_live_number";
    public static final String PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT = "auto_refresh_partitions_limit";
    public static final String PROPERTIES_PARTITION_REFRESH_NUMBER = "partition_refresh_number";
    public static final String PROPERTIES_EXCLUDED_TRIGGER_TABLES = "excluded_trigger_tables";
    public static final String PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE = "force_external_table_query_rewrite";
    public static final String PROPERTIES_QUERY_REWRITE_CONSISTENCY = "query_rewrite_consistency";
    public static final String PROPERTIES_RESOURCE_GROUP = "resource_group";

    public static final String PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX = "session.";

    public static final String PROPERTIES_STORAGE_VOLUME = "storage_volume";

    // constraint for rewrite
    public static final String PROPERTIES_FOREIGN_KEY_CONSTRAINT = "foreign_key_constraints";
    public static final String PROPERTIES_UNIQUE_CONSTRAINT = "unique_constraints";
    public static final String PROPERTIES_DATACACHE_ENABLE = "datacache.enable";
    public static final String PROPERTIES_DATACACHE_PARTITION_DURATION = "datacache.partition_duration";

    // Materialized View properties
    public static final String PROPERTIES_MV_REWRITE_STALENESS_SECOND = "mv_rewrite_staleness_second";
    // Randomized start interval
    // 0(default value): automatically chosed between [0, min(300, INTERVAL/2))
    // -1: disable randomize, use current time as start
    // positive value: use [0, mv_randomize_start) as random interval
    public static final String PROPERTY_MV_RANDOMIZE_START = "mv_randomize_start";

    /**
     * Materialized View sort keys
     */
    public static final String PROPERTY_MV_SORT_KEYS = "mv_sort_keys";

    // light schema change
    public static final String PROPERTIES_USE_LIGHT_SCHEMA_CHANGE = "light_schema_change";

    public static final String PROPERTIES_DEFAULT_PREFIX = "default.";

    public static DataProperty analyzeDataProperty(Map<String, String> properties,
                                                   DataProperty inferredDataProperty,
                                                   boolean isDefault)
            throws AnalysisException {
        String mediumKey = PROPERTIES_STORAGE_MEDIUM;
        String coolDownTimeKey = PROPERTIES_STORAGE_COOLDOWN_TIME;
        String coolDownTTLKey = PROPERTIES_STORAGE_COOLDOWN_TTL;
        if (isDefault) {
            mediumKey = PROPERTIES_DEFAULT_PREFIX + PROPERTIES_STORAGE_MEDIUM;
            coolDownTimeKey = PROPERTIES_DEFAULT_PREFIX + PROPERTIES_STORAGE_COOLDOWN_TIME;
            coolDownTTLKey = PROPERTIES_DEFAULT_PREFIX + PROPERTIES_STORAGE_COOLDOWN_TTL;
        }

        if (properties == null) {
            return inferredDataProperty;
        }

        TStorageMedium storageMedium = null;
        long coolDownTimeStamp = DataProperty.MAX_COOLDOWN_TIME_MS;

        boolean hasMedium = false;
        boolean hasCooldownTime = false;
        boolean hasCoolDownTTL = false;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!hasMedium && key.equalsIgnoreCase(mediumKey)) {
                hasMedium = true;
                if (value.equalsIgnoreCase(TStorageMedium.SSD.name())) {
                    storageMedium = TStorageMedium.SSD;
                } else if (value.equalsIgnoreCase(TStorageMedium.HDD.name())) {
                    storageMedium = TStorageMedium.HDD;
                } else {
                    throw new AnalysisException("Invalid storage medium: " + value);
                }
            } else if (!hasCooldownTime && key.equalsIgnoreCase(coolDownTimeKey)) {
                hasCooldownTime = true;
                DateLiteral dateLiteral = new DateLiteral(value, Type.DATETIME);
                coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
            } else if (!hasCoolDownTTL && key.equalsIgnoreCase(coolDownTTLKey)) {
                hasCoolDownTTL = true;
            }
        } // end for properties

        if (!hasCooldownTime && !hasMedium && !hasCoolDownTTL) {
            return inferredDataProperty;
        }

        if (hasCooldownTime && hasCoolDownTTL) {
            throw new AnalysisException("Invalid data property. "
                    + coolDownTimeKey + " and " + coolDownTTLKey + " conflict. you can only use one of them. ");
        }

        properties.remove(mediumKey);
        properties.remove(coolDownTimeKey);
        properties.remove(coolDownTTLKey);

        if (hasCooldownTime) {
            if (!hasMedium) {
                throw new AnalysisException("Invalid data property. storage medium property is not found");
            }
            if (storageMedium == TStorageMedium.HDD) {
                throw new AnalysisException("Can not assign cooldown timestamp to HDD storage medium");
            }
            long currentTimeMs = System.currentTimeMillis();
            if (coolDownTimeStamp <= currentTimeMs) {
                throw new AnalysisException("Cooldown time should be later than now");
            }

        } else if (hasCoolDownTTL) {
            if (!hasMedium) {
                throw new AnalysisException("Invalid data property. storage medium property is not found");
            }
            if (storageMedium == TStorageMedium.HDD) {
                throw new AnalysisException("Can not assign cooldown ttl to table with HDD storage medium");
            }
        }

        if (storageMedium == TStorageMedium.SSD && !hasCooldownTime && !hasCoolDownTTL) {
            // set default cooldown time
            coolDownTimeStamp = DataProperty.getSsdCooldownTimeMs();
        }

        Preconditions.checkNotNull(storageMedium);
        return new DataProperty(storageMedium, coolDownTimeStamp);
    }

    public static short analyzeShortKeyColumnCount(Map<String, String> properties) throws AnalysisException {
        short shortKeyColumnCount = (short) -1;
        if (properties != null && properties.containsKey(PROPERTIES_SHORT_KEY)) {
            // check and use specified short key
            try {
                shortKeyColumnCount = Short.parseShort(properties.get(PROPERTIES_SHORT_KEY));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Short key: " + e.getMessage());
            }

            if (shortKeyColumnCount <= 0) {
                throw new AnalysisException("Short key column count should larger than 0.");
            }

            properties.remove(PROPERTIES_SHORT_KEY);
        }

        return shortKeyColumnCount;
    }

    public static int analyzePartitionTimeToLive(Map<String, String> properties) throws AnalysisException {
        int partitionTimeToLive = INVALID;
        if (properties != null && properties.containsKey(PROPERTIES_PARTITION_TTL_NUMBER)) {
            try {
                partitionTimeToLive = Integer.parseInt(properties.get(PROPERTIES_PARTITION_TTL_NUMBER));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Partition TTL Number: " + e.getMessage());
            }
            if (partitionTimeToLive <= 0 && partitionTimeToLive != INVALID) {
                throw new AnalysisException("Illegal Partition TTL Number: " + partitionTimeToLive);
            }
            properties.remove(PROPERTIES_PARTITION_TTL_NUMBER);
        }
        return partitionTimeToLive;
    }

    public static Pair<String, PeriodDuration> analyzePartitionTTL(Map<String, String> properties) {
        if (properties != null && properties.containsKey(PROPERTIES_PARTITION_TTL)) {
            String ttlStr = properties.get(PROPERTIES_PARTITION_TTL);
            PeriodDuration duration;
            try {
                duration = TimeUtils.parseHumanReadablePeriodOrDuration(ttlStr);
            } catch (NumberFormatException e) {
                throw new SemanticException(String.format("illegal %s: %s", PROPERTIES_PARTITION_TTL, e.getMessage()));
            }
            properties.remove(PROPERTIES_PARTITION_TTL);
            return Pair.create(ttlStr, duration);
        }
        return Pair.create(null, PeriodDuration.ZERO);
    }

    public static int analyzePartitionLiveNumber(Map<String, String> properties,
                                                 boolean removeProperties) throws AnalysisException {
        int partitionLiveNumber = INVALID;
        if (properties != null && properties.containsKey(PROPERTIES_PARTITION_LIVE_NUMBER)) {
            try {
                partitionLiveNumber = Integer.parseInt(properties.get(PROPERTIES_PARTITION_LIVE_NUMBER));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Partition Live Number: " + e.getMessage());
            }
            if (partitionLiveNumber <= 0 && partitionLiveNumber != INVALID) {
                throw new AnalysisException("Illegal Partition Live Number: " + partitionLiveNumber);
            }
            if (removeProperties) {
                properties.remove(PROPERTIES_PARTITION_LIVE_NUMBER);
            }
        }
        return partitionLiveNumber;
    }

    public static int analyzeAutoRefreshPartitionsLimit(Map<String, String> properties, MaterializedView mv)
            throws AnalysisException {
        if (mv.getRefreshScheme().getType() == MaterializedView.RefreshType.MANUAL) {
            throw new AnalysisException(
                    "The auto_refresh_partitions_limit property does not support manual refresh mode.");
        }
        int autoRefreshPartitionsLimit = -1;
        if (properties != null && properties.containsKey(PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            try {
                autoRefreshPartitionsLimit = Integer.parseInt(properties.get(PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Auto Refresh Partitions Limit: " + e.getMessage());
            }
            if (autoRefreshPartitionsLimit <= 0 && autoRefreshPartitionsLimit != INVALID) {
                throw new AnalysisException("Illegal Auto Refresh Partitions Limit: " + autoRefreshPartitionsLimit);
            }
            properties.remove(PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT);
        }
        return autoRefreshPartitionsLimit;
    }

    public static int analyzePartitionRefreshNumber(Map<String, String> properties) throws AnalysisException {
        int partitionRefreshNumber = -1;
        if (properties != null && properties.containsKey(PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            try {
                partitionRefreshNumber = Integer.parseInt(properties.get(PROPERTIES_PARTITION_REFRESH_NUMBER));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Partition Refresh Number: " + e.getMessage());
            }
            if (partitionRefreshNumber <= 0 && partitionRefreshNumber != INVALID) {
                throw new AnalysisException("Illegal Partition Refresh Number: " + partitionRefreshNumber);
            }
            properties.remove(PROPERTIES_PARTITION_REFRESH_NUMBER);
        }
        return partitionRefreshNumber;
    }

    public static List<TableName> analyzeExcludedTriggerTables(Map<String, String> properties, MaterializedView mv)
            throws AnalysisException {
        if (mv.getRefreshScheme().getType() != MaterializedView.RefreshType.ASYNC) {
            throw new AnalysisException("The excluded_trigger_tables property only applies to asynchronous refreshes.");
        }
        List<TableName> tables = Lists.newArrayList();
        if (properties != null && properties.containsKey(PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            String tableStr = properties.get(PROPERTIES_EXCLUDED_TRIGGER_TABLES);
            List<String> tableList = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(tableStr);
            for (String table : tableList) {
                TableName tableName = AnalyzerUtils.stringToTableName(table);
                if (mv.containsBaseTable(tableName)) {
                    tables.add(tableName);
                } else {
                    throw new AnalysisException(tableName.toSql() +
                            " is not base table of materialized view " + mv.getName());
                }
            }
            properties.remove(PROPERTIES_EXCLUDED_TRIGGER_TABLES);
        }
        return tables;
    }

    public static int analyzeMVRewriteStaleness(Map<String, String> properties)
            throws AnalysisException {
        int maxMVRewriteStaleness = INVALID;
        if (properties != null && properties.containsKey(PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            try {
                maxMVRewriteStaleness = Integer.parseInt(properties.get(PROPERTIES_MV_REWRITE_STALENESS_SECOND));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid maxMVRewriteStaleness Number: " + e.getMessage());
            }
            if (maxMVRewriteStaleness != INVALID && maxMVRewriteStaleness < 0) {
                throw new AnalysisException("Illegal maxMVRewriteStaleness: " + maxMVRewriteStaleness);
            }
            properties.remove(PROPERTIES_MV_REWRITE_STALENESS_SECOND);
        }
        return maxMVRewriteStaleness;
    }

    public static Short analyzeReplicationNum(Map<String, String> properties, short oldReplicationNum)
            throws AnalysisException {
        short replicationNum = oldReplicationNum;
        if (properties != null && properties.containsKey(PROPERTIES_REPLICATION_NUM)) {
            try {
                replicationNum = Short.parseShort(properties.get(PROPERTIES_REPLICATION_NUM));
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
            checkReplicationNum(replicationNum);
            properties.remove(PROPERTIES_REPLICATION_NUM);
        }
        return replicationNum;
    }

    public static Short analyzeReplicationNum(Map<String, String> properties, boolean isDefault)
            throws AnalysisException {
        String key = PROPERTIES_DEFAULT_PREFIX;
        if (isDefault) {
            key += PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        } else {
            key = PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        }
        short replicationNum = Short.parseShort(properties.get(key));
        checkReplicationNum(replicationNum);
        return replicationNum;
    }

    public static String analyzeResourceGroup(Map<String, String> properties) {
        String resourceGroup = null;
        if (properties != null && properties.containsKey(PROPERTIES_RESOURCE_GROUP)) {
            resourceGroup = properties.get(PROPERTIES_RESOURCE_GROUP);
            properties.remove(PROPERTIES_RESOURCE_GROUP);
        }
        return resourceGroup;
    }

    private static void checkReplicationNum(short replicationNum) throws AnalysisException {
        if (replicationNum <= 0) {
            throw new AnalysisException("Replication num should larger than 0");
        }

        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getAvailableBackendIds();
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            backendIds.addAll(GlobalStateMgr.getCurrentSystemInfo().getAvailableComputeNodeIds());
            if (RunMode.defaultReplicationNum() > backendIds.size()) {
                throw new AnalysisException("Number of available CN nodes is " + backendIds.size()
                        + ", less than " + RunMode.defaultReplicationNum());
            }
        } else {
            if (replicationNum > backendIds.size()) {
                throw new AnalysisException("Table replication num should be less than " +
                        "of equal to the number of available BE nodes. "
                        + "You can change this default by setting the replication_num table properties. "
                        + "Current alive backend is [" + Joiner.on(",").join(backendIds) + "].");
            }
        }
    }

    public static String analyzeColumnSeparator(Map<String, String> properties, String oldColumnSeparator) {
        String columnSeparator = oldColumnSeparator;
        if (properties != null && properties.containsKey(PROPERTIES_COLUMN_SEPARATOR)) {
            columnSeparator = properties.get(PROPERTIES_COLUMN_SEPARATOR);
            properties.remove(PROPERTIES_COLUMN_SEPARATOR);
        }
        return columnSeparator;
    }

    public static String analyzeRowDelimiter(Map<String, String> properties, String oldRowDelimiter) {
        String rowDelimiter = oldRowDelimiter;
        if (properties != null && properties.containsKey(PROPERTIES_LINE_DELIMITER)) {
            rowDelimiter = properties.get(PROPERTIES_LINE_DELIMITER);
            properties.remove(PROPERTIES_LINE_DELIMITER);
        }
        return rowDelimiter;
    }

    public static TStorageType analyzeStorageType(Map<String, String> properties) throws AnalysisException {
        // default is COLUMN
        TStorageType tStorageType = TStorageType.COLUMN;
        if (properties != null && properties.containsKey(PROPERTIES_STORAGE_TYPE)) {
            String storageType = properties.get(PROPERTIES_STORAGE_TYPE);
            if (storageType.equalsIgnoreCase(TStorageType.COLUMN.name())) {
                tStorageType = TStorageType.COLUMN;
            } else {
                throw new AnalysisException("Invalid storage type: " + storageType);
            }

            properties.remove(PROPERTIES_STORAGE_TYPE);
        }

        return tStorageType;
    }

    public static TTabletType analyzeTabletType(Map<String, String> properties) throws AnalysisException {
        // default is TABLET_TYPE_DISK
        TTabletType tTabletType = TTabletType.TABLET_TYPE_DISK;
        if (properties != null && properties.containsKey(PROPERTIES_TABLET_TYPE)) {
            String tabletType = properties.get(PROPERTIES_TABLET_TYPE);
            if (tabletType.equalsIgnoreCase("memory")) {
                tTabletType = TTabletType.TABLET_TYPE_MEMORY;
            } else if (tabletType.equalsIgnoreCase("disk")) {
                tTabletType = TTabletType.TABLET_TYPE_DISK;
            } else {
                throw new AnalysisException(("Invalid tablet type"));
            }
            properties.remove(PROPERTIES_TABLET_TYPE);
        }
        return tTabletType;
    }

    public static Long analyzeVersionInfo(Map<String, String> properties) throws AnalysisException {
        long versionInfo = Partition.PARTITION_INIT_VERSION;
        if (properties != null && properties.containsKey(PROPERTIES_VERSION_INFO)) {
            String versionInfoStr = properties.get(PROPERTIES_VERSION_INFO);
            try {
                versionInfo = Long.parseLong(versionInfoStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("version info format error.");
            }

            properties.remove(PROPERTIES_VERSION_INFO);
        }

        return versionInfo;
    }

    public static int analyzeSchemaVersion(Map<String, String> properties) throws AnalysisException {
        int schemaVersion = 0;
        if (properties != null && properties.containsKey(PROPERTIES_SCHEMA_VERSION)) {
            String schemaVersionStr = properties.get(PROPERTIES_SCHEMA_VERSION);
            try {
                schemaVersion = Integer.parseInt(schemaVersionStr);
            } catch (Exception e) {
                throw new AnalysisException("schema version format error");
            }

            properties.remove(PROPERTIES_SCHEMA_VERSION);
        }

        return schemaVersion;
    }

    public static Set<String> analyzeBloomFilterColumns(Map<String, String> properties, List<Column> columns,
                                                        boolean isPrimaryKey) throws AnalysisException {
        Set<String> bfColumns = null;
        if (properties != null && properties.containsKey(PROPERTIES_BF_COLUMNS)) {
            bfColumns = Sets.newHashSet();
            String bfColumnsStr = properties.get(PROPERTIES_BF_COLUMNS);
            if (Strings.isNullOrEmpty(bfColumnsStr)) {
                return bfColumns;
            }

            String[] bfColumnArr = bfColumnsStr.split(COMMA_SEPARATOR);
            Set<String> bfColumnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (String bfColumn : bfColumnArr) {
                bfColumn = bfColumn.trim();
                String finalBfColumn = bfColumn;
                Column column = columns.stream().filter(col -> col.getName().equalsIgnoreCase(finalBfColumn))
                        .findFirst()
                        .orElse(null);
                if (column == null) {
                    throw new AnalysisException(
                            String.format("Invalid bloom filter column '%s': not exists", bfColumn));
                }

                Type type = column.getType();

                // tinyint/float/double columns don't support
                if (!type.supportBloomFilter()) {
                    throw new AnalysisException(String.format("Invalid bloom filter column '%s': unsupported type %s",
                            bfColumn, type));
                }

                // Only support create bloom filter on DUPLICATE/PRIMARY table or key columns of UNIQUE/AGGREGATE table.
                if (!(column.isKey() || isPrimaryKey || column.getAggregationType() == AggregateType.NONE)) {
                    // Although the implementation supports bloom filter for replace non-key column,
                    // for simplicity and unity, we don't expose that to user.
                    throw new AnalysisException("Bloom filter index only used in columns of DUP_KEYS/PRIMARY table or "
                            + "key columns of UNIQUE_KEYS/AGG_KEYS table. invalid column: " + bfColumn);
                }

                if (bfColumnSet.contains(bfColumn)) {
                    throw new AnalysisException(String.format("Duplicate bloom filter column '%s'", bfColumn));
                }

                bfColumnSet.add(bfColumn);
                bfColumns.add(column.getName());
            }

            properties.remove(PROPERTIES_BF_COLUMNS);
        }

        return bfColumns;
    }

    public static double analyzeBloomFilterFpp(Map<String, String> properties) throws AnalysisException {
        double bfFpp = 0;
        if (properties != null && properties.containsKey(PROPERTIES_BF_FPP)) {
            String bfFppStr = properties.get(PROPERTIES_BF_FPP);
            try {
                bfFpp = Double.parseDouble(bfFppStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Bloom filter fpp is not Double");
            }

            // check range
            if (bfFpp < MIN_FPP || bfFpp > MAX_FPP) {
                throw new AnalysisException("Bloom filter fpp should in [" + MIN_FPP + ", " + MAX_FPP + "]");
            }

            properties.remove(PROPERTIES_BF_FPP);
        }

        return bfFpp;
    }

    public static String analyzeColocate(Map<String, String> properties) {
        String colocateGroup = null;
        if (properties != null && properties.containsKey(PROPERTIES_COLOCATE_WITH)) {
            colocateGroup = properties.get(PROPERTIES_COLOCATE_WITH);
            properties.remove(PROPERTIES_COLOCATE_WITH);
        }
        return colocateGroup;
    }

    public static long analyzeTimeout(Map<String, String> properties, long defaultTimeout) throws AnalysisException {
        long timeout = defaultTimeout;
        if (properties != null && properties.containsKey(PROPERTIES_TIMEOUT)) {
            String timeoutStr = properties.get(PROPERTIES_TIMEOUT);
            try {
                timeout = Long.parseLong(timeoutStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid timeout format: " + timeoutStr);
            }
            properties.remove(PROPERTIES_TIMEOUT);
        }
        return timeout;
    }

    // analyzeCompressionType will parse the compression type from properties
    public static TCompressionType analyzeCompressionType(Map<String, String> properties) throws AnalysisException {
        TCompressionType compressionType = TCompressionType.LZ4_FRAME;
        if (ConnectContext.get() != null) {
            String defaultCompression = ConnectContext.get().getSessionVariable().getDefaultTableCompression();
            compressionType = CompressionUtils.getCompressTypeByName(defaultCompression);
        }
        if (properties == null || !properties.containsKey(PROPERTIES_COMPRESSION)) {
            return compressionType;
        }
        String compressionName = properties.get(PROPERTIES_COMPRESSION);
        properties.remove(PROPERTIES_COMPRESSION);

        if (CompressionUtils.getCompressTypeByName(compressionName) != null) {
            return CompressionUtils.getCompressTypeByName(compressionName);
        } else {
            throw new AnalysisException("unknown compression type: " + compressionName);
        }
    }

    // analyzeWriteQuorum will parse to write quorum from properties
    public static String analyzeWriteQuorum(Map<String, String> properties) throws AnalysisException {
        String writeQuorum;
        if (properties == null || !properties.containsKey(PROPERTIES_WRITE_QUORUM)) {
            return WriteQuorum.MAJORITY;
        }
        writeQuorum = properties.get(PROPERTIES_WRITE_QUORUM);
        properties.remove(PROPERTIES_WRITE_QUORUM);

        if (WriteQuorum.findTWriteQuorumByName(writeQuorum) != null) {
            return writeQuorum;
        } else {
            throw new AnalysisException("unknown write quorum: " + writeQuorum);
        }
    }

    // analyze common boolean properties, such as "in_memory" = "false"
    public static boolean analyzeBooleanProp(Map<String, String> properties, String propKey, boolean defaultVal) {
        if (properties != null && properties.containsKey(propKey)) {
            String val = properties.get(propKey);
            properties.remove(propKey);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    public static Pair<Boolean, Boolean> analyzeEnablePersistentIndex(Map<String, String> properties, boolean isPrimaryKey) {
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
            String val = properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX);
            properties.remove(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX);
            return Pair.create(Boolean.parseBoolean(val), true);
        } else {
            if (isPrimaryKey) {
                return Pair.create(Config.enable_persistent_index_by_default, false);
            }
            return Pair.create(false, false);
        }
    }

    // analyze property like : "type" = "xxx";
    public static String analyzeType(Map<String, String> properties) {
        String type = null;
        if (properties != null && properties.containsKey(PROPERTIES_TYPE)) {
            type = properties.get(PROPERTIES_TYPE);
            properties.remove(PROPERTIES_TYPE);
        }
        return type;
    }

    public static String analyzeType(Property property) {
        String type = null;
        if (PROPERTIES_TYPE.equals(property.getKey())) {
            type = property.getValue();
        }
        return type;
    }

    public static long analyzeLongProp(Map<String, String> properties, String propKey, long defaultVal)
            throws AnalysisException {
        long val = defaultVal;
        if (properties != null && properties.containsKey(propKey)) {
            String valStr = properties.get(propKey);
            try {
                val = Long.parseLong(valStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid " + propKey + " format: " + valStr);
            }
            properties.remove(propKey);
        }
        return val;
    }

    public static List<UniqueConstraint> analyzeUniqueConstraint(
            Map<String, String> properties, Database db, OlapTable table) throws AnalysisException {
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        List<UniqueConstraint> analyzedUniqueConstraints = Lists.newArrayList();

        if (properties != null && properties.containsKey(PROPERTIES_UNIQUE_CONSTRAINT)) {
            String uniqueConstraintStr = properties.get(PROPERTIES_UNIQUE_CONSTRAINT);
            if (Strings.isNullOrEmpty(uniqueConstraintStr)) {
                return uniqueConstraints;
            }
            uniqueConstraints = UniqueConstraint.parse(uniqueConstraintStr);
            if (uniqueConstraints == null || uniqueConstraints.isEmpty()) {
                throw new AnalysisException(String.format("invalid unique constraint:%s", uniqueConstraintStr));
            }

            for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                if (table.isMaterializedView()) {
                    String catalogName = uniqueConstraint.getCatalogName() != null ? uniqueConstraint.getCatalogName()
                            : InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                    String dbName = uniqueConstraint.getDbName() != null ? uniqueConstraint.getDbName()
                            : db.getFullName();
                    if (uniqueConstraint.getTableName() == null) {
                        throw new AnalysisException("must set table name for unique constraint in materialized view");
                    }
                    String tableName = uniqueConstraint.getTableName();
                    Table uniqueConstraintTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName,
                            dbName, tableName);
                    if (uniqueConstraintTable == null) {
                        throw new AnalysisException(
                                String.format("table: %s.%s.%s does not exist", catalogName, dbName, tableName));
                    }
                    boolean columnExist = uniqueConstraint.getUniqueColumns().stream()
                            .allMatch(uniqueConstraintTable::containColumn);
                    if (!columnExist) {
                        throw new AnalysisException(
                                String.format("some columns of:%s do not exist in table:%s.%s.%s",
                                        uniqueConstraint.getUniqueColumns(), catalogName, dbName, tableName));
                    }
                    analyzedUniqueConstraints.add(new UniqueConstraint(catalogName, dbName, tableName,
                            uniqueConstraint.getUniqueColumns()));
                } else {
                    boolean columnExist = uniqueConstraint.getUniqueColumns().stream().allMatch(table::containColumn);
                    if (!columnExist) {
                        throw new AnalysisException(
                                String.format("some columns of:%s do not exist in table:%s",
                                        uniqueConstraint.getUniqueColumns(), table.getName()));
                    }
                    analyzedUniqueConstraints.add(uniqueConstraint);
                }
            }
            properties.remove(PROPERTIES_UNIQUE_CONSTRAINT);
        }
        return analyzedUniqueConstraints;
    }

    private static Pair<BaseTableInfo, Table> analyzeForeignKeyConstraintTablePath(String tablePath,
                                                                                   String foreignKeyConstraintDesc,
                                                                                   Database db)
            throws AnalysisException {
        String[] parts = tablePath.split("\\.");
        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        String dbName = db.getFullName();
        String tableName = "";
        if (parts.length == 3) {
            catalogName = parts[0];
            dbName = parts[1];
            tableName = parts[2];
        } else if (parts.length == 2) {
            dbName = parts[0];
            tableName = parts[1];
        } else if (parts.length == 1) {
            tableName = parts[0];
        } else {
            throw new AnalysisException(String.format("invalid foreign key constraint:%s," +
                    "table path is invalid", foreignKeyConstraintDesc));
        }

        if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
            throw new AnalysisException(String.format("catalog: %s do not exist", catalogName));
        }
        Database parentDb = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        if (parentDb == null) {
            throw new AnalysisException(
                    String.format("catalog: %s, database: %s do not exist", catalogName, dbName));
        }
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(catalogName, dbName, tableName);
        if (table == null) {
            throw new AnalysisException(String.format("catalog:%s, database: %s, table:%s do not exist",
                    catalogName, dbName, tableName));
        }

        BaseTableInfo tableInfo;
        if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            tableInfo = new BaseTableInfo(parentDb.getId(), dbName, table.getId());
        } else {
            tableInfo = new BaseTableInfo(catalogName, dbName, table.getName(), table.getTableIdentifier());
        }

        return Pair.create(tableInfo, table);
    }

    private static void analyzeForeignKeyUniqueConstraint(Table parentTable, List<String> parentColumns,
                                                          Table analyzedTable)
            throws AnalysisException {
        KeysType parentTableKeyType = KeysType.DUP_KEYS;
        if (parentTable.isNativeTableOrMaterializedView()) {
            OlapTable parentOlapTable = (OlapTable) parentTable;
            parentTableKeyType =
                    parentOlapTable.getIndexMetaByIndexId(parentOlapTable.getBaseIndexId()).getKeysType();
        }

        List<UniqueConstraint> mvUniqueConstraints = Lists.newArrayList();
        if (analyzedTable.isMaterializedView() && analyzedTable.hasUniqueConstraints()) {
            mvUniqueConstraints = analyzedTable.getUniqueConstraints().stream().filter(
                    uniqueConstraint -> parentTable.getName().equals(uniqueConstraint.getTableName()))
                    .collect(Collectors.toList());
        }

        if (parentTableKeyType == KeysType.AGG_KEYS) {
            throw new AnalysisException(
                    String.format("do not support reference agg table:%s", parentTable.getName()));
        } else if (parentTableKeyType == KeysType.DUP_KEYS) {
            // for DUP_KEYS type olap table or external table
            if (!parentTable.hasUniqueConstraints() && mvUniqueConstraints.isEmpty()) {
                throw new AnalysisException(
                        String.format("dup table:%s has no unique constraint", parentTable.getName()));
            } else {
                List<UniqueConstraint> uniqueConstraints = parentTable.getUniqueConstraints();
                if (uniqueConstraints == null) {
                    uniqueConstraints = mvUniqueConstraints;
                } else {
                    uniqueConstraints.addAll(mvUniqueConstraints);
                }
                boolean matched = false;
                for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                    if (uniqueConstraint.isMatch(parentTable, Sets.newHashSet(parentColumns))) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    throw new AnalysisException(
                            String.format("columns:%s are not dup table:%s's unique constraint", parentColumns,
                                    parentTable.getName()));
                }
            }
        } else {
            // for PRIMARY_KEYS and UNIQUE_KEYS type table
            // parent columns should be keys
            if (!((OlapTable) parentTable).isKeySet(Sets.newHashSet(parentColumns))) {
                throw new AnalysisException(String.format("columns:%s are not key columns of table:%s",
                        parentColumns, parentTable.getName()));
            }
        }
    }

    public static List<ForeignKeyConstraint> analyzeForeignKeyConstraint(
            Map<String, String> properties, Database db, Table analyzedTable) throws AnalysisException {
        List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();
        if (properties != null && properties.containsKey(PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            String foreignKeyConstraintsDesc = properties.get(PROPERTIES_FOREIGN_KEY_CONSTRAINT);
            if (Strings.isNullOrEmpty(foreignKeyConstraintsDesc)) {
                return foreignKeyConstraints;
            }

            String[] foreignKeyConstraintDescArray = foreignKeyConstraintsDesc.trim().split(";");
            for (String foreignKeyConstraintDesc : foreignKeyConstraintDescArray) {
                String trimed = foreignKeyConstraintDesc.trim();
                if (Strings.isNullOrEmpty(trimed)) {
                    continue;
                }
                Matcher foreignKeyMatcher = ForeignKeyConstraint.FOREIGN_KEY_PATTERN.matcher(trimed);
                if (!foreignKeyMatcher.find() || foreignKeyMatcher.groupCount() != 9) {
                    throw new AnalysisException(
                            String.format("invalid foreign key constraint:%s", foreignKeyConstraintDesc));
                }
                String sourceTablePath = foreignKeyMatcher.group(1);
                String sourceColumns = foreignKeyMatcher.group(3);

                String targetTablePath = foreignKeyMatcher.group(6);
                String targetColumns = foreignKeyMatcher.group(8);
                // case insensitive
                List<String> childColumns = Arrays.stream(sourceColumns.split(",")).
                        map(String::trim).map(String::toLowerCase).collect(Collectors.toList());
                List<String> parentColumns = Arrays.stream(targetColumns.split(",")).
                        map(String::trim).map(String::toLowerCase).collect(Collectors.toList());
                if (childColumns.size() != parentColumns.size()) {
                    throw new AnalysisException(String.format("invalid foreign key constraint:%s," +
                            " columns' size does not match", foreignKeyConstraintDesc));
                }
                // analyze table exist for foreign key constraint
                Pair<BaseTableInfo, Table> parentTablePair = analyzeForeignKeyConstraintTablePath(targetTablePath,
                        foreignKeyConstraintDesc, db);
                BaseTableInfo parentTableInfo = parentTablePair.first;
                Table parentTable = parentTablePair.second;
                if (!parentColumns.stream().allMatch(parentTable::containColumn)) {
                    throw new AnalysisException(String.format("some columns of:%s do not exist in parent table:%s",
                            parentColumns, parentTable.getName()));
                }

                Pair<BaseTableInfo, Table> childTablePair = Pair.create(null, analyzedTable);
                Table childTable = analyzedTable;
                if (analyzedTable.isMaterializedView()) {
                    childTablePair = analyzeForeignKeyConstraintTablePath(sourceTablePath, foreignKeyConstraintDesc,
                            db);
                    childTable = childTablePair.second;
                    if (!childColumns.stream().allMatch(childTable::containColumn)) {
                        throw new AnalysisException(String.format("some columns of:%s do not exist in table:%s",
                                childColumns, childTable.getName()));
                    }
                } else {
                    if (!analyzedTable.isNativeTable()) {
                        throw new AnalysisException("do not support add foreign key on external table");
                    }
                    if (!childColumns.stream().allMatch(analyzedTable::containColumn)) {
                        throw new AnalysisException(String.format("some columns of:%s do not exist in table:%s",
                                childColumns, analyzedTable.getName()));
                    }
                }

                analyzeForeignKeyUniqueConstraint(parentTable, parentColumns, analyzedTable);

                List<Pair<String, String>> columnRefPairs = Streams.zip(childColumns.stream(),
                        parentColumns.stream(), Pair::create).collect(Collectors.toList());
                for (Pair<String, String> pair : columnRefPairs) {
                    Column childColumn = childTable.getColumn(pair.first);
                    Column parentColumn = parentTable.getColumn(pair.second);
                    if (!childColumn.getType().equals(parentColumn.getType())) {
                        throw new AnalysisException(String.format(
                                "column:%s type does mot match referenced column:%s type", pair.first, pair.second));
                    }
                }

                BaseTableInfo childTableInfo = childTablePair.first;
                ForeignKeyConstraint foreignKeyConstraint = new ForeignKeyConstraint(parentTableInfo, childTableInfo,
                        columnRefPairs);
                foreignKeyConstraints.add(foreignKeyConstraint);
            }
            if (foreignKeyConstraints.isEmpty()) {
                throw new AnalysisException(
                        String.format("invalid foreign key constrain:%s", foreignKeyConstraintsDesc));
            }
            properties.remove(PROPERTIES_FOREIGN_KEY_CONSTRAINT);
        }

        return foreignKeyConstraints;
    }

    public static DataCacheInfo analyzeDataCacheInfo(Map<String, String> properties) throws AnalysisException {
        boolean enableDataCache = analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, true);

        boolean enableAsyncWriteBack =
                analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, false);
        if (enableAsyncWriteBack) {
            throw new AnalysisException("enable_async_write_back is disabled since version 3.1.4");
        }
        return new DataCacheInfo(enableDataCache, enableAsyncWriteBack);
    }

    public static PeriodDuration analyzeDataCachePartitionDuration(Map<String, String> properties) throws AnalysisException {
        String text = properties.get(PROPERTIES_DATACACHE_PARTITION_DURATION);
        if (text == null) {
            return null;
        }
        properties.remove(PROPERTIES_DATACACHE_PARTITION_DURATION);
        return TimeUtils.parseHumanReadablePeriodOrDuration(text);
    }

    public static TPersistentIndexType analyzePersistentIndexType(Map<String, String> properties) throws AnalysisException {
        if (properties != null && properties.containsKey(PROPERTIES_PERSISTENT_INDEX_TYPE)) {
            String type = properties.get(PROPERTIES_PERSISTENT_INDEX_TYPE);
            properties.remove(PROPERTIES_PERSISTENT_INDEX_TYPE);
            if (type.equalsIgnoreCase("LOCAL")) {
                return TPersistentIndexType.LOCAL;
            } else {
                throw new AnalysisException("Invalid persistent index type: " + type);
            }
        }
        return TPersistentIndexType.LOCAL;
    }

    public static PeriodDuration analyzeStorageCoolDownTTL(Map<String, String> properties,
                                                           boolean removeProperties) throws AnalysisException {
        String text = properties.get(PROPERTIES_STORAGE_COOLDOWN_TTL);
        if (removeProperties) {
            properties.remove(PROPERTIES_STORAGE_COOLDOWN_TTL);
        }
        if (Strings.isNullOrEmpty(text)) {
            return null;
        }
        PeriodDuration periodDuration;
        try {
            periodDuration = TimeUtils.parseHumanReadablePeriodOrDuration(text);
        } catch (DateTimeParseException ex) {
            throw new AnalysisException(ex.getMessage());
        }
        return periodDuration;
    }

}
