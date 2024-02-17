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

package com.starrocks.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.PartitionsProcDir;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TGetPartitionsMetaRequest;
import com.starrocks.thrift.TGetPartitionsMetaResponse;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TPartitionMetaInfo;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.thrift.TTableInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InformationSchemaDataSource {

    private static final Logger LOG = LogManager.getLogger(InformationSchemaDataSource.class);

    private static final String DEF = "def";
    private static final String DEFAULT_EMPTY_STRING = "";
    public static final long DEFAULT_EMPTY_NUM = -1L;
    public static final String UTF8_GENERAL_CI = "utf8_general_ci";

    @NotNull
    private static AuthDbRequestResult getAuthDbRequestResult(TAuthInfo authInfo) throws TException {

        List<String> authorizedDbs = Lists.newArrayList();
        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.DATABASE.getCaseSensibility();
        if (authInfo.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(authInfo.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (SemanticException e) {
                throw new TException("Pattern is in bad format: " + authInfo.getPattern());
            }
        }

        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        if (authInfo.isSetCatalog_name()) {
            catalogName = authInfo.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> dbNames = metadataMgr.listDbNames(catalogName);
        LOG.debug("get db names: {}", dbNames);

        UserIdentity currentUser;
        if (authInfo.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(authInfo.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
        }
        for (String fullName : dbNames) {

            try {
                Authorizer.checkAnyActionOnOrInDb(currentUser, null, catalogName, fullName);
            } catch (AccessDeniedException e) {
                continue;
            }

            final String dbName = ClusterNamespace.getNameFromFullName(fullName);

            if (!PatternMatcher.matchPattern(authInfo.getPattern(), dbName, matcher, caseSensitive)) {
                continue;
            }
            authorizedDbs.add(fullName);
        }
        return new AuthDbRequestResult(authorizedDbs, currentUser);
    }

    private static class AuthDbRequestResult {
        public final List<String> authorizedDbs;
        public final UserIdentity currentUser;

        public AuthDbRequestResult(List<String> authorizedDbs, UserIdentity currentUser) {
            this.authorizedDbs = authorizedDbs;
            this.currentUser = currentUser;
        }
    }

    // tables_config
    public static TGetTablesConfigResponse generateTablesConfigResponse(TGetTablesConfigRequest request)
            throws TException {

        TGetTablesConfigResponse resp = new TGetTablesConfigResponse();
        List<TTableConfigInfo> tList = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        for (String dbName : result.authorizedDbs) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db != null) {
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    List<Table> allTables = db.getTables();
                    for (Table table : allTables) {
                        try {
                            Authorizer.checkAnyActionOnTableLikeObject(result.currentUser,
                                    null, dbName, table);
                        } catch (AccessDeniedException e) {
                            continue;
                        }

                        TTableConfigInfo tableConfigInfo = new TTableConfigInfo();
                        tableConfigInfo.setTable_schema(dbName);
                        tableConfigInfo.setTable_name(table.getName());

                        if (table.isNativeTableOrMaterializedView() || table.getType() == TableType.OLAP_EXTERNAL) {
                            // OLAP (done)
                            // OLAP_EXTERNAL (done)
                            // MATERIALIZED_VIEW (done)
                            // LAKE (done)
                            // LAKE_MATERIALIZED_VIEW (done)
                            genNormalTableConfigInfo(table, tableConfigInfo);
                        }
                        // TODO(cjs): other table type (HIVE, MYSQL, ICEBERG, HUDI, JDBC, ELASTICSEARCH)
                        tList.add(tableConfigInfo);
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }
        resp.tables_config_infos = tList;
        return resp;
    }

    private static Map<String, String> genProps(Table table) {
        if (table.isMaterializedView()) {
            MaterializedView mv = (MaterializedView) table;
            return mv.getMaterializedViewPropMap();
        }

        OlapTable olapTable = (OlapTable) table;
        Map<String, String> propsMap = new HashMap<>();

        propsMap.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(olapTable.getDefaultReplicationNum()));

        // bloom filter
        Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
        if (bfColumnNames != null) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, Joiner.on(", ")
                    .join(olapTable.getCopiedBfColumns()));
        }

        // colocateTable
        String colocateTable = olapTable.getColocateGroup();
        if (colocateTable != null) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, colocateTable);
        }

        // dynamic partition
        if (olapTable.dynamicPartitionExists()) {
            propsMap.put("dynamic_partition", olapTable.getTableProperty()
                    .getDynamicPartitionProperty().getPropString());
        }

        // in memory
        propsMap.put(PropertyAnalyzer.PROPERTIES_INMEMORY, String.valueOf(olapTable.isInMemory()));

        if (table.isCloudNativeTable()) {
            Map<String, String> storageProperties = olapTable.getProperties();
            propsMap.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE,
                    storageProperties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE));
            propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK,
                    storageProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK));
        }

        // enable_persistent_index
        propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                String.valueOf(olapTable.enablePersistentIndex()));

        // primary index cache expire sec
        if (olapTable.primaryIndexCacheExpireSec() > 0) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC,
                    String.valueOf(olapTable.primaryIndexCacheExpireSec()));
        }

        // compression type
        if (olapTable.getCompressionType() == TCompressionType.LZ4_FRAME ||
                olapTable.getCompressionType() == TCompressionType.LZ4) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_COMPRESSION, "LZ4");
        } else {
            propsMap.put(PropertyAnalyzer.PROPERTIES_COMPRESSION, olapTable.getCompressionType().name());
        }

        // storage media
        Map<String, String> properties = olapTable.getTableProperty().getProperties();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                    properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM));
        }
        return propsMap;
    }

    private static TTableConfigInfo genNormalTableConfigInfo(Table table, TTableConfigInfo tableConfigInfo) {
        OlapTable olapTable = (OlapTable) table;
        tableConfigInfo.setTable_engine(olapTable.getType().toString());
        tableConfigInfo.setTable_model(olapTable.getKeysType().toString());
        // Distribution info
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        String distributeKey = distributionInfo.getDistributionKey();
        // Partition info
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        StringBuilder partitionKeySb = new StringBuilder();
        if (partitionInfo.isRangePartition()) {
            int idx = 0;
            for (Column column : partitionInfo.getPartitionColumns()) {
                if (idx != 0) {
                    partitionKeySb.append(", ");
                }
                partitionKeySb.append("`").append(column.getName()).append("`");
                idx++;
            }
        } else {
            partitionKeySb.append(DEFAULT_EMPTY_STRING);
        }

        // PRIMARY KEYS
        List<String> keysColumnNames = Lists.newArrayList();
        for (Column column : olapTable.getBaseSchema()) {
            if (column.isKey()) {
                keysColumnNames.add("`" + column.getName() + "`");
            }
        }
        String pkSb = Joiner.on(", ").join(keysColumnNames);
        tableConfigInfo.setPrimary_key(olapTable.getKeysType().equals(KeysType.PRIMARY_KEYS)
                || olapTable.getKeysType().equals(KeysType.UNIQUE_KEYS) ? pkSb : DEFAULT_EMPTY_STRING);
        tableConfigInfo.setPartition_key(partitionKeySb.toString());
        tableConfigInfo.setDistribute_bucket(distributionInfo.getBucketNum());
        tableConfigInfo.setDistribute_type("HASH");
        tableConfigInfo.setDistribute_key(distributeKey);

        // SORT KEYS
        MaterializedIndexMeta index = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
        if (index.getSortKeyIdxes() == null) {
            tableConfigInfo.setSort_key(pkSb);
        } else {
            List<String> sortKeysColumnNames = Lists.newArrayList();
            for (Integer i : index.getSortKeyIdxes()) {
                sortKeysColumnNames.add("`" + table.getBaseSchema().get(i).getName() + "`");
            }
            tableConfigInfo.setSort_key(Joiner.on(", ").join(sortKeysColumnNames));
        }
        tableConfigInfo.setProperties(new Gson().toJson(genProps(table)));
        tableConfigInfo.setTable_id(table.getId());
        return tableConfigInfo;
    }

    // partitions_meta
    public static TGetPartitionsMetaResponse generatePartitionsMetaResponse(TGetPartitionsMetaRequest request)
            throws TException {
        TGetPartitionsMetaResponse resp = new TGetPartitionsMetaResponse();
        List<TPartitionMetaInfo> pList = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        for (String dbName : result.authorizedDbs) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                continue;
            }
            List<Table> allTables = db.getTables();
            for (Table table : allTables) {
                try {
                    Authorizer.checkAnyActionOnTableLikeObject(result.currentUser,
                            null, dbName, table);
                } catch (AccessDeniedException e) {
                    continue;
                }
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }
                // only olap table/mv or cloud table/mv will reach here;
                // use the same lock level with `SHOW PARTITIONS FROM XXX` to ensure other modification to
                // partition does not trigger crash
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();
                    // normal partition
                    for (Partition partition : olapTable.getPartitions()) {
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            TPartitionMetaInfo partitionMetaInfo = new TPartitionMetaInfo();
                            partitionMetaInfo.setDb_name(dbName);
                            partitionMetaInfo.setTable_name(olapTable.getName());
                            genPartitionMetaInfo(db, olapTable, tblPartitionInfo, partition, physicalPartition,
                                    partitionMetaInfo, false /* isTemp */);
                            pList.add(partitionMetaInfo);
                        }
                    }
                    // temp partition
                    for (Partition partition : olapTable.getTempPartitions()) {
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            TPartitionMetaInfo partitionMetaInfo = new TPartitionMetaInfo();
                            partitionMetaInfo.setDb_name(dbName);
                            partitionMetaInfo.setTable_name(olapTable.getName());
                            genPartitionMetaInfo(db, olapTable, tblPartitionInfo, partition, physicalPartition,
                                    partitionMetaInfo, true /* isTemp */);
                            pList.add(partitionMetaInfo);
                        }
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }
        resp.partitions_meta_infos = pList;
        return resp;
    }

    private static void genPartitionMetaInfo(Database db, OlapTable table,
            PartitionInfo partitionInfo, Partition partition, PhysicalPartition physicalPartition,
            TPartitionMetaInfo partitionMetaInfo, boolean isTemp) {
        // PARTITION_NAME
        partitionMetaInfo.setPartition_name(partition.getName());
        // PARTITION_ID
        partitionMetaInfo.setPartition_id(physicalPartition.getId());
        // VISIBLE_VERSION
        partitionMetaInfo.setVisible_version(physicalPartition.getVisibleVersion());
        // VISIBLE_VERSION_TIME
        partitionMetaInfo.setVisible_version_time(physicalPartition.getVisibleVersionTime() / 1000);
        // PARTITION_KEY
        partitionMetaInfo.setPartition_key(
                Joiner.on(", ").join(PartitionsProcDir.findPartitionColNames(partitionInfo)));
        // PARTITION_VALUE
        partitionMetaInfo.setPartition_value(
                PartitionsProcDir.findRangeOrListValues(partitionInfo, partition.getId()));
        DistributionInfo distributionInfo = partition.getDistributionInfo();
        // DISTRIBUTION_KEY
        partitionMetaInfo.setDistribution_key(PartitionsProcDir.distributionKeyAsString(distributionInfo));
        // BUCKETS
        partitionMetaInfo.setBuckets(distributionInfo.getBucketNum());
        // REPLICATION_NUM
        partitionMetaInfo.setReplication_num(partitionInfo.getReplicationNum(partition.getId()));
        // DATA_SIZE
        ByteSizeValue byteSizeValue = new ByteSizeValue(physicalPartition.storageDataSize());
        partitionMetaInfo.setData_size(byteSizeValue.toString());
        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
        // STORAGE_MEDIUM
        partitionMetaInfo.setStorage_medium(dataProperty.getStorageMedium().name());
        // COOLDOWN_TIME
        partitionMetaInfo.setCooldown_time(dataProperty.getCooldownTimeMs() / 1000);
        // LAST_CONSISTENCY_CHECK_TIME
        partitionMetaInfo.setLast_consistency_check_time(partition.getLastCheckTime() / 1000);
        // IS_IN_MEMORY
        partitionMetaInfo.setIs_in_memory(partitionInfo.getIsInMemory(partition.getId()));
        // ROW_COUNT
        partitionMetaInfo.setRow_count(physicalPartition.storageRowCount());
        // IS_TEMP
        partitionMetaInfo.setIs_temp(isTemp);
        // NEXT_VERSION
        partitionMetaInfo.setNext_version(physicalPartition.getNextVersion());
        if (table.isCloudNativeTableOrMaterializedView()) {
            PartitionIdentifier identifier = new PartitionIdentifier(db.getId(), table.getId(), physicalPartition.getId());
            PartitionStatistics statistics = GlobalStateMgr.getCurrentState().getCompactionMgr().getStatistics(identifier);
            Quantiles compactionScore = statistics != null ? statistics.getCompactionScore() : null;
            // COMPACT_VERSION
            partitionMetaInfo.setCompact_version(statistics != null ? statistics.getCompactionVersion().getVersion() : 0);
            DataCacheInfo cacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
            // ENABLE_DATACACHE
            partitionMetaInfo.setEnable_datacache(cacheInfo.isEnabled());
            // AVG_CS
            partitionMetaInfo.setAvg_cs(compactionScore != null ? compactionScore.getAvg() : 0.0);
            // P50_CS
            partitionMetaInfo.setP50_cs(compactionScore != null ? compactionScore.getP50() : 0.0);
            // MAX_CS
            partitionMetaInfo.setMax_cs(compactionScore != null ? compactionScore.getMax() : 0.0);
            // STORAGE_PATH
            partitionMetaInfo.setStorage_path(
                    table.getPartitionFilePathInfo(physicalPartition.getId()).getFullPath());
        }
    }

    // tables
    public static TGetTablesInfoResponse generateTablesInfoResponse(TGetTablesInfoRequest request) throws TException {

        TGetTablesInfoResponse response = new TGetTablesInfoResponse();
        List<TTableInfo> infos = new ArrayList<>();

        TAuthInfo authInfo = request.getAuth_info();
        AuthDbRequestResult result = getAuthDbRequestResult(authInfo);

        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        if (authInfo.isSetCatalog_name()) {
            catalogName = authInfo.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        for (String dbName : result.authorizedDbs) {
            Database db = metadataMgr.getDb(catalogName, dbName);

            if (db != null) {
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    List<String> tableNames = metadataMgr.listTableNames(catalogName, dbName);
                    for (String tableName : tableNames) {
                        BasicTable table = null;
                        try {
                            table = metadataMgr.getBasicTable(catalogName, dbName, tableName);
                        } catch (Exception e) {
                            LOG.warn(e.getMessage());
                        }

                        if (table == null) {
                            continue;
                        }

                        try {
                            Authorizer.checkAnyActionOnTableLikeObject(result.currentUser, null, dbName, table);
                        } catch (AccessDeniedException e) {
                            continue;
                        }

                        TTableInfo info = new TTableInfo();

                        // refer to https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html
                        // the catalog name is always `def`
                        info.setTable_catalog(DEF);
                        info.setTable_schema(dbName);
                        info.setTable_name(table.getName());
                        info.setTable_type(table.getMysqlType());
                        info.setEngine(table.getEngine());
                        info.setVersion(DEFAULT_EMPTY_NUM);
                        // TABLE_ROWS (depend on the table type)
                        // AVG_ROW_LENGTH (depend on the table type)
                        // DATA_LENGTH (depend on the table type)
                        info.setMax_data_length(DEFAULT_EMPTY_NUM);
                        info.setIndex_length(DEFAULT_EMPTY_NUM);
                        info.setData_free(DEFAULT_EMPTY_NUM);
                        info.setAuto_increment(DEFAULT_EMPTY_NUM);
                        info.setCreate_time(table.getCreateTime());
                        // UPDATE_TIME (depend on the table type)
                        info.setCheck_time(table.getLastCheckTime() / 1000);
                        info.setTable_collation(UTF8_GENERAL_CI);
                        info.setChecksum(DEFAULT_EMPTY_NUM);
                        info.setTable_comment(table.getComment());

                        if (table.isNativeTableOrMaterializedView() || table.getType() == TableType.OLAP_EXTERNAL) {
                            // OLAP (done)
                            // OLAP_EXTERNAL (done)
                            // MATERIALIZED_VIEW (done)
                            // LAKE (done)
                            // LAKE_MATERIALIZED_VIEW (done)
                            genNormalTableInfo(table, info);
                        } else {
                            // SCHEMA (use default)
                            // INLINE_VIEW (use default)
                            // VIEW (use default)
                            // BROKER (use default)
                            // EXTERNAL TABLE (use default)
                            genDefaultConfigInfo(info);
                        }
                        infos.add(info);
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }
        response.setTables_infos(infos);
        return response;
    }

    public static TTableInfo genNormalTableInfo(BasicTable table, TTableInfo info) {

        OlapTable olapTable = (OlapTable) table;
        Collection<PhysicalPartition> partitions = olapTable.getPhysicalPartitions();
        long lastUpdateTime = 0L;
        long totalRowsOfTable = 0L;
        long totalBytesOfTable = 0L;
        for (PhysicalPartition partition : partitions) {
            if (partition.getVisibleVersionTime() > lastUpdateTime) {
                lastUpdateTime = partition.getVisibleVersionTime();
            }
            totalRowsOfTable = partition.getBaseIndex().getRowCount() + totalRowsOfTable;
            totalBytesOfTable = partition.getBaseIndex().getDataSize() + totalBytesOfTable;
        }
        // TABLE_ROWS
        info.setTable_rows(totalRowsOfTable);
        // AVG_ROW_LENGTH
        if (totalRowsOfTable == 0) {
            info.setAvg_row_length(0L);
        } else {
            info.setAvg_row_length(totalBytesOfTable / totalRowsOfTable);
        }
        // DATA_LENGTH
        info.setData_length(olapTable.getDataSize());
        // UPDATE_TIME
        info.setUpdate_time(lastUpdateTime / 1000);
        return info;
    }

    public static TTableInfo genDefaultConfigInfo(TTableInfo info) {
        info.setTable_rows(DEFAULT_EMPTY_NUM);
        info.setAvg_row_length(DEFAULT_EMPTY_NUM);
        info.setData_length(DEFAULT_EMPTY_NUM);
        info.setUpdate_time(DEFAULT_EMPTY_NUM);
        return info;
    }
}
