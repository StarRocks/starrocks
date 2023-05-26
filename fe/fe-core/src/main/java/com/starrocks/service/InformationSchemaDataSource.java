// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTable;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
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
    private static final long DEFAULT_EMPTY_NUM = -1L;
    private static final String UTF8_GENERAL_CI = "utf8_general_ci";

    @NotNull
    private static AuthDbRequestResult getAuthDbRequestResult(TAuthInfo authInfo) throws TException {


        List<String> authorizedDbs = Lists.newArrayList();
        PatternMatcher matcher = null;
        if (authInfo.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(authInfo.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + authInfo.getPattern());
            }
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getDbNames();
        LOG.debug("get db names: {}", dbNames);

        UserIdentity currentUser;
        if (authInfo.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(authInfo.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
        }
        for (String fullName : dbNames) {
            if (!globalStateMgr.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                continue;
            }

            final String db1 = ClusterNamespace.getNameFromFullName(fullName);
            if (matcher != null && !matcher.match(db1)) {
                continue;
            }
            authorizedDbs.add(fullName);
        }
        AuthDbRequestResult result = new AuthDbRequestResult(authorizedDbs, currentUser);
        return result;
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
    public static TGetTablesConfigResponse generateTablesConfigResponse(TGetTablesConfigRequest request) throws TException {
        
        TGetTablesConfigResponse resp = new TGetTablesConfigResponse();
        List<TTableConfigInfo> tList = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        for (String dbName : result.authorizedDbs) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db != null) {
                db.readLock();
                try {
                    List<Table> allTables = db.getTables();
                    for (Table table : allTables) {

                        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(result.currentUser, dbName,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }

                        TTableConfigInfo tableConfigInfo = new TTableConfigInfo();
                        tableConfigInfo.setTable_schema(dbName);
                        tableConfigInfo.setTable_name(table.getName());
                        
                        if (table.isOlapOrLakeTable() || 
                                table.getType() == TableType.OLAP_EXTERNAL ||
                                table.getType() == TableType.MATERIALIZED_VIEW) {
                            // OLAP (done)
                            // OLAP_EXTERNAL (done)
                            // MATERIALIZED_VIEW (done)
                            // LAKE (done)
                            genNormalTableConfigInfo(table, tableConfigInfo);
                        }
                        // TODO(cjs): other table type (HIVE, MYSQL, ICEBERG, HUDI, JDBC, ELASTICSEARCH)
                        tList.add(tableConfigInfo);
                    }
                } finally {
                    db.readUnlock();
                }                
            }            
        }
        resp.tables_config_infos = tList;
        return resp;
    }



    private static Map<String, String> genProps(Table table) {

        if (table.getType() == TableType.MATERIALIZED_VIEW) {
            MaterializedView mv = (MaterializedView) table;
            return mv.getMaterializedViewPropMap();
        }

        OlapTable olapTable = (OlapTable) table;
        Map<String, String> propsMap = new HashMap<>(); 
        
        propsMap.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(olapTable.getDefaultReplicationNum()));
        
        // bloom filter
        Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
        if (bfColumnNames != null) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, Joiner.on(", ").join(olapTable.getCopiedBfColumns()));
        }

        // colocateTable
        String colocateTable = olapTable.getColocateGroup();
        if (colocateTable != null) {
            propsMap.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, colocateTable);
        }

        // dynamic partition
        if (olapTable.dynamicPartitionExists()) {
            propsMap.put("dynamic_partition", olapTable.getTableProperty().getDynamicPartitionProperty().getPropString());
        }

        // in memory
        propsMap.put(PropertyAnalyzer.PROPERTIES_INMEMORY, String.valueOf(olapTable.isInMemory()));

        // enable storage cache && cache ttl
        if (table.isLakeTable()) {
            Map<String, String> storageProperties = ((LakeTable) olapTable).getProperties();
            propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE, 
                    storageProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE));
            propsMap.put(PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL, 
                    storageProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL));
            propsMap.put(PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK, 
                    storageProperties.get(PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK));
        }

        // storage type
        propsMap.put(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, olapTable.getStorageFormat().name());
        
        // enable_persistent_index
        propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, String.valueOf(olapTable.enablePersistentIndex()));

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
        if (partitionInfo.getType().equals(PartitionType.RANGE)) {
            int idx = 0;
            try {
                for (Column column : partitionInfo.getPartitionColumns()) {
                    if (idx != 0) {
                        partitionKeySb.append(", ");
                    }
                    partitionKeySb.append("`").append(column.getName()).append("`");
                    idx++;
                }
            } catch (NotImplementedException e) {
                partitionKeySb.append(DEFAULT_EMPTY_STRING);
                LOG.warn("The partition of type range seems not implement getPartitionColumns");
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
        return tableConfigInfo;
    }

    // tables
    public static TGetTablesInfoResponse generateTablesInfoResponse(TGetTablesInfoRequest request) throws TException {
        
        TGetTablesInfoResponse response = new TGetTablesInfoResponse();
        List<TTableInfo> infos = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        for (String dbName : result.authorizedDbs) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db != null) {
                db.readLock();
                try {
                    List<Table> allTables = db.getTables();
                    for (Table table : allTables) {

                        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(result.currentUser, dbName,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }

                        TTableInfo info = new TTableInfo();

                        info.setTable_catalog(DEF);
                        info.setTable_schema(dbName);
                        info.setTable_name(table.getName());
                        info.setTable_type(transferTableTypeToAdaptMysql(table.getType()));
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

                        if (table.isOlapOrLakeTable() || 
                                table.getType() == TableType.OLAP_EXTERNAL ||
                                table.getType() == TableType.MATERIALIZED_VIEW) {
                            // OLAP (done)
                            // OLAP_EXTERNAL (done)
                            // MATERIALIZED_VIEW (done)
                            // LAKE (done)
                            genNormalTableInfo(table, info);
                        } else {
                            // SCHEMA (use default)
                            // INLINE_VIEW (use default)
                            // VIEW (use default)
                            // BROKER (use default)
                            genDefaultConfigInfo(info);
                        }
                        // TODO(cjs): other table type (HIVE, MYSQL, ICEBERG, HUDI, JDBC, ELASTICSEARCH)
                        infos.add(info);
                    }
                } finally {
                    db.readUnlock();
                }                
            }            
        }
        response.setTables_infos(infos);
        return response;
    }

    private static String transferTableTypeToAdaptMysql(TableType tableType) {
        // 'BASE TABLE','SYSTEM VERSIONED','PARTITIONED TABLE','VIEW','FOREIGN TABLE','MATERIALIZED VIEW','EXTERNAL TABLE'
        switch (tableType) {
            case MYSQL:
            case HIVE:
            case ICEBERG:
            case HUDI:
            case LAKE:
            case ELASTICSEARCH:
            case JDBC:
                return "EXTERNAL TABLE";
            case OLAP:
            case OLAP_EXTERNAL:
                return "BASE TABLE";
            case MATERIALIZED_VIEW:
            case VIEW:
                return "VIEW";
            case SCHEMA:
                return "SYSTEM VIEW";
            default:
                // INLINE_VIEW
                // BROKER
                return "BASE TABLE";
        }
    }

    private static TTableInfo genNormalTableInfo(Table table, TTableInfo info) {
        
        OlapTable olapTable = (OlapTable) table;
        Collection<Partition> partitions = table.getPartitions();
        long lastUpdateTime = 0L;
        long totalRowsOfTable = 0L;
        long totalBytesOfTable = 0L;
        for (Partition partition : partitions) {
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

    private static TTableInfo genDefaultConfigInfo(TTableInfo info) {
        info.setTable_rows(DEFAULT_EMPTY_NUM);
        info.setAvg_row_length(DEFAULT_EMPTY_NUM);
        info.setData_length(DEFAULT_EMPTY_NUM);
        info.setUpdate_time(DEFAULT_EMPTY_NUM);
        return info;
    }
}
