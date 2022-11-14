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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class InformationSchemaDataSource {
    
    private static final Logger LOG = LogManager.getLogger(InformationSchemaDataSource.class);

    private static final String DEF = "def";
    private static final String DEF_NULL = "NULL";
    // If a column uses this as a default value means that the column will be displayed as NULL
    private static final long DEF_NULL_NUM = -1L;
    private static final String UTF8_GENERAL_CI = "utf8_general_ci";

    private static List<String> getAuthorizedDbs(TAuthInfo authInfo) throws TException {

        List<String> dbs = Lists.newArrayList();
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

        UserIdentity currentUser = null;
        if (authInfo.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(authInfo.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
        }
        for (String fullName : dbNames) {
            if (!globalStateMgr.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                continue;
            }

            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (matcher != null && !matcher.match(db)) {
                continue;
            }
            dbs.add(fullName);
        }
        return dbs;
    }

    // tables_config
    public static TGetTablesConfigResponse generateTablesConfigResponse(TGetTablesConfigRequest request) throws TException {
        
        TGetTablesConfigResponse resp = new TGetTablesConfigResponse();
        List<TTableConfigInfo> tList = new ArrayList<>();

        List<String> authorizedDbs = getAuthorizedDbs(request.getAuth_info());
        authorizedDbs.forEach(dbName -> {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db != null) {
                db.readLock();
                try {
                    List<Table> allTables = db.getTables();
                    allTables.forEach(table -> {
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
                            tableConfigInfo = genNormalTableConfigInfo(table, tableConfigInfo);
                        } else {
                            // SCHEMA (use default)
                            // INLINE_VIEW (use default)
                            // VIEW (use default)
                            // BROKER (use default)                           
                            tableConfigInfo = genDefaultConfigInfo(tableConfigInfo);
                        }
                        // TODO(cjs): other table type (HIVE, MYSQL, ICEBERG, HUDI, JDBC, ELASTICSEARCH)
                        tList.add(tableConfigInfo);
                    });
                } finally {
                    db.readUnlock();
                }                
            }            
        });
        resp.tables_config_infos = tList;
        return resp;
    }

    private static Map<String, String> genProps(Table table) {

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
            propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "LZ4");
        } else {
            propsMap.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, olapTable.getCompressionType().name());
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
                partitionKeySb.append(DEF_NULL);
                LOG.warn("The partition of type range seems not implement getPartitionColumns");
            }            
        } else {
            partitionKeySb.append(DEF_NULL);
        }

        // keys
        StringBuilder keysSb = new StringBuilder();
        List<String> keysColumnNames = Lists.newArrayList();
        for (Column column : olapTable.getBaseSchema()) {
            if (column.isKey()) {
                keysColumnNames.add("`" + column.getName() + "`");
            }
        }
        keysSb.append(Joiner.on(", ").join(keysColumnNames));

        tableConfigInfo.setPrimary_key(isSortKey(olapTable.getKeysType()) ? DEF_NULL : keysSb.toString());
        tableConfigInfo.setPartition_key(partitionKeySb.toString());
        tableConfigInfo.setDistribute_bucket(distributionInfo.getBucketNum());
        tableConfigInfo.setDistribute_type("HASH");
        tableConfigInfo.setDistribute_key(distributeKey);
        tableConfigInfo.setSort_key(isSortKey(olapTable.getKeysType()) ? keysSb.toString() : DEF_NULL);
        tableConfigInfo.setProperties(new Gson().toJson(genProps(table)));
        return tableConfigInfo;
    }

    private static boolean isSortKey(KeysType kType) {
        if (kType.equals(KeysType.PRIMARY_KEYS) || kType.equals(KeysType.UNIQUE_KEYS)) {
            return false;
        }
        return true;
    }

    private static TTableConfigInfo genDefaultConfigInfo(TTableConfigInfo tableConfigInfo) {
        tableConfigInfo.setTable_engine(DEF);
        tableConfigInfo.setTable_model(DEF);
        tableConfigInfo.setPrimary_key(DEF);
        tableConfigInfo.setPartition_key(DEF);
        tableConfigInfo.setDistribute_bucket(0);
        tableConfigInfo.setDistribute_type(DEF);
        tableConfigInfo.setDistribute_key(DEF);
        tableConfigInfo.setSort_key(DEF);
        tableConfigInfo.setProperties(DEF);
        return tableConfigInfo;
    }

    // tables
    public static TGetTablesInfoResponse generateTablesInfoResponse(TGetTablesInfoRequest request) throws TException {
        
        TGetTablesInfoResponse response = new TGetTablesInfoResponse();
        List<TTableInfo> infos = new ArrayList<>();
        List<String> authorizedDbs = getAuthorizedDbs(request.getAuth_info());
        authorizedDbs.forEach(dbName -> {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db != null) {
                db.readLock();
                try {
                    List<Table> allTables = db.getTables();
                    allTables.forEach(table -> {
                        TTableInfo info = new TTableInfo();

                        info.setTable_catalog(DEF);
                        info.setTable_schema(dbName);
                        info.setTable_name(table.getName());
                        info.setTable_type(transferTableTypeToAdaptMysql(table.getType()));
                        info.setEngine(table.getEngine());
                        info.setVersion(DEF_NULL_NUM);
                        info.setRow_format(DEF_NULL);
                        // TABLE_ROWS (depend on the table type)
                        // AVG_ROW_LENGTH (depend on the table type)
                        // DATA_LENGTH (depend on the table type)
                        info.setMax_data_length(DEF_NULL_NUM);
                        info.setIndex_length(DEF_NULL_NUM);
                        info.setData_free(DEF_NULL_NUM);
                        info.setAuto_increment(DEF_NULL_NUM);
                        info.setCreate_time(table.getCreateTime());
                        // UPDATE_TIME (depend on the table type)
                        info.setCheck_time(table.getLastCheckTime());
                        info.setTable_collation(UTF8_GENERAL_CI);
                        info.setChecksum(DEF_NULL_NUM);
                        info.setCreate_options(DEF_NULL);
                        info.setTable_comment(table.getComment());

                        if (table.isOlapOrLakeTable() || 
                                table.getType() == TableType.OLAP_EXTERNAL ||
                                table.getType() == TableType.MATERIALIZED_VIEW) {
                            // OLAP (done)
                            // OLAP_EXTERNAL (done)
                            // MATERIALIZED_VIEW (done)
                            // LAKE (done)
                            info = genNormalTableInfo(table, info);
                        } else {
                            // SCHEMA (use default)
                            // INLINE_VIEW (use default)
                            // VIEW (use default)
                            // BROKER (use default)                           
                            info = genDefaultConfigInfo(info);
                        }
                        // TODO(cjs): other table type (HIVE, MYSQL, ICEBERG, HUDI, JDBC, ELASTICSEARCH)
                        infos.add(info);
                    });
                } finally {
                    db.readUnlock();
                }                
            }            
        });    
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
                return "MATERIALIZED VIEW";
            case VIEW:
                return "VIEW";
            default:
                // SCHEMA
                // INLINE_VIEW
                // BROKER
                return "BASE TABLE";
        }
    }

    private static TTableInfo genNormalTableInfo(Table table, TTableInfo info) {
        
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
        info.setData_length(totalBytesOfTable);
        // UPDATE_TIME
        info.setUpdate_time(lastUpdateTime / 1000);
        return info;
    }

    private static TTableInfo genDefaultConfigInfo(TTableInfo info) {
        info.setTable_rows(DEF_NULL_NUM);
        info.setAvg_row_length(DEF_NULL_NUM);
        info.setData_length(DEF_NULL_NUM);
        info.setUpdate_time(DEF_NULL_NUM);
        return info;
    }
}
