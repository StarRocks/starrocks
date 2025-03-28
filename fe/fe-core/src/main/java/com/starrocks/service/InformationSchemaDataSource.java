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
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
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
import com.starrocks.common.Config;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.PartitionsProcDir;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TApplicableRolesInfo;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetApplicableRolesRequest;
import com.starrocks.thrift.TGetApplicableRolesResponse;
import com.starrocks.thrift.TGetKeywordsRequest;
import com.starrocks.thrift.TGetKeywordsResponse;
import com.starrocks.thrift.TGetPartitionsMetaRequest;
import com.starrocks.thrift.TGetPartitionsMetaResponse;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTemporaryTablesInfoRequest;
import com.starrocks.thrift.TGetTemporaryTablesInfoResponse;
import com.starrocks.thrift.TKeywordInfo;
import com.starrocks.thrift.TPartitionMetaInfo;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.thrift.TTableInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

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

        UserIdentity currentUser;
        if (authInfo.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(authInfo.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
        }
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(currentUser);
        context.setCurrentRoleIds(currentUser);

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> dbNames = metadataMgr.listDbNames(context, catalogName);
        LOG.debug("get db names: {}", dbNames);

        for (String fullName : dbNames) {

            try {
                Authorizer.checkAnyActionOnOrInDb(context, catalogName, fullName);
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

    // keywords
    public static TGetKeywordsResponse generateKeywordsResponse(TGetKeywordsRequest request) throws TException {
        TGetKeywordsResponse resp = new TGetKeywordsResponse();
        List<TKeywordInfo> keywordList = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        if (result.authorizedDbs.isEmpty()) {
            resp.setKeywords(keywordList);
            return resp;
        }
        class KeywordElement {
            public KeywordElement(String keyword, boolean reserved) {
                this.keyword = keyword;
                this.reserved = reserved;
            }
            public String getKeyword() {
                return keyword;
            }
            public String keyword;
            public boolean reserved;
        }

        TreeSet<KeywordElement> sortedKeywords = new TreeSet<>(Comparator.comparing(KeywordElement::getKeyword));

        List<KeywordElement> keywords = new ArrayList<>();
        keywords.add(new KeywordElement("SELECT", true));
        keywords.add(new KeywordElement("INSERT", true));
        keywords.add(new KeywordElement("UPDATE", true));
        keywords.add(new KeywordElement("DELETE", true));
        keywords.add(new KeywordElement("TABLE", true));
        keywords.add(new KeywordElement("INDEX", true));
        keywords.add(new KeywordElement("VIEW", true));
        keywords.add(new KeywordElement("USER", false));
        keywords.add(new KeywordElement("PASSWORD", false));
        sortedKeywords.addAll(keywords);

        long startTableIdOffset = request.isSetStart_table_id_offset() ? request.getStart_table_id_offset() : 0;
        boolean startOffsetReached = startTableIdOffset == 0;
        long currentOffset = 0;

        for (KeywordElement ele : sortedKeywords) {
            if (!startOffsetReached) {
                if (currentOffset == startTableIdOffset) {
                    startOffsetReached = true;
                }
                currentOffset++;
                continue;
            }

            TKeywordInfo keywordInfo = new TKeywordInfo();
            keywordInfo.setKeyword(ele.keyword);
            keywordInfo.setReserved(ele.reserved);
            keywordList.add(keywordInfo);

            if (keywordList.size() >= 10000) {
                resp.setNext_table_id_offset(currentOffset + 1);
                break;
            }
            currentOffset++;
        }

        resp.setKeywords(keywordList);
        return resp;
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
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (db != null) {
                Locker locker = new Locker();
                locker.lockDatabase(db.getId(), LockType.READ);
                try {
                    List<Table> allTables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
                    for (Table table : allTables) {
                        try {
                            ConnectContext context = new ConnectContext();
                            context.setCurrentUserIdentity(result.currentUser);
                            context.setCurrentRoleIds(result.currentUser);
                            Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
                        } catch (AccessDeniedException e) {
                            LOG.warn("failed to check db: {} table: {} authorization", dbName, table, e);
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
                    locker.unLockDatabase(db.getId(), LockType.READ);
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
        return table.getProperties();
    }

    private static TTableConfigInfo genNormalTableConfigInfo(Table table, TTableConfigInfo tableConfigInfo) {
        OlapTable olapTable = (OlapTable) table;
        tableConfigInfo.setTable_engine(olapTable.getType().toString());
        tableConfigInfo.setTable_model(olapTable.getKeysType().toString());

        // Partition info
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        StringBuilder partitionKeySb = new StringBuilder();
        int idx = 0;
        for (Column column : partitionInfo.getPartitionColumns(table.getIdToColumn())) {
            if (idx != 0) {
                partitionKeySb.append(", ");
            }
            partitionKeySb.append("`").append(column.getName()).append("`");
            idx++;
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
        tableConfigInfo.setPartition_key(partitionKeySb.length() > 0 ? partitionKeySb.toString() : DEFAULT_EMPTY_STRING);

        // Distribution info
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        tableConfigInfo.setDistribute_bucket(distributionInfo.getBucketNum());
        tableConfigInfo.setDistribute_type(distributionInfo.getType().name());
        tableConfigInfo.setDistribute_key(distributionInfo.getDistributionKey(olapTable.getIdToColumn()));

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

        class Element {
            public Element(String dbName, long dbId, Table table) {
                this.dbName = dbName;
                this.dbId = dbId;
                this.table = table;
            }
            public long getTableId() {
                return table.getId();
            }
            public String dbName;
            public long dbId;
            public Table table;
        };
        long startTableIdOffset = request.isSetStart_table_id_offset() ? request.getStart_table_id_offset() : 0;
        TreeSet<Element> sortedElements = new TreeSet<>(Comparator.comparing(Element::getTableId));
        for (String dbName : result.authorizedDbs) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (db == null) {
                continue;
            }
            List<Table> tables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
            for (Table table : tables) {
                if (table.getId() < startTableIdOffset) {
                    continue;
                }
                sortedElements.add(new Element(dbName, db.getId(), table));
            }
        }

        for (Element ele : sortedElements) {
            Table table = ele.table;
            if (!table.isNativeTableOrMaterializedView()) {
                continue;
            }
            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(result.currentUser);
                context.setCurrentRoleIds(result.currentUser);
                Authorizer.checkAnyActionOnTableLikeObject(context, ele.dbName, table);
            } catch (AccessDeniedException e) {
                LOG.warn("failed to check db: {} table: {} authorization", ele.dbName, table, e);
                continue;
            }
            // only olap table/mv or cloud table/mv will reach here;
            // use the same lock level with `SHOW PARTITIONS FROM XXX` to ensure other modification to
            // partition does not trigger crash
            Locker locker = new Locker();
            locker.lockDatabase(ele.dbId, LockType.READ);
            try {
                OlapTable olapTable = (OlapTable) table;
                PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();
                // normal partition
                for (Partition partition : olapTable.getPartitions()) {
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        TPartitionMetaInfo partitionMetaInfo = new TPartitionMetaInfo();
                        partitionMetaInfo.setDb_name(ele.dbName);
                        partitionMetaInfo.setTable_name(olapTable.getName());
                        genPartitionMetaInfo(ele.dbId, olapTable, tblPartitionInfo, partition, physicalPartition,
                                partitionMetaInfo, false /* isTemp */);
                        pList.add(partitionMetaInfo);
                    }
                }
                // temp partition
                for (Partition partition : olapTable.getTempPartitions()) {
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        TPartitionMetaInfo partitionMetaInfo = new TPartitionMetaInfo();
                        partitionMetaInfo.setDb_name(ele.dbName);
                        partitionMetaInfo.setTable_name(olapTable.getName());
                        genPartitionMetaInfo(ele.dbId, olapTable, tblPartitionInfo, partition, physicalPartition,
                                partitionMetaInfo, true /* isTemp */);
                        pList.add(partitionMetaInfo);
                    }
                }
                if (pList.size() >= Config.max_get_partitions_meta_result_count) {
                    resp.setNext_table_id_offset(table.getId() + 1);
                    break;
                }
            } finally {
                locker.unLockDatabase(ele.dbId, LockType.READ);
            }
        }
        resp.partitions_meta_infos = pList;
        return resp;
    }

    private static void genPartitionMetaInfo(long dbId, OlapTable table,
                                             PartitionInfo partitionInfo, Partition partition,
                                             PhysicalPartition physicalPartition,
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
                Joiner.on(", ").join(partitionInfo.getPartitionColumns(table.getIdToColumn())));
        // PARTITION_VALUE
        partitionMetaInfo.setPartition_value(
                PartitionsProcDir.findRangeOrListValues(partitionInfo, partition.getId()));
        DistributionInfo distributionInfo = partition.getDistributionInfo();
        // DISTRIBUTION_KEY
        partitionMetaInfo.setDistribution_key(PartitionsProcDir.distributionKeyAsString(table, distributionInfo));
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
            PartitionIdentifier identifier = new PartitionIdentifier(dbId, table.getId(), physicalPartition.getId());
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

        partitionMetaInfo.setData_version(physicalPartition.getDataVersion());
        partitionMetaInfo.setVersion_epoch(physicalPartition.getVersionEpoch());
        partitionMetaInfo.setVersion_txn_type(physicalPartition.getVersionTxnType().toThrift());
    }

    // tables
    public static TGetTablesInfoResponse generateTablesInfoResponse(TGetTablesInfoRequest request) throws TException {
        TGetTablesInfoResponse response = new TGetTablesInfoResponse();
        List<TTableInfo> infos = new ArrayList<>();

        TAuthInfo authInfo = request.getAuth_info();
        AuthDbRequestResult result = getAuthDbRequestResult(authInfo);
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(result.currentUser);
        context.setCurrentRoleIds(result.currentUser);

        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        if (authInfo.isSetCatalog_name()) {
            catalogName = authInfo.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        for (String dbName : result.authorizedDbs) {
            Database db = metadataMgr.getDb(context, catalogName, dbName);
            if (db == null) {
                continue;
            }

            List<BasicTable> tables = new ArrayList<>();
            Locker locker = new Locker();
            try {
                locker.lockDatabase(db.getId(), LockType.READ);
                List<String> tableNames = metadataMgr.listTableNames(context, catalogName, dbName);
                for (String tableName : tableNames) {
                    if (request.isSetTable_name()) {
                        if (!tableName.equals(request.getTable_name())) {
                            continue;
                        }
                    }

                    BasicTable table = null;
                    try {
                        table = metadataMgr.getBasicTable(context, catalogName, dbName, tableName);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage(), e);
                    }
                    if (table == null) {
                        continue;
                    }

                    try {
                        Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    tables.add(table);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }

            for (BasicTable table : tables) {
                Locker tableLocker = new Locker();
                try {
                    if (table.isNativeTableOrMaterializedView()) {
                        tableLocker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(((OlapTable) table).getId()),
                                LockType.READ);
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
                } finally {
                    if (table.isNativeTableOrMaterializedView()) {
                        tableLocker.unLockTablesWithIntensiveDbLock(db.getId(),
                                Lists.newArrayList(((OlapTable) table).getId()), LockType.READ);
                    }
                }
            }
        }
        response.setTables_infos(infos);
        return response;
    }

    // applicable_roles
    public static TGetApplicableRolesResponse generateApplicableRolesResp(TGetApplicableRolesRequest request) throws TException {
        TGetApplicableRolesResponse response = new TGetApplicableRolesResponse();
        List<TApplicableRolesInfo> roleList = new ArrayList<>();

        AuthDbRequestResult result = getAuthDbRequestResult(request.getAuth_info());

        if (result.authorizedDbs.isEmpty()) {
            response.setRoles(roleList);
            return response;
        }

        UserIdentity currentUser = result.currentUser;
        if (currentUser == null) {
            response.setRoles(roleList);
            return response;
        }

        AuthorizationMgr authMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            for (RolePrivilegeCollectionV2 role : authMgr.getApplicableRoles(currentUser)) {
                String roleName = role.getName();
                String roleHost = "%";

                // TODO The related functions of 'isGrantable' may be expanded in the future
                String isGrantable = "NO";
                String isDefault = "NO";
                String isMandatory = "NO";

                TApplicableRolesInfo roleInfo = new TApplicableRolesInfo();
                roleInfo.setUser(currentUser.getUser());
                roleInfo.setHost(currentUser.getHost());
                roleInfo.setGrantee(currentUser.getUser());
                roleInfo.setGrantee_host(currentUser.getHost());
                roleInfo.setRole_name(roleName);
                roleInfo.setRole_host(roleHost);
                roleInfo.setIs_grantable(isGrantable);
                roleInfo.setIs_default(isDefault);
                roleInfo.setIs_mandatory(isMandatory);

                roleList.add(roleInfo);
            }
        } catch (PrivilegeException e) {
            throw new RuntimeException(e);
        }

        response.setRoles(roleList);
        return response;
    }

    public static TGetTemporaryTablesInfoResponse generateTemporaryTablesInfoResponse(TGetTemporaryTablesInfoRequest request)
            throws TException {
        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        TAuthInfo authInfo = request.getAuth_info();
        AuthDbRequestResult result = getAuthDbRequestResult(authInfo);
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(result.currentUser);
        context.setCurrentRoleIds(result.currentUser);

        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        if (authInfo.isSetCatalog_name()) {
            catalogName = authInfo.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        Set<Long> requiredDbIds = new HashSet<>();
        for (String dbName : result.authorizedDbs) {
            Database db = metadataMgr.getDb(context, catalogName, dbName);
            if (db != null) {
                requiredDbIds.add(db.getId());
            }
        }

        com.google.common.collect.Table<Long, Long, UUID> allTables = temporaryTableMgr.getAllTemporaryTables(requiredDbIds);

        List<TTableInfo> tableInfos = new ArrayList<>();
        for (Long databaseId : allTables.rowKeySet()) {
            Database db = metadataMgr.getDb(databaseId);
            if (db != null) {
                Map<Long, UUID> tableMap = allTables.row(databaseId);
                Locker locker = new Locker();
                locker.lockDatabase(db.getId(), LockType.READ);
                try {
                    for (Map.Entry<Long, UUID> entry : tableMap.entrySet()) {
                        UUID sessionId = entry.getValue();
                        Long tableId = entry.getKey();
                        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                        if (table != null) {
                            TTableInfo info = new TTableInfo();

                            // the catalog name is always `def`
                            info.setTable_catalog(DEF);
                            info.setTable_schema(db.getFullName());
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
                            info.setSession_id(sessionId.toString());
                            info.setTable_id(table.getId());
                            genNormalTableInfo(table, info);
                            tableInfos.add(info);
                            if (request.isSetLimit() && tableInfos.size() >= request.getLimit()) {
                                break;
                            }
                        }
                    }
                    if (request.isSetLimit() && tableInfos.size() >= request.getLimit()) {
                        break;
                    }
                } finally {
                    locker.unLockDatabase(db.getId(), LockType.READ);
                }
            }
        }

        TGetTemporaryTablesInfoResponse response = new TGetTemporaryTablesInfoResponse();
        response.setTables_infos(tableInfos);
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
