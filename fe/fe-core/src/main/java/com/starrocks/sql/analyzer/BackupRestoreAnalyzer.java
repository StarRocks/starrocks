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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CatalogRef;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class BackupRestoreAnalyzer {
    private static final Logger LOG = LogManager.getLogger(BackupRestoreAnalyzer.class);

    public static void analyze(StatementBase statement, ConnectContext session) {
        new BackupRestoreStmtAnalyzerVisitor().analyze(statement, session);
    }

    public static class BackupRestoreStmtAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

        private static final String PROP_TIMEOUT = "timeout";
        private static final long MIN_TIMEOUT_MS = 600_000L; // 10 min
        private static final String PROP_TYPE = "type";
        private static final String PROP_ALLOW_LOAD = "allow_load";
        private static final String PROP_REPLICATION_NUM = "replication_num";
        private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
        private static final String PROP_META_VERSION = "meta_version";
        private static final String PROP_STARROCKS_META_VERSION = "starrocks_meta_version";

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitBackupStatement(BackupStmt backupStmt, ConnectContext context) {
            if (backupStmt.containsExternalCatalog()) {
                analyzeBackupProperties(backupStmt);

                if (backupStmt.allExternalCatalog()) {
                    backupStmt.getExternalCatalogRefs().clear();
                    // get all catalog from CatalogMgr
                    backupStmt.getExternalCatalogRefs().addAll(
                                GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().keySet()
                                .stream().filter(x -> !x.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                                .map(x -> new CatalogRef(x)).collect(Collectors.toList()));
                }

                if (backupStmt.getExternalCatalogRefs().isEmpty()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                                        "No external catalog can be backed up");
                }

                backupStmt.getExternalCatalogRefs().stream().forEach(x -> x.analyzeForBackup());
                return null;
            }

            String dbName = getDbName(backupStmt.getDbName(), context);
            Database database = getDatabase(dbName, context);
            analyzeLabelAndRepo(backupStmt.getLabel(), backupStmt.getRepoName());

            boolean withOnClause = backupStmt.withOnClause();
            boolean allFunction = backupStmt.allFunction();
            boolean allTable = backupStmt.allTable();
            boolean allMV = backupStmt.allMV();
            boolean allView = backupStmt.allView();

            Map<String, TableRef> tblPartsMap = Maps.newTreeMap();
            List<TableRef> tableRefs = backupStmt.getTableRefs();
            // There are several cases:
            // 1. Backup all table/mv/view without `ON` clause.
            // 2. Backup all table if specify `ALL` for table.
            // 3. Backup all MV if specify `ALL` for MV.
            // 4. Backup all VIEW if specify `ALL` for VIEW.
            if (!withOnClause || allTable || allMV || allView) {
                for (Table tbl : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(database.getId())) {
                    if (!Config.enable_backup_materialized_view && tbl.isMaterializedView()) {
                        LOG.info("Skip backup materialized view: {} because " +
                                        "`Config.enable_backup_materialized_view=false`", tbl.getName());
                        continue;
                    }
                    if (tbl.isTemporaryTable()) {
                        continue;
                    }

                    if (withOnClause && ((tbl.isOlapTable() && !allTable) || (tbl.isOlapMaterializedView() && !allMV) ||
                                         (tbl.isOlapView() && !allView))) {
                        continue;
                    }

                    if (tableRefs.stream().anyMatch(tableRef -> tableRef.getName().getTbl().equalsIgnoreCase(tbl.getName()))) {
                        continue;
                    }

                    TableName tableName = new TableName(dbName, tbl.getName());
                    TableRef tableRef = new TableRef(tableName, null, null);
                    tableRefs.add(tableRef);
                }
            }

            Map<Long, TableRef> tableIdToTableRefMap = Maps.newHashMap();
            Map<String, MaterializedView> mvNameMVMap = Maps.newHashMap();
            for (TableRef tableRef : tableRefs) {
                analyzeTableRef(tableRef, dbName, database, tblPartsMap, context.getCurrentCatalog(),
                        mvNameMVMap, tableIdToTableRefMap);
                if (tableRef.hasExplicitAlias()) {
                    throw new SemanticException("Can not set alias for table in Backup Stmt: " + tableRef,
                            tableRef.getPos());
                }
            }

            if (!mvNameMVMap.isEmpty()) {
                // check base table existed in the same db.
                for (Map.Entry<String, MaterializedView> e : mvNameMVMap.entrySet()) {
                    MaterializedView mv = e.getValue();
                    for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
                        if (!tableIdToTableRefMap.containsKey(baseTableInfo.getTableId())) {
                            LOG.warn(String.format("Backup/restore materialized view %s's" +
                                    " base table %s is not in the same db with the mv", mv.getName(),
                                    baseTableInfo.getTableId()));
                        }
                    }
                }

                // reorder the tableRefs to ensure ref tables of materialized views are ahead of the materialized view
                Map<String, TableRef> newTableRefs = reorderTableRefsWithMaterializedView(database, tblPartsMap, mvNameMVMap,
                        tableIdToTableRefMap);
                tableRefs.clear();
                tableRefs.addAll(newTableRefs.values());
            } else {
                tableRefs.clear();
                tableRefs.addAll(tblPartsMap.values());
            }

            // analyze and get Function for stmt
            List<FunctionRef> fnRefs = backupStmt.getFnRefs();
            if (!withOnClause || allFunction) /* without `On` or contains `ALL` */ {
                if (!fnRefs.isEmpty()) {
                    fnRefs.stream().forEach(x -> x.analyzeForBackup(database));
                }

                for (Map.Entry<String, List<Function>> entry : database.getNameToFunction().entrySet()) {
                    String fnName = entry.getKey();
                    List<Function> fns = entry.getValue();

                    if (fnRefs.stream().anyMatch(x -> x.getFunctions().get(0)
                                                 .getFunctionName().getFunction().equalsIgnoreCase(fnName))) {
                        continue;
                    }

                    fnRefs.add(new FunctionRef(fns));
                }
            } else {
                backupStmt.getFnRefs().stream().forEach(x -> x.analyzeForBackup(database));
            }

            analyzeBackupProperties(backupStmt);

            return null;
        }

        private void analyzeBackupProperties(BackupStmt backupStmt) {
            Map<String, String> properties = backupStmt.getProperties();
            long timeoutMs = Config.backup_job_default_timeout_ms;
            Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
            Map<String, String> copiedProperties = new HashMap<>();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                String value = next.getValue();
                switch (next.getKey()) {
                    case PROP_TIMEOUT:
                        timeoutMs = setPropTimeout(value, MIN_TIMEOUT_MS);
                        iterator.remove();
                        break;
                    case PROP_TYPE:
                        try {
                            BackupStmt.BackupType type =
                                    BackupStmt.BackupType.valueOf(value.toUpperCase());
                            backupStmt.setType(type);
                        } catch (Exception e) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                    "Invalid backup job type: "
                                            + value);
                        }
                        iterator.remove();
                        break;
                    default:
                        copiedProperties.put(next.getKey(), value);
                        break;
                }
            }

            backupStmt.setTimeoutMs(timeoutMs);
            if (!copiedProperties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unknown backup job properties: " + copiedProperties.keySet());
            }
        }

        private Map<String, TableRef> reorderTableRefsWithMaterializedView(Database database,
                                                                           Map<String, TableRef> tblPartsMap,
                                                                           Map<String, MaterializedView> mvPartsMap,
                                                                           Map<Long, TableRef> tableIdToTableRefMap) {
            Map<String, TableRef> orderedTableNameRefMap = Maps.newLinkedHashMap();
            for (Map.Entry<String, TableRef> e : tblPartsMap.entrySet()) {
                collectTableRefAndDependencies(database, e.getKey(), e.getValue(), mvPartsMap, tableIdToTableRefMap,
                        orderedTableNameRefMap);
            }
            return orderedTableNameRefMap;
        }

        private void collectTableRefAndDependencies(Database database,
                                                    String tableName,
                                                    TableRef tableRef,
                                                    Map<String, MaterializedView> mvPartsMap,
                                                    Map<Long, TableRef> tableIdToTableRefMap,
                                                    Map<String, TableRef> result) {
            // table is already collected.
            if (result.containsKey(tableName)) {
                return;
            }

            // post-order the table ref, first iterate the materialized view's base tables
            if (mvPartsMap.containsKey(tableName)) {
                MaterializedView mv = mvPartsMap.get(tableName);
                for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
                    Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .getDatabase(new ConnectContext(), baseTableInfo);
                    if (dbOpt.isEmpty()) {
                        LOG.warn("database {} do not exist when collect table info for table: {}",
                                baseTableInfo.getDbInfoStr(), tableName);
                        continue;
                    }
                    if (dbOpt.get().getId() != database.getId()) {
                        // if the referred base table is not the same with the current database, skip it.
                        LOG.warn("The referred base table {} 's database is different from the materialized view {}, " +
                                        "skip backup it", baseTableInfo.getTableName(), tableRef.getName());
                        continue;
                    }
                    Optional<Table> baseTableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                    if (baseTableOpt.isEmpty()) {
                        LOG.warn("The referred base table {} is not found in catalog, " +
                                "skip backup it", baseTableInfo.getTableName());
                    }
                    Table baseTable = baseTableOpt.get();
                    if (!tableIdToTableRefMap.containsKey(baseTable.getId())) {
                        // if the table_id->table_ref map not contains the ref base table, skip it
                        LOG.warn("The referred base table {} is not found in the collected table ref map, " +
                                        "skip backup it", baseTable.getName());
                        continue;
                    }
                    collectTableRefAndDependencies(database, baseTable.getName(),
                            tableIdToTableRefMap.get(baseTable.getId()), mvPartsMap, tableIdToTableRefMap, result);
                }
            }

            // then collect the table ref at the last
            result.put(tableName, tableRef);
        }

        @Override
        public Void visitCancelBackupStatement(CancelBackupStmt cancelBackupStmt, ConnectContext context) {
            if (cancelBackupStmt.isExternalCatalog()) {
                return null;
            }
            String dbName = getDbName(cancelBackupStmt.getDbName(), context);
            cancelBackupStmt.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitShowBackupStatement(ShowBackupStmt showBackupStmt, ConnectContext context) {
            String dbName = showBackupStmt.getDbName();
            if (dbName != null) {
                getDatabase(dbName, context);
            }
            return null;
        }

        @Override
        public Void visitRestoreStatement(RestoreStmt restoreStmt, ConnectContext context) {
            List<TableRef> tableRefs = restoreStmt.getTableRefs();
            Set<String> aliasSet = Sets.newHashSet();
            Map<String, TableRef> tblPartsMap = Maps.newTreeMap();
            for (TableRef tableRef : tableRefs) {
                TableName tableName = tableRef.getName();

                if (!tblPartsMap.containsKey(tableName.getTbl())) {
                    tblPartsMap.put(tableName.getTbl(), tableRef);
                } else {
                    throw new SemanticException("Duplicated table: " + tableName.getTbl(), tableRef.getPos());
                }

                aliasSet.add(tableRef.getName().getTbl());
            }

            for (TableRef tblRef : tableRefs) {
                if (tblRef.hasExplicitAlias() && !aliasSet.add(tblRef.getExplicitAlias())) {
                    throw new SemanticException("Duplicated alias name: " + tblRef.getExplicitAlias(), tblRef.getPos());
                }
            }

            tableRefs.clear();
            tableRefs.addAll(tblPartsMap.values());
            Map<String, String> properties = restoreStmt.getProperties();
            Map<String, String> copiedProperties = Maps.newHashMap(properties);
            long timeoutMs = Config.backup_job_default_timeout_ms;
            boolean allowLoad = false;
            int replicationNum = RunMode.defaultReplicationNum();
            String backupTimestamp = null;
            int metaVersion = -1;
            int starrocksMetaVersion = -1;
            Iterator<Map.Entry<String, String>> iterator = copiedProperties.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                String key = next.getKey();
                String value = next.getValue();
                switch (key) {
                    case PROP_TIMEOUT:
                        timeoutMs = setPropTimeout(value, MIN_TIMEOUT_MS);
                        iterator.remove();
                        break;
                    case PROP_ALLOW_LOAD:
                        if ("true".equalsIgnoreCase(value)) {
                            allowLoad = true;
                        } else if ("false".equalsIgnoreCase(value)) {
                            allowLoad = false;
                        } else {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                    "Invalid allow load value: "
                                            + copiedProperties.get(PROP_ALLOW_LOAD));
                        }
                        iterator.remove();
                        break;
                    case PROP_REPLICATION_NUM:
                        replicationNum = parseInt(value);
                        iterator.remove();
                        break;
                    case PROP_BACKUP_TIMESTAMP:
                        backupTimestamp = value;
                        iterator.remove();
                        break;
                    case PROP_META_VERSION:
                        metaVersion = parseInt(value);
                        iterator.remove();
                        break;
                    case PROP_STARROCKS_META_VERSION:
                        starrocksMetaVersion = parseInt(value);
                        iterator.remove();
                        break;
                    default:
                        copiedProperties.put(key, value);
                        break;
                }
            }
            restoreStmt.setTimeoutMs(timeoutMs);
            restoreStmt.setAllowLoad(allowLoad);
            restoreStmt.setReplicationNum(replicationNum);
            restoreStmt.setMetaVersion(metaVersion);
            restoreStmt.setStarrocksMetaVersion(starrocksMetaVersion);
            if (null == backupTimestamp) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Missing " + PROP_BACKUP_TIMESTAMP + " property");
            }

            restoreStmt.setBackupTimestamp(backupTimestamp);
            if (!copiedProperties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unknown restore job properties: " + copiedProperties.keySet());
            }

            return null;
        }

        @Override
        public Void visitShowRestoreStatement(ShowRestoreStmt showRestoreStmt, ConnectContext context) {
            String dbName = showRestoreStmt.getDbName();
            if (dbName != null) {
                getDatabase(dbName, context);
            }
            return null;
        }
    }

    public static String getDbName(String dbName, ConnectContext context) {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
        } else {
            FeNameFormat.checkDbName(dbName);
        }
        return dbName;
    }

    public static Database getDatabase(String dbName, ConnectContext context) {
        Database db = context.getGlobalStateMgr().getLocalMetastore().getDb(dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        return db;
    }

    public static void analyzeLabelAndRepo(String label, String repoName) {
        if (Strings.isNullOrEmpty(label)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_LABEL_NAME, label);
        }

        if (Strings.isNullOrEmpty(repoName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository is empty");
        }

        Repository repo =
                GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
        if (null == repo) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Repository [" + repoName + "] does not exist");
        }
    }

    public static void analyzeTableRef(TableRef tableRef, String dbName, Database db,
                                       Map<String, TableRef> tblPartsMap, String catalog,
                                       Map<String, MaterializedView> tblMaterializedViewMap,
                                       Map<Long, TableRef> tableIdToTableRefMap) {
        TableName tableName = tableRef.getName();
        tableName.setCatalog(catalog);
        tableName.setDb(dbName);

        PartitionNames partitionNames = tableRef.getPartitionNames();
        Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName.getTbl());
        if (null == tbl) {
            throw new SemanticException(ErrorCode.ERR_WRONG_TABLE_NAME.formatErrorMsg(tableName.getTbl()));
        }

        String alias = tableRef.getAlias();
        if (!tableName.getTbl().equalsIgnoreCase(alias)) {
            Table tblAlias = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), alias);
            if (tblAlias != null && tbl != tblAlias) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "table [" + alias + "] existed");
            }
        }

        tableIdToTableRefMap.put(tbl.getId(), tableRef);
        if (tbl.isMaterializedView()) {
            MaterializedView mv = (MaterializedView) tbl;
            // check its base tables exist for materialized view.
            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                Optional<Table> refTableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (refTableOpt.isEmpty()) {
                    throw new SemanticException(String.format("Base table %s doest not existed in materialized view %s when " +
                            "backup/restore", baseTableInfo.getTableName(), mv.getName()));
                }
                tblMaterializedViewMap.put(mv.getName(), mv);
            }
        }

        if (partitionNames != null) {
            if (!tbl.isNativeTableOrMaterializedView()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName.getTbl());
            }
            OlapTable olapTbl = (OlapTable) tbl;
            for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                Partition partition = olapTbl.getPartition(partName);
                if (partition == null) {
                    throw new SemanticException(
                            "partition[" + partName + "] does not exist  in table" + tableName.getTbl(),
                            tableRef.getPartitionNames().getPos());
                }
            }
        }

        if (!tblPartsMap.containsKey(tableName.getTbl())) {
            tblPartsMap.put(tableName.getTbl(), tableRef);
        } else {
            throw new SemanticException("Duplicated table: " + tableName.getTbl(), tableName.getPos());
        }
    }

    public static long setPropTimeout(String value, long defaultTimeout) {
        long timeoutMs = Config.backup_job_default_timeout_ms;
        try {
            timeoutMs = Long.parseLong(value);
        } catch (NumberFormatException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Invalid timeout format: "
                            + value);
        }

        if (timeoutMs * 1000 < defaultTimeout) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "timeout must be at least 10 min");
        }
        return timeoutMs * 1000;
    }

    public static int parseInt(String value) {
        int res = 0;
        try {
            res = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Invalid meta version format: "
                            + value);
        }
        return res;
    }

}
