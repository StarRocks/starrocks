// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.ShowBackupStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BackupStmtAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext session) {
        new BackupStmtAnalyzerVisitor().analyze(statement, session);
    }

    public static class BackupStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private static final String PROP_TIMEOUT = "timeout";
        private static final long MIN_TIMEOUT_MS = 600_000L; // 10 min
        private static final String PROP_TYPE = "type";

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitBackupStmt(BackupStmt backupStmt, ConnectContext context) {
            String dbName = getDbName(backupStmt.getDbName(), context);
            Database database = getDatabase(dbName, context);
            analyzeLabelAndRepo(backupStmt.getLabel(), backupStmt.getRepoName());
            Map<String, TableRef> tblPartsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            List<TableRef> tableRefs = backupStmt.getTableRefs();
            for (TableRef tableRef : tableRefs) {
                analyzeTableRef(tableRef, dbName, database, tblPartsMap, context.getCurrentCatalog());
                if (tableRef.hasExplicitAlias()) {
                    throw new SemanticException("Can not set alias for table in Backup Stmt: " + tableRef);
                }
            }

            tableRefs.clear();
            tableRefs.addAll(tblPartsMap.values());
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

            return null;
        }

        @Override
        public Void visitShowBackupStmt(ShowBackupStmt showBackupStmt, ConnectContext context) {
            String dbName = getDbName(showBackupStmt.getDbName(), context);
            showBackupStmt.setDbName(dbName);
            getDatabase(dbName, context);
            return null;
        }
    }

    public static String getDbName(String dbName, ConnectContext context) {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
        } else {
            try {
                FeNameFormat.checkDbName(dbName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
            }
        }
        return dbName;
    }

    public static Database getDatabase(String dbName, ConnectContext context) {
        Database db = context.getGlobalStateMgr().getDb(dbName);
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
                                       Map<String, TableRef> tblPartsMap, String catalog) {
        TableName tableName = tableRef.getName();
        tableName.setCatalog(catalog);
        tableName.setDb(dbName);
        PartitionNames partitionNames = tableRef.getPartitionNames();
        Table tbl = db.getTable(tableName.getTbl());
        if (null == tbl) {
            throw new SemanticException(ErrorCode.ERR_WRONG_TABLE_NAME.formatErrorMsg(tableName.getTbl()));
        }

        String alias = tableRef.getAlias();
        if (!tableName.getTbl().equalsIgnoreCase(alias)) {
            Table tblAlias = db.getTable(alias);
            if (tblAlias != null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "table [" + alias + "] existed");
            }
        }

        if (partitionNames != null) {
            if (tbl.getType() != Table.TableType.OLAP) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName.getTbl());
            }

            OlapTable olapTbl = (OlapTable) tbl;
            for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                Partition partition = olapTbl.getPartition(partName);
                if (partition == null) {
                    throw new SemanticException(
                            "partition[" + partName + "] does not exist  in table" + tableName.getTbl());
                }
            }
        }

        if (!tblPartsMap.containsKey(tableName.getTbl())) {
            tblPartsMap.put(tableName.getTbl(), tableRef);
        } else {
            throw new SemanticException("Duplicated table: " + tableName.getTbl());
        }

        try {
            CatalogUtils.checkIsLakeTable(dbName, tableName.getTbl());
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
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

}
