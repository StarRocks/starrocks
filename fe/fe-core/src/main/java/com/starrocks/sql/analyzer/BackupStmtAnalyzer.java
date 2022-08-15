// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.PartitionNames;
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

    public static void analyze(BackupStmt backupStmt, ConnectContext session) {
        new BackupStmtAnalyzerVisitor().analyze(backupStmt, session);
    }

    static class BackupStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private static final String PROP_TIMEOUT = "timeout";
        private static final long MIN_TIMEOUT_MS = 600_000L; // 10 min
        private static final String PROP_TYPE = "type";

        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitBackupStmt(BackupStmt statement, ConnectContext context) {
            String label = statement.getLabel();
            if (Strings.isNullOrEmpty(label)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_LABEL_NAME, label);
            }

            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
            } else {
                try {
                    FeNameFormat.checkDbName(dbName);
                } catch (AnalysisException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
                }
            }

            String catalog = context.getCurrentCatalog();

            String currentCatalog = context.getCurrentCatalog();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(currentCatalog, dbName);
            if (null == db) {
                throw new SemanticException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(dbName));
            }

            String repoName = statement.getRepoName();
            if (Strings.isNullOrEmpty(repoName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not empty");
            }

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (null == repo) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            Map<String, TableRef> tblPartsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            List<TableRef> tableRefs = statement.getTableRefs();
            for (TableRef tableRef : tableRefs) {
                TableName tableName = tableRef.getName();
                PartitionNames partitionNames = tableRef.getPartitionNames();
                Table tbl = db.getTable(tableName.getTbl());
                if (null == tbl) {
                    throw new SemanticException(ErrorCode.ERR_WRONG_TABLE_NAME.formatErrorMsg(tableName.getTbl()));
                }

                if (!Strings.isNullOrEmpty(tableName.getDb())) {
                    throw new SemanticException("Cannot specify database name on backup objects: "
                            + tableName.getTbl() + ". Sepcify database name before label");
                }

                tableName.setCatalog(catalog);
                tableName.setDb(dbName);
                if (partitionNames != null) {
                    if (tbl.getType() != Table.TableType.OLAP) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName.getTbl());
                    }

                    OlapTable olapTbl = (OlapTable) tbl;
                    for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            throw new SemanticException(
                                    "partition[" + partName + "] does not exist in table" + tableName.getTbl());
                        }
                    }
                }

                if (!tblPartsMap.containsKey(tableName.getTbl())) {
                    tblPartsMap.put(tableName.getTbl(), tableRef);
                } else {
                    throw new SemanticException("Duplicated restore table: " + tableName.getTbl());
                }

                try {
                    CatalogUtils.checkIsLakeTable(dbName, tableName.getTbl());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }

                if (tableRef.hasExplicitAlias()) {
                    throw new SemanticException("Can not set alias for table in Backup Stmt: " + tableRef);
                }
            }

            tableRefs.clear();
            for (TableRef tableRef : tblPartsMap.values()) {
                statement.getTableRefs().add(tableRef);
            }

            Map<String, String> properties = statement.getProperties();
            long timeoutMs = Config.backup_job_default_timeout_ms;
            Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
            Map<String, String> copiedProperties = new HashMap<>();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                switch (next.getKey()) {
                    case PROP_TIMEOUT:
                        try {
                            timeoutMs = Long.parseLong(next.getValue());
                        } catch (NumberFormatException e) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                    "Invalid timeout format: "
                                            + next.getValue());
                        }

                        if (timeoutMs * 1000 < MIN_TIMEOUT_MS) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                    "timeout must be at least 10 min");
                        }

                        timeoutMs = timeoutMs * 1000;
                        iterator.remove();
                        break;
                    case PROP_TYPE:
                        try {
                            BackupStmt.BackupType type =
                                    BackupStmt.BackupType.valueOf(next.getValue().toUpperCase());
                            statement.setType(type);
                        } catch (Exception e) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                    "Invalid backup job type: "
                                            + next.getValue());
                        }
                        iterator.remove();
                        break;
                    default:
                        copiedProperties.put(next.getValue(), next.getValue());
                        break;
                }
            }

            statement.setTimeoutMs(timeoutMs);
            if (!copiedProperties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unknown backup job properties: " + copiedProperties.keySet());
            }

            return null;
        }

    }

}
