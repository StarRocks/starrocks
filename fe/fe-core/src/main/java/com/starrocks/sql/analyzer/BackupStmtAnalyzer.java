// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;

public class BackupStmtAnalyzer {

    public static void analyze(BackupStmt backupStmt, ConnectContext session) {
        new BackupStmtAnalyzerVisitor().analyze(backupStmt, session);
    }

    public static class BackupStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
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
            List<TableRef> tableRefs = statement.getTableRefs();
            if (tableRefs == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, "");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
            } else {
                dbName = context.getClusterName() + ":" + dbName;
            }

            String currentCatalog = context.getCurrentCatalog();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(currentCatalog, dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }

            String repoName = statement.getRepoName();
            if (Strings.isNullOrEmpty(repoName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not empty");
            }

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            db.readLock();
            try {
                for (TableRef tableRef : tableRefs) {
                    TableName tableName = tableRef.getName();
                    MetaUtils.normalizationTableName(context, tableName);
                    PartitionNames partitionNames = tableRef.getPartitionNames();
                    Table tbl = db.getTable(tableName.getTbl());
                    if (tbl == null) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName.getTbl());
                    }
                    tableName.setDb(null);
                    if (partitionNames != null) {
                        if (tbl.getType() != Table.TableType.OLAP) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName.getTbl());
                        }

                        OlapTable olapTbl = (OlapTable) tbl;
                        for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                            Partition partition = olapTbl.getPartition(partName);
                            if (partition == null) {
                                throw new SemanticException(
                                        "partition[" + partName + " does not exist  in table" + tableName.getTbl());
                            }
                        }
                    }
                }

            } finally {
                db.readUnlock();
            }

            try {
                statement.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }

}
