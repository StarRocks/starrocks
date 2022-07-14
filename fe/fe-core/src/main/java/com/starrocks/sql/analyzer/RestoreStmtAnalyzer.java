// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.RestoreStmt;
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

public class RestoreStmtAnalyzer {

    public static void analyze(RestoreStmt restoreStmt, ConnectContext session) {
        new RestoreStmtAnalyzerVisitor().analyze(restoreStmt, session);
    }

    public static class RestoreStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitRestoreStmt(RestoreStmt restoreStmt, ConnectContext context) {
            String label = restoreStmt.getLabel();
            if (Strings.isNullOrEmpty(label)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_LABEL_NAME, label);
            }

            String dbName = restoreStmt.getDbName();
            List<TableRef> tableRefs = restoreStmt.getTableRefs();
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

            String repoName = restoreStmt.getRepoName();
            if (Strings.isNullOrEmpty(repoName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_LABEL_NAME, label);
            }

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository [" + repoName + "] does not exist");
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
                                        "partition[" + partName + " does not exist  in table" + tableName.getTbl());
                            }
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }

            try {
                restoreStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }

}
