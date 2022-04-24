// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class BackupAnalyzer {
    private static final String PROP_TYPE = "type";

    public enum BackupType {
        INCREMENTAL, FULL
    }

    private static BackupStmt.BackupType type = BackupStmt.BackupType.FULL;

    public static void analyze(BackupStmt backupStmt, ConnectContext session) {
        new BackupStmtAnalyzerVisitor().visit(backupStmt, session);
    }

    static class BackupStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(BackupStmt stmt, ConnectContext session) {
            visit(stmt, session);
        }

        @Override
        public Void visitBackupStatement(BackupStmt backupStmt, ConnectContext session) {
            String db = backupStmt.getDbName();
            backupStmt.setDb(MetaUtils.getFullDatabaseName(db, session));
            backupStmt.setClusterName(session.getClusterName());
            List<TableRef> tblRefs = backupStmt.getTblRefs();
            for (TableRef tableRef : tblRefs) {
                TableName tableName = tableRef.getName();
                MetaUtils.normalizationTableName(session, tableName);
                MetaUtils.getStarRocks(session, tableName);
                Table table = MetaUtils.getStarRocksTable(session, tableName);

                if (!(table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS)) {
                    throw unsupportedException("only support updating primary key table");
                }
                // tbl refs can not set alias in backup
                if (tableRef.hasExplicitAlias()) {
                    throw new SemanticException("Can not set alias for table in Backup Stmt: " + tableRef);
                }
            }

            Map<String, String> copiedProperties = Maps.newHashMap(backupStmt.getProperties());
            // type
            if (copiedProperties.containsKey(PROP_TYPE)) {
                try {
                    type = BackupStmt.BackupType.valueOf(copiedProperties.get(PROP_TYPE).toUpperCase());
                } catch (Exception e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid backup job type: "
                                    + copiedProperties.get(PROP_TYPE));
                }
                copiedProperties.remove(PROP_TYPE);
            }

            if (!copiedProperties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unknown backup job properties: " + copiedProperties.keySet());
            }
            return null;
        }
    }
}

