// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class DropStmtAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new DropStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class DropStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext context) {
            String db = statement.getDbName();
            db = getFullDatabaseName(db, context);
            statement.setDb(db);
            String tableName = statement.getTableName();
            if (Strings.isNullOrEmpty(tableName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            // check access
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), db,
                    tableName, PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }
        String getFullDatabaseName(String db, ConnectContext session) {
            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
                db = ClusterNamespace.getFullName(session.getClusterName(), db);
                if (Strings.isNullOrEmpty(db)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            } else {
                db = ClusterNamespace.getFullName(session.getClusterName(), db);
            }
            return db;
        }
    }

}
