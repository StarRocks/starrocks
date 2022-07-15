// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ShowBackupStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Database;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowBackupAnalyzer {

    public static void analyze(ShowBackupStmt showBackupStmt, ConnectContext session) {
        new ShowBackupAnalyzerVisitor().analyze(showBackupStmt, session);
    }

    public static class ShowBackupAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitShowBackupStmt(ShowBackupStmt showBackupStmt, ConnectContext context) {
            String dbName = showBackupStmt.getDbName();
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

            try {
                showBackupStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }
}
