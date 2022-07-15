// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ShowRestoreStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Database;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowRestoreAnalyzer {

    public static void analyze(ShowRestoreStmt showRestoreStmt, ConnectContext session) {
        new ShowRestoreAnalyzerVisitor().analyze(showRestoreStmt, session);
    }

    public static class ShowRestoreAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);

        }

        @Override
        public Void visitShowRestoreStmt(ShowRestoreStmt showRestoreStmt, ConnectContext context) {
            String dbName = showRestoreStmt.getDbName();
            if (!Strings.isNullOrEmpty(dbName)) {
                dbName = context.getClusterName() + ":" + dbName;
                String currentCatalog = context.getCurrentCatalog();
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(currentCatalog, dbName);
                if (db == null) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }
            }

            try {
                showRestoreStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

    }
}
