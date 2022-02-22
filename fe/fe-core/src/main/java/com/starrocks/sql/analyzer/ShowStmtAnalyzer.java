// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class ShowStmtAnalyzer {
    private final ConnectContext session;

    public ShowStmtAnalyzer(ConnectContext session) {
        this.session = session;
    }

    public void analyze(ShowStmt stmt) {
        new ShowStmtAnalyzerVisitor(this.session).visit(stmt);
    }

    static class ShowStmtAnalyzerVisitor extends AstVisitor<Void, Void> {
        private final ConnectContext session;

        public ShowStmtAnalyzerVisitor(ConnectContext session) {
            this.session = session;
        }

        @Override
        public Void visitShowTableStmt(ShowTableStmt node, Void context) {
            String db = node.getDb();

            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
                db = ClusterNamespace.getFullName(session.getClusterName(), db);
                if (Strings.isNullOrEmpty(db)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            } else {
                db = ClusterNamespace.getFullName(session.getClusterName(), db);
            }

            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowDatabasesStmt(ShowDbStmt node, Void context) {
            return null;
        }
    }
}
