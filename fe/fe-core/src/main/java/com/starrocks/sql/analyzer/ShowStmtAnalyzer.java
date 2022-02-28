// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
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
            db = getFullDatabaseName(db);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowVariablesStmt(ShowVariablesStmt node, Void context) {
            if (node.getType() == null) {
                node.setType(SetType.DEFAULT);
            }
            return null;
        }

        @Override
        public Void visitShowColumnStmt(ShowColumnStmt node, Void context) {
            node.init();
            String db = node.getTableName().getDb();
            db = getFullDatabaseName(db);
            node.getTableName().setDb(db);
            return null;
        }

        @Override
        public Void visitShowTableStatusStmt(ShowTableStatusStmt node, Void context) {
            String db = node.getDb();
            db = getFullDatabaseName(db);
            node.setDb(db);
            return null;
        }

        String getFullDatabaseName(String db) {
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
