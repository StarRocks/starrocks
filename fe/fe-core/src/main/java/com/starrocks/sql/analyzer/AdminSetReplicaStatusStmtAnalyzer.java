// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.Analyzer;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class AdminSetReplicaStatusStmtAnalyzer {
    public static void analyze(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt, ConnectContext session) {
        new AdminSetStmtAnalyzerVisitor().visit(adminSetReplicaStatusStmt, session);
    }

    static class AdminSetStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt, ConnectContext session) {
            visit(adminSetReplicaStatusStmt, session);
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt,
                                                        ConnectContext session) {
            try {
                adminSetReplicaStatusStmt.analyze(new Analyzer(session.getCatalog(), session));
            } catch (UserException e) {
                throw new SemanticException(e.getMessage(), e);
            }
            return null;
        }
    }
}
