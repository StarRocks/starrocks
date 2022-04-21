// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
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
                // check auth
                if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
                }
                adminSetReplicaStatusStmt.checkProperties();
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage(), e);
            }
            return null;
        }
    }
}
