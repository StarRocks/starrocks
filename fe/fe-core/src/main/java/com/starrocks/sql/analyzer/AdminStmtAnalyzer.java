// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class AdminStmtAnalyzer {
    public static void analyze(AdminShowReplicaDistributionStmt adminShowReplicaDistributionStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminShowReplicaDistributionStmt, session);
    }

    static class AdminStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminShowReplicaDistributionStmt adminShowReplicaDistributionStmt, ConnectContext session) {
            visit(adminShowReplicaDistributionStmt, session);
        }

        @Override
        public Void visitAdminShowReplicaDistributionStatement(
                AdminShowReplicaDistributionStmt adminShowReplicaDistributionStmt,
                ConnectContext session) {
            String dbName = adminShowReplicaDistributionStmt.getDbName();
            String tblName = adminShowReplicaDistributionStmt.getTblName();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                } else {
                    dbName = ClusterNamespace.getFullName(session.getClusterName(), session.getDatabase());
                }
            }

            try {
                CatalogUtils.checkOlapTableHasStarOSPartition(dbName, tblName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }
    }
}
