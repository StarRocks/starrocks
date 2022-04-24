// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

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
                checkProperties(adminSetReplicaStatusStmt);
            } catch (SemanticException e) {
                throw e;
            }
            return null;
        }

        private void checkProperties(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt) {
            long tabletId = -1;
            long backendId = -1;
            ReplicaStatus status = null;
            Map<String, String> properties = adminSetReplicaStatusStmt.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();

                if (key.equalsIgnoreCase("tablet_id")) {
                    try {
                        tabletId = Long.valueOf(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException("Invalid tablet id format: " + val);
                    }
                } else if (key.equalsIgnoreCase("backend_id")) {
                    try {
                        backendId = Long.valueOf(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException("Invalid backend id format: " + val);
                    }
                } else if (key.equalsIgnoreCase("status")) {
                    status = ReplicaStatus.valueOf(val.toUpperCase());
                    if (status != ReplicaStatus.BAD && status != ReplicaStatus.OK) {
                        throw new SemanticException("Do not support setting replica status as " + val);
                    }
                } else {
                    throw new SemanticException("Unknown property: " + key);
                }
            }

            if (tabletId == -1 || backendId == -1 || status == null) {
                throw new SemanticException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
            }
        }
    }
}
