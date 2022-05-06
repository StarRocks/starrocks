// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.analysis.AdminShowReplicaStatusStmt;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Replica;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;
import java.util.Map;

public class AdminStmtAnalyzer {
    public static void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminSetConfigStmt, session);
    }

    public static void analyze(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminSetReplicaStatusStmt, session);
    }

    public static void analyze(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminShowConfigStmt, session);
    }

    public static void analyze(AdminShowReplicaDistributionStmt adminShowReplicaDistributionStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminShowReplicaDistributionStmt, session);
    }

    public static void analyze(AdminShowReplicaStatusStmt adminShowReplicaStatusStmt, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().visit(adminShowReplicaStatusStmt, session);
    }

    static class AdminStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            visit(adminSetConfigStmt, session);
        }

        public void analyze(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt, ConnectContext session) {
            visit(adminSetReplicaStatusStmt, session);
        }

        public void analyze(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
            visit(adminShowConfigStmt, session);
        }

        public void analyze(AdminShowReplicaDistributionStmt adminShowReplicaDistributionStmt, ConnectContext session) {
            visit(adminShowReplicaDistributionStmt, session);
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt adminSetReplicaStatusStmt,
                                                        ConnectContext session) {
            long tabletId = -1;
            long backendId = -1;
            Replica.ReplicaStatus status = null;
            Map<String, String> properties = adminSetReplicaStatusStmt.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();

                if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.TABLET_ID)) {
                    try {
                        tabletId = Long.valueOf(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException("Invalid tablet id format: " + val);
                    }
                } else if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.BACKEND_ID)) {
                    try {
                        backendId = Long.valueOf(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException("Invalid backend id format: " + val);
                    }
                } else if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.STATUS)) {
                    status = Replica.ReplicaStatus.valueOf(val.toUpperCase());
                    if (status != Replica.ReplicaStatus.BAD && status != Replica.ReplicaStatus.OK) {
                        throw new SemanticException("Do not support setting replica status as " + val);
                    }
                } else {
                    throw new SemanticException("Unknown property: " + key);
                }
            }

            if (tabletId == -1 || backendId == -1 || status == null) {
                throw new SemanticException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
            }
            return null;
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

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt adminShowReplicaStatusStmt,
                                                        ConnectContext session) {
            String dbName = adminShowReplicaStatusStmt.getDbName();
            String tblName = adminShowReplicaStatusStmt.getTblName();
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

            List<String> partitions = adminShowReplicaStatusStmt.getPartitions();

            if (!analyzeWhere(adminShowReplicaStatusStmt)) {
                throw new SemanticException(
                        "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
            }
            return null;
        }

        private boolean analyzeWhere(AdminShowReplicaStatusStmt adminShowReplicaStatusStmt) {
            Expr where = adminShowReplicaStatusStmt.getWhere();
            Replica.ReplicaStatus statusFilter = null;

            // analyze where clause if not null
            if (where == null) {
                return true;
            }

            if (!(where instanceof BinaryPredicate)) {
                return false;
            }

            BinaryPredicate binaryPredicate = (BinaryPredicate) where;
            BinaryPredicate.Operator op = binaryPredicate.getOp();
            if (op != BinaryPredicate.Operator.EQ && op != BinaryPredicate.Operator.NE) {
                return false;
            }

            Expr leftChild = binaryPredicate.getChild(0);
            if (!(leftChild instanceof SlotRef)) {
                return false;
            }

            String leftKey = ((SlotRef) leftChild).getColumnName();
            if (!leftKey.equalsIgnoreCase("status")) {
                return false;
            }

            Expr rightChild = binaryPredicate.getChild(1);
            if (!(rightChild instanceof StringLiteral)) {
                return false;
            }

            try {
                statusFilter = Replica.ReplicaStatus.valueOf(((StringLiteral) rightChild).getStringValue().toUpperCase());
            } catch (Exception e) {
                return false;
            }

            if (statusFilter == null) {
                return false;
            }

            return true;
        }
    }
}
