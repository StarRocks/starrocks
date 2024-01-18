// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Replica;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;
import java.util.Map;

public class AdminStmtAnalyzer {
    public static final long DEFAULT_PRIORITY_REPAIR_TIMEOUT_SEC = 4 * 3600L;

    public static void analyze(StatementBase statementBase, ConnectContext session) {
        new AdminStmtAnalyzerVisitor().analyze(statementBase, session);
    }

    static class AdminStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statementBase, ConnectContext session) {
            visit(statementBase, session);
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
                        tabletId = Long.parseLong(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException("Invalid tablet id format: " + val);
                    }
                } else if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.BACKEND_ID)) {
                    try {
                        backendId = Long.parseLong(val);
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
            adminSetReplicaStatusStmt.setTabletId(tabletId);
            adminSetReplicaStatusStmt.setBackendId(backendId);
            adminSetReplicaStatusStmt.setStatus(status);
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
                    dbName = session.getDatabase();
                }
            }
            adminShowReplicaDistributionStmt.setDbName(dbName);

            try {
                CatalogUtils.checkIsLakeTable(dbName, tblName);
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
                    dbName = session.getDatabase();
                }
            }
            adminShowReplicaStatusStmt.setDbName(dbName);

            try {
                CatalogUtils.checkIsLakeTable(dbName, tblName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            List<String> partitions = Lists.newArrayList();
            PartitionNames partitionNames = adminShowReplicaStatusStmt.getTblRef().getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException("Do not support showing replica status of temporary partitions");
                }
                partitions.addAll(partitionNames.getPartitionNames());
                adminShowReplicaStatusStmt.setPartitions(partitions);
            }

            if (!analyzeWhere(adminShowReplicaStatusStmt)) {
                throw new SemanticException(
                        "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
            }
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt stmt, ConnectContext session) {
            if (stmt.getConfigs().size() != 1) {
                throw new SemanticException("config parameter size is not equal to 1");
            }
            if (stmt.getType() != AdminSetConfigStmt.ConfigType.FRONTEND) {
                throw new SemanticException("Only support setting Frontend configs now");
            }
            return null;
        }

        @Override
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt adminRepairTableStmt, ConnectContext session) {
            String dbName = adminRepairTableStmt.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminRepairTableStmt.setDbName(dbName);
            PartitionNames partitionNames = adminRepairTableStmt.getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException("Do not support repair temporary partitions");
                }
                adminRepairTableStmt.setPartitions(partitionNames);
            }
            adminRepairTableStmt.setTimeoutSec(DEFAULT_PRIORITY_REPAIR_TIMEOUT_SEC); // default 4 hours
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt adminCancelRepairTableStmt,
                                                         ConnectContext session) {
            String dbName = adminCancelRepairTableStmt.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminCancelRepairTableStmt.setDbName(dbName);
            PartitionNames partitionNames = adminCancelRepairTableStmt.getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException("Do not support (cancel)repair temporary partitions");
                }
                adminCancelRepairTableStmt.setPartitions(partitionNames);
            }
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext session) {
            Map<String, String> properties = statement.getProperties();
            String typeStr = PropertyAnalyzer.analyzeType(properties);
            if (typeStr == null) {
                throw new SemanticException("Should specify 'type' property");
            }
            try {
                statement.setType(AdminCheckTabletsStmt.CheckType.getTypeFromString(typeStr));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            if (properties != null && !properties.isEmpty()) {
                throw new SemanticException("Unknown properties: " + properties.keySet());
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
            adminShowReplicaStatusStmt.setOp(op);

            Expr leftChild = binaryPredicate.getChild(0);
            Expr rightChild = binaryPredicate.getChild(1);
            String leftKey = ((SlotRef) leftChild).getColumnName();
            if (!(rightChild instanceof StringLiteral) || !leftKey.equalsIgnoreCase("status")) {
                return false;
            }

            try {
                statusFilter = Replica.ReplicaStatus.valueOf(((StringLiteral) rightChild).getStringValue().toUpperCase());
                adminShowReplicaStatusStmt.setStatusFilter(statusFilter);
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }
}
