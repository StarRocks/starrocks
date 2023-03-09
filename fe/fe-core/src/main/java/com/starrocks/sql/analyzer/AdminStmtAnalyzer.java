// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.base.Enums;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Replica;
import com.starrocks.common.AnalysisException;
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
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AdminStmtAnalyzer {
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
            NodePosition pos = NodePosition.ZERO;
            for (Property property : adminSetReplicaStatusStmt.getProperties().getPropertySet()) {
                String key = property.getKey();
                String val = property.getValue();
                pos = property.getPos();
                if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.TABLET_ID)) {
                    try {
                        tabletId = Long.parseLong(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException(PARSER_ERROR_MSG.invalidIdFormat("table", val), pos);
                    }
                } else if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.BACKEND_ID)) {
                    try {
                        backendId = Long.parseLong(val);
                    } catch (NumberFormatException e) {
                        throw new SemanticException(PARSER_ERROR_MSG.invalidIdFormat("backend", val), pos);
                    }
                } else if (key.equalsIgnoreCase(AdminSetReplicaStatusStmt.STATUS)) {
                    status = Enums.getIfPresent(Replica.ReplicaStatus.class, val.toUpperCase()).orNull();
                    if (status != Replica.ReplicaStatus.BAD && status != Replica.ReplicaStatus.OK) {
                        throw new SemanticException(PARSER_ERROR_MSG.invalidPropertyValue("replica status", val), pos);
                    }
                } else {
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedProps(key), pos);
                }
            }

            if (tabletId == -1 || backendId == -1 || status == null) {
                throw new SemanticException(PARSER_ERROR_MSG.missingProps("TABLET_ID, BACKEND_ID and STATUS"), pos);
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
            NodePosition pos = adminShowReplicaDistributionStmt.getPos();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noDbSelected(), pos);
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminShowReplicaDistributionStmt.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt adminShowReplicaStatusStmt,
                                                         ConnectContext session) {
            String dbName = adminShowReplicaStatusStmt.getDbName();
            String tblName = adminShowReplicaStatusStmt.getTblName();
            NodePosition pos = adminShowReplicaStatusStmt.getPos();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noDbSelected(), pos);
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminShowReplicaStatusStmt.setDbName(dbName);

            try {
                CatalogUtils.checkIsLakeTable(dbName, tblName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage(), pos);
            }

            List<String> partitions = Lists.newArrayList();
            PartitionNames partitionNames = adminShowReplicaStatusStmt.getTblRef().getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedOpWithInfo("temporary partitions"), pos);
                }
                partitions.addAll(partitionNames.getPartitionNames());
                adminShowReplicaStatusStmt.setPartitions(partitions);
            }

            if (!analyzeWhere(adminShowReplicaStatusStmt)) {
                Expr where = adminShowReplicaStatusStmt.getWhere();
                throw new SemanticException(PARSER_ERROR_MSG.invalidWhereExpr("status =|!= " +
                        "'OK'|'DEAD'|'VERSION_ERROR'|'SCHEMA_ERROR'|'MISSING'"),
                        where.getPos());
            }
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt stmt, ConnectContext session) {
            if (stmt.getType() != AdminSetConfigStmt.ConfigType.FRONTEND) {
                throw new SemanticException("Only support setting Frontend configs now", stmt.getPos());
            }
            return null;
        }

        @Override
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt adminRepairTableStmt, ConnectContext session) {
            String dbName = adminRepairTableStmt.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noDbSelected());
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminRepairTableStmt.setDbName(dbName);
            PartitionNames partitionNames = adminRepairTableStmt.getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedOpWithInfo("temp partitions"),
                            partitionNames.getPos());
                }
                adminRepairTableStmt.setPartitions(partitionNames);
            }
            adminRepairTableStmt.setTimeoutSec(4 * 3600L); // default 4 hours
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt adminCancelRepairTableStmt,
                                                         ConnectContext session) {
            String dbName = adminCancelRepairTableStmt.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                if (Strings.isNullOrEmpty(session.getDatabase())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noDbSelected(), adminCancelRepairTableStmt.getPos());
                } else {
                    dbName = session.getDatabase();
                }
            }
            adminCancelRepairTableStmt.setDbName(dbName);
            PartitionNames partitionNames = adminCancelRepairTableStmt.getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedOpWithInfo("temp partitions"),
                            partitionNames.getPos());
                }
                adminCancelRepairTableStmt.setPartitions(partitionNames);
            }
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext session) {
            Property property = statement.getProperty();
            NodePosition pos = property.getPos();
            String typeStr = PropertyAnalyzer.analyzeType(property);
            if (typeStr == null) {
                throw new SemanticException(PARSER_ERROR_MSG.missingProps("type"), pos);
            }
            AdminCheckTabletsStmt.CheckType checkType = Enums.getIfPresent(
                    AdminCheckTabletsStmt.CheckType.class, typeStr.toUpperCase())
                    .orNull();
            if (checkType == null) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidPropertyValue("type", typeStr), pos);
            } else {
                statement.setType(checkType);
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
            statusFilter = Enums.getIfPresent(Replica.ReplicaStatus.class,
                    ((StringLiteral) rightChild).getStringValue().toUpperCase())
                    .orNull();

            return statusFilter != null;
        }
    }
}
