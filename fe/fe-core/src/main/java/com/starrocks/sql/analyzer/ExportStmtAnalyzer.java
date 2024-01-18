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

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.proc.ExportProcNode;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.ExportJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


// [CANCEL | SHOW] Export Statement Analyzer
// ExportStmtAnalyzer
public class ExportStmtAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ExportStmtAnalyzer.ExportAnalyzerVisitor().visit(stmt, session);
    }


    static class ExportAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
            GlobalStateMgr mgr = context.getGlobalStateMgr();
            TableName tableName = statement.getTableRef().getName();
            // make sure catalog, db, table
            MetaUtils.normalizationTableName(context, tableName);
            Table table = MetaUtils.getTable(context, tableName);
            if (table.getType() == Table.TableType.OLAP &&
                    (((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE ||
                            ((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE_WITH_LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
            }
            statement.setTblName(tableName);
            PartitionNames partitionNames = statement.getTableRef().getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support exporting temporary partitions");
                }
                statement.setPartitions(partitionNames.getPartitionNames());
            }

            // check db, table && partitions && columns whether exist
            // check path is valid
            // generate file name prefix
            analyzeDbName(tableName.getDb(), context);
            statement.checkTable(mgr);
            statement.checkPath();

            // check broker whether exist
            BrokerDesc brokerDesc = statement.getBrokerDesc();
            if (brokerDesc == null) {
                throw new SemanticException("broker is not provided");
            }

            if (brokerDesc.hasBroker()) {
                if (!mgr.getBrokerMgr().containsBroker(brokerDesc.getName())) {
                    throw new SemanticException("broker " + brokerDesc.getName() + " does not exist", brokerDesc.getPos());
                }

                FsBroker broker = mgr.getBrokerMgr().getAnyBroker(brokerDesc.getName());
                if (broker == null) {
                    throw new SemanticException("failed to get alive broker", brokerDesc.getPos());
                }
            }
            // check properties
            try {
                statement.checkProperties(statement.getProperties());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            SemanticException exception = new SemanticException(
                    "Where clause should look like: queryid = \"your_query_id\"");
            Expr whereClause = statement.getWhereClause();
            if (whereClause == null) {
                throw exception;
            }
            // analyze where
            checkPredicateType(whereClause, exception);

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                throw exception;
            }
            if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("queryid")) {
                throw exception;
            }
            // right child with query id
            statement.setQueryId(analyzeQueryID(whereClause, exception));
            return null;
        }

        @Override
        public Void visitShowExportStatement(ShowExportStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            // analyze where clause if not null
            Expr whereExpr = statement.getWhereClause();
            if (whereExpr != null) {
                boolean hasJobId = false;
                boolean hasState = false;
                boolean hasQueryId = false;
                SemanticException exception = new SemanticException(
                        "Where clause should look like : queryid = \"your_query_id\" " +
                                "or STATE = \"PENDING|EXPORTING|FINISHED|CANCELLED\"");
                checkPredicateType(whereExpr, exception);

                // left child
                if (!(whereExpr.getChild(0) instanceof SlotRef)) {
                    throw exception;
                }
                String leftKey = ((SlotRef) whereExpr.getChild(0)).getColumnName();
                if (leftKey.equalsIgnoreCase("id")) {
                    hasJobId = true;
                } else if (leftKey.equalsIgnoreCase("state")) {
                    hasState = true;
                } else if (leftKey.equalsIgnoreCase("queryid")) {
                    hasQueryId = true;
                } else {
                    throw exception;
                }

                // right child
                if (hasState) {
                    if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                        throw exception;
                    }

                    String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
                    if (Strings.isNullOrEmpty(value)) {
                        throw exception;
                    }

                    statement.setStateValue(value.toUpperCase());

                    try {
                        statement.setJobState(ExportJob.JobState.valueOf(value.toUpperCase()));
                    } catch (IllegalArgumentException e) {
                        throw exception;
                    }
                } else if (hasJobId) {
                    if (!(whereExpr.getChild(1) instanceof IntLiteral)) {
                        throw exception;
                    }
                    statement.setJobId(((IntLiteral) whereExpr.getChild(1)).getLongValue());
                } else if (hasQueryId) {
                    statement.setQueryId(analyzeQueryID(whereExpr, exception));
                }
            }

            // order by
            List<OrderByElement> orderByElements = statement.getOrderByElements();
            if (orderByElements != null && !orderByElements.isEmpty()) {
                ArrayList<OrderByPair> orderByPairs = new ArrayList<>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = 0;
                    try {
                        index = ExportProcNode.analyzeColumn(slotRef.getColumnName());
                    } catch (AnalysisException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                    }
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
                statement.setOrderByPairs(orderByPairs);
            }
            return null;
        }

        private UUID analyzeQueryID(Expr whereExpr, SemanticException exception) {
            if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
            }
            UUID uuid = null;
            String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
            try {
                uuid = UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid UUID string as queryid: " + value);
            }
            return uuid;
        }

        private String analyzeDbName(String dbName, ConnectContext context) {
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            return dbName;
        }

        private void checkPredicateType(Expr whereExpr, SemanticException exception) throws SemanticException {
            // check predicate type
            if (whereExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereExpr;
                if (binaryPredicate.getOp() != BinaryType.EQ) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
                }
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
            }
        }
    }
}