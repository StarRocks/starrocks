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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcService;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowAlterStmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nonnull;

public class ShowAlterStmtAnalyzer {

    public static void analyze(ShowAlterStmt statement, ConnectContext context) {
        new ShowAlterStmtAnalyzerVisitor().visit(statement, context);
    }

    static class ShowAlterStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private final HashMap<String, Expr> filterMap = new HashMap<>();

        public void analyze(ShowAlterStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowAlterStatement(ShowAlterStmt statement, ConnectContext context) {
            //first analyze syntax.
            analyzeSyntax(statement, context);
            // check auth when get job info.
            handleShowAlterTable(statement, context);

            return null;
        }

        private void handleShowAlterTable(ShowAlterStmt statement, ConnectContext context) throws SemanticException {
            // build proc path
            @Nonnull Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
            ShowAlterStmt.AlterType type = statement.getType();
            StringBuilder sb = new StringBuilder();
            sb.append("/jobs/");
            sb.append(db.getId());
            if (type == ShowAlterStmt.AlterType.COLUMN) {
                sb.append("/schema_change");
            } else if (type == ShowAlterStmt.AlterType.ROLLUP || type == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
                sb.append("/rollup");
            } else if (type == ShowAlterStmt.AlterType.OPTIMIZE) {
                sb.append("/optimize");
            }

            // create show proc stmt
            // '/jobs/db_name/rollup|schema_change/
            ProcNodeInterface node = null;
            try {
                node = ProcService.getInstance().open(sb.toString());
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_PROC_PATH, sb.toString());
            }
            statement.setNode(node);
        }

        public void analyzeSyntax(ShowAlterStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR, dbName);
                }
            }
            statement.setDbName(dbName);
            // Check db.
            if (context.getGlobalStateMgr().getDb(dbName) == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }

            ShowAlterStmt.AlterType type = statement.getType();
            Preconditions.checkNotNull(type);

            Expr whereClause = statement.getWhereClause();
            // analyze where clause if not null
            if (whereClause != null) {
                analyzeSubPredicate(whereClause);
            }
            statement.setFilter(filterMap);

            // order by
            List<OrderByElement> orderByElements = statement.getOrderByElements();
            if (orderByElements != null && !orderByElements.isEmpty()) {
                ArrayList<OrderByPair> orderByPairs = new ArrayList<OrderByPair>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = 0;
                    try {
                        index = SchemaChangeProcDir.analyzeColumn(slotRef.getColumnName());
                    } catch (AnalysisException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                    }
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
                statement.setOrderByPairs(orderByPairs);
            }
        }

        private void getPredicateValue(Expr subExpr) {
            if (!(subExpr instanceof BinaryPredicate)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "The operator =|>=|<=|>|<|!= are supported.");
            }
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Only support column = xxx syntax.");
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName().toLowerCase();
            if (leftKey.equals("tablename") || leftKey.equals("state")) {
                if (!(subExpr.getChild(1) instanceof StringLiteral) ||
                        binaryPredicate.getOp() != BinaryType.EQ) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Where clause : TableName = \"table1\" or "
                                    + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
                }
            } else if (leftKey.equals("createtime") || leftKey.equals("finishtime")) {
                if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                                    + "\"2019-12-02|2019-12-02 14:54:00\"");
                }
                try {
                    subExpr.setChild(1, ((StringLiteral) subExpr.getChild(1)).castTo(Type.DATETIME));
                } catch (AnalysisException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                }
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "The columns of TableName/CreateTime/FinishTime/State are supported.");
            }
            filterMap.put(leftKey, subExpr);
        }

        private void analyzeSubPredicate(Expr subExpr) {
            if (subExpr == null) {
                return;
            }
            if (subExpr instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) subExpr;
                if (cp.getOp() != com.starrocks.analysis.CompoundPredicate.Operator.AND) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Only allow compound predicate with operator AND");
                }
                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
                return;
            }
            getPredicateValue(subExpr);
        }
    }

}
