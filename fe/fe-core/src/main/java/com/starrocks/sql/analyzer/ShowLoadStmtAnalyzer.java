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
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.LoadProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowLoadStmt;

import java.util.ArrayList;
import java.util.List;

public class ShowLoadStmtAnalyzer {

    public static void analyze(ShowLoadStmt statement, ConnectContext context) {
        new ShowLoadStmtAnalyzerVisitor().visit(statement, context);
    }

    static class ShowLoadStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private boolean isAccurateMatch;
        private String labelValue;
        private String stateValue;

        public ShowLoadStmtAnalyzerVisitor() {
            this.labelValue = null;
            this.stateValue = null;
            this.isAccurateMatch = false;
        }

        public void analyze(ShowLoadStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitShowLoadStatement(ShowLoadStmt statement, ConnectContext context) {
            analyzeDbName(statement, context);
            analyzeWhereClause(statement, context);
            analyzeOrderByElements(statement, context);
            return null;
        }

        private void analyzeDbName(ShowLoadStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName) && !statement.isAll()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            statement.setDbName(dbName);
        }

        private void analyzeWhereClause(ShowLoadStmt statement, ConnectContext context) {
            Expr whereClause = statement.getWhereClause();
            if (whereClause != null) {
                analyzeSubPredicate(whereClause);
                statement.setLabelValue(labelValue);
                statement.setStateValue(stateValue);
                statement.setIsAccurateMatch(isAccurateMatch);
            }
        }

        private void analyzeOrderByElements(ShowLoadStmt statement, ConnectContext context) {
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
                        index = LoadProcDir.analyzeColumn(slotRef.getColumnName());
                    } catch (AnalysisException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                    }
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
                statement.setOrderByPairs(orderByPairs);
            }
        }

        private void analyzeSubPredicate(Expr expr) {
            if (expr == null) {
                return;
            }

            if (expr instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) expr;
                if (cp.getOp() != com.starrocks.analysis.CompoundPredicate.Operator.AND) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
                return;
            }

            boolean valid = true;
            boolean hasLabel = false;
            boolean hasState = false;

            CHECK:
            {
                if (expr instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                    if (binaryPredicate.getOp() != BinaryType.EQ) {
                        valid = false;
                        break CHECK;
                    }
                } else if (expr instanceof LikePredicate) {
                    LikePredicate likePredicate = (LikePredicate) expr;
                    if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                        valid = false;
                        break CHECK;
                    }
                } else {
                    valid = false;
                    break CHECK;
                }

                // left child
                if (!(expr.getChild(0) instanceof SlotRef)) {
                    valid = false;
                    break CHECK;
                }
                String leftKey = ((SlotRef) expr.getChild(0)).getColumnName();
                if (leftKey.equalsIgnoreCase("label")) {
                    hasLabel = true;
                } else if (leftKey.equalsIgnoreCase("state")) {
                    hasState = true;
                } else {
                    valid = false;
                    break CHECK;
                }

                if (hasState && !(expr instanceof BinaryPredicate)) {
                    valid = false;
                    break CHECK;
                }

                if (hasLabel && expr instanceof BinaryPredicate) {
                    isAccurateMatch = true;
                }

                // right child
                if (!(expr.getChild(1) instanceof StringLiteral)) {
                    valid = false;
                    break CHECK;
                }

                String value = ((StringLiteral) expr.getChild(1)).getStringValue();
                if (Strings.isNullOrEmpty(value)) {
                    valid = false;
                    break CHECK;
                }

                if (hasLabel) {
                    labelValue = value;
                } else if (hasState) {
                    stateValue = value.toUpperCase();

                    try {
                        JobState.valueOf(stateValue);
                    } catch (Exception e) {
                        valid = false;
                        break CHECK;
                    }
                }
            }

            if (!valid) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Where clause should looks like: LABEL = \"your_load_label\","
                                + " or LABEL LIKE \"matcher\", "
                                + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED|QUEUEING\", "
                                + " or compound predicate with operator AND");
            }
        }
    }
}
