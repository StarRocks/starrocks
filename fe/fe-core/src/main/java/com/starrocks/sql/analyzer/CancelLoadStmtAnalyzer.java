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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelLoadStmt;

public class CancelLoadStmtAnalyzer {

    public static void analyze(CancelLoadStmt statement, ConnectContext context) {
        new CancelLoadStmtAnalyzerVisitor().analyze(statement, context);
    }

    static class CancelLoadStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private String label;

        public CancelLoadStmtAnalyzerVisitor() {
            this.label = null;
        }

        public void analyze(CancelLoadStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCancelLoadStatement(CancelLoadStmt statement, ConnectContext context) {
            analyzeDbName(statement, context);
            analyzeWhereClause(statement, context);
            return null;
        }

        private void analyzeDbName(CancelLoadStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            statement.setDbName(dbName);
        }

        private void analyzeWhereClause(CancelLoadStmt statement, ConnectContext context) {
            Expr whereClause = statement.getWhereClause();
            boolean valid = true;
            do {
                if (whereClause == null) {
                    valid = false;
                    break;
                }

                if (whereClause instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                    if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                        valid = false;
                        break;
                    }
                } else {
                    valid = false;
                    break;
                }

                // left child
                if (!(whereClause.getChild(0) instanceof SlotRef)) {
                    valid = false;
                    break;
                }
                if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("label")) {
                    valid = false;
                    break;
                }

                // right child
                if (!(whereClause.getChild(1) instanceof StringLiteral)) {
                    valid = false;
                    break;
                }

                label = ((StringLiteral) whereClause.getChild(1)).getStringValue();
                if (Strings.isNullOrEmpty(label)) {
                    valid = false;
                    break;
                }
            } while (false);

            if (!valid) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Where clause should looks like: LABEL = \"your_load_label\"");
            }

            statement.setLabel(label);
        }
    }
}
