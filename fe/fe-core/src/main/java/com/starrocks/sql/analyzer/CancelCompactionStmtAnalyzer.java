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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelCompactionStmt;

public class CancelCompactionStmtAnalyzer {

    public static void analyze(CancelCompactionStmt statement, ConnectContext context) {
        new CancelCompactionStmtAnalyzerVisitor().analyze(statement, context);
    }

    static class CancelCompactionStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(CancelCompactionStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCancelCompactionStatement(CancelCompactionStmt statement, ConnectContext context) {
            analyzeWhereClause(statement, context);
            return null;
        }

        private void analyzeWhereClause(CancelCompactionStmt statement, ConnectContext context) {
            Expr whereClause = statement.getWhereClause();
            boolean valid = true;
            long txnId = 0;
            do {
                if (whereClause == null) {
                    valid = false;
                    break;
                }

                if (whereClause instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                    if (binaryPredicate.getOp() != BinaryType.EQ) {
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
                if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("txn_id")) {
                    valid = false;
                    break;
                }

                // right child
                if (!(whereClause.getChild(1) instanceof IntLiteral)) {
                    valid = false;
                    break;
                }

                txnId = ((IntLiteral) whereClause.getChild(1)).getValue();
            } while (false);

            if (!valid) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Where clause should looks like: TXN_ID = compaction_txn_id");
            }

            statement.setTxnId(txnId);
        }
    }
}
