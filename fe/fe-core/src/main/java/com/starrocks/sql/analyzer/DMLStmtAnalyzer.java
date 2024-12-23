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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.transaction.ExplicitTxnState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DMLStmtAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DMLStmtAnalyzer.DMLStmtAnalyzerVisitor.class);

    public static void analyze(DmlStmt stmt, ConnectContext context) {
        new DMLStmtAnalyzer.DMLStmtAnalyzerVisitor().analyze(stmt, context);
    }

    static class DMLStmtAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        public void analyze(DmlStmt dmlStmt, ConnectContext context) {
            dmlStmt.getTableName().normalization(context);

            if (context.getExplicitTxnState() != null) {
                ExplicitTxnState globalTransactionState = context.getExplicitTxnState();
                List<ExplicitTxnState.ExplicitTxnStateItem> transactionStateItemList =
                        globalTransactionState.getTransactionStateItems();

                for (ExplicitTxnState.ExplicitTxnStateItem transactionStateItem : transactionStateItemList) {
                    DmlStmt ds = transactionStateItem.getDmlStmt();
                    if (ds.getTableName().equals(dmlStmt.getTableName())) {
                        throw ErrorReportException.report(ErrorCode.ERR_TXN_IMPORT_SAME_TABLE);
                    }

                    if (!ds.getTableName().getDb().equals(dmlStmt.getTableName().getDb())) {
                        throw ErrorReportException.report(ErrorCode.ERR_TXN_FORBID_CROSS_DB);
                    }
                }
            }

            visit(dmlStmt, context);
        }

        @Override
        public Void visitInsertStatement(InsertStmt stmt, ConnectContext context) {
            InsertAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt stmt, ConnectContext context) {
            UpdateAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt stmt, ConnectContext context) {
            DeleteAnalyzer.analyze(stmt, context);
            return null;
        }
    }
}
