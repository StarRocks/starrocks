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
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.transaction.ExplicitTxnState;

public class TransactionAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new TransactionAnalyzerVisitor().analyze(statement, context);
    }

    static class TransactionAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitBeginStatement(BeginStmt statement, ConnectContext context) {
            ExplicitTxnState explicitTxnState = context.getExplicitTxnState();
            if (context.getExplicitTxnState() != null) {
                throw ErrorReportException.report(ErrorCode.ERR_BEGIN_TXN_FAILED, "Nested transactions are not allowed");
            }

            return null;
        }

        @Override
        public Void visitCommitStatement(CommitStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitRollbackStatement(RollbackStmt statement, ConnectContext context) {
            return null;
        }
    }
}
