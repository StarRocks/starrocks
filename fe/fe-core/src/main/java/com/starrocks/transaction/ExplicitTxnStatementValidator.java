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

package com.starrocks.transaction;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;

import java.util.Map;
import java.util.Set;

/**
 * Performs explicit transaction specific validation after a statement has been analyzed.
 */
public final class ExplicitTxnStatementValidator {
    private ExplicitTxnStatementValidator() {
    }

    public static boolean validate(StatementBase statement, ConnectContext context) {
        if (context == null || context.getTxnId() == 0) {
            return false;
        }

        boolean isSet = statement instanceof SetStmt;
        boolean isDml = statement instanceof DmlStmt; // insert/update/delete
        boolean isSelect = statement instanceof QueryStatement;
        boolean isTransactionStmt = statement instanceof BeginStmt
                || statement instanceof CommitStmt || statement instanceof RollbackStmt;
        if (!(isSet || isDml || isSelect || isTransactionStmt)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT);
            return true;
        }

        if (statement instanceof InsertStmt insertStmt && insertStmt.isOverwrite()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT);
            return true;
        }

        // We validate two categories:
        // 1. Pure QueryStatement (SELECT ...).
        // 2. INSERT ... SELECT, where the source query should not read tables modified earlier.
        QueryStatement queryForValidation = null;
        if (statement instanceof QueryStatement) {
            queryForValidation = (QueryStatement) statement;
        } else if (statement instanceof InsertStmt insertStmt && insertStmt.getQueryStatement() != null) {
            // Normal INSERT (non-overwrite already checked) that has a source query.
            queryForValidation = insertStmt.getQueryStatement();
        } else {
            return false; // Other statements are not validated here.
        }

        ExplicitTxnState explicitTxnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr()
                .getExplicitTxnState(context.getTxnId());
        if (explicitTxnState == null) {
            return false;
        }

        Set<Long> modifiedTableIds = explicitTxnState.getModifiedTableIds();
        if (modifiedTableIds.isEmpty()) {
            return false;
        }

        Map<TableName, Table> referencedTables = AnalyzerUtils.collectAllTable(queryForValidation);
        for (Table table : referencedTables.values()) {
            if (table == null) {
                continue;
            }
            if (modifiedTableIds.contains(table.getId())) {
                ErrorReport.reportSemanticException(
                        ErrorCode.ERR_EXPLICIT_TXN_SELECT_ON_MODIFIED_TABLE, table.getName());
                return true;
            }
        }
        return false;
    }
}
