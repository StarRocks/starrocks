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
import com.starrocks.sql.ast.ShowStmt;
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
        boolean isShow = statement instanceof ShowStmt;
        boolean isTransactionStmt = statement instanceof BeginStmt
                || statement instanceof CommitStmt || statement instanceof RollbackStmt;
        if (!(isSet || isDml || isSelect || isShow || isTransactionStmt)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT);
            return true;
        }

        if (statement instanceof InsertStmt insertStmt && insertStmt.isOverwrite()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT);
            return true;
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

        // A partial-update INSERT writes only a subset of columns and implicitly reads the target
        // table at publish time to fill the columns it does not write. When that table was already
        // modified earlier in the same transaction, all statements are batch-applied together as
        // multiple op_writes on one tablet; the publish path cannot correctly merge a partial update
        // against those uncommitted same-transaction writes (the multi-op_write batch apply would
        // mis-key rewritten segments / desync the primary index from the pending rowset and silently
        // corrupt earlier rows). Reject it up front, consistent with the SELECT rule below. (UPDATE
        // and DELETE on an already-touched table are already blocked by ERR_..._STMT_ORDER.)
        if (statement instanceof InsertStmt insertStmt && insertStmt.usePartialUpdate()) {
            Table target = insertStmt.getTargetTable();
            if (target != null && modifiedTableIds.contains(target.getId())) {
                ErrorReport.reportSemanticException(
                        ErrorCode.ERR_EXPLICIT_TXN_PARTIAL_UPDATE_ON_MODIFIED_TABLE, target.getName());
                return true;
            }
        }

        // Validate read sources against tables modified earlier in the same transaction:
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
