// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class CancelAlterTableStatementAnalyzer {

    public static void analyze(CancelAlterTableStmt statement, ConnectContext context) {
        MetaUtils.normalizationTableName(context, statement.getDbTableName());
        // Check db.
        if (context.getGlobalStateMgr().getDb(statement.getDbName()) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, statement.getDbName());
        }
    }
}
