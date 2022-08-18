// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateDbStmt;

public class CreateDbAnalyzer {
    public static void analyze(CreateDbStmt statement, ConnectContext context) {
        String dbName = statement.getFullDbName();
        try {
            FeNameFormat.checkDbName(dbName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
    }
}
