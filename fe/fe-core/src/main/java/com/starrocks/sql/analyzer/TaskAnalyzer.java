// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.SubmitTaskStmt;
import org.apache.parquet.Strings;

public class TaskAnalyzer {

    public static void analyzeSubmitTaskStmt(SubmitTaskStmt submitTaskStmt, ConnectContext session) {
        String dbName = submitTaskStmt.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = session.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        submitTaskStmt.setDbName(dbName);
    }

}
