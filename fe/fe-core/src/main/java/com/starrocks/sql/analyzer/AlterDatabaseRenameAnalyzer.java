// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterDatabaseRename;

public class AlterDatabaseRenameAnalyzer {
    public static void analyze(AlterDatabaseRename statement, ConnectContext context) {
        String dbName = statement.getDbName();
        String newName = statement.getNewDbName();

        try {
            FeNameFormat.checkDbName(newName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
    }
}
