// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.common.MetaUtils;

public class CreateTableLikeAnalyzer {

    public static void analyze(CreateTableLikeStmt statement, ConnectContext context) {
        MetaUtils.normalizationTableName(context, statement.getDbTbl());
        MetaUtils.normalizationTableName(context, statement.getExistedDbTbl());
        String tableName = statement.getTableName();
        try {
            FeNameFormat.checkTableName(tableName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
        }
    }
}
