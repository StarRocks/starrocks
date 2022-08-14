// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;

public class PauseRoutineLoadAnalyzer {
    public static void analyze(PauseRoutineLoadStmt statement, ConnectContext context) {
        String db = statement.getDbFullName();
        if (Strings.isNullOrEmpty(db)) {
            db = context.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        statement.setLabelName(new LabelName(db,  statement.getName()));
    }
}
