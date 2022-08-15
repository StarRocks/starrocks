// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;

public class AlterRoutineLoadAnalyzer {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    public static void analyze(AlterRoutineLoadStmt statement, ConnectContext context) {
        String db = statement.getDbName();
        if (Strings.isNullOrEmpty(db)) {
            db = context.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        LabelName labelName = new LabelName(db,  statement.getLabel());
        statement.setLabelName(labelName);
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
            statement.setRoutineLoadDesc(CreateRoutineLoadStmt.buildLoadDesc(statement.getLoadPropertyList()));
            statement.checkJobProperties();
            statement.checkDataSourceProperties();
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }
}
