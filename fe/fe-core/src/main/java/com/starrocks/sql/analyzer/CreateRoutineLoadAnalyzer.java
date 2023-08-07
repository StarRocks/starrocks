// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateRoutineLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadAnalyzer.class);
    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private CreateRoutineLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    public static void analyze(CreateRoutineLoadStmt statement, ConnectContext context) {
        LabelName label = statement.getLabelName();
        String dbName = label.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(statement.getTableName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR);
        }
        statement.setDBName(dbName);
        statement.setName(label.getLabelName());
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, label.getLabelName());
            FeNameFormat.checkLabel(label.getLabelName());
            statement.setRoutineLoadDesc(CreateRoutineLoadStmt.buildLoadDesc(statement.getLoadPropertyList()));
            statement.checkJobProperties();
            statement.checkDataSourceProperties();
        } catch (UserException e) {
            LOG.error(e);
            throw new SemanticException(e.getMessage());
        }
    }
}
