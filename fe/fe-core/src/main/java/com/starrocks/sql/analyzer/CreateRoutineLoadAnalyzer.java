// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateRoutineLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadAnalyzer.class);

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
        label.setDbName(dbName);
        if (Strings.isNullOrEmpty(statement.getTableName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR);
        }
        Preconditions.checkArgument(context.getDatabase().equalsIgnoreCase(dbName),
                "session's dbname not equal lable's dbname", context.getDatabase(), dbName);
        statement.setDBName(dbName);
        statement.setName(label.getLabelName());
        try {
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
