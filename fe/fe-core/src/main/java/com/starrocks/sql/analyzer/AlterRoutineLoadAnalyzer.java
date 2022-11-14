// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlterRoutineLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(AlterRoutineLoadAnalyzer.class);

    private AlterRoutineLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    public static void analyze(AlterRoutineLoadStmt statement, ConnectContext context) {
        LabelName label = statement.getLabelName();
        String dbName = label.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        Preconditions.checkArgument(context.getDatabase().equalsIgnoreCase(dbName)
                        || context.getDatabase().equalsIgnoreCase(""),
                "session's dbname not equal lable's dbname", context.getDatabase(), dbName);
        LabelName labelName = new LabelName(dbName, statement.getLabel());
        statement.setLabelName(labelName);
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
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

