// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.AlterLoadStmt;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlterLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(AlterLoadAnalyzer.class);

    private AlterLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    public static void analyze(AlterLoadStmt statement, ConnectContext context) {
        String dbName = statement.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        Preconditions.checkArgument(context.getDatabase().equalsIgnoreCase(dbName)
                        || context.getDatabase().equalsIgnoreCase(""),
                "session's dbname not equal lable's dbname", context.getDatabase(), dbName);
        statement.setDbName(dbName);
        try {
            FeNameFormat.checkLabel(statement.getLabel());
            statement.checkJobProperties();
        } catch (UserException e) {
            LOG.error(e);
            throw new SemanticException(e.getMessage());
        }
    }
}

