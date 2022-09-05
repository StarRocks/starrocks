// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class RecoverTableAnalyzer {

    public static void analyze(RecoverTableStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableNameObject();
        MetaUtils.normalizationTableName(context, tableName);
    }
}
