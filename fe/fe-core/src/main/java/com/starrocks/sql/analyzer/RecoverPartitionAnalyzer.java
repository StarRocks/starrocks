// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.common.MetaUtils;

public class RecoverPartitionAnalyzer {
    public static void analyze(RecoverPartitionStmt statement, ConnectContext context) {
        TableName tbl = statement.getDbTblName();
        MetaUtils.normalizationTableName(context, tbl);
    }
}
