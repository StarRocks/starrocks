// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class TruncateTableAnalyzer {

    public static void analyze(TruncateTableStmt statement, ConnectContext context) {
        MetaUtils.normalizationTableName(context, statement.getTblRef().getName());
        if (statement.getTblRef().hasExplicitAlias()) {
            throw new SemanticException("Not support truncate table with alias");
        }

        PartitionNames partitionNames = statement.getTblRef().getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new SemanticException("Not support truncate temp partitions");
            }
            // check if partition name is not empty string
            if (partitionNames.getPartitionNames().stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                throw new SemanticException("there are empty partition name");
            }
        }
    }
}
