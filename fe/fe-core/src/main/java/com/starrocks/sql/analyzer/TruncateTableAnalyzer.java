// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;

public class TruncateTableAnalyzer {

    public static void analyze(TruncateTableStmt statement, ConnectContext context) {
        String dbName = statement.getDbName();
        String tblName = statement.getTblName();
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(context.getDatabase())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            } else {
                dbName = ClusterNamespace.getFullName(context.getDatabase());
            }
        } else {
            dbName = ClusterNamespace.getFullName(dbName);
        }
        statement.setDbName(dbName);
        try {
            CatalogUtils.checkOlapTableHasStarOSPartition(dbName, tblName);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

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
