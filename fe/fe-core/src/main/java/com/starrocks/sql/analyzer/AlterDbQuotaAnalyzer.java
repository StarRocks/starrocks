// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;

public class AlterDbQuotaAnalyzer {
    public static void analyze(AlterDatabaseQuotaStmt statement, ConnectContext context) {
        if (Strings.isNullOrEmpty(statement.getCatalogName())) {
            if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                throw new SemanticException("No catalog selected");
            }
            statement.setCatalogName(context.getCurrentCatalog());
        }

        AlterDatabaseQuotaStmt.QuotaType quotaType = statement.getQuotaType();
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            try {
                statement.setQuota(ParseUtil.analyzeDataVolumn(statement.getQuotaValue()));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            try {
                statement.setQuota(ParseUtil.analyzeReplicaNumber(statement.getQuotaValue()));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }
    }

}
