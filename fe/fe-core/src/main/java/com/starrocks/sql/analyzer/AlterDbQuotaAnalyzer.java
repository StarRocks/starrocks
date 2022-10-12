// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;

public class AlterDbQuotaAnalyzer {
    public static void analyze(AlterDatabaseQuotaStmt statement, ConnectContext context) {
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
