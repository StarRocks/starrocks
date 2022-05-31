// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class RefreshTableStatementAnalyzer {
    public static void analyze(RefreshTableStmt statement, ConnectContext context) {
        new RefreshTableStatementAnalyzer.RefreshTableStatementAnalyzerVisitor().visit(statement, context);
    }

    static class RefreshTableStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getTbl();
            MetaUtils.normalizationTableName(context, tableName);
            if (tableName == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            return null;
        }
    }
}
