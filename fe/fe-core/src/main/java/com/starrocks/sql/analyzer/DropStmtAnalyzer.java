// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

public class DropStmtAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new DropStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class DropStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext context) {
            MetaUtils.normalizationTableName(context, statement.getTableNameObject());
            return null;
        }
    }

}
