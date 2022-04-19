// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

public class AdminSetStmtAnalyzer {

    public static void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
        new AdminSetStmtAnalyzerVisitor().visit(adminSetConfigStmt, session);
    }

    static class AdminSetStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            visit(adminSetConfigStmt, session);
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt adminSetConfigStmt, ConnectContext context) {
            Map<String, String> configs = adminSetConfigStmt.getConfigs();
            if (configs.size() != 1) {
                throw new SemanticException("config parameter size is not equal to 1");
            }
            return null;
        }

    }
}