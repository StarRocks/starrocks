// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class AdminSetStmtAnalyzer {

    public static void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
        new AdminSetStmtAnalyzerVisitor().visit(adminSetConfigStmt, session);
    }

    static class AdminSetStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            visit(adminSetConfigStmt, session);
        }
    }
}