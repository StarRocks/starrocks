// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class AdminShowConfigStmtAnalyzer {
    public static void analyze(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
        new AdminShowConfigStmtAnalyzerVisitor().visit(adminShowConfigStmt, session);
    }

    static class AdminShowConfigStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
            visit(adminShowConfigStmt, session);
        }
    }
}
