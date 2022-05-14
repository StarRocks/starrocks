package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class GrantPrivilegeAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new GrantPrivilegeStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class GrantPrivilegeStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitGrantPrivilegeStmt(GrantStmt statement, ConnectContext context) {
            return null;
        }
    }
}
