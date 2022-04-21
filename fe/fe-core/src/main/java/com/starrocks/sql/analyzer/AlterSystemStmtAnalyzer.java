// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.BackendClause;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.FrontendClause;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class AlterSystemStmtAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class AlterSystemStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAlterSystemStmt(AlterSystemStmt statement, ConnectContext context) {
            AlterClause alterClause = statement.getAlterClause();
            if (alterClause instanceof BackendClause) {
                try {
                    ((BackendClause) alterClause).transferHostPorts();
                } catch (AnalysisException e) {
                    throw new RuntimeException(e);
                }
            }

            if (alterClause instanceof FrontendClause) {
                try {
                    ((FrontendClause) alterClause).transferHostPort();
                } catch (AnalysisException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }
    }
}
