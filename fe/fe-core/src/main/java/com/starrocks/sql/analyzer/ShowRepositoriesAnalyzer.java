// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ShowRepositoriesStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class ShowRepositoriesAnalyzer {

    public static void analyze(ShowRepositoriesStmt showRepositoriesStmt, ConnectContext session) {
        new ShowRepositoriesAnalyzerVisitor().analyze(showRepositoriesStmt, session);
    }

    public static class ShowRepositoriesAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowRepositoriesStmt(ShowRepositoriesStmt showRepositoriesStmt, ConnectContext context) {
            return null;
        }

    }
}
