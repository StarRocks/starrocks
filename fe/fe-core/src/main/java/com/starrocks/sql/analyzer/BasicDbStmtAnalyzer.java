// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UseDbStmt;

/**
 * This is a basic analyzer for some database statements, currently used by
 * `use database`, `recover database`, and `show create database`, mainly to
 * initialize the catalog name to which the database belongs.
 */
public class BasicDbStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new BasicDbStmtAnalyzerVisitor().analyze(statement, session);
    }

    private static class BasicDbStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitUseDbStatement(UseDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        @Override
        public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        private String getCatalogNameIfNotSet(String currentCatalogName, ConnectContext context) {
            String result = currentCatalogName;
            if (currentCatalogName == null) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException("No catalog selected");
                }
                result = context.getCurrentCatalog();
            }
            return result;
        }
    }
}
