// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.Optional;

public class FileAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new FileAnalyzer.FileAnalyzerVisitor().visit(statement, context);
    }

    static class FileAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
            statement.setDbName(AnalyzerUtils.getOrDefaultDatabase(statement.getDbName(), context));
            if (Strings.isNullOrEmpty(statement.getFileName())) {
                throw new SemanticException("File name is not specified");
            }
            statement.analyzeProperties();
            return null;
        }

        @Override
        public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
            statement.setDbName(AnalyzerUtils.getOrDefaultDatabase(statement.getDbName(), context));

            if (Strings.isNullOrEmpty(statement.getFileName())) {
                throw new SemanticException("File name is not specified");
            }

            Optional<String> optional = statement.getProperties().keySet().stream().filter(
                    entity -> !DropFileStmt.PROP_CATALOG.equals(entity)).findFirst();
            if (optional.isPresent()) {
                throw new SemanticException(optional.get() + " is invalid property");
            }

            String catalogName = statement.getProperties().get(DropFileStmt.PROP_CATALOG);
            if (Strings.isNullOrEmpty(catalogName)) {
                throw new SemanticException("globalStateMgr name is missing");
            }
            statement.setCatalogName(catalogName);
            return null;
        }

        @Override
        public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            statement.setDbName(AnalyzerUtils.getOrDefaultDatabase(statement.getDbName(), context));
            return null;
        }
    }
}
