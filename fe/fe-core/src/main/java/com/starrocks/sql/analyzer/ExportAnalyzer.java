// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

public class ExportAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new ExportAnalyzerVisitor().analyze(statement, session);
    }

    static class ExportAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext session) {
            return null;
        }
    }
}
