// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseMaterializedViewStatement;

public class MaterializedViewAnalyzer {

    public static void analyze(BaseMaterializedViewStatement stmt, ConnectContext session) {
        new MaterializedViewAnalyzer.ShowStmtAnalyzerVisitor().visit(stmt, session);
    }

    static class ShowStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(BaseMaterializedViewStatement statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }


    }

}