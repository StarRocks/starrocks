package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreatePipeStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableFunctionRelation;

public class PipeAnalyzer {

    // only analyze create pipe right now
    public static void analyze(CreatePipeStmt stmt, ConnectContext context) {
        String name = stmt.getPipeName();
        if (Strings.isNullOrEmpty(name)) {
            throw new SemanticException("empty pipe name");
        }
        FeNameFormat.checkCommonName("pipe", name);

        // Analyze Insert Statement
        // 1. Source table must be s3 table function
        InsertStmt insertStmt = stmt.getInsertStmt();
        InsertAnalyzer.analyze(insertStmt, context);
        if (!Strings.isNullOrEmpty(insertStmt.getLabel())) {
            throw new SemanticException("INSERT INTO cannot with label");
        }
        if (insertStmt.isOverwrite()) {
            throw new SemanticException("INSERT INTO cannot be OVERWRITE");
        }

        QueryStatement queryStatement = insertStmt.getQueryStatement();
        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("INSERT INTO can only with SELECT");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        if (!(selectRelation.getRelation() instanceof TableFunctionRelation)) {
            throw new SemanticException("SELECT must FROM table function");
        }
        // FIXME: change to the real relation
        TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) selectRelation.getRelation();
        if (!tableFunctionRelation.getFunctionName().getFunction().equalsIgnoreCase("s3")) {
            throw new SemanticException("Only support S3 table function");
        }
        stmt.setTableFunctionRelation(tableFunctionRelation);
        stmt.setTargetTable(insertStmt.getTargetTable());
    }

}
