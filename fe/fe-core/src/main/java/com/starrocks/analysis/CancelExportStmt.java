// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;

import java.util.UUID;

/**
 * syntax:
 * CANCEL EXPORT WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122"
 */
public class CancelExportStmt extends DdlStmt {
    private String dbName;
    private Expr whereClause;

    private UUID queryId;

    public String getDbName() {
        return dbName;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public CancelExportStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        }

        // check auth after we get real export job

        // analyze where expr if not null
        AnalysisException exception = new AnalysisException(
                "Where clause should look like: queryid = \"your_query_id\"");
        if (whereClause == null) {
            throw exception;
        }

        if (whereClause instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
            if (binaryPredicate.getOp() != Operator.EQ) {
                throw exception;
            }
        } else {
            throw exception;
        }

        // left child
        if (!(whereClause.getChild(0) instanceof SlotRef)) {
            throw exception;
        }
        if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("queryid")) {
            throw exception;
        }

        // right child
        if (!(whereClause.getChild(1) instanceof StringLiteral)) {
            throw exception;
        }

        String value = ((StringLiteral) whereClause.getChild(1)).getStringValue();
        try {
            queryId = UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Invalid UUID string: " + value);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelExportStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
