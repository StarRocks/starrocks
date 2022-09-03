// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseViewStmt;
import com.starrocks.sql.ast.QueryRelation;

import java.util.List;

public class ViewAnalyzer {
    public static void analyze(BaseViewStmt stmt, ConnectContext session) {
        // normalize & validate view name
        stmt.getTableName().normalization(session);
        final String tableName = stmt.getTableName().getTbl();
        try {
            FeNameFormat.checkTableName(tableName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
        }
        Analyzer.analyze(stmt.getQueryStatement(), session);
        QueryRelation queryRelation = stmt.getQueryStatement().getQueryRelation();

        List<Column> viewColumns = Lists.newArrayList();
        if (stmt.getCols() != null) {
            List<ColWithComment> columnOutputNames = stmt.getCols();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            if (stmt.getCols().size() != queryRelation.getOutputExpression().size()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < stmt.getCols().size(); ++i) {
                Column col = new Column(columnOutputNames.get(i).getColName(), outputExpression.get(i).getType());
                col.setComment(columnOutputNames.get(i).getComment());
                viewColumns.add(col);
            }
        } else {
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                viewColumns.add(new Column(columnOutputNames.get(i), outputExpression.get(i).getType()));
            }
        }
        stmt.setFinalCols(viewColumns);

        String viewSql = ViewDefBuilder.build(stmt.getQueryStatement());
        stmt.setInlineViewDef(viewSql);
    }
}
