// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.QueryRelation;

import java.util.List;
import java.util.stream.Collectors;

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

        List<Column> viewColumns = queryRelation.getScope().getRelationFields().getAllFields().stream()
                .map(f -> new Column(f.getName(), f.getType())).collect(Collectors.toList());
        if (stmt.getCols() != null) {
            if (stmt.getCols().size() != viewColumns.size()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            List<ColWithComment> colWithComments = stmt.getCols();
            for (int i = 0; i < stmt.getCols().size(); ++i) {
                Column col = viewColumns.get(i);
                ColWithComment colWithComment = colWithComments.get(i);
                col.setName(colWithComment.getColName());
                col.setComment(colWithComment.getComment());
            }
        }
        stmt.setFinalCols(viewColumns);

        String viewSql = ViewDefBuilder.build(stmt.getQueryStatement());
        stmt.setInlineViewDef(viewSql);
    }
}
