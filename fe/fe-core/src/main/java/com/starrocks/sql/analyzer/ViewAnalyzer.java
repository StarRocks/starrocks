// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.BaseViewStmt;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
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

        // We create view columns from output fields of queryRelation if user-specified view columns are absent.
        // in such cases, field.name and field.type instead of field.outputExpression.type are used to construct the
        // columns of view's schema, since when queryRelation is union all stmt, field.outputExpression
        // is always the leftmost child of the UnionOperator, so if one of the left most child's output column
        // is const NULL and the corresponding output columns of the remaining children of UnionOperator
        // are not NULL(i.e. VARCHAR), then field.type(VARCHAR) is inconsistent with the
        // field.outputExpression.type(NULL) and the latter is incorrect. Wrong plans would be produced
        // from such views, so we always adopts of field.type instead of field.outputExpression.type
        List<Column> viewColumns = queryRelation.getScope().getRelationFields().getAllFields().stream()
                .map(f -> new Column(f.getName(), f.getType(), f.getOriginExpression().isNullable()))
                .collect(Collectors.toList());
        // When user-specified view's columns are present, we set names of comments of viewColumns according
        // to user-specified column information.
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
