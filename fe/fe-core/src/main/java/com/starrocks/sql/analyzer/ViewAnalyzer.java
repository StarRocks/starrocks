// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.QueryRelation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

        Set<String> columnNameSet = new HashSet<>();
        for (Column column : viewColumns) {
            if (columnNameSet.contains(column.getName())) {
                throw new SemanticException("Duplicate column name '%s'", column.getName());
            } else {
                columnNameSet.add(column.getName());
            }
        }

        String viewSql = AstToSQLBuilder.toSQL(stmt.getQueryStatement());
        stmt.setInlineViewDef(viewSql);
    }
}
