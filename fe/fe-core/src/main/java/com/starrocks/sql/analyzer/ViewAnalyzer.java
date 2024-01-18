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
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ViewAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new ViewAnalyzer.ViewAnalyzerVisitor().visit(statement, context);
    }

    static class ViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateViewStatement(CreateViewStmt stmt, ConnectContext context) {
            // normalize & validate view name
            stmt.getTableName().normalization(context);
            final String tableName = stmt.getTableName().getTbl();
            FeNameFormat.checkTableName(tableName);

            Analyzer.analyze(stmt.getQueryStatement(), context);

            List<Column> viewColumns = analyzeViewColumns(stmt.getQueryStatement().getQueryRelation(), stmt.getColWithComments());
            stmt.setColumns(viewColumns);
            String viewSql = AstToSQLBuilder.toSQL(stmt.getQueryStatement());
            stmt.setInlineViewDef(viewSql);
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt stmt, ConnectContext context) {
            // normalize & validate view name
            stmt.getTableName().normalization(context);
            final String tableName = stmt.getTableName().getTbl();
            FeNameFormat.checkTableName(tableName);

            Table table = MetaUtils.getTable(stmt.getTableName());
            if (!(table instanceof View)) {
                throw new SemanticException("The specified table [" + tableName + "] is not a view");
            }

            AlterClause alterClause = stmt.getAlterClause();
            AlterViewClause alterViewClause = (AlterViewClause) alterClause;

            Analyzer.analyze(alterViewClause.getQueryStatement(), context);

            List<Column> viewColumns = analyzeViewColumns(alterViewClause.getQueryStatement().getQueryRelation(),
                    alterViewClause.getColWithComments());
            alterViewClause.setColumns(viewColumns);
            String viewSql = AstToSQLBuilder.toSQL(alterViewClause.getQueryStatement());
            alterViewClause.setInlineViewDef(viewSql);
            return null;
        }

        private List<Column> analyzeViewColumns(QueryRelation queryRelation, List<ColWithComment> colWithComments) {
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
            if (colWithComments != null) {
                if (colWithComments.size() != viewColumns.size()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
                }
                for (int i = 0; i < colWithComments.size(); ++i) {
                    Column col = viewColumns.get(i);
                    ColWithComment colWithComment = colWithComments.get(i);
                    col.setName(colWithComment.getColName());
                    col.setComment(colWithComment.getComment());
                }
            }

            Set<String> columnNameSet = new HashSet<>();
            for (Column column : viewColumns) {
                if (columnNameSet.contains(column.getName())) {
                    throw new SemanticException("Duplicate column name '%s'", column.getName());
                } else {
                    columnNameSet.add(column.getName());
                }
            }
            return viewColumns;
        }
    }
}
