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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.connector.ConnectorType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.StatementBase;
import org.apache.commons.collections4.MapUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ViewAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new ViewAnalyzer.ViewAnalyzerVisitor().visit(statement, context);
    }

    static class ViewAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateViewStatement(CreateViewStmt stmt, ConnectContext context) {
            // normalize & validate view name
            stmt.getTableName().normalization(context);
            final String tableName = stmt.getTableName().getTbl();
            FeNameFormat.checkTableName(tableName);

            // Only allow setting properties for Iceberg views
            if (!MapUtils.isEmpty(stmt.getProperties())) {
                String catalog = stmt.getTableName().getCatalog();
                if (Strings.isNullOrEmpty(catalog) ||
                        !GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalog)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalog);
                }
                if (CatalogMgr.isInternalCatalog(catalog) ||
                        ConnectorType.from(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogType(catalog)) !=
                                ConnectorType.ICEBERG) {
                    throw new SemanticException("Setting properties is only supported for Iceberg views");
                }
            }

            Analyzer.analyze(stmt.getQueryStatement(), context);
            boolean hasTemporaryTable = AnalyzerUtils.hasTemporaryTables(stmt.getQueryStatement());
            if (hasTemporaryTable) {
                throw new SemanticException("View can't base on temporary table");
            }
            List<Column> viewColumns = analyzeViewColumns(stmt.getQueryStatement().getQueryRelation(), stmt.getColWithComments());
            stmt.setColumns(viewColumns);

            // reserve the original view sql

            String viewSql = AstToSQLBuilder.toSQLWithCredential(stmt.getQueryStatement());
            stmt.setInlineViewDef(viewSql);
            Preconditions.checkArgument(stmt.getOrigStmt() != null, "View's original statement is null");
            String originalViewDef = stmt.getOrigStmt().originStmt;
            Preconditions.checkArgument(originalViewDef != null, "View's original view definition is null");
            Preconditions.checkArgument(stmt.getQueryStartIndex() >= 0 && stmt.getQueryStopIndex() >= stmt.getQueryStartIndex(),
                    "View's query start or stop index is invalid");
            stmt.setOriginalViewDefineSql(originalViewDef.substring(stmt.getQueryStartIndex(), stmt.getQueryStopIndex()));
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt stmt, ConnectContext context) {
            // normalize & validate view name
            stmt.getTableName().normalization(context);
            final String tableName = stmt.getTableName().getTbl();
            FeNameFormat.checkTableName(tableName);

            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(context, stmt.getTableName().getCatalog(), stmt.getTableName().getDb(),
                            stmt.getTableName().getTbl());
            if (table == null) {
                throw new SemanticException("Table %s is not found", tableName);
            }

            if (!table.isView()) {
                throw new SemanticException("The specified table [" + tableName + "] is not a view");
            }

            if (stmt.getAlterClause() == null) {
                return null;
            }

            AlterClause alterClause = stmt.getAlterClause();
            AlterViewClause alterViewClause = (AlterViewClause) alterClause;

            Analyzer.analyze(alterViewClause.getQueryStatement(), context);
            boolean hasTemporaryTable = AnalyzerUtils.hasTemporaryTables(((AlterViewClause) alterClause).getQueryStatement());
            if (hasTemporaryTable) {
                throw new SemanticException("View can't base on temporary table");
            }

            List<Column> viewColumns = analyzeViewColumns(alterViewClause.getQueryStatement().getQueryRelation(),
                    alterViewClause.getColWithComments());
            alterViewClause.setColumns(viewColumns);
            String viewSql = AstToSQLBuilder.toSQL(alterViewClause.getQueryStatement());
            alterViewClause.setInlineViewDef(viewSql);
            Preconditions.checkArgument(stmt.getOrigStmt() != null, "View's original statement is null");
            String originalViewDef = stmt.getOrigStmt().originStmt;
            Preconditions.checkArgument(originalViewDef != null, "View's original view definition is null");
            Preconditions.checkArgument(alterViewClause.getQueryStartIndex() >= 0 &&
                            alterViewClause.getQueryStopIndex() >= alterViewClause.getQueryStartIndex(),
                    "View's query start or stop index is invalid");
            alterViewClause.setOriginalViewDefineSql(
                    originalViewDef.substring(alterViewClause.getQueryStartIndex(), alterViewClause.getQueryStopIndex()));
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
                    Column newColumn = new Column(colWithComment.getColName(), col.getType(), col.isAllowNull());
                    newColumn.setComment(colWithComment.getComment());
                    viewColumns.set(i, newColumn);
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
