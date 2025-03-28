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

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableSampleClause;
import com.starrocks.sql.ast.ViewRelation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * AstToSQLBuilder inherits AstToStringBuilder and rewrites some special AST logic to
 * ensure that the generated SQL must be a legal SQL,
 * which can be used in some scenarios that require serialization and deserialization.
 * Such as string serialization of views
 */
public class AstToSQLBuilder {
    private static final Logger LOG = LogManager.getLogger(AstToSQLBuilder.class);

    public static String buildSimple(StatementBase statement) {
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(statement);
        boolean sameCatalogDb = tables.keySet().stream().map(TableName::getCatalogAndDb).distinct().count() == 1;
        return new AST2SQLBuilderVisitor(sameCatalogDb, false, true).visit(statement);
    }

    public static String toSQL(ParseNode statement) {
        return new AST2SQLBuilderVisitor(false, false, true).visit(statement);
    }

    // for executable SQL with credential, such as pipe insert sql
    public static String toSQLWithCredential(ParseNode statement) {
        return new AST2SQLBuilderVisitor(false, false, false).visit(statement);
    }

    // return sql from ast or default sql if builder throws exception.
    // for example, `select from files` needs file schema to generate sql from ast.
    // If BE is down, the schema will be null, and an exception will be thrown when writing audit log.
    public static String toSQLOrDefault(ParseNode statement, String defaultSql) {
        try {
            return toSQL(statement);
        } catch (Exception e) {
            LOG.info("Ast to sql failed.", e);
            return defaultSql;
        }
    }

    public static class AST2SQLBuilderVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {

        protected final boolean simple;
        protected final boolean withoutTbl;

        public AST2SQLBuilderVisitor(boolean simple, boolean withoutTbl, boolean hideCredential) {
            super(hideCredential);
            this.simple = simple;
            this.withoutTbl = withoutTbl;
        }

        private String buildColumnName(TableName tableName, String fieldName, String columnName) {
            String res = "";
            if (tableName != null && !withoutTbl) {
                if (!simple) {
                    res = tableName.toSql();
                } else {
                    res = "`" + tableName.getTbl() + "`";
                }
                res += ".";
            }

            res += '`' + fieldName + '`';
            if (columnName != null && !fieldName.equalsIgnoreCase(columnName)) {
                res += " AS `" + columnName + "`";
            }
            return res;
        }

        private String buildStructColumnName(TableName tableName, String fieldName, String columnName) {
            String res = "";
            if (tableName != null) {
                if (!simple) {
                    res = tableName.toSql();
                } else {
                    res = "`" + tableName.getTbl() + "`";
                }
                res += ".";
            }

            fieldName = handleColumnName(fieldName);
            res += fieldName;
            
            if (columnName != null) {
                columnName = handleColumnName(columnName);
                if (!fieldName.equalsIgnoreCase(columnName)) {
                    res += " AS " + columnName;
                }
            }
            return res;
        }

        // Consider struct, like fieldName = a.b.c, columnName = a.b.c
        private String handleColumnName(String name) {
            String[] fields = name.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                sb.append("`");
                sb.append(fields[i]);
                sb.append("`");
                if (i < fields.length - 1) {
                    sb.append(".");
                }
            }
            return sb.toString();
        }

        @Override
        public String visitNode(ParseNode node, Void context) {
            return "";
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            SelectList selectList = stmt.getSelectList();
            sqlBuilder.append("SELECT ");

            // add hint
            if (selectList.getHintNodes() != null) {
                sqlBuilder.append(extractHintStr(selectList.getHintNodes()));
            }

            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }

            sqlBuilder.append(Joiner.on(", ").join(visitSelectItemList(stmt)));

            String fromClause = visit(stmt.getRelation());
            if (fromClause != null) {
                sqlBuilder.append("\nFROM ");
                sqlBuilder.append(fromClause);
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append("\nWHERE ");
                sqlBuilder.append(visit(stmt.getWhereClause()));
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append("\nGROUP BY ");
                sqlBuilder.append(visit(stmt.getGroupByClause()));
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append("\nHAVING ");
                sqlBuilder.append(visit(stmt.getHavingClause()));
            }

            return sqlBuilder.toString();
        }

        protected List<String> visitSelectItemList(SelectRelation stmt) {
            if (CollectionUtils.isNotEmpty(stmt.getOutputExpression())) {
                List<String> selectListString = new ArrayList<>();
                List<String> columnNameList = stmt.getColumnOutputNames();
                for (int i = 0; i < stmt.getOutputExpression().size(); ++i) {
                    Expr expr = stmt.getOutputExpression().get(i);
                    String columnName = columnNameList.get(i);

                    if (expr instanceof FieldReference) {
                        Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
                        selectListString.add(buildColumnName(field.getRelationAlias(), field.getName(), columnName));
                    } else if (expr instanceof SlotRef slot) {
                        if (slot.getOriginType().isStructType()) {
                            selectListString.add(buildStructColumnName(slot.getTblNameWithoutAnalyzed(),
                                    slot.getColumnName(), columnName));
                        } else {
                            selectListString.add(buildColumnName(slot.getTblNameWithoutAnalyzed(), slot.getColumnName(),
                                    columnName));
                        }
                    } else if (columnName != null) {
                        selectListString.add(visit(expr) + " AS `" + columnName + "`");
                    } else {
                        selectListString.add(visit(expr));
                    }
                }
                return selectListString;
            } else {
                List<String> selectListString = new ArrayList<>();
                for (SelectListItem item : stmt.getSelectList().getItems()) {
                    if (item.isStar()) {
                        if (item.getTblName() != null) {
                            selectListString.add(item.getTblName() + ".*");
                        } else {
                            selectListString.add("*");
                        }
                    } else if (item.getExpr() != null) {
                        Expr expr = item.getExpr();
                        String str = visit(expr);
                        if (StringUtils.isNotEmpty(item.getAlias())) {
                            str += " AS " + ParseUtil.backquote(item.getAlias());
                        }
                        selectListString.add(str);
                    }
                }
                return selectListString;
            }
        }

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("`" + relation.getName() + "`");

            if (relation.isResolvedInFromClause()) {
                if (relation.getAlias() != null) {
                    sqlBuilder.append(" AS ").append(relation.getAlias().getTbl());
                }
                return sqlBuilder.toString();
            }

            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append(" (")
                        .append(Joiner.on(", ").join(
                                relation.getColumnOutputNames().stream().map(c -> "`" + c + "`").collect(toList())))
                        .append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitSubqueryRelation(SubqueryRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder("(" + visit(node.getQueryStatement()) + ")");

            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(ParseUtil.backquote(node.getAlias().getTbl()));

                if (node.getExplicitColumnNames() != null) {
                    List<String> explicitColNames = new ArrayList<>();
                    node.getExplicitColumnNames().forEach(e -> explicitColNames.add(ParseUtil.backquote(e)));
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(explicitColNames));
                    sqlBuilder.append(")");
                }
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toSql());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toSql());

            if (node.getPartitionNames() != null) {
                List<String> partitionNames = node.getPartitionNames().getPartitionNames();
                if (partitionNames != null && !partitionNames.isEmpty()) {
                    sqlBuilder.append(" PARTITION (");
                    sqlBuilder.append(partitionNames.stream().map(c -> "`" + c + "`")
                            .collect(Collectors.joining(", ")));
                    sqlBuilder.append(")");
                }
            }

            for (TableRelation.TableHint hint : CollectionUtils.emptyIfNull(node.getTableHints())) {
                sqlBuilder.append(" [");
                sqlBuilder.append(hint.name());
                sqlBuilder.append("] ");
            }

            if (node.getSampleClause() != null) {
                TableSampleClause sample = node.getSampleClause();
                sqlBuilder.append(" ").append(sample.toSql());
            }

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias().getTbl()).append("`");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTableFunction(TableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(node.getFunctionName());
            sqlBuilder.append("(");

            List<String> childSql = Optional.ofNullable(node.getChildExpressions())
                    .orElse(Collections.emptyList()).stream().map(this::visit).collect(toList());
            sqlBuilder.append(Joiner.on(",").join(childSql));

            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias().getTbl());

                if (node.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    String names = node.getColumnOutputNames().stream().map(c -> "`" + c + "`")
                            .collect(Collectors.joining(","));
                    sqlBuilder.append(names);
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("TABLE(");

            TableFunctionRelation tableFunction = (TableFunctionRelation) node.getRight();
            sqlBuilder.append(tableFunction.getFunctionName());
            sqlBuilder.append("(");
            sqlBuilder.append(
                    Optional.ofNullable(tableFunction.getChildExpressions())
                            .orElse(Collections.emptyList()).stream().map(this::visit)
                            .collect(Collectors.joining(",")));
            sqlBuilder.append(")");
            sqlBuilder.append(")"); // TABLE(

            if (tableFunction.getAlias() != null) {
                sqlBuilder.append(" ").append(tableFunction.getAlias().getTbl());
                if (tableFunction.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    String names = tableFunction.getColumnOutputNames().stream().map(c -> "`" + c + "`")
                            .collect(Collectors.joining(","));
                    sqlBuilder.append(names);
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitExpression(Expr expr, Void context) {
            return expr.toSql();
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            if (expr.getOriginType().isStructType()) {
                return buildStructColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            } else {
                return buildColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            }
        }

        @Override
        public String visitArrayExpr(ArrayExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
            sb.append(type.toString());
            sb.append('[');
            sb.append(node.getChildren().stream().map(this::visit).collect(Collectors.joining(", ")));
            sb.append(']');
            return sb.toString();
        }

        @Override
        public String visitMapExpr(MapExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
            sb.append(type.toString());
            sb.append("{");
            for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(visit(node.getChild(i)) + ":" + visit(node.getChild(i + 1)));
            }
            sb.append("}");
            return sb.toString();
        }

    }
}
