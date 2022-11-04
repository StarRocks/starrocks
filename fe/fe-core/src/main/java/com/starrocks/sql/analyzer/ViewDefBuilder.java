// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class ViewDefBuilder {

    public static String buildSimple(StatementBase statement) {
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(statement);
        boolean sameCatalogDb = tables.keySet().stream().map(TableName::getCatalogAndDb).distinct().count() == 1;
        return new ViewDefBuilderVisitor(sameCatalogDb).visit(statement);
    }

    public static String build(StatementBase statement) {
        return new ViewDefBuilderVisitor(false).visit(statement);
    }

    private static class ViewDefBuilderVisitor extends AST2SQL.SQLBuilder {

        private final boolean simple;

        public ViewDefBuilderVisitor(boolean simple) {
            this.simple = simple;
        }

        private String buildColumnName(TableName tableName, String fieldName, String columnName) {
            String res = "";
            if (tableName != null) {
                if (!simple) {
                    res = tableName.toSql();
                } else {
                    res = "`" + tableName.getTbl() + "`";
                }
                res += ".";
            }
            res += "`" + fieldName + "`";
            if (!fieldName.equalsIgnoreCase(columnName)) {
                res += " AS `" + columnName + "`";
            }
            return res;
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
            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }

            List<String> selectListString = new ArrayList<>();
            for (int i = 0; i < stmt.getOutputExpr().size(); ++i) {
                Expr expr = stmt.getOutputExpr().get(i);
                String columnName = stmt.getScope().getRelationFields().getFieldByIndex(i).getName();

                if (expr instanceof FieldReference) {
                    Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
                    selectListString.add(buildColumnName(field.getRelationAlias(), field.getName(), columnName));
                } else if (expr instanceof SlotRef) {
                    SlotRef slot = (SlotRef) expr;
                    selectListString.add(buildColumnName(slot.getTblNameWithoutAnalyzed(), slot.getColumnName(), columnName));
                } else {
                    selectListString.add(visit(expr) + " AS `" + columnName + "`");
                }
            }

            sqlBuilder.append(Joiner.on(", ").join(selectListString));

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

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(relation.getName());

            if (relation.isResolvedInFromClause()) {
                if (relation.getAlias() != null) {
                    sqlBuilder.append(" AS ").append(relation.getAlias());
                }
                return sqlBuilder.toString();
            }

            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append("(")
                        .append(Joiner.on(", ").join(
                                relation.getColumnOutputNames().stream().map(c -> "`" + c + "`").collect(toList())))
                        .append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toSql());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias()).append("`");
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
                    sqlBuilder.append(" PARTITION(");
                }
                for (String partitionName : partitionNames) {
                    sqlBuilder.append("`").append(partitionName).append("`").append(",");
                }
                sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
                sqlBuilder.append(")");
            }
            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("`").append(node.getAlias()).append("`");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitExpression(Expr expr, Void context) {
            return expr.toSql();
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            return buildColumnName(expr.getTblNameWithoutAnalyzed(), expr.getColumnName(), expr.getColumnName());
        }
    }
}
