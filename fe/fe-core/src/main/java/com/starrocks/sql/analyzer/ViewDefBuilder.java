// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ViewDefBuilder {
    public static String build(StatementBase statement) {
        return new ViewDefBuilderVisitor().visit(statement);
    }

    private static class ViewDefBuilderVisitor extends AST2SQL.SQLBuilder {
<<<<<<< HEAD
=======

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

            res += '`' + fieldName + '`';
            if (!fieldName.equalsIgnoreCase(columnName)) {
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
            columnName = handleColumnName(columnName);

            res += fieldName;
            if (!fieldName.equalsIgnoreCase(columnName)) {
                res += " AS " + columnName;
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

>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
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
                String columnName = stmt.getColumnOutputNames().get(i);

                if (expr instanceof FieldReference) {
                    Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
<<<<<<< HEAD
                    selectListString.add(
                            (field.getRelationAlias() == null ? "" : field.getRelationAlias().toSql() + ".")
                                    + "`" + field.getName() + "`" + " AS `" + columnName + "`");
=======
                    selectListString.add(buildColumnName(field.getRelationAlias(), field.getName(), columnName));
                } else if (expr instanceof SlotRef) {
                    SlotRef slot = (SlotRef) expr;
                    if (slot.getOriginType().isStructType()) {
                        selectListString.add(buildStructColumnName(slot.getTblNameWithoutAnalyzed(),
                                slot.getColumnName(), columnName));
                    } else {
                        selectListString.add(buildColumnName(slot.getTblNameWithoutAnalyzed(), slot.getColumnName(),
                                columnName));
                    }
>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
                } else {
                    selectListString.add(visit(expr) + " AS `" + columnName + "`");
                }
            }

            sqlBuilder.append(Joiner.on(", ").join(selectListString));

            String fromClause = visit(stmt.getRelation());
            if (fromClause != null) {
                sqlBuilder.append(" FROM ");
                sqlBuilder.append(fromClause);
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append(" WHERE ");
                sqlBuilder.append(visit(stmt.getWhereClause()));
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(visit(stmt.getGroupByClause()));
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append(" HAVING ");
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
<<<<<<< HEAD
            return expr.toSql();
=======
            if (expr.getOriginType().isStructType()) {
                return buildStructColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            } else {
                return buildColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            }
>>>>>>> e3b6d66c4 ([BugFix] Fix when output has duplicate item, order by works on wrong column-ref (#13754))
        }
    }
}
