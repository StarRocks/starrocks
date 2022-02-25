// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.sql.ast.SelectRelation;

import java.util.ArrayList;
import java.util.List;

public class ViewDefBuilder {
    public static String build(StatementBase statement) {
        return new ViewDefBuilderVisitor().visit(statement);
    }

    private static class ViewDefBuilderVisitor extends AST2SQL.SQLLabelBuilderImpl {
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
                sqlBuilder.append("DISTINCT");
            }

            List<String> selectListString = new ArrayList<>();
            for (int i = 0; i < selectList.getItems().size(); ++i) {
                SelectListItem item = selectList.getItems().get(i);
                if (item.isStar()) {
                    List<Expr> outputExpression = SelectAnalyzer.expandStar(item, stmt.getRelation());
                    for (Expr expr : outputExpression) {
                        selectListString.add(visit(expr) + " AS `" + AST2SQL.toString(expr) + "`");
                    }
                } else {
                    if (item.getAlias() != null) {
                        selectListString.add((visit(item.getExpr()) + " AS `" + item.getAlias()) + "`");
                    } else {
                        selectListString.add(visit(item.getExpr()) + " AS `" + AST2SQL.toString(item.getExpr()) + "`");
                    }
                }
            }
            sqlBuilder.append(Joiner.on(", ").join(selectListString));

            if (stmt.getRelation() != null) {
                sqlBuilder.append(" FROM ");
                sqlBuilder.append(visit(stmt.getRelation()));
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append(" WHERE ");
                sqlBuilder.append(visit(stmt.getWhereClause()));
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(stmt.getGroupByClause().toSql());
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append(" HAVING ");
                sqlBuilder.append(visit(stmt.getHavingClause()));
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitExpression(Expr expr, Void context) {
            return expr.toSql();
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            return expr.toSql();
        }
    }
}
