// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.Subquery;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.SQLLabelBuilder.toSQL;

public class ViewDefBuilder {
    public static String build(StatementBase statement, ConnectContext session) {
        return new ViewDefBuilderVisitor(session).visit(statement);
    }

    private static class ViewDefBuilderVisitor extends AstVisitor<String, Void> {
        private ConnectContext session;

        public ViewDefBuilderVisitor(ConnectContext session) {
            this.session = session;
        }

        @Override
        public String visitNode(ParseNode node, Void context) {
            return "";
        }

        @Override
        public String visitQueryStatement(QueryStatement stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            QueryRelation queryRelation = stmt.getQueryRelation();

            if (queryRelation.hasWithClause()) {
                sqlBuilder.append("WITH ");
                List<String> cteStrings =
                        queryRelation.getCteRelations().stream().map(this::visit).collect(Collectors.toList());
                sqlBuilder.append(Joiner.on(",").join(cteStrings));
            }

            sqlBuilder.append(visit(queryRelation));

            if (queryRelation.hasOrderByClause()) {
                List<OrderByElement> sortClause = queryRelation.getOrderBy();
                sqlBuilder.append(" order by ");
                for (int i = 0; i < sortClause.size(); ++i) {
                    sqlBuilder.append(visit(sortClause.get(i).getExpr()));
                    sqlBuilder.append((sortClause.get(i).getIsAsc()) ? " asc" : " desc");
                    sqlBuilder.append((i + 1 != sortClause.size()) ? ", " : "");
                }
            }

            // Limit clause.
            if (queryRelation.getLimit() != null) {
                sqlBuilder.append(queryRelation.getLimit().toSql());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(relation.getName());

            if (relation.isResolvedInFromClause()) {
                if (relation.getAliasWithoutNameRewrite() != null) {
                    sqlBuilder.append(" AS ").append(relation.getAliasWithoutNameRewrite());
                }
                return sqlBuilder.toString();
            }

            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append("(").append(Joiner.on(", ").join(relation.getColumnOutputNames())).append(")");
            }
            sqlBuilder.append(" AS (").append(visit(new QueryStatement(relation.getCteQuery()))).append(") ");
            return sqlBuilder.toString();
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
                        selectListString.add(visit(expr) + " AS `" + toSQL(expr) + "`");
                    }
                } else {
                    if (item.getAlias() != null) {
                        selectListString.add((visit(item.getExpr()) + " AS `" + item.getAlias()) + "`");
                    } else {
                        selectListString.add(visit(item.getExpr()) + " AS `" + toSQL(item.getExpr()) + "`");
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
        public String visitSubquery(SubqueryRelation subquery, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("(");
            sqlBuilder.append(visit(new QueryStatement(subquery.getQuery())));
            sqlBuilder.append(")");
            sqlBuilder.append(" ").append(subquery.getAlias());
            return sqlBuilder.toString();
        }

        @Override
        public String visitJoin(JoinRelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(visit(relation.getLeft())).append(" ");
            sqlBuilder.append(relation.getType());
            if (relation.getJoinHint() != null && !relation.getJoinHint().isEmpty()) {
                sqlBuilder.append(" [").append(relation.getJoinHint()).append("]");
            }
            sqlBuilder.append(" ");
            sqlBuilder.append(visit(relation.getRight())).append(" ");

            if (relation.getUsingColNames() != null) {
                sqlBuilder.append("USING (").append(Joiner.on(", ").join(relation.getUsingColNames())).append(")");
            }
            if (relation.getOnPredicate() != null) {
                sqlBuilder.append("ON ").append(visit(relation.getOnPredicate()));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitUnion(UnionRelation relation, Void context) {
            return processSetOp(relation);
        }

        @Override
        public String visitExcept(ExceptRelation relation, Void context) {
            return processSetOp(relation);
        }

        @Override
        public String visitIntersect(IntersectRelation relation, Void context) {
            return processSetOp(relation);
        }

        private String processSetOp(SetOperationRelation relation) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(visit(relation.getRelations().get(0)));

            for (int i = 1; i < relation.getRelations().size(); ++i) {
                if (relation instanceof UnionRelation) {
                    sqlBuilder.append(" UNION ");
                } else if (relation instanceof ExceptRelation) {
                    sqlBuilder.append(" EXCEPT ");
                } else {
                    sqlBuilder.append(" INTERSECT ");
                }

                sqlBuilder.append(relation.getQualifier() == SetQualifier.ALL ? "ALL " : "");

                Relation setChildRelation = relation.getRelations().get(i);
                if (setChildRelation instanceof SetOperationRelation) {
                    sqlBuilder.append("(");
                }
                sqlBuilder.append(visit(setChildRelation));
                if (setChildRelation instanceof SetOperationRelation) {
                    sqlBuilder.append(")");
                }
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName());

            if (node.getAliasWithoutNameRewrite() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append(node.getAliasWithoutNameRewrite());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (node.getRows().size() == 1) {
                sqlBuilder.append("SELECT ");
                List<String> fieldLis = Lists.newArrayList();
                for (int i = 0; i < node.getRows().get(0).size(); ++i) {
                    String field = visit(node.getRows().get(0).get(i));
                    String alias = " AS `" + node.getColumnOutputNames().get(i) + "`";
                    fieldLis.add(field + alias);
                }

                sqlBuilder.append(Joiner.on(", ").join(fieldLis));
            } else {
                sqlBuilder.append("VALUES(");

                for (int i = 0; i < node.getRows().size(); ++i) {
                    sqlBuilder.append("(");
                    List<String> rowStrings =
                            node.getRows().get(i).stream().map(Expr::toSql).collect(Collectors.toList());
                    sqlBuilder.append(Joiner.on(", ").join(rowStrings));
                    sqlBuilder.append(")");
                }
                sqlBuilder.append(")");
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTableFunction(TableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(node.getFunctionName());
            sqlBuilder.append("(");
            sqlBuilder.append(node.getFunctionParams());
            sqlBuilder.append(")");
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

        @Override
        public String visitExistsPredicate(ExistsPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            if (node.isNotExists()) {
                strBuilder.append("NOT ");

            }
            strBuilder.append("EXISTS ");
            strBuilder.append(visit(node.getChild(0)));
            return strBuilder.toString();
        }

        @Override
        public String visitInPredicate(InPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            String notStr = (node.isNotIn()) ? "NOT " : "";
            strBuilder.append(visit(node.getChild(0))).append(" ").append(notStr).append("IN (");
            for (int i = 1; i < node.getChildren().size(); ++i) {
                strBuilder.append(node.getChild(i).toSql());
                strBuilder.append((i + 1 != node.getChildren().size()) ? ", " : "");
            }
            strBuilder.append(")");
            return strBuilder.toString();
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicate node, Void context) {
            return visit(node.getChild(0)) + " " + node.getOp().toString() + " " + visit(node.getChild(1));
        }

        @Override
        public String visitSubquery(Subquery subquery, Void context) {
            return "(" + visit(new QueryStatement(subquery.getQueryBlock())) + ")";
        }
    }
}
