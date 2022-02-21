// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.StatementBase;
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

import java.util.List;
import java.util.stream.Collectors;

//Used to build sql digests
public class SqlDigestBuilder {
    public static String build(StatementBase statement) {
        return new SqlDigestBuilderVisitor().visit(statement);
    }

    private static class SqlDigestBuilderVisitor extends AstVisitor<String, Void> {
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
                sqlBuilder.append(Joiner.on(", ").join(cteStrings));
            }

            sqlBuilder.append(visit(queryRelation));

            if (queryRelation.hasOrderByClause()) {
                List<OrderByElement> sortClause = queryRelation.getOrderBy();
                sqlBuilder.append(" order by ");
                for (int i = 0; i < sortClause.size(); ++i) {
                    sqlBuilder.append(sortClause.get(i).getExpr().toDigest());
                    sqlBuilder.append((sortClause.get(i).getIsAsc()) ? " asc" : " desc");
                    sqlBuilder.append((i + 1 != sortClause.size()) ? ", " : "");
                }
            }

            // Limit clause.
            if (queryRelation.getLimit() != null) {
                sqlBuilder.append(queryRelation.getLimit().toDigest());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(relation.getName());
            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append("(").append(Joiner.on(", ").join(relation.getColumnOutputNames())).append(")");
            }
            sqlBuilder.append(" AS (").append(visit(new QueryStatement(relation.getCteQuery()))).append(")");
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

            for (int i = 0; i < selectList.getItems().size(); ++i) {
                if (i != 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(selectList.getItems().get(i).toDigest());
            }

            if (stmt.getRelation() != null) {
                sqlBuilder.append(" FROM ");
                sqlBuilder.append(visit(stmt.getRelation()));
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append(" WHERE ");
                sqlBuilder.append(stmt.getWhereClause().toDigest());
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(stmt.getGroupByClause().toSql());
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append(" HAVING ");
                sqlBuilder.append(stmt.getHavingClause().toDigest());
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
            sqlBuilder.append(visit(relation.getRight())).append(" ");
            if (relation.getJoinHint() != null && !relation.getJoinHint().isEmpty()) {
                sqlBuilder.append("[").append(relation.getJoinHint()).append("] ");
            }
            if (relation.getUsingColNames() != null) {
                sqlBuilder.append("USING (").append(Joiner.on(", ").join(relation.getUsingColNames())).append(")");
            }
            if (relation.getOnPredicate() != null) {
                sqlBuilder.append("ON ").append(relation.getOnPredicate().toSql());
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
                    sqlBuilder.append("UNION");
                } else if (relation instanceof ExceptRelation) {
                    sqlBuilder.append("EXCEPT");
                } else {
                    sqlBuilder.append("INTERSECT");
                }

                sqlBuilder.append(" ").append(relation.getQualifier() == SetQualifier.ALL ? "ALL" : "");

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

            if (node.getAlias() != null) {
                sqlBuilder.append(node.getAlias());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("VALUES(");

            for (int i = 0; i < node.getRows().size(); ++i) {
                sqlBuilder.append("(");
                sqlBuilder.append(node.getRows().get(i));
                sqlBuilder.append(")");
            }
            sqlBuilder.append(")");
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
            return expr.toDigest();
        }
    }
}
