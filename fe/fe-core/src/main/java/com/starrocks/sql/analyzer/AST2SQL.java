// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SysVariableDesc;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FieldReference;
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
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AST2SQL {
    public static String toString(ParseNode expr) {
        return new SQLBuilder().visit(expr);
    }

    public static class SQLBuilder extends AstVisitor<String, Void> {
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
                sqlBuilder.append(" ORDER BY ").append(visitAstList(sortClause)).append(" ");
            }

            // Limit clause.
            if (queryRelation.getLimit() != null) {
                sqlBuilder.append(visit(queryRelation.getLimit()));
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
                        .append(Joiner.on(", ").join(relation.getColumnOutputNames())).append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            SelectList selectList = stmt.getSelectList();
            sqlBuilder.append("SELECT ");
            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }

            for (int i = 0; i < selectList.getItems().size(); ++i) {
                if (i != 0) {
                    sqlBuilder.append(", ");
                }

                String selectItemLabel;
                SelectListItem item = selectList.getItems().get(i);
                if (!item.isStar()) {
                    String aliasSql = null;
                    if (item.getAlias() != null) {
                        aliasSql = "AS " + item.getAlias();
                    }
                    selectItemLabel = visit(item.getExpr()) + ((aliasSql == null) ? "" : " " + aliasSql);
                } else if (item.getTblName() != null) {
                    selectItemLabel = item.getTblName().toString() + ".*";
                } else {
                    selectItemLabel = "*";
                }

                sqlBuilder.append(selectItemLabel);
            }

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
        public String visitSubquery(SubqueryRelation subquery, Void context) {
            return "(" + visit(subquery.getQueryStatement()) + ")" + " " + subquery.getAlias();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(node.getName().toSql());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append(node.getAlias());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitJoin(JoinRelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(visit(relation.getLeft())).append(" ");
            if (relation.isImplicit()) {
                sqlBuilder.append(",");
            } else {
                sqlBuilder.append(relation.getJoinOp());
            }
            if (relation.getJoinHint() != null && !relation.getJoinHint().isEmpty()) {
                sqlBuilder.append(" [").append(relation.getJoinHint()).append("]");
            }
            sqlBuilder.append(" ");
            if (relation.isLateral()) {
                sqlBuilder.append("LATERAL ");
            }
            sqlBuilder.append(visit(relation.getRight())).append(" ");

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
            sqlBuilder.append(node.getName().toString());

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append(node.getAlias());
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (node.isNullValues()) {
                return null;
            }

            sqlBuilder.append("(VALUES");
            List<String> values = new ArrayList<>();
            for (int i = 0; i < node.getRows().size(); ++i) {
                StringBuilder rowBuilder = new StringBuilder();
                rowBuilder.append("(");
                List<String> rowStrings =
                        node.getRows().get(i).stream().map(this::visit).collect(Collectors.toList());
                rowBuilder.append(Joiner.on(", ").join(rowStrings));
                rowBuilder.append(")");
                values.add(rowBuilder.toString());
            }
            sqlBuilder.append(Joiner.on(", ").join(values));
            sqlBuilder.append(") ").append(node.getAlias());

            return sqlBuilder.toString();
        }

        @Override
        public String visitTableFunction(TableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(node.getFunctionName());
            sqlBuilder.append("(");

            List<String> childSql = node.getChildExpressions().stream().map(this::visit).collect(toList());
            sqlBuilder.append(Joiner.on(",").join(childSql));

            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias());
            }
            return sqlBuilder.toString();
        }

        // ---------------------------------- Expression --------------------------------

        @Override
        public String visitExpression(Expr node, Void context) {
            return node.toSql();
        }

        @Override
        public String visitArithmeticExpr(ArithmeticExpr node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();

            if (node.getChildren().size() == 1) {
                sqlBuilder.append(node.getOp());
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
            } else {
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
                sqlBuilder.append(" ").append(node.getOp()).append(" ");
                sqlBuilder.append(printWithParentheses(node.getChild(1)));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitAnalyticExpr(AnalyticExpr node, Void context) {
            FunctionCallExpr fnCall = node.getFnCall();
            List<Expr> partitionExprs = node.getPartitionExprs();
            List<OrderByElement> orderByElements = node.getOrderByElements();
            AnalyticWindow window = node.getWindow();

            StringBuilder sb = new StringBuilder();
            sb.append(visit(fnCall)).append(" OVER (");
            if (!partitionExprs.isEmpty()) {
                sb.append("PARTITION BY ").append(visitAstList(partitionExprs)).append(" ");
            }
            if (!orderByElements.isEmpty()) {
                sb.append("ORDER BY ").append(visitAstList(orderByElements)).append(" ");
            }
            if (window != null) {
                sb.append(window.toSql());
            }
            sb.append(")");
            return sb.toString();
        }

        public String visitArrayExpr(ArrayExpr node, Void context) {
            boolean explicitType = node.isExplicitType();

            StringBuilder sb = new StringBuilder();
            if (explicitType) {
                sb.append(node.getType().toString());
            }
            sb.append('[');
            sb.append(visitAstList(node.getChildren()));
            sb.append(']');
            return sb.toString();
        }

        @Override
        public String visitArrayElementExpr(ArrayElementExpr node, Void context) {
            return visit(node.getChild(0)) + "[" + visit(node.getChild(1)) + "]";
        }

        public String visitArrowExpr(ArrowExpr node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitBetweenPredicate(BetweenPredicate node, Void context) {
            String notStr = (node.isNotBetween()) ? "NOT " : "";
            return printWithParentheses(node.getChild(0)) + " " + notStr + "BETWEEN " +
                    printWithParentheses(node.getChild(1)) + " AND " + printWithParentheses(node.getChild(2));
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicate node, Void context) {
            return printWithParentheses(node.getChild(0)) +
                    " " + node.getOp() + " " +
                    printWithParentheses(node.getChild(1));
        }

        @Override
        public String visitCaseWhenExpr(CaseExpr node, Void context) {
            boolean hasCaseExpr = node.hasCaseExpr();
            boolean hasElseExpr = node.hasElseExpr();
            StringBuilder output = new StringBuilder("CASE");
            int childIdx = 0;
            if (hasCaseExpr) {
                output.append(" ").append(printWithParentheses(node.getChild(childIdx++)));
            }
            while (childIdx + 2 <= node.getChildren().size()) {
                output.append(" WHEN ").append(printWithParentheses(node.getChild(childIdx++)));
                output.append(" THEN ").append(printWithParentheses(node.getChild(childIdx++)));
            }
            if (hasElseExpr) {
                output.append(" ELSE ").append(printWithParentheses(node.getChild(node.getChildren().size() - 1)));
            }
            output.append(" END");
            return output.toString();
        }

        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            boolean isImplicit = node.isImplicit();
            if (isImplicit) {
                return visit(node.getChild(0));
            }
            return "CAST(" + printWithParentheses(node.getChild(0)) + " AS " + node.getTargetTypeDef().toString() + ")";
        }

        public String visitCompoundPredicate(CompoundPredicate node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (CompoundPredicate.Operator.NOT.equals(node.getOp())) {
                sqlBuilder.append("NOT ");
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
            } else {
                sqlBuilder.append(printWithParentheses(node.getChild(0)));
                sqlBuilder.append(" ").append(node.getOp()).append(" ");
                sqlBuilder.append(printWithParentheses(node.getChild(1)));
            }
            return sqlBuilder.toString();
        }

        public String visitDefaultValueExpr(DefaultValueExpr node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitExistsPredicate(ExistsPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            if (node.isNotExists()) {
                strBuilder.append("NOT ");

            }
            strBuilder.append("EXISTS ");
            strBuilder.append(printWithParentheses(node.getChild(0)));
            return strBuilder.toString();
        }

        public String visitFieldReference(FieldReference node, Void context) {
            return String.valueOf(node.getFieldIndex() + 1);
        }

        @Override
        public String visitFunctionCall(FunctionCallExpr node, Void context) {
            FunctionParams fnParams = node.getParams();
            StringBuilder sb = new StringBuilder();
            sb.append(node.getFnName().getFunction());

            sb.append("(");
            if (fnParams.isStar()) {
                sb.append("*");
            }
            if (fnParams.isDistinct()) {
                sb.append("DISTINCT ");
            }
            List<String> p = node.getChildren().stream().map(this::visit).collect(Collectors.toList());
            sb.append(Joiner.on(", ").join(p)).append(")");
            return sb.toString();
        }

        public String visitGroupingFunctionCall(GroupingFunctionCallExpr node, Void context) {
            return visitExpression(node, context);
        }

        public String visitInformationFunction(InformationFunction node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitInPredicate(InPredicate node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            String notStr = (node.isNotIn()) ? "NOT " : "";
            strBuilder.append(printWithParentheses(node.getChild(0))).append(" ").append(notStr).append("IN (");
            for (int i = 1; i < node.getChildren().size(); ++i) {
                strBuilder.append(printWithParentheses(node.getChild(i)));
                strBuilder.append((i + 1 != node.getChildren().size()) ? ", " : "");
            }
            strBuilder.append(")");
            return strBuilder.toString();
        }

        public String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return printWithParentheses(node.getChild(0)) + (node.isNotNull() ? " IS NOT NULL" : " IS NULL");
        }

        public String visitLikePredicate(LikePredicate node, Void context) {
            return printWithParentheses(node.getChild(0))
                    + " " + node.getOp() + " " + printWithParentheses(node.getChild(1));
        }

        public String visitLiteral(LiteralExpr node, Void context) {
            if (node instanceof DecimalLiteral) {
                if ((((DecimalLiteral) node).getValue().scale() == 0)) {
                    return ((DecimalLiteral) node).getValue().toString() + "E0";
                } else {
                    return visitExpression(node, context);
                }
            } else {
                return visitExpression(node, context);
            }
        }

        public String visitSlot(SlotRef node, Void context) {
            return node.getColumnName();
        }

        public String visitSubquery(Subquery node, Void context) {
            return "(" + visit(node.getQueryStatement()) + ")";
        }

        public String visitSysVariableDesc(SysVariableDesc node, Void context) {
            return visitExpression(node, context);
        }

        @Override
        public String visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
            String funcName = node.getFuncName();
            String timeUnitIdent = node.getTimeUnitIdent();
            boolean intervalFirst = node.isIntervalFirst();
            ArithmeticExpr.Operator op = node.getOp();

            StringBuilder strBuilder = new StringBuilder();
            if (funcName != null) {
                if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                    strBuilder.append(funcName).append("(");
                    strBuilder.append(timeUnitIdent).append(", ");
                    strBuilder.append(visit(node.getChild(1))).append(", ");
                    strBuilder.append(visit(node.getChild(0))).append(")");
                    return strBuilder.toString();
                }
                // Function-call like version.
                strBuilder.append(funcName).append("(");
                strBuilder.append(visit(node.getChild(0))).append(", ");
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1)));
                strBuilder.append(" ").append(timeUnitIdent);
                strBuilder.append(")");
                return strBuilder.toString();
            }
            if (intervalFirst) {
                // Non-function-call like version with interval as first operand.
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1))).append(" ");
                strBuilder.append(timeUnitIdent);
                strBuilder.append(" ").append(op.toString()).append(" ");
                strBuilder.append(visit(node.getChild(0)));
            } else {
                // Non-function-call like version with interval as second operand.
                strBuilder.append(visit(node.getChild(0)));
                strBuilder.append(" ").append(op.toString()).append(" ");
                strBuilder.append("INTERVAL ");
                strBuilder.append(visit(node.getChild(1))).append(" ");
                strBuilder.append(timeUnitIdent);
            }
            return strBuilder.toString();
        }

        // ----------------- AST ---------------
        @Override
        public String visitLimitElement(LimitElement node, Void context) {
            if (node.getLimit() == -1) {
                return "";
            }
            StringBuilder sb = new StringBuilder(" LIMIT ");
            if (node.getOffset() != 0) {
                sb.append(node.getOffset()).append(", ");
            }
            sb.append(node.getLimit());
            return sb.toString();
        }

        @Override
        public String visitOrderByElement(OrderByElement node, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(visit(node.getExpr()));
            strBuilder.append(node.getIsAsc() ? " ASC" : " DESC");

            // When ASC and NULLS FIRST or DESC and NULLS LAST, we do not print NULLS FIRST/LAST
            // because it is the default behavior
            if (node.getNullsFirstParam() != null) {
                if (node.getIsAsc() && !node.getNullsFirstParam()) {
                    // If ascending, nulls are first by default, so only add if nulls last.
                    strBuilder.append(" NULLS LAST");
                } else if (!node.getIsAsc() && node.getNullsFirstParam()) {
                    // If descending, nulls are last by default, so only add if nulls first.
                    strBuilder.append(" NULLS FIRST");
                }
            }
            return strBuilder.toString();
        }

        @Override
        public String visitGroupByClause(GroupByClause node, Void context) {
            GroupByClause.GroupingType groupingType = node.getGroupingType();
            List<ArrayList<Expr>> groupingSetList = node.getGroupingSetList();
            List<Expr> oriGroupingExprs = node.getOriGroupingExprs();

            StringBuilder strBuilder = new StringBuilder();
            switch (groupingType) {
                case GROUP_BY:
                    if (oriGroupingExprs != null) {
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                    }
                    break;
                case GROUPING_SETS:
                    if (groupingSetList != null) {
                        strBuilder.append("GROUPING SETS (");
                        boolean first = true;
                        for (List<Expr> groupingExprs : groupingSetList) {
                            if (first) {
                                strBuilder.append("(");
                                first = false;
                            } else {
                                strBuilder.append(", (");
                            }
                            for (int i = 0; i < groupingExprs.size(); ++i) {
                                strBuilder.append(visit(groupingExprs.get(i)));
                                strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                            }
                            strBuilder.append(")");
                        }
                        strBuilder.append(")");
                    }
                    break;
                case CUBE:
                    if (oriGroupingExprs != null) {
                        strBuilder.append("CUBE (");
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    break;
                case ROLLUP:
                    if (oriGroupingExprs != null) {
                        strBuilder.append("ROLLUP (");
                        for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                            strBuilder.append(visit(oriGroupingExprs.get(i)));
                            strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    break;
                default:
                    break;
            }
            return strBuilder.toString();
        }

        private String visitAstList(List<? extends ParseNode> contexts) {
            return Joiner.on(", ").join(contexts.stream().map(this::visit).collect(toList()));
        }

        private String printWithParentheses(ParseNode node) {
            if (node instanceof SlotRef || node instanceof LiteralExpr) {
                return visit(node);
            } else {
                return "(" + visit(node) + ")";
            }
        }
    }
}
