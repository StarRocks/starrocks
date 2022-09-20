// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * AggregationAnalyzer is used to analyze aggregation
 */
public class AggregationAnalyzer {
    private final ConnectContext session;
    private final AnalyzeState analyzeState;

    /**
     * All grouping expression field
     */
    private final Set<FieldId> groupingFields;

    /**
     * All grouping expression in this SQL.
     * If the expression being verify is equal to any expr
     * in groupingExpressions is considered to be valid
     */
    private final List<Expr> groupingExpressions;

    private final Scope sourceScope;

    private final Scope orderByScope;

    public void verify(List<Expr> expressions) {
        expressions.forEach(this::analyze);
    }

    public AggregationAnalyzer(ConnectContext session, AnalyzeState analyzeState, List<Expr> groupingExpressions,
                               Scope sourceScope, Scope orderByScope) {
        this.session = session;
        this.sourceScope = sourceScope;
        this.orderByScope = orderByScope;
        this.analyzeState = analyzeState;
        this.groupingExpressions = groupingExpressions;
        this.groupingFields = groupingExpressions.stream()
                .filter(analyzeState.getColumnReferences()::containsKey)
                .map(analyzeState.getColumnReferences()::get)
                .collect(toImmutableSet());
    }

    private void analyze(Expr expression) {
        if (!new VerifyExpressionVisitor().visit(expression)) {
            throw new SemanticException("'%s' must be an aggregate expression or appear in GROUP BY clause",
                    expression.toSql());
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class VerifyExpressionVisitor extends AstVisitor<Boolean, Void> {
        @Override
        public Boolean visit(ParseNode expr) {
            if (groupingExpressions.stream().anyMatch(expr::equals)) {
                return true;
            }
            return super.visit(expr);
        }

        @Override
        public Boolean visitExpression(Expr node, Void context) {
            throw new SemanticException("%s is not support in GROUP BY clause", node.toSql());
        }

        private boolean isGroupingKey(Expr node) {
            /*
             * A normalization process is needed here
             * to ensure that equal expressions can be parsed correctly
             */
            FieldId fieldId = analyzeState.getColumnReferences().get(node);
            if (orderByScope != null &&
                    Objects.equals(fieldId.getRelationId(), orderByScope.getRelationId())) {
                return true;
            }

            if (groupingFields.contains(fieldId)) {
                return true;
            } else if (!SqlModeHelper.check(session.getSessionVariable().getSqlMode(),
                    SqlModeHelper.MODE_ONLY_FULL_GROUP_BY)) {
                if (!analyzeState.getColumnNotInGroupBy().contains(node)) {
                    analyzeState.getColumnNotInGroupBy().add(node);
                }
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitArithmeticExpr(ArithmeticExpr node, Void context) {
            return visit(node.getChild(0)) && visit(node.getChild(1));
        }

        @Override
        public Boolean visitAnalyticExpr(AnalyticExpr node, Void context) {
            if (!node.getFnCall().getChildren().stream().allMatch(this::visit)) {
                return false;
            }

            if (!node.getOrderByElements().stream().map(OrderByElement::getExpr).allMatch(this::visit)) {
                return false;
            }

            return node.getPartitionExprs().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitArrayExpr(ArrayExpr node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitArrayElementExpr(ArrayElementExpr node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitArrowExpr(ArrowExpr node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitBetweenPredicate(BetweenPredicate node, Void context) {
            return visit(node.getChild(0)) && visit(node.getChild(1)) && visit(node.getChild(2));
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicate node, Void context) {
            return visit(node.getChild(0)) && visit(node.getChild(1));
        }

        @Override
        public Boolean visitCaseWhenExpr(CaseExpr node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitCastExpr(CastExpr node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitCompoundPredicate(CompoundPredicate node, Void context) {
            if (node.getOp() == CompoundPredicate.Operator.NOT) {
                return visit(node.getChild(0));
            } else {
                return visit(node.getChild(0)) && visit(node.getChild(1));
            }
        }

        @Override
        public Boolean visitExistsPredicate(ExistsPredicate node, Void context) {
            List<Subquery> subqueries = Lists.newArrayList();
            node.collect(Subquery.class, subqueries);
            Preconditions.checkState(subqueries.size() == 1, "Exist must have exact one subquery");
            return visit(subqueries.get(0));
        }

        @Override
        public Boolean visitFunctionCall(FunctionCallExpr expr, Void context) {
            if (expr.getFn() instanceof AggregateFunction) {
                List<FunctionCallExpr> aggFunc = Lists.newArrayList();
                if (expr.getChildren().stream().anyMatch(childExpr -> {
                    childExpr.collectAll((Predicate<Expr>) arg -> arg instanceof FunctionCallExpr &&
                            arg.getFn() instanceof AggregateFunction, aggFunc);
                    return !aggFunc.isEmpty();
                })) {
                    throw new SemanticException("Cannot nest aggregations inside aggregation '%s'", expr.toSql());
                }

                if (expr.getChildren().stream().anyMatch(childExpr -> {
                    childExpr.collectAll((Predicate<Expr>) arg -> arg instanceof AnalyticExpr, aggFunc);
                    return !aggFunc.isEmpty();
                })) {
                    throw new SemanticException("Cannot nest window function inside aggregation '%s'", expr.toSql());
                }

                return true;
            }
            return expr.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitGroupingFunctionCall(GroupingFunctionCallExpr node, Void context) {
            if (orderByScope != null) {
                throw new SemanticException("Grouping operations are not allowed in order by");
            }

            if (node.getChildren().stream().anyMatch(argument ->
                    !analyzeState.getColumnReferences().containsKey(argument) || !isGroupingKey(argument))) {
                throw new SemanticException("The arguments to GROUPING must be expressions referenced by GROUP BY");
            }

            return true;
        }

        @Override
        public Boolean visitInformationFunction(InformationFunction node, Void context) {
            return true;
        }

        @Override
        public Boolean visitInPredicate(InPredicate node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitIsNullPredicate(IsNullPredicate node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitLikePredicate(LikePredicate node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitLiteral(LiteralExpr node, Void context) {
            return true;
        }

        @Override
        public Boolean visitSlot(SlotRef node, Void context) {
            return isGroupingKey(node);
        }

        @Override
        public Boolean visitSubquery(Subquery node, Void context) {
            QueryStatement queryStatement = node.getQueryStatement();
            List<FieldId> fieldIds = queryStatement.getQueryRelation().getColumnReferences().values().stream()
                    .filter(fieldId -> fieldId.getRelationId().equals(sourceScope.getRelationId()))
                    .collect(Collectors.toList());

            return groupingFields.containsAll(fieldIds);
        }

        @Override
        public Boolean visitVariableExpr(VariableExpr node, Void context) {
            return true;
        }

        @Override
        public Boolean visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
            return visit(node.getChild(0)) && visit(node.getChild(1));
        }

        @Override
        public Boolean visitCloneExpr(CloneExpr node, Void context) {
            return visit(node.getChild(0));
        }
    }
}
