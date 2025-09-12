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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CloneExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DictQueryExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.GroupingFunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.VariableExpr;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

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
            throw new SemanticException(PARSER_ERROR_MSG.shouldBeAggFunc(expression.toSql()), expression.getPos());
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class VerifyExpressionVisitor implements AstVisitorExtendInterface<Boolean, Void> {
        @Override
        public Boolean visit(ParseNode expr) {
            if (groupingExpressions.stream().anyMatch(expr::equals)) {
                return true;
            }
            return expr.accept(this, null);
        }

        @Override
        public Boolean visitFieldReference(FieldReference node, Void context) {
            String colInfo = node.getTblName() == null ? "column" : "column of " + node.getTblName().toString();
            throw new SemanticException(colInfo + " must appear in the GROUP BY clause or be used in an aggregate function",
                    node.getPos());
        }

        @Override
        public Boolean visitExpression(Expr node, Void context) {
            if (node instanceof Parameter) {
                return true;
            }
            throw new SemanticException(node.toSql() + " must appear in the GROUP BY clause or be used in an aggregate function",
                    node.getPos());
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

        // only check lambda body here.
        @Override
        public Boolean visitLambdaFunctionExpr(LambdaFunctionExpr node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitCollectionElementExpr(CollectionElementExpr node, Void context) {
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
            if (subqueries.size() != 1) {
                throw new SemanticException(PARSER_ERROR_MSG.canOnlyOneExistSub(), node.getPos());
            }
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
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedNestAgg("aggregation function"), expr.getPos());
                }

                if (expr.getChildren().stream().anyMatch(childExpr -> {
                    childExpr.collectAll((Predicate<Expr>) arg -> arg instanceof AnalyticExpr, aggFunc);
                    return !aggFunc.isEmpty();
                })) {
                    throw new SemanticException(PARSER_ERROR_MSG.unsupportedNestAgg("window function"), expr.getPos());
                }

                return true;
            }
            return expr.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitGroupingFunctionCall(GroupingFunctionCallExpr node, Void context) {
            if (orderByScope != null) {
                throw new SemanticException(PARSER_ERROR_MSG.unsupportedExprWithInfo(node.toSql(), "ORDER BY"),
                        node.getPos());
            }

            if (node.getChildren().stream().anyMatch(argument -> !analyzeState.getGroupBy().contains(argument))) {
                throw new SemanticException(PARSER_ERROR_MSG.argsCanOnlyFromGroupBy(), node.getPos());
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
        public Boolean visitMatchExpr(MatchExpr node, Void context) {
            return visit(node.getChild(0));
        }

        @Override
        public Boolean visitLiteral(LiteralExpr node, Void context) {
            return true;
        }

        @Override
        public Boolean visitSlot(SlotRef node, Void context) {
            if (node.isFromLambda()) {
                return true;
            }
            return isGroupingKey(node);
        }

        @Override
        public Boolean visitSubqueryExpr(Subquery node, Void context) {
            QueryStatement queryStatement = node.getQueryStatement();
            for (Map.Entry<Expr, FieldId> entry : queryStatement.getQueryRelation().getColumnReferences().entrySet()) {
                Expr expr = entry.getKey();
                FieldId id = entry.getValue();

                if (!id.getRelationId().equals(sourceScope.getRelationId())) {
                    continue;
                }

                if (!groupingFields.contains(id)) {
                    if (!SqlModeHelper.check(session.getSessionVariable().getSqlMode(),
                            SqlModeHelper.MODE_ONLY_FULL_GROUP_BY)) {
                        if (!analyzeState.getColumnNotInGroupBy().contains(expr)) {
                            throw new SemanticException(
                                    PARSER_ERROR_MSG.unsupportedNoGroupBySubquery(expr.toSql(), node.toSql()),
                                    expr.getPos());
                        }
                    } else {
                        return false;
                    }
                }
            }
            return true;
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

        @Override
        public Boolean visitDictQueryExpr(DictQueryExpr node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }

        @Override
        public Boolean visitDictionaryGetExpr(DictionaryGetExpr node, Void context) {
            return node.getChildren().stream().allMatch(this::visit);
        }
    }
}
