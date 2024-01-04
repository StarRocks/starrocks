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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.TreeNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.starrocks.analysis.Expr.pushNegationToOperands;
import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class SelectAnalyzer {
    private final ConnectContext session;

    public SelectAnalyzer(ConnectContext session) {
        this.session = session;
    }

    public void analyze(AnalyzeState analyzeState,
                        SelectList selectList,
                        Relation fromRelation,
                        Scope sourceScope,
                        GroupByClause groupByClause,
                        Expr havingClause,
                        Expr whereClause,
                        List<OrderByElement> sortClause,
                        LimitElement limitElement) {
        analyzeWhere(whereClause, analyzeState, sourceScope);

        List<Expr> outputExpressions =
                analyzeSelect(selectList, fromRelation, groupByClause != null, analyzeState, sourceScope);
        Scope outputScope = analyzeState.getOutputScope();

        List<Expr> groupByExpressions = new ArrayList<>(
                analyzeGroupBy(groupByClause, analyzeState, sourceScope, outputScope, outputExpressions));
        if (selectList.isDistinct()) {
            groupByExpressions.addAll(outputExpressions);
        }

        analyzeHaving(havingClause, analyzeState, sourceScope, outputScope, outputExpressions);

        // Construct sourceAndOutputScope with sourceScope and outputScope
        Scope sourceAndOutputScope = computeAndAssignOrderScope(analyzeState, sourceScope, outputScope,
                selectList.isDistinct());

        List<OrderByElement> orderByElements =
                analyzeOrderBy(sortClause, analyzeState, sourceAndOutputScope, outputExpressions, selectList.isDistinct());
        List<Expr> orderByExpressions =
                orderByElements.stream().map(OrderByElement::getExpr).collect(Collectors.toList());

        analyzeGroupingOperations(analyzeState, groupByClause, outputExpressions);

        List<Expr> sourceExpressions = new ArrayList<>(outputExpressions);
        if (havingClause != null) {
            sourceExpressions.add(analyzeState.getHaving());
        }

        List<FunctionCallExpr> aggregates = analyzeAggregations(analyzeState, sourceScope,
                Stream.concat(sourceExpressions.stream(), orderByExpressions.stream()).collect(Collectors.toList()));
        if (AnalyzerUtils.isAggregate(aggregates, groupByExpressions)) {
            if (!groupByExpressions.isEmpty() &&
                    selectList.getItems().stream().anyMatch(SelectListItem::isStar) &&
                    !selectList.isDistinct()) {
                throw new SemanticException("cannot combine '*' in select list with GROUP BY: *");
            }

            if (!aggregates.isEmpty() && selectList.isDistinct()) {
                throw new SemanticException("cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
            }

            new AggregationAnalyzer(session, analyzeState, groupByExpressions, sourceScope, null)
                    .verify(sourceExpressions);

            if (orderByElements.size() > 0) {
                new AggregationAnalyzer(session, analyzeState, groupByExpressions, sourceScope, sourceAndOutputScope)
                        .verify(orderByExpressions);
            }
        }

        // If columnNotInGroupBy is not empty, it means that the case where
        // MODE_ONLY_FULL_GROUP_BY is false needs to be handled.
        // Change the columns that are not in group by to any_value aggregate function
        if (!analyzeState.getColumnNotInGroupBy().isEmpty()) {
            Map<Expr, Expr> notInGroupByMap = new HashMap<>();

            for (Expr g : analyzeState.getColumnNotInGroupBy()) {
                FunctionCallExpr anyValue = new FunctionCallExpr(FunctionSet.ANY_VALUE, Lists.newArrayList(g));
                ExpressionAnalyzer.analyzeExpression(anyValue, analyzeState, sourceScope, session);
                analyzeState.getAggregate().add(anyValue);
                notInGroupByMap.put(g, anyValue);
            }

            analyzeState.setOutputExpression(outputExpressions.stream()
                    .map(e -> e.accept(new NotFullGroupByRewriter(notInGroupByMap), null))
                    .collect(Collectors.toList()));
            orderByElements.forEach(orderByElement -> orderByElement.setExpr(
                    orderByElement.getExpr().accept(new NotFullGroupByRewriter(notInGroupByMap), null)));
            orderByExpressions =
                    orderByElements.stream().map(OrderByElement::getExpr).collect(Collectors.toList());
            if (havingClause != null) {
                analyzeState.setHaving(analyzeState.getHaving().accept(new NotFullGroupByRewriter(notInGroupByMap), null));
            }
        }

        analyzeWindowFunctions(analyzeState, outputExpressions, orderByExpressions);

        if (AnalyzerUtils.isAggregate(aggregates, groupByExpressions) &&
                (sortClause != null || havingClause != null)) {
            /*
             * Create scope for order by when aggregation is present.
             * This is because transformer requires scope in order to resolve names against fields.
             * Original ORDER BY see source scope. However, if aggregation is present,
             * ORDER BY  expressions should only be resolvable against output scope,
             * group by expressions and aggregation expressions.
             */
            List<FunctionCallExpr> aggregationsInOrderBy = Lists.newArrayList();
            TreeNode.collect(orderByExpressions, Expr.isAggregatePredicate(), aggregationsInOrderBy);

            /*
             * Prohibit the use of aggregate sorting for non-aggregated query,
             * To prevent the generation of incorrect data during non-scalar aggregation (at least 1 row in no-scalar agg)
             * eg. select 1 from t0 order by sum(v)
             */
            List<FunctionCallExpr> aggregationsInOutput = Lists.newArrayList();
            TreeNode.collect(sourceExpressions, Expr.isAggregatePredicate(), aggregationsInOutput);
            if (!AnalyzerUtils.isAggregate(aggregationsInOutput, groupByExpressions) &&
                    !aggregationsInOrderBy.isEmpty()) {
                throw new SemanticException(
                        "ORDER BY contains aggregate function and applies to the result of a non-aggregated query");
            }

            List<Expr> orderSourceExpressions = Streams.concat(
                    aggregationsInOrderBy.stream(), groupByExpressions.stream()).collect(Collectors.toList());

            List<Field> sourceForOrderFields = orderSourceExpressions.stream()
                    .map(expression ->
                            new Field("anonymous", expression.getType(), null, expression))
                    .collect(Collectors.toList());

            Scope sourceScopeForOrder = new Scope(RelationId.anonymous(), new RelationFields(sourceForOrderFields));
            computeAndAssignOrderScope(analyzeState, sourceScopeForOrder, outputScope, selectList.isDistinct());
            analyzeState.setOrderSourceExpressions(orderSourceExpressions);
        }

        if (limitElement != null && limitElement.hasLimit()) {
            analyzeState.setLimit(new LimitElement(limitElement.getOffset(), limitElement.getLimit()));
        }
    }

    private List<Expr> analyzeSelect(SelectList selectList, Relation fromRelation, boolean hasGroupByClause,
                                     AnalyzeState analyzeState, Scope scope) {
        ImmutableList.Builder<Expr> outputExpressionBuilder = ImmutableList.builder();
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
        List<Integer> outputExprInOrderByScope = new ArrayList<>();

        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                List<Field> fields = (item.getTblName() == null ? scope.getRelationFields().getAllFields()
                        : scope.getRelationFields().resolveFieldsWithPrefix(item.getTblName()))
                        .stream().filter(Field::isVisible).collect(Collectors.toList());
                List<String> unknownTypeFields = fields.stream()
                        .filter(field -> field.getType().getPrimitiveType().equals(PrimitiveType.UNKNOWN_TYPE))
                        .map(Field::getName).collect(Collectors.toList());
                if (!unknownTypeFields.isEmpty()) {
                    throw new SemanticException("Datatype of external table column " + unknownTypeFields
                            + " is not supported!");
                }
                if (fields.isEmpty()) {
                    if (item.getTblName() != null) {
                        throw new SemanticException("Unknown table '%s'", item.getTblName());
                    }
                    if (fromRelation == null) {
                        throw new SemanticException("SELECT * not allowed in queries without FROM clause");
                    }
                    throw new StarRocksPlannerException("SELECT * not allowed from relation that has no columns",
                            INTERNAL_ERROR);
                }

                for (Field field : fields) {
                    int fieldIndex = scope.getRelationFields().indexOf(field);
                    /*
                     * Generate a special "SlotRef" as FieldReference,
                     * which represents a reference to the expression in the source scope.
                     * Because the real expression cannot be obtained in star
                     * eg: "select * from (select count(*) from table) t"
                     */
                    FieldReference fieldReference = new FieldReference(fieldIndex, item.getTblName());
                    analyzeExpression(fieldReference, analyzeState, scope);
                    outputExpressionBuilder.add(fieldReference);
                }
                outputFields.addAll(fields);

            } else {
                String name;
                if (item.getExpr() instanceof SlotRef) {
                    name = item.getAlias() == null ? ((SlotRef) item.getExpr()).getColumnName() : item.getAlias();
                } else {
                    name = item.getAlias() == null ? AstToStringBuilder.toString(item.getExpr(), false) : item.getAlias();
                }

                analyzeExpression(item.getExpr(), analyzeState, scope);
                outputExpressionBuilder.add(item.getExpr());

                if (item.getExpr() instanceof SlotRef) {
                    outputFields.add(new Field(name, item.getExpr().getType(),
                            ((SlotRef) item.getExpr()).getTblNameWithoutAnalyzed(), item.getExpr(),
                            true, item.getExpr().isNullable()));
                } else {
                    outputFields.add(new Field(name, item.getExpr().getType(), null, item.getExpr(),
                            true, item.getExpr().isNullable()));
                }

                // outputExprInOrderByScope is used to record which expressions in outputExpression are to be
                // recorded in the first level of OrderByScope (order by expressions can refer to columns in output)
                // Which columns satisfy the condition?
                // 1. An expression of type SlotRef.
                //    When both tables t0 and t1 contain the v1 column, select t0.v1 from t0, t1 order by v1 will
                //    refer to v1 in outputExpr instead of reporting an error.
                // 2. There is an aliased output expression

                // Why can't use all output expression?
                // Because output expression and outputScope do not correspond one-to-one,
                // you can refer to computeAndAssignOrderScope.
                // Therefore, the first level Scope in orderByScope must be able to be resolved according to
                // Expr in the projectForOrder function. If all Expr in outputExpression are used,
                // FieldReference will fail to resolve.
                // For example, "select *, v1 from t0 order by v2". star will be parsed into multiple FieldReferences.
                // FieldReference cannot directly resolve the corresponding position in Scope according to resolve
                // in Scope. Because Field records the location of sourceScope, and in projectForOrder,
                // it needs to be resolved in orderScope.
                // Summary: The output expression that can be resolved with a resolve name is meaningful
                // for the analysis of the order by position, because this is an external legal Scope.
                // And because in Transform, it is also necessary to parse the name according to the Scope
                // to correspond to the corresponding ColumnRefOperator, so only the parsed Expr can find
                // the corresponding fieldMappings in the Scope position according to the analysis.
                if (item.getAlias() != null || item.getExpr() instanceof SlotRef) {
                    outputExprInOrderByScope.add(outputFields.build().size() - 1);
                }
            }

            if (selectList.isDistinct()) {
                outputExpressionBuilder.build().forEach(expr -> {
                    if (!expr.getType().canDistinct()) {
                        throw new SemanticException("DISTINCT can only be applied to comparable types : %s",
                                expr.getType());
                    }
                    if (expr.isAggregate()) {
                        throw new SemanticException(
                                "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
                    }
                });

                if (hasGroupByClause) {
                    throw new SemanticException("cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
                }
                analyzeState.setIsDistinct(true);
            }
        }

        List<Expr> outputExpressions = outputExpressionBuilder.build();
        analyzeState.setOutputExpression(outputExpressions);
        analyzeState.setOutputExprInOrderByScope(outputExprInOrderByScope);
        analyzeState.setOutputScope(new Scope(RelationId.anonymous(), new RelationFields(outputFields.build())));
        return outputExpressions;
    }

    private List<OrderByElement> analyzeOrderBy(List<OrderByElement> orderByElements, AnalyzeState analyzeState,
                                                Scope orderByScope,
                                                List<Expr> outputExpressions,
                                                boolean isDistinct) {
        if (orderByElements == null) {
            analyzeState.setOrderBy(Collections.emptyList());
            return Collections.emptyList();
        }

        for (OrderByElement orderByElement : orderByElements) {
            Expr expression = orderByElement.getExpr();
            AnalyzerUtils.verifyNoGroupingFunctions(expression, "ORDER BY");
            AnalyzerUtils.verifyNoSubQuery(expression, "ORDER BY");

            if (expression instanceof IntLiteral) {
                long ordinal = ((IntLiteral) expression).getLongValue();
                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                    throw new SemanticException("ORDER BY position %s is not in select list", ordinal);
                }
                // index can ensure no ambiguous, we don't need to re-analyze this output expression
                expression = outputExpressions.get((int) ordinal - 1);
            } else if (expression instanceof FieldReference) {
                // If the expression of order by is a FieldReference, and it's not a distinct select,
                // it means that the type of sql is
                // "select * from t order by 1", then this FieldReference cannot be parsed in OrderByScope,
                // but should be parsed in sourceScope
                if (isDistinct) {
                    analyzeExpression(expression, analyzeState, orderByScope);
                } else {
                    analyzeExpression(expression, analyzeState, orderByScope.getParent());
                }
            } else {
                ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(session);
                expressionAnalyzer.analyzeWithoutUpdateState(expression, analyzeState, orderByScope);
                List<Expr> aggregations = Lists.newArrayList();
                expression.collectAll(e -> e.isAggregate(), aggregations);
                if (isDistinct && !aggregations.isEmpty()) {
                    throw new SemanticException("for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                            expression.getPos());
                }

                if (!aggregations.isEmpty()) {
                    aggregations.forEach(e -> analyzeExpression(e, analyzeState, orderByScope.getParent()));
                }
                analyzeExpression(expression, analyzeState, orderByScope);
            }

            if (!expression.getType().canOrderBy()) {
                throw new SemanticException(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
            }

            orderByElement.setExpr(expression);
        }

        analyzeState.setOrderBy(orderByElements);
        return orderByElements;
    }

    private void analyzeWindowFunctions(AnalyzeState analyzeState, List<Expr> outputExpressions,
                                        List<Expr> orderByExpressions) {
        List<AnalyticExpr> outputWindowFunctions = new ArrayList<>();
        for (Expr expression : outputExpressions) {
            List<AnalyticExpr> window = Lists.newArrayList();
            expression.collect(AnalyticExpr.class, window);
            if (outputWindowFunctions.stream()
                    .anyMatch((e -> TreeNode.contains(e.getChildren(), AnalyticExpr.class)))) {
                throw new SemanticException("Nesting of analytic expressions is not allowed: " + expression.toSql());
            }
            outputWindowFunctions.addAll(window);
        }
        analyzeState.setOutputAnalytic(outputWindowFunctions);

        List<AnalyticExpr> orderByWindowFunctions = new ArrayList<>();
        for (Expr expression : orderByExpressions) {
            List<AnalyticExpr> window = Lists.newArrayList();
            expression.collect(AnalyticExpr.class, window);
            if (orderByWindowFunctions.stream()
                    .anyMatch((e -> TreeNode.contains(e.getChildren(), AnalyticExpr.class)))) {
                throw new SemanticException("Nesting of analytic expressions is not allowed: " + expression.toSql());
            }
            orderByWindowFunctions.addAll(window);
        }
        analyzeState.setOrderByAnalytic(orderByWindowFunctions);
    }

    private void analyzeWhere(Expr whereClause, AnalyzeState analyzeState, Scope scope) {
        if (whereClause == null) {
            return;
        }

        Expr predicate = pushNegationToOperands(whereClause);
        analyzeExpression(predicate, analyzeState, scope);

        AnalyzerUtils.verifyNoAggregateFunctions(predicate, "WHERE");
        AnalyzerUtils.verifyNoWindowFunctions(predicate, "WHERE");
        AnalyzerUtils.verifyNoGroupingFunctions(predicate, "WHERE");

        if (predicate.getType().isBoolean() || predicate.getType().isNull()) {
            // do nothing
        } else if (!session.getSessionVariable().isEnableStrictType() && Type.canCastTo(predicate.getType(), Type.BOOLEAN)) {
            predicate = new CastExpr(Type.BOOLEAN, predicate);
        } else {
            throw new SemanticException("WHERE clause %s can not be converted to boolean type", predicate.toSql());
        }

        analyzeState.setPredicate(predicate);
    }

    private void analyzeGroupingOperations(AnalyzeState analyzeState, GroupByClause groupByClause,
                                           List<Expr> outputExpressions) {
        List<Expr> groupingFunctionCallExprs = Lists.newArrayList();

        TreeNode.collect(outputExpressions, expr -> expr instanceof GroupingFunctionCallExpr,
                groupingFunctionCallExprs);

        if (!groupingFunctionCallExprs.isEmpty() &&
                (groupByClause == null ||
                        groupByClause.getGroupingType().equals(GroupByClause.GroupingType.GROUP_BY))) {
            throw new SemanticException("cannot use GROUPING functions without [grouping sets|rollup|cube] clause");
        }

        analyzeState.setGroupingFunctionCallExprs(groupingFunctionCallExprs);
    }

    private List<FunctionCallExpr> analyzeAggregations(AnalyzeState analyzeState, Scope sourceScope,
                                                          List<Expr> outputAndOrderByExpressions) {
        List<FunctionCallExpr> aggregations = Lists.newArrayList();
        TreeNode.collect(outputAndOrderByExpressions, Expr.isAggregatePredicate()::apply, aggregations);
        aggregations.forEach(e -> analyzeExpression(e, analyzeState, sourceScope));

        for (FunctionCallExpr agg : aggregations) {
            if (agg.isDistinct() && agg.getChildren().size() > 0) {
                Type[] args = agg.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
                if (Arrays.stream(args).anyMatch(t -> (t.isJsonType() || t.isComplexType()) && !t.canGroupBy())) {
                    throw new SemanticException(agg.toSql() + " can't rewrite distinct to group by on (" +
                            Arrays.stream(args).map(Type::toSql).collect(Collectors.joining(",")) + ")");
                }
            }
        }

        analyzeState.setAggregate(aggregations);

        return aggregations;
    }

    private List<Expr> analyzeGroupBy(GroupByClause groupByClause, AnalyzeState analyzeState, Scope sourceScope,
                                      Scope outputScope, List<Expr> outputExpressions) {
        List<Expr> groupByExpressions = new ArrayList<>();
        if (groupByClause != null) {
            if (groupByClause.getGroupingType() == GroupByClause.GroupingType.GROUP_BY) {
                List<Expr> groupingExprs = groupByClause.getGroupingExprs();
                for (Expr groupingExpr : groupingExprs) {
                    if (groupingExpr instanceof IntLiteral) {
                        long ordinal = ((IntLiteral) groupingExpr).getLongValue();
                        if (ordinal < 1 || ordinal > outputExpressions.size()) {
                            throw new SemanticException("Group by position %s is not in select list", ordinal);
                        }
                        groupingExpr = outputExpressions.get((int) ordinal - 1);
                    } else {
                        RewriteAliasVisitor visitor =
                                new RewriteAliasVisitor(sourceScope, outputScope, outputExpressions, session);
                        groupingExpr = groupingExpr.accept(visitor, null);
                        analyzeExpression(groupingExpr, analyzeState, sourceScope);
                    }

                    if (!groupingExpr.getType().canGroupBy()) {
                        throw new SemanticException(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
                    }

                    if (analyzeState.getColumnReferences().get(groupingExpr) == null) {
                        AnalyzerUtils.verifyNoAggregateFunctions(groupingExpr, "GROUP BY");
                        AnalyzerUtils.verifyNoWindowFunctions(groupingExpr, "GROUP BY");
                        AnalyzerUtils.verifyNoGroupingFunctions(groupingExpr, "GROUP BY");
                    }

                    groupByExpressions.add(groupingExpr);
                }
            } else {
                if (groupByClause.getGroupingType().equals(GroupByClause.GroupingType.GROUPING_SETS)) {
                    List<List<Expr>> groupingSets = new ArrayList<>();
                    ArrayList<Expr> groupByList = Lists.newArrayList();
                    for (ArrayList<Expr> g : groupByClause.getGroupingSetList()) {
                        List<Expr> rewriteGrouping = rewriteGroupByAlias(g, analyzeState,
                                sourceScope, outputScope, outputExpressions);
                        rewriteGrouping.forEach(e -> {
                            if (!groupByList.contains(e)) {
                                groupByList.add(e);
                            }
                        });
                        groupingSets.add(rewriteGrouping);
                    }

                    groupByExpressions.addAll(groupByList);
                    analyzeState.setGroupingSetsList(groupingSets);
                } else if (groupByClause.getGroupingType().equals(GroupByClause.GroupingType.CUBE)) {
                    groupByExpressions.addAll(rewriteGroupByAlias(groupByClause.getGroupingExprs(), analyzeState,
                            sourceScope, outputScope, outputExpressions));
                    List<Expr> rewriteOriGrouping =
                            rewriteGroupByAlias(groupByClause.getOriGroupingExprs(), analyzeState,
                                    sourceScope, outputScope, outputExpressions);

                    List<List<Expr>> groupingSets =
                            Sets.powerSet(IntStream.range(0, rewriteOriGrouping.size())
                                            .boxed().collect(Collectors.toSet())).stream()
                                    .map(l -> l.stream().map(rewriteOriGrouping::get).collect(Collectors.toList()))
                                    .collect(Collectors.toList());

                    analyzeState.setGroupingSetsList(groupingSets);
                } else if (groupByClause.getGroupingType().equals(GroupByClause.GroupingType.ROLLUP)) {
                    groupByExpressions.addAll(rewriteGroupByAlias(groupByClause.getGroupingExprs(), analyzeState,
                            sourceScope, outputScope, outputExpressions));
                    List<Expr> rewriteOriGrouping =
                            rewriteGroupByAlias(groupByClause.getOriGroupingExprs(), analyzeState, sourceScope,
                                    outputScope, outputExpressions);

                    List<List<Expr>> groupingSets = IntStream.rangeClosed(0, rewriteOriGrouping.size())
                            .mapToObj(i -> rewriteOriGrouping.subList(0, i)).collect(Collectors.toList());

                    analyzeState.setGroupingSetsList(groupingSets);
                } else {
                    throw new StarRocksPlannerException("unknown grouping type", INTERNAL_ERROR);
                }
            }
        }
        analyzeState.setGroupBy(groupByExpressions);
        return groupByExpressions;
    }

    private List<Expr> rewriteGroupByAlias(List<Expr> groupingExprs, AnalyzeState analyzeState, Scope sourceScope,
                                           Scope outputScope, List<Expr> outputExpressions) {
        return groupingExprs.stream().map(e -> {
            RewriteAliasVisitor visitor =
                    new RewriteAliasVisitor(sourceScope, outputScope, outputExpressions, session);
            Expr rewrite = e.accept(visitor, null);
            analyzeExpression(rewrite, analyzeState, sourceScope);
            return rewrite;
        }).collect(Collectors.toList());
    }

    private void analyzeHaving(Expr havingClause, AnalyzeState analyzeState,
                               Scope sourceScope, Scope outputScope, List<Expr> outputExprs) {
        if (havingClause != null) {
            Expr predicate = pushNegationToOperands(havingClause);

            RewriteAliasVisitor visitor = new RewriteAliasVisitor(sourceScope, outputScope, outputExprs, session);
            predicate = predicate.accept(visitor, null);

            AnalyzerUtils.verifyNoWindowFunctions(predicate, "HAVING");
            AnalyzerUtils.verifyNoGroupingFunctions(predicate, "HAVING");

            analyzeExpression(predicate, analyzeState, sourceScope);

            if (!predicate.getType().matchesType(Type.BOOLEAN) && !predicate.getType().matchesType(Type.NULL)) {
                throw new SemanticException("HAVING clause must evaluate to a boolean: actual type %s",
                        predicate.getType());
            }
            analyzeState.setHaving(predicate);
        }
    }

    // If alias is same with table column name, we directly use table name.
    // otherwise, we use output expression according to the alias
    public static class RewriteAliasVisitor extends AstVisitor<Expr, Void> {
        private final Scope sourceScope;
        private final Scope outputScope;
        private final List<Expr> outputExprs;
        private final ConnectContext session;

        public RewriteAliasVisitor(Scope sourceScope, Scope outputScope, List<Expr> outputExprs,
                                   ConnectContext session) {
            this.sourceScope = sourceScope;
            this.outputScope = outputScope;
            this.outputExprs = outputExprs;
            this.session = session;
        }

        @Override
        public Expr visit(ParseNode expr) {
            return visit(expr, null);
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            if (sourceScope.tryResolveField(slotRef).isPresent() &&
                    !session.getSessionVariable().getEnableGroupbyUseOutputAlias()) {
                return slotRef;
            }

            Optional<ResolvedField> resolvedField = outputScope.tryResolveField(slotRef);
            if (resolvedField.isPresent()) {
                return outputExprs.get(resolvedField.get().getRelationFieldIndex());
            }
            return slotRef;
        }
    }

    private static class NotFullGroupByRewriter extends AstVisitor<Expr, Void> {
        private final Map<Expr, Expr> columnsNotInGroupBy;

        public NotFullGroupByRewriter(Map<Expr, Expr> columnsNotInGroupBy) {
            this.columnsNotInGroupBy = columnsNotInGroupBy;
        }

        @Override
        public Expr visit(ParseNode expr) {
            return visit(expr, null);
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitFunctionCall(FunctionCallExpr expr, Void context) {
            if (!expr.isAggregateFunction()) {
                return visitExpression(expr, context);
            } else {
                // Columns inside aggregates are not rewritten
                return expr;
            }
        }

        @Override
        public Expr visitAnalyticExpr(AnalyticExpr expr, Void context) {
            for (int i = 0; i < expr.getFnCall().getChildren().size(); ++i) {
                expr.getFnCall().setChild(i, visit(expr.getChild(i)));
            }

            List<OrderByElement> orderByElements = expr.getOrderByElements();
            orderByElements.forEach(orderByElement -> orderByElement.setExpr(visit(orderByElement.getExpr())));

            for (int i = 0; i < expr.getPartitionExprs().size(); ++i) {
                expr.getPartitionExprs().set(i, visit(expr.getPartitionExprs().get(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            if (columnsNotInGroupBy.containsKey(slotRef)) {
                return columnsNotInGroupBy.get(slotRef);
            }
            return slotRef;
        }
    }

    private Scope computeAndAssignOrderScope(AnalyzeState analyzeState, Scope sourceScope, Scope outputScope,
                                             boolean isDistinct) {

        List<Field> allFields = Lists.newArrayList();
        if (isDistinct) {
            allFields = removeDuplicateField(outputScope.getRelationFields().getAllFields());
            Scope orderScope = new Scope(outputScope.getRelationId(), new RelationFields(allFields));
            orderScope.setParent(sourceScope);
            analyzeState.setOrderScope(orderScope);
            return orderScope;
        }

        for (int i = 0; i < analyzeState.getOutputExprInOrderByScope().size(); ++i) {
            Field field = outputScope.getRelationFields()
                    .getFieldByIndex(analyzeState.getOutputExprInOrderByScope().get(i));
            allFields.add(field);
        }
        allFields = removeDuplicateField(allFields);

        Scope orderScope = new Scope(outputScope.getRelationId(), new RelationFields(allFields));

        /*
         * ORDER BY or HAVING should "see" both output and FROM fields
         * Because output scope and source scope may contain the same columns,
         * so they cannot be in the same level of scope to avoid ambiguous semantics
         */
        orderScope.setParent(sourceScope);
        analyzeState.setOrderScope(orderScope);
        return orderScope;
    }

    private void analyzeExpression(Expr expr, AnalyzeState analyzeState, Scope scope) {
        ExpressionAnalyzer.analyzeExpression(expr, analyzeState, scope, session);
    }


    // The Scope used by order by allows parsing of the same column,
    // such as 'select v1 as v, v1 as v from t0 order by v'
    // but normal parsing does not allow it. So add a de-duplication operation here.
    private List<Field> removeDuplicateField(List<Field> originalFields) {
        List<Field> allFields = Lists.newArrayList();
        for (Field field : originalFields) {
            if (session.getSessionVariable().isEnableStrictOrderBy()) {
                if (field.getName() != null && field.getOriginExpression() != null &&
                        allFields.stream().anyMatch(f -> f.getOriginExpression() != null
                                && f.getName() != null && field.getName().equals(f.getName())
                                && field.getOriginExpression().equals(f.getOriginExpression()))) {
                    continue;
                }
            } else {
                if (field.getName() != null &&
                        allFields.stream().anyMatch(f -> f.getName() != null && field.getName().equals(f.getName()))) {
                    continue;
                }
            }
            allFields.add(field);
        }
        return allFields;
    }
}
