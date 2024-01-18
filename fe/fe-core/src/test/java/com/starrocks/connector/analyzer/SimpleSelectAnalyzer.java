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


package com.starrocks.connector.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Type;
import com.starrocks.common.structure.TreeNode;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.starrocks.analysis.Expr.pushNegationToOperands;
import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;


// This analyzer is used to test the compatibility of query SQL, do not need to analyze
// the scope for select clauses
public class SimpleSelectAnalyzer {

    public void analyze(AnalyzeState analyzeState,
                        SelectList selectList,
                        Relation fromRelation,
                        GroupByClause groupByClause,
                        Expr havingClause,
                        Expr whereClause,
                        List<OrderByElement> sortClause,
                        LimitElement limitElement) {
        analyzeWhere(whereClause, analyzeState);

        List<Expr> outputExpressions = analyzeSelect(selectList, groupByClause != null, analyzeState);

        List<Expr> groupByExpressions = new ArrayList<>(analyzeGroupBy(groupByClause, analyzeState, outputExpressions));
        if (selectList.isDistinct()) {
            groupByExpressions.addAll(outputExpressions);
        }

        analyzeHaving(havingClause, analyzeState);

        List<OrderByElement> orderByElements = analyzeOrderBy(sortClause, analyzeState, outputExpressions);
        List<Expr> orderByExpressions =
                orderByElements.stream().map(OrderByElement::getExpr).collect(Collectors.toList());

        analyzeGroupingOperations(analyzeState, groupByClause, outputExpressions);

        List<Expr> sourceExpressions = new ArrayList<>(outputExpressions);
        if (havingClause != null) {
            sourceExpressions.add(analyzeState.getHaving());
        }

        List<FunctionCallExpr> aggregates = analyzeAggregations(analyzeState,
                Stream.concat(sourceExpressions.stream(), orderByExpressions.stream()).collect(Collectors.toList()));
        if (AnalyzerUtils.isAggregate(aggregates, groupByExpressions)) {
            if (!groupByExpressions.isEmpty() &&
                    selectList.getItems().stream().anyMatch(SelectListItem::isStar) &&
                    !selectList.isDistinct()) {
                throw new SemanticException("cannot combine '*' in select list with GROUP BY: *");
            }
        }

        analyzeWindowFunctions(analyzeState, outputExpressions, orderByExpressions);

        if (limitElement != null && limitElement.hasLimit()) {
            if (limitElement.getOffset() > 0 && orderByElements.isEmpty()) {
                // The offset can only be processed in sort,
                // so when there is no order by, we manually set offset to 0
                analyzeState.setLimit(new LimitElement(0, limitElement.getLimit()));
            } else {
                analyzeState.setLimit(new LimitElement(limitElement.getOffset(), limitElement.getLimit()));
            }
        }
    }

    private List<Expr> analyzeSelect(SelectList selectList, boolean hasGroupByClause, AnalyzeState analyzeState) {
        ImmutableList.Builder<Expr> outputExpressionBuilder = ImmutableList.builder();
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        for (SelectListItem item : selectList.getItems()) {
            if (!item.isStar()) {
                analyzeExpression(item.getExpr(), analyzeState);
                outputExpressionBuilder.add(item.getExpr());

                // We need get column name after analyzerExpression, because StructType's col name maybe a wrong value.
                // The name here only refer to column name.
                String name = item.getAlias() == null ? AstToStringBuilder.toString(item.getExpr()) : item.getAlias();

                if (item.getExpr() instanceof SlotRef) {
                    outputFields.add(new Field(name, item.getExpr().getType(),
                            ((SlotRef) item.getExpr()).getTblNameWithoutAnalyzed(), item.getExpr()));
                } else {
                    outputFields.add(new Field(name, item.getExpr().getType(), null, item.getExpr()));
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
        analyzeState.setOutputScope(new Scope(RelationId.anonymous(), new RelationFields(outputFields.build())));
        return outputExpressions;
    }

    private List<OrderByElement> analyzeOrderBy(List<OrderByElement> orderByElements, AnalyzeState analyzeState,
                                                List<Expr> outputExpressions) {
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
                expression = outputExpressions.get((int) ordinal - 1);
            }

            analyzeExpression(expression, analyzeState);

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

    private void analyzeWhere(Expr whereClause, AnalyzeState analyzeState) {
        if (whereClause == null) {
            return;
        }

        Expr predicate = pushNegationToOperands(whereClause);
        analyzeExpression(predicate, analyzeState);

        AnalyzerUtils.verifyNoAggregateFunctions(predicate, "WHERE");
        AnalyzerUtils.verifyNoWindowFunctions(predicate, "WHERE");
        AnalyzerUtils.verifyNoGroupingFunctions(predicate, "WHERE");

        if (!predicate.getType().matchesType(Type.BOOLEAN) && !predicate.getType().matchesType(Type.NULL)) {
            throw new SemanticException("WHERE clause must evaluate to a boolean: actual type %s", predicate.getType());
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

    private List<FunctionCallExpr> analyzeAggregations(AnalyzeState analyzeState, List<Expr> outputAndOrderByExpressions) {
        List<FunctionCallExpr> aggregations = Lists.newArrayList();
        TreeNode.collect(outputAndOrderByExpressions, Expr.isAggregatePredicate()::apply, aggregations);
        aggregations.forEach(e -> analyzeExpression(e, analyzeState));

        if (aggregations.stream().filter(FunctionCallExpr::isDistinct).count() > 1) {
            for (FunctionCallExpr agg : aggregations) {
                if (agg.isDistinct() && agg.getChildren().size() > 0 && agg.getChild(0).getType().isArrayType()) {
                    throw new SemanticException("No matching function with signature: multi_distinct_count(ARRAY)");
                }
            }
        }

        analyzeState.setAggregate(aggregations);

        return aggregations;
    }

    private List<Expr> analyzeGroupBy(GroupByClause groupByClause, AnalyzeState analyzeState,
                                      List<Expr> outputExpressions) {
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
                        analyzeExpression(groupingExpr, analyzeState);
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
                        List<Expr> rewriteGrouping = analyzeGroupByExpr(g, analyzeState);
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
                    groupByExpressions.addAll(analyzeGroupByExpr(groupByClause.getGroupingExprs(), analyzeState));
                    List<Expr> rewriteOriGrouping =
                            analyzeGroupByExpr(groupByClause.getOriGroupingExprs(), analyzeState);

                    List<List<Expr>> groupingSets =
                            Sets.powerSet(IntStream.range(0, rewriteOriGrouping.size())
                                    .boxed().collect(Collectors.toSet())).stream()
                                    .map(l -> l.stream().map(rewriteOriGrouping::get).collect(Collectors.toList()))
                                    .collect(Collectors.toList());

                    analyzeState.setGroupingSetsList(groupingSets);
                } else if (groupByClause.getGroupingType().equals(GroupByClause.GroupingType.ROLLUP)) {
                    groupByExpressions.addAll(analyzeGroupByExpr(groupByClause.getGroupingExprs(), analyzeState));
                    List<Expr> rewriteOriGrouping =
                            analyzeGroupByExpr(groupByClause.getOriGroupingExprs(), analyzeState);

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

    private List<Expr> analyzeGroupByExpr(List<Expr> groupingExprs,  AnalyzeState analyzeState) {
        return groupingExprs.stream().peek(e -> analyzeExpression(e, analyzeState)).collect(Collectors.toList());
    }

    private void analyzeHaving(Expr havingClause, AnalyzeState analyzeState) {
        if (havingClause != null) {
            Expr predicate = pushNegationToOperands(havingClause);

            AnalyzerUtils.verifyNoWindowFunctions(predicate, "HAVING");
            AnalyzerUtils.verifyNoGroupingFunctions(predicate, "HAVING");

            analyzeExpression(predicate, analyzeState);

            if (!predicate.getType().matchesType(Type.BOOLEAN) && !predicate.getType().matchesType(Type.NULL)) {
                throw new SemanticException("HAVING clause must evaluate to a boolean: actual type %s",
                        predicate.getType());
            }
            analyzeState.setHaving(predicate);
        }
    }

    private void analyzeExpression(Expr expr, AnalyzeState state) {
        SimpleExpressionAnalyzer.analyzeExpression(expr, state);
    }
}
