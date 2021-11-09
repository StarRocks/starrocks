// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.relation.QuerySpecification;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator.findOrCreateColumnRefForExpr;

class QueryTransformer {
    private final ColumnRefFactory columnRefFactory;
    private final ExpressionMapping outer;
    private final List<ColumnRefOperator> correlation = new ArrayList<>();

    QueryTransformer(ColumnRefFactory columnRefFactory, ExpressionMapping outer) {
        this.columnRefFactory = columnRefFactory;
        this.outer = outer;
    }

    public LogicalPlan plan(QuerySpecification queryBlock) {
        OptExprBuilder builder = planFrom(queryBlock.getRelation());

        builder = filter(builder, queryBlock.getPredicate());
        builder =
                aggregate(builder, queryBlock.getGroupBy(), queryBlock.getAggregate(), queryBlock.getGroupingSetsList(),
                        queryBlock.getGroupingFunctionCallExprs());
        builder = filter(builder, queryBlock.getHaving());
        builder = window(builder, queryBlock.getOutputAnalytic());

        if (queryBlock.hasOrderBy()) {
            if (!queryBlock.hasAggregation()) {
                //requires both output and source fields to be visible if there are no aggregations
                builder = projectForOrderWithoutAggregation(
                        builder, queryBlock.getOutputExpr(), builder.getFieldMappings(), queryBlock.getOrderScope());
            } else {
                //requires output fields, groups and translated aggregations to be visible for queries with aggregation
                builder = projectForOrderWithAggregation(builder,
                        Iterables.concat(queryBlock.getOutputExpr(), queryBlock.getOrderSourceExpressions()),
                        queryBlock.getOrderScope());
            }

            builder = window(builder, queryBlock.getOrderByAnalytic());
        }

        builder = distinct(builder, queryBlock.isDistinct(), queryBlock.getOutputExpr());
        // add project to express order by expression
        builder = project(builder, Iterables.concat(queryBlock.getOrderByExpressions(), queryBlock.getOutputExpr()));
        List<ColumnRefOperator> orderByColumns = Lists.newArrayList();
        builder = sort(builder, queryBlock.getOrderBy(), orderByColumns);
        builder = limit(builder, queryBlock.getLimit());

        List<ColumnRefOperator> outputColumns = computeOutputs(builder, queryBlock.getOutputExpr());

        // Add project operator to prune order by columns
        if (!orderByColumns.isEmpty() && !outputColumns.containsAll(orderByColumns)) {
            builder = project(builder, queryBlock.getOutputExpr());
        }

        return new LogicalPlan(builder, outputColumns, correlation);
    }

    private static List<ColumnRefOperator> computeOutputs(OptExprBuilder builder, List<Expr> outputExpressions) {
        List<ColumnRefOperator> outputs = new ArrayList<>();
        for (Expr expression : outputExpressions) {
            outputs.add((ColumnRefOperator) SqlToScalarOperatorTranslator
                    .translate(expression, builder.getExpressionMapping()));
        }
        return outputs;
    }

    private OptExprBuilder planFrom(Relation node) {
        return new RelationTransformer(columnRefFactory).visit(node);
    }

    private OptExprBuilder projectForOrderWithoutAggregation(OptExprBuilder subOpt, Iterable<Expr> outputExpression,
                                                             List<ColumnRefOperator> sourceExpression, Scope scope) {
        ExpressionMapping outputTranslations = new ExpressionMapping(scope);

        List<ColumnRefOperator> fieldMappings = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();

        for (Expr expression : outputExpression) {
            ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                    subOpt.getExpressionMapping(), projections, columnRefFactory);
            fieldMappings.add(columnRef);
            outputTranslations.put(expression, columnRef);
        }

        for (ColumnRefOperator expression : sourceExpression) {
            projections.put(expression, expression);
            fieldMappings.add(expression);
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
        outputTranslations.setFieldMappings(fieldMappings);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }

    private OptExprBuilder projectForOrderWithAggregation(OptExprBuilder subOpt, Iterable<Expr> expressions,
                                                          Scope scope) {
        ExpressionMapping outputTranslations = new ExpressionMapping(scope);

        List<ColumnRefOperator> fieldMappings = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
        for (Expr expression : expressions) {
            ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                    subOpt.getExpressionMapping(), projections, columnRefFactory);
            fieldMappings.add(columnRef);
            outputTranslations.put(expression, columnRef);
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
        outputTranslations.setFieldMappings(fieldMappings);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }

    private OptExprBuilder project(OptExprBuilder subOpt, Iterable<Expr> expressions) {
        ExpressionMapping outputTranslations = new ExpressionMapping(subOpt.getScope(), subOpt.getFieldMappings());

        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
        SubqueryTransformer subqueryTransformer = new SubqueryTransformer();

        for (Expr expression : expressions) {
            subOpt = subqueryTransformer.handleScalarSubqueries(columnRefFactory, subOpt, expression);
            ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                    subOpt.getExpressionMapping(), projections, columnRefFactory);
            outputTranslations.put(expression, columnRef);
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }

    private OptExprBuilder filter(OptExprBuilder subOpt, Expr predicate) {
        if (predicate == null) {
            return subOpt;
        }

        SubqueryTransformer subqueryTransformer = new SubqueryTransformer();
        subOpt = subqueryTransformer.handleSubqueries(columnRefFactory, subOpt, predicate);

        ScalarOperator scalarPredicate =
                subqueryTransformer.rewriteSubqueryScalarOperator(predicate, subOpt, outer, correlation);

        if (scalarPredicate != null) {
            LogicalFilterOperator filterOperator = new LogicalFilterOperator(scalarPredicate);
            return subOpt.withNewRoot(filterOperator);
        } else {
            return subOpt;
        }
    }

    private OptExprBuilder window(OptExprBuilder subOpt, List<AnalyticExpr> window) {
        if (window.isEmpty()) {
            return subOpt;
        }

        /*
         * Build ProjectOperator of partition expression and order by expression in window function.
         */
        List<Expr> projectExpressions = new ArrayList<>();
        for (AnalyticExpr analyticExpr : window) {
            projectExpressions.addAll(analyticExpr.getPartitionExprs());
            projectExpressions.addAll(analyticExpr.getOrderByElements()
                    .stream().map(OrderByElement::getExpr).collect(Collectors.toList()));
        }

        final ExpressionMapping expressionMapping = subOpt.getExpressionMapping();
        boolean allColumnRef = projectExpressions.stream()
                .map(expression -> SqlToScalarOperatorTranslator.translate(expression, expressionMapping))
                .allMatch(ScalarOperator::isColumnRef);

        /*
         * If there is no expression calculate in partition and order by,
         * there is no need to add ProjectOperator here
         */
        if (!allColumnRef) {
            ExpressionMapping outputTranslations = new ExpressionMapping(subOpt.getScope());
            List<ColumnRefOperator> fieldMappings = new ArrayList<>();
            Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
            for (ColumnRefOperator expression : subOpt.getFieldMappings()) {
                ColumnRefOperator variable =
                        columnRefFactory.create(expression, expression.getType(), expression.isNullable());
                projections.put(variable, expression);
                fieldMappings.add(variable);
            }
            for (Expr expression : projectExpressions) {
                ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                        subOpt.getExpressionMapping(), projections, columnRefFactory);
                fieldMappings.add(columnRef);
                outputTranslations.put(expression, columnRef);
            }

            LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
            subOpt.setExpressionMapping(outputTranslations);
            subOpt.getExpressionMapping().setFieldMappings(fieldMappings);
            subOpt = subOpt.withNewRoot(projectOperator);
        }

        /*
         * If necessary, rewrites the analytic function, window, and/or order-by elements
         * into a standard format for the purpose of simpler backend execution
         */
        List<WindowTransformer.WindowOperator> windowOperators = new ArrayList<>();
        for (AnalyticExpr analyticExpr : window) {
            WindowTransformer.WindowOperator rewriteOperator = WindowTransformer.standardize(analyticExpr);
            if (windowOperators.contains(rewriteOperator)) {
                windowOperators.get(windowOperators.indexOf(rewriteOperator))
                        .addFunction(analyticExpr.getFnCall(), analyticExpr);
            } else {
                windowOperators.add(rewriteOperator);
            }
        }

        List<WindowTransformer.PartitionGroup> partitionGroups =
                WindowTransformer.reorderWindowOperator(windowOperators, columnRefFactory, subOpt);
        for (WindowTransformer.PartitionGroup partitionGroup : partitionGroups) {
            for (WindowTransformer.SortGroup sortGroup : partitionGroup.getSortGroups()) {
                for (LogicalWindowOperator logicalWindowOperator : sortGroup.getWindowOperators()) {
                    LogicalWindowOperator newLogicalWindowOperator =
                            new LogicalWindowOperator.Builder().withOperator(logicalWindowOperator)
                                    .setEnforceSortColumns(sortGroup.getEnforceSortColumns()).build();
                    subOpt = subOpt.withNewRoot(newLogicalWindowOperator);
                }
            }
        }

        return subOpt;
    }

    private OptExprBuilder limit(OptExprBuilder subOpt, LimitElement limit) {
        if (limit == null) {
            return subOpt;
        }
        LogicalLimitOperator limitOperator = new LogicalLimitOperator(limit.getLimit(), limit.getOffset());
        return subOpt.withNewRoot(limitOperator);
    }

    private OptExprBuilder aggregate(OptExprBuilder subOpt, List<Expr> groupByExpressions,
                                     List<FunctionCallExpr> aggregates,
                                     List<List<Expr>> groupingSetsList, List<Expr> groupingFunctionCallExprs) {
        if (aggregates.size() == 0 && groupByExpressions.size() == 0) {
            return subOpt;
        }

        ImmutableList.Builder<Expr> arguments = ImmutableList.builder();
        aggregates.stream().map(FunctionCallExpr::getParams)
                .filter(e -> !(e.isStar()))
                .map(FunctionParams::exprs)
                .flatMap(List::stream)
                .filter(e -> !(e.isConstant()))
                .forEach(arguments::add);

        Iterable<Expr> inputs = Iterables.concat(groupByExpressions, arguments.build());

        if (!Iterables.isEmpty(inputs)) {
            subOpt = project(subOpt, inputs);
        }
        ExpressionMapping groupingTranslations = new ExpressionMapping(subOpt.getScope(), subOpt.getFieldMappings());

        List<ColumnRefOperator> groupByColumnRefs = new ArrayList<>();

        boolean groupAllConst = groupByExpressions.stream().allMatch(Expr::isConstant);

        for (Expr groupingItem : groupByExpressions) {
            // grouping columns save one least
            if (groupingItem.isConstant() && !(groupAllConst && groupByColumnRefs.isEmpty())) {
                continue;
            }

            ScalarOperator groupingKey =
                    SqlToScalarOperatorTranslator.translate(groupingItem, subOpt.getExpressionMapping());
            ColumnRefOperator colRef = (ColumnRefOperator) groupingKey;

            //remove repeated grouping expr, such as group by v1, v1
            if (!groupByColumnRefs.contains(colRef)) {
                groupByColumnRefs.add(colRef);
            }

            groupingTranslations.put(groupingItem, colRef);
        }

        Map<ColumnRefOperator, CallOperator> aggregationsMap = Maps.newHashMap();
        for (FunctionCallExpr aggregate : aggregates) {
            ScalarOperator aggCallOperator =
                    SqlToScalarOperatorTranslator.translate(aggregate, subOpt.getExpressionMapping());
            CallOperator aggOperator = (CallOperator) aggCallOperator;

            ColumnRefOperator colRef =
                    columnRefFactory.create(aggOperator.toString(), aggregate.getType(), aggregate.isNullable());
            aggregationsMap.put(colRef, aggOperator);
            groupingTranslations.put(aggregate, colRef);
        }

        //Add repeatOperator to support grouping sets
        if (groupingSetsList != null) {
            /*
             * repeatOutput is used to record the output column of repeatOperator,
             * this output column only represents the generated grouping_id column
             */
            List<ColumnRefOperator> repeatOutput = new ArrayList<>();

            /*
             * groupingIdsBitSets is used to record the complete grouping_id,
             * which contains all the group by columns.
             * groupingIds is converted by groupingIdsBitSets
             */
            ArrayList<BitSet> groupingIdsBitSets = new ArrayList<>();
            List<List<Long>> groupingIds = new ArrayList<>();

            /*
             * repeatColumnRefList is used to record the column reference
             * that needs to be repeatedly calculated.
             * This column reference is come from the child of repeat operator
             */
            List<Set<ColumnRefOperator>> repeatColumnRefList = new ArrayList<>();

            for (List<Expr> grouping : groupingSetsList) {
                Set<ColumnRefOperator> repeatColumnRef = new HashSet<>();
                BitSet groupingIdBitSet = new BitSet(groupByColumnRefs.size());
                groupingIdBitSet.set(0, groupByExpressions.size(), true);

                for (Expr groupingField : grouping) {
                    ColumnRefOperator groupingKey = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(
                            groupingField, subOpt.getExpressionMapping());
                    repeatColumnRef.add(groupingKey);
                    if (groupByColumnRefs.contains(groupingKey)) {
                        groupingIdBitSet.set(groupByColumnRefs.indexOf(groupingKey), false);
                    }
                }

                groupingIdsBitSets.add(groupingIdBitSet);
                repeatColumnRefList.add(repeatColumnRef);
            }

            //Build grouping_id(all grouping columns)
            ColumnRefOperator grouping = columnRefFactory.create("GROUPING_ID", Type.BIGINT, false);
            groupingIds.add(groupingIdsBitSets.stream().map(bitset ->
                    Utils.convertBitSetToLong(bitset, groupByColumnRefs.size())).collect(Collectors.toList()));
            groupByColumnRefs.add(grouping);
            repeatOutput.add(grouping);

            //Build grouping function in select item
            for (Expr groupingFunction : groupingFunctionCallExprs) {
                grouping = columnRefFactory.create("GROUPING", Type.BIGINT, false);

                ArrayList<BitSet> tempGroupingIdsBitSets = new ArrayList<>();
                for (int i = 0; i < repeatColumnRefList.size(); ++i) {
                    tempGroupingIdsBitSets.add(new BitSet(groupingFunction.getChildren().size()));
                }

                for (int childIdx = 0; childIdx < groupingFunction.getChildren().size(); ++childIdx) {
                    SlotRef slotRef = (SlotRef) groupingFunction.getChild(childIdx);

                    ColumnRefOperator groupingKey = (ColumnRefOperator) SqlToScalarOperatorTranslator
                            .translate(slotRef, subOpt.getExpressionMapping());
                    for (Set<ColumnRefOperator> repeatColumns : repeatColumnRefList) {
                        if (repeatColumns.contains(groupingKey)) {
                            for (int repeatColIdx = 0; repeatColIdx < repeatColumnRefList.size(); ++repeatColIdx) {
                                tempGroupingIdsBitSets.get(repeatColIdx).set(childIdx,
                                        groupingIdsBitSets.get(repeatColIdx)
                                                .get(groupByColumnRefs.indexOf(groupingKey)));
                            }
                        }
                    }
                }
                groupingTranslations.put(groupingFunction, grouping);

                groupingIds.add(tempGroupingIdsBitSets.stream().map(bitset ->
                                Utils.convertBitSetToLong(bitset, groupingFunction.getChildren().size()))
                        .collect(Collectors.toList()));
                groupByColumnRefs.add(grouping);
                repeatOutput.add(grouping);
            }

            LogicalRepeatOperator repeatOperator =
                    new LogicalRepeatOperator(repeatOutput, repeatColumnRefList, groupingIds);
            subOpt = new OptExprBuilder(repeatOperator, Lists.newArrayList(subOpt), groupingTranslations);
        }

        return new OptExprBuilder(new LogicalAggregationOperator(AggType.GLOBAL, groupByColumnRefs, aggregationsMap),
                Lists.newArrayList(subOpt), groupingTranslations);
    }

    private OptExprBuilder sort(OptExprBuilder subOpt, List<OrderByElement> orderByExpressions,
                                List<ColumnRefOperator> orderByColumns) {
        if (orderByExpressions.isEmpty()) {
            return subOpt;
        }

        List<Ordering> orderings = new ArrayList<>();
        for (OrderByElement item : orderByExpressions) {
            ColumnRefOperator column =
                    (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(item.getExpr(),
                            subOpt.getExpressionMapping());
            Ordering ordering = new Ordering(column, item.getIsAsc(),
                    OrderByElement.nullsFirst(item.getNullsFirstParam(), item.getIsAsc()));
            if (!orderByColumns.contains(column)) {
                orderings.add(ordering);
                orderByColumns.add(column);
            }
        }
        LogicalTopNOperator sortOperator = new LogicalTopNOperator(orderings);

        return subOpt.withNewRoot(sortOperator);
    }

    private OptExprBuilder distinct(OptExprBuilder subOpt, boolean isDistinct, List<Expr> outputExpressions) {
        if (isDistinct) {
            // Add project before DISTINCT to express select item
            subOpt = project(subOpt, outputExpressions);
            List<ColumnRefOperator> groupByColumns = Lists.newArrayList();
            for (Expr expr : outputExpressions) {
                ColumnRefOperator column = (ColumnRefOperator) SqlToScalarOperatorTranslator
                        .translate(expr, subOpt.getExpressionMapping());
                if (!groupByColumns.contains(column)) {
                    groupByColumns.add(column);
                }
            }
            return subOpt.withNewRoot(new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, new HashMap<>()));
        } else {
            return subOpt;
        }
    }
}
