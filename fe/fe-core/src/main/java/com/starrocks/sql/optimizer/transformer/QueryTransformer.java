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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.common.TreeNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.SubqueryUtils;
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
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator.findOrCreateColumnRefForExpr;

public class QueryTransformer {
    private final ColumnRefFactory columnRefFactory;
    private final ConnectContext session;
    private final List<ColumnRefOperator> correlation = new ArrayList<>();
    private final CTETransformerContext cteContext;
    private final boolean inlineView;
    private final MVTransformerContext mvTransformerContext;
    public static final String GROUPING_ID = "GROUPING_ID";
    public static final String GROUPING = "GROUPING";

    public QueryTransformer(ColumnRefFactory columnRefFactory, ConnectContext session,
                            CTETransformerContext cteContext, boolean inlineView,
                            MVTransformerContext mvTransformerContext) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.cteContext = cteContext;
        this.inlineView = inlineView;
        this.mvTransformerContext = mvTransformerContext;
    }

    public LogicalPlan plan(SelectRelation queryBlock, ExpressionMapping outer) {
        OptExprBuilder builder = planFrom(queryBlock.getRelation(), cteContext);
        builder.setExpressionMapping(new ExpressionMapping(builder.getScope(), builder.getFieldMappings(), outer,
                builder.getColumnRefToConstOperators()));

        Map<Expr, SlotRef> generatedExprToColumnRef = queryBlock.getGeneratedExprToColumnRef();
        Map<ScalarOperator, ColumnRefOperator> generatedColumnExprOpToColumnRef = new HashMap<>();
        for (Map.Entry<Expr, SlotRef> m : generatedExprToColumnRef.entrySet()) {
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(m.getKey(),
                    builder.getExpressionMapping(), columnRefFactory);
            ColumnRefOperator columnRefOp = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(m.getValue(),
                    builder.getExpressionMapping(), columnRefFactory);
            generatedColumnExprOpToColumnRef.put(scalarOperator, columnRefOp);
        }
        builder.getExpressionMapping().addGeneratedColumnExprOpToColumnRef(generatedColumnExprOpToColumnRef);

        builder = filter(builder, queryBlock.getPredicate());
        builder = aggregate(builder, queryBlock.getGroupBy(), queryBlock.getAggregate(),
                queryBlock.getGroupingSetsList(), queryBlock.getGroupingFunctionCallExprs());
        builder = filter(builder, queryBlock.getHaving());

        List<AnalyticExpr> analyticExprList = new ArrayList<>(queryBlock.getOutputAnalytic());
        analyticExprList.addAll(queryBlock.getOrderByAnalytic());
        builder = window(builder, analyticExprList);

        if (queryBlock.hasOrderByClause()) {
            if (!queryBlock.getGroupBy().isEmpty() || !queryBlock.getAggregate().isEmpty()) {
                // requires output fields, groups and translated aggregations to be visible for queries with aggregation
                List<String> outputNames = new ArrayList<>(queryBlock.getColumnOutputNames());
                for (int i = 0; i < queryBlock.getOrderSourceExpressions().size(); ++i) {
                    outputNames.add(queryBlock.getOrderSourceExpressions().get(i).toString());
                }

                builder = projectForOrder(builder,
                        Iterables.concat(queryBlock.getOutputExpression(),
                                queryBlock.getOrderSourceExpressions(),
                                queryBlock.getOrderByAnalytic()),
                        queryBlock,
                        outputNames,
                        builder.getFieldMappings(),
                        true);

            } else {
                // requires both output and source fields to be visible if there is no aggregation or distinct
                // if there is a distinct, we still no need to project the orderSourceExpressions because it must
                // in the outputExpression.
                builder = projectForOrder(builder,
                        Iterables.concat(queryBlock.getOutputExpression(), queryBlock.getOrderByAnalytic()),
                        queryBlock,
                        queryBlock.getColumnOutputNames(),
                        builder.getFieldMappings(),
                        queryBlock.isDistinct());
            }
        }

        builder = distinct(builder, queryBlock.isDistinct(), queryBlock.getOutputExpression());
        // add project to express order by expression
        builder = project(builder, Iterables.concat(queryBlock.getOrderByExpressions(), queryBlock.getOutputExpression()));
        List<ColumnRefOperator> orderByColumns = Lists.newArrayList();
        builder = sort(builder, queryBlock.getOrderBy(), orderByColumns);
        builder = limit(builder, queryBlock.getLimit());

        List<ColumnRefOperator> outputColumns = computeOutputs(builder, queryBlock.getOutputExpression(), columnRefFactory);

        // Add project operator to prune order by columns
        if (!orderByColumns.isEmpty() && !outputColumns.containsAll(orderByColumns)) {
            long limit = queryBlock.hasLimit() ? queryBlock.getLimit().getLimit() : -1;
            builder = project(builder, queryBlock.getOutputExpression(), limit);
        }

        return new LogicalPlan(builder, outputColumns, correlation);
    }

    private static List<ColumnRefOperator> computeOutputs(OptExprBuilder builder, List<Expr> outputExpressions,
                                                          ColumnRefFactory columnRefFactory) {
        List<ColumnRefOperator> outputs = new ArrayList<>();
        for (Expr expression : outputExpressions) {
            outputs.add((ColumnRefOperator) SqlToScalarOperatorTranslator
                    .translate(expression, builder.getExpressionMapping(), columnRefFactory));
        }
        return outputs;
    }

    private OptExprBuilder planFrom(Relation node, CTETransformerContext cteContext) {
        TransformerContext transformerContext = new TransformerContext(
                columnRefFactory, session, new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                cteContext, inlineView, mvTransformerContext);
        return new RelationTransformer(transformerContext).visit(node).getRootBuilder();
    }

    private OptExprBuilder projectForOrder(OptExprBuilder subOpt,
                                           Iterable<Expr> outputExpression,
                                           SelectRelation queryBlock,
                                           List<String> outputNames,
                                           List<ColumnRefOperator> sourceExpression,
                                           boolean withAggregation) {
        List<Integer> outputExprInOrderByScope = queryBlock.getOutputExprInOrderByScope();
        Scope scope = queryBlock.getOrderScope();
        ExpressionMapping outputTranslations = new ExpressionMapping(scope);
        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();

        int outputExprIdx = 0;
        for (Expr expression : outputExpression) {
            Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders = Maps.newHashMap();
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expression,
                    subOpt.getExpressionMapping(), columnRefFactory,
                    session, cteContext, subOpt, subqueryPlaceholders, false);
            Pair<ScalarOperator, OptExprBuilder> pair =
                    SubqueryUtils.rewriteScalarOperator(scalarOperator, subOpt, subqueryPlaceholders);
            scalarOperator = pair.first;
            subOpt = pair.second;
            ColumnRefOperator columnRefOperator = getOrCreateColumnRefOperator(expression, scalarOperator, projections);
            projections.put(columnRefOperator, scalarOperator);

            if (outputExprInOrderByScope.contains(outputExprIdx)) {
                outputTranslations.put(expression, columnRefOperator);
                TableName resolveTableName = queryBlock.getResolveTableName();
                if (expression instanceof SlotRef) {
                    resolveTableName = queryBlock.getRelation().getResolveTableName();
                }
                SlotRef alias = new SlotRef(resolveTableName, outputNames.get(outputExprIdx));
                // order by expr may reference the alias. We need put the alias into the fieldMappings or the
                // expressionToColumns.
                // if the alias not be used in the order by expr like:
                // select v1 cc, v2 cc from tbl order by v3. We just add the slotRef(cc) to columnRefOperator in the
                // expressionToColumns to ensure this projection can output v1 with alias cc, v2 with alias cc and v3.
                // We don't need to ensure the uniqueness of the alias in the fieldMappings.
                // if the alias used in the order by expr like:
                // select v1 v3, v2 cc from (select abs(v1) v1, abs(v2) v2, v3 from t0) t1 order by t1.v3.
                // order by expr t1.v3 referenced the v1 with alias v3, we need set the fieldMappings to ensure order by
                // expr can be resolved.
                if (scope.getRelationFields().resolveFields(alias).size() > 1) {
                    outputTranslations.getExpressionToColumns()
                            .put(new SlotRef(resolveTableName, outputNames.get(outputExprIdx)), columnRefOperator);
                } else {
                    outputTranslations.put(alias, columnRefOperator);
                }

            } else {
                outputTranslations.put(expression, columnRefOperator);
            }
            outputExprIdx++;
        }

        //1. If there is no aggregation, there are two namespaces in orderByScope.
        //   The first layer is outputScope and the second layer is sourceScope.
        //   Here, directly set the parsed sourceExpression to the corresponding offset position
        //
        //2. If there is aggregation, other expressions (agg in orderBy and grouping key not in output)
        //   will be put into orderSourceExpressions, and they will be used as outputExpression for parsing processing.

        if (!withAggregation) {
            List<ColumnRefOperator> fieldMappings = new ArrayList<>(outputTranslations.getFieldMappings());
            for (int i = 0; i < sourceExpression.size(); ++i) {
                ColumnRefOperator columnRefOperator = sourceExpression.get(i);
                projections.put(columnRefOperator, columnRefOperator);
                // Why not use outputExprIdx, but scope.getRelationFields().size()?
                // Because the length of outputExpression and the first layer of orderByScope are not equal.
                // The specific logic is in computeAndAssignOrderScope
                fieldMappings.set(scope.getRelationFields().size() + i, columnRefOperator);
            }

            outputTranslations.setFieldMappings(fieldMappings);
        }

        outputTranslations.addExpressionToColumns(subOpt.getExpressionMapping().getExpressionToColumns());
        outputTranslations.addColumnRefToConstOperators(subOpt.getColumnRefToConstOperators());
        outputTranslations.addGeneratedColumnExprOpToColumnRef(subOpt.getGeneratedColumnExprOpToColumnRef());

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }

    private OptExprBuilder project(OptExprBuilder subOpt, Iterable<Expr> expressions) {
        return project(subOpt, expressions, -1);
    }

    private OptExprBuilder project(OptExprBuilder subOpt, Iterable<Expr> expressions, long limit) {
        ExpressionMapping outputTranslations = new ExpressionMapping(subOpt.getScope(), subOpt.getFieldMappings(),
                subOpt.getColumnRefToConstOperators());

        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
        for (Expr expression : expressions) {
            Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders = Maps.newHashMap();
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expression,
                    subOpt.getExpressionMapping(), columnRefFactory,
                    session, cteContext, subOpt, subqueryPlaceholders, false);
            Pair<ScalarOperator, OptExprBuilder> pair =
                    SubqueryUtils.rewriteScalarOperator(scalarOperator, subOpt, subqueryPlaceholders);
            scalarOperator = pair.first;
            subOpt = pair.second;
            ColumnRefOperator columnRefOperator = getOrCreateColumnRefOperator(expression, scalarOperator, projections);
            projections.put(columnRefOperator, scalarOperator);
            outputTranslations.put(expression, columnRefOperator);
            if (scalarOperator.isConstant()) {
                outputTranslations.putConstOperator(columnRefOperator, scalarOperator);
            }
        }

        outputTranslations.addExpressionToColumns(subOpt.getExpressionMapping().getExpressionToColumns());
        outputTranslations.addColumnRefToConstOperators(subOpt.getColumnRefToConstOperators());
        outputTranslations.addGeneratedColumnExprOpToColumnRef(subOpt.getGeneratedColumnExprOpToColumnRef());

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections, limit);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }

    private OptExprBuilder filter(OptExprBuilder subOpt, Expr predicate) {
        if (predicate == null) {
            return subOpt;
        }

        Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders = Maps.newHashMap();
        ScalarOperator scalarPredicate = SqlToScalarOperatorTranslator.translate(predicate,
                subOpt.getExpressionMapping(), correlation, columnRefFactory,
                session, cteContext, subOpt, subqueryPlaceholders, true);

        Pair<ScalarOperator, OptExprBuilder> pair =
                SubqueryUtils.rewriteScalarOperator(scalarPredicate, subOpt, subqueryPlaceholders);
        scalarPredicate = pair.first;
        subOpt = pair.second;

        if (scalarPredicate == null) {
            return subOpt;
        }

        LogicalFilterOperator filterOperator = new LogicalFilterOperator(scalarPredicate);
        return subOpt.withNewRoot(filterOperator);
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
        boolean allColumnRef = true;
        Map<Expr, ColumnRefOperator> tempMapping = new HashMap<>();
        for (Expr expression : projectExpressions) {
            ScalarOperator operator = SqlToScalarOperatorTranslator.translate(expression, expressionMapping,
                    columnRefFactory);
            if (!operator.isColumnRef()) {
                allColumnRef = false;
                tempMapping.clear();
                break;
            } else {
                tempMapping.put(expression, (ColumnRefOperator) operator);
            }
        }
        if (allColumnRef) {
            expressionMapping.getExpressionToColumns().putAll(tempMapping);
        }

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
            outputTranslations.setFieldMappings(fieldMappings);

            // child output expressions
            for (Expr expression : subOpt.getExpressionMapping().getAllExpressions()) {
                ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                        subOpt.getExpressionMapping(), projections, columnRefFactory);
                outputTranslations.put(expression, columnRef);
            }

            for (Expr expression : projectExpressions) {
                ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                        subOpt.getExpressionMapping(), projections, columnRefFactory);
                outputTranslations.put(expression, columnRef);
            }

            LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
            subOpt.setExpressionMapping(outputTranslations);
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
                WindowTransformer.WindowOperator windowOperator =
                        windowOperators.get(windowOperators.indexOf(rewriteOperator));
                if (rewriteOperator.isSkewed()) {
                    windowOperator.setSkewed();
                }
                windowOperator.addFunction(analyticExpr);
            } else {
                windowOperators.add(rewriteOperator);
            }
        }

        List<LogicalWindowOperator> logicalWindowOperators =
                WindowTransformer.reorderWindowOperator(windowOperators, columnRefFactory, subOpt);
        for (LogicalWindowOperator logicalWindowOperator : logicalWindowOperators) {
            subOpt = subOpt.withNewRoot(logicalWindowOperator);
        }

        return subOpt;
    }

    private OptExprBuilder limit(OptExprBuilder subOpt, LimitElement limit) {
        if (limit == null) {
            return subOpt;
        }
        LogicalLimitOperator limitOperator = LogicalLimitOperator.init(limit.getLimit(), limit.getOffset());
        return subOpt.withNewRoot(limitOperator);
    }

    public OptExprBuilder aggregate(OptExprBuilder subOpt,
                                     List<Expr> groupByExpressions, List<FunctionCallExpr> aggregates,
                                     List<List<Expr>> groupingSetsList, List<Expr> groupingFunctionCallExprs) {
        if (aggregates.isEmpty() && groupByExpressions.isEmpty()) {
            return subOpt;
        }

        // handle aggregate function use grouping set columns, use clone column replace original column
        // e.g:
        // before: select sum(a) from xx group by rollup(a);
        // after: select sum(clone(a)) from xx group by rollup(a);
        List<FunctionCallExpr> copyAggregates;
        if (groupingSetsList != null) {
            copyAggregates = aggregates.stream().map(e -> (FunctionCallExpr) e.clone())
                    .collect(Collectors.toList());
            for (Expr groupBy : groupByExpressions) {
                copyAggregates.replaceAll(
                        root -> (FunctionCallExpr) replaceExprBottomUp(root, groupBy, new CloneExpr(groupBy)));
            }
        } else {
            copyAggregates = aggregates;
        }

        ImmutableList.Builder<Expr> arguments = ImmutableList.builder();
        copyAggregates.stream().filter(f -> !f.getParams().isStar())
                .map(TreeNode::getChildren).flatMap(List::stream)
                .filter(e -> !(e.isConstant())).forEach(arguments::add);

        Iterable<Expr> inputs = Iterables.concat(groupByExpressions, arguments.build());

        if (!Iterables.isEmpty(inputs)) {
            subOpt = project(subOpt, inputs);
        }
        ExpressionMapping groupingTranslations =
                new ExpressionMapping(subOpt.getScope(), subOpt.getFieldMappings(),
                        subOpt.getColumnRefToConstOperators());

        List<ColumnRefOperator> groupByColumnRefs = new ArrayList<>();

        boolean groupAllConst = groupByExpressions.stream().allMatch(Expr::isConstant);

        for (Expr groupingItem : groupByExpressions) {
            //Grouping columns save one least
            //Grouping set type aggregation cannot delete constant aggregation columns
            if (groupingItem.isConstant() && !(groupAllConst && groupByColumnRefs.isEmpty()) &&
                    groupingSetsList == null) {
                continue;
            }

            ScalarOperator groupingKey =
                    SqlToScalarOperatorTranslator.translate(groupingItem, subOpt.getExpressionMapping(),
                            columnRefFactory);
            ColumnRefOperator colRef = (ColumnRefOperator) groupingKey;

            //remove repeated grouping expr, such as group by v1, v1
            if (!groupByColumnRefs.contains(colRef)) {
                groupByColumnRefs.add(colRef);
            }

            groupingTranslations.put(groupingItem, colRef);
        }

        Map<ColumnRefOperator, CallOperator> aggregationsMap = Maps.newHashMap();
        for (int i = 0; i < aggregates.size(); i++) {
            FunctionCallExpr copyAggregate = copyAggregates.get(i);
            ScalarOperator aggCallOperator =
                    SqlToScalarOperatorTranslator.translate(copyAggregate, subOpt.getExpressionMapping(), columnRefFactory);
            CallOperator aggOperator = (CallOperator) aggCallOperator;

            ColumnRefOperator colRef =
                    columnRefFactory.create(aggOperator.getFnName(), copyAggregate.getType(), copyAggregate.isNullable());
            aggregationsMap.put(colRef, aggOperator);

            // the output key -> value pair must use the original aggregate expr as key, because
            // the top node may ref the original aggregate expr
            groupingTranslations.put(aggregates.get(i), colRef);
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
            List<List<ColumnRefOperator>> repeatColumnRefList = new ArrayList<>();

            for (List<Expr> grouping : groupingSetsList) {
                List<ColumnRefOperator> repeatColumnRef = new ArrayList<>();
                BitSet groupingIdBitSet = new BitSet(groupByColumnRefs.size());
                groupingIdBitSet.set(0, groupByExpressions.size(), true);

                for (Expr groupingField : grouping) {
                    ColumnRefOperator groupingKey = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(
                            groupingField, subOpt.getExpressionMapping(), columnRefFactory);
                    repeatColumnRef.add(groupingKey);
                    if (groupByColumnRefs.contains(groupingKey)) {
                        groupingIdBitSet.set(groupByColumnRefs.indexOf(groupingKey), false);
                    }
                }

                groupingIdsBitSets.add(groupingIdBitSet);
                repeatColumnRefList.add(repeatColumnRef);
            }

            //Build grouping_id(all grouping columns)
            ColumnRefOperator grouping = columnRefFactory.create(GROUPING_ID, Type.BIGINT, false);
            List<Long> groupingID = new ArrayList<>();
            for (BitSet bitSet : groupingIdsBitSets) {
                long gid = Utils.convertBitSetToLong(bitSet, groupByColumnRefs.size());

                // Under normal circumstances, grouping_id is unlikely to be duplicated,
                // but if there are duplicate columns in grouping sets, the grouping_id of the two columns may be the same,
                // eg: grouping sets((v1), (v1))
                // causing the data to be aggregated in advance.
                // So add pow here to ensure that the grouping_id is not repeated, to ensure that the data will not be aggregated in advance
                while (groupingID.contains(gid)) {
                    gid += (1L << groupByColumnRefs.size());
                }
                groupingID.add(gid);
            }
            groupingIds.add(groupingID);
            groupByColumnRefs.add(grouping);
            repeatOutput.add(grouping);

            //Build grouping function in select item
            Map<ColumnRefOperator, List<ColumnRefOperator>> groupingFnArgs = Maps.newHashMap();
            for (Expr groupingFunction : groupingFunctionCallExprs) {
                grouping = columnRefFactory.create(GROUPING, Type.BIGINT, false);
                List<ColumnRefOperator> fnArgs = Lists.newArrayList();

                ArrayList<BitSet> tempGroupingIdsBitSets = new ArrayList<>();
                for (int i = 0; i < repeatColumnRefList.size(); ++i) {
                    tempGroupingIdsBitSets.add(new BitSet(groupingFunction.getChildren().size()));
                }

                for (int childIdx = 0; childIdx < groupingFunction.getChildren().size(); ++childIdx) {
                    Expr expr = groupingFunction.getChild(childIdx);

                    ColumnRefOperator groupingKey = (ColumnRefOperator) SqlToScalarOperatorTranslator
                            .translate(expr, subOpt.getExpressionMapping(), columnRefFactory);
                    fnArgs.add(groupingKey);
                    for (List<ColumnRefOperator> repeatColumns : repeatColumnRefList) {
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
                groupingFnArgs.put(grouping, fnArgs);
            }

            LogicalRepeatOperator repeatOperator =
                    new LogicalRepeatOperator(repeatOutput, repeatColumnRefList, groupingIds, groupingFnArgs);
            subOpt = new OptExprBuilder(repeatOperator, Lists.newArrayList(subOpt), groupingTranslations);
        }

        return new OptExprBuilder(
                new LogicalAggregationOperator(AggType.GLOBAL, groupByColumnRefs, aggregationsMap),
                Lists.newArrayList(subOpt), groupingTranslations);
    }

    private Expr replaceExprBottomUp(Expr root, Expr pattern, Expr replace) {
        if (root.getChildren().size() > 0) {
            for (int i = 0; i < root.getChildren().size(); i++) {
                Expr result = replaceExprBottomUp(root.getChild(i), pattern, replace);
                root.setChild(i, result);
            }
        }

        if (root.equals(pattern)) {
            return replace;
        }
        return root;
    }

    private OptExprBuilder sort(OptExprBuilder subOpt, List<OrderByElement> orderByExpressions,
                                List<ColumnRefOperator> orderByColumns) {
        if (orderByExpressions.isEmpty()) {
            return subOpt;
        }

        List<Ordering> orderings = new ArrayList<>();
        for (OrderByElement item : orderByExpressions) {
            if (item.getExpr().isLiteral()) {
                continue;
            }
            ColumnRefOperator column =
                    (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(item.getExpr(),
                            subOpt.getExpressionMapping(), columnRefFactory);
            Ordering ordering = new Ordering(column, item.getIsAsc(),
                    OrderByElement.nullsFirst(item.getNullsFirstParam()));
            if (!orderByColumns.contains(column)) {
                orderings.add(ordering);
                orderByColumns.add(column);
            }
        }

        if (orderByColumns.isEmpty()) {
            return subOpt;
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
                        .translate(expr, subOpt.getExpressionMapping(), columnRefFactory);
                if (!groupByColumns.contains(column)) {
                    groupByColumns.add(column);
                }
            }
            return subOpt.withNewRoot(
                    new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, new HashMap<>()));
        } else {
            return subOpt;
        }
    }

    private ColumnRefOperator getOrCreateColumnRefOperator(Expr expression, ScalarOperator scalarOperator,
                                                           Map<ColumnRefOperator, ScalarOperator> projections) {
        ColumnRefOperator columnRefOperator;
        if (scalarOperator.isColumnRef()) {
            columnRefOperator = (ColumnRefOperator) scalarOperator;
        } else if (scalarOperator.isVariable() && projections.containsValue(scalarOperator)) {
            columnRefOperator = projections.entrySet().stream()
                    .filter(e -> scalarOperator.equals(e.getValue()))
                    .findAny()
                    .map(Map.Entry::getKey)
                    .orElse(null);
            Preconditions.checkNotNull(columnRefOperator);
        } else {
            columnRefOperator = columnRefFactory.create(expression, expression.getType(), scalarOperator.isNullable());
        }
        return columnRefOperator;
    }
}
