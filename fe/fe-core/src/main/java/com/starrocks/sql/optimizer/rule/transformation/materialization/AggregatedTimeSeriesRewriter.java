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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.ReplaceShuttle;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggregatePushDownContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.isSupportedAggFunctionPushDown;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.doRewritePushDownAgg;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.getPushDownRollupFinalAggregateOpt;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.getRollupPartialAggregate;

public class AggregatedTimeSeriesRewriter extends MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(AggregatedTimeSeriesRewriter.class);

    private final Rule rule;
    private final ColumnRefFactory queryColumnRefFactory;
    private final Map<ColumnRefOperator, ColumnRefOperator> remapping = Maps.newHashMap();

    public AggregatedTimeSeriesRewriter(MvRewriteContext mvRewriteContext,
                                        Rule rule) {
        super(mvRewriteContext);
        this.rule = rule;
        queryColumnRefFactory = mvRewriteContext.getMaterializationContext().getQueryRefFactory();
    }

    @Override
    public boolean isValidPlan(OptExpression expression) {
        return MvUtils.isLogicalSPJG(expression);
    }

    private boolean isEnableTimeGranularityRollup(Table refBaseTable,
                                                  MaterializedView mv,
                                                  OptExpression queryExpression) {
        if (!mv.isPartitionedTable()) {
            return false;
        }
        // if mv rewrite generates the table, skip it
        if (refBaseTable instanceof MaterializedView && mv.equals(refBaseTable)) {
            return false;
        }
        if (refBaseTable instanceof OlapTable) {
            OlapTable refBaseOlapTable = (OlapTable) refBaseTable;
            if (!refBaseOlapTable.getPartitionInfo().isRangePartition()) {
                return false;
            }
        } else {
            // external table should be partitioned table.
            if (!ConnectorPartitionTraits.isSupportPCTRefresh(refBaseTable.getType())) {
                return false;
            }
            try {
                List<Column> partitionCols = refBaseTable.getPartitionColumns();
                if (partitionCols.isEmpty()) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }

        // check input query expression
        if (!(queryExpression.getOp() instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator origAggOperator = (LogicalAggregationOperator) queryExpression.getOp();
        // ensure all aggregate functions can be rolled up
        if (origAggOperator.getAggregations().values().stream()
                .anyMatch(x -> !isSupportedAggFunctionPushDown(x))) {
            return false;
        }
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        if (scanOperators.size() != 1 || !scanOperators.get(0).getTable().equals(refBaseTable)) {
            return false;
        }
        return true;
    }

    @Override
    public OptExpression doRewrite(MvRewriteContext mvRewriteContext) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        // check mv's base table
        List<Table> baseTables = mvContext.getBaseTables();
        if (baseTables.size() != 1) {
            logMVRewrite(optimizerContext, rule, "AggTimeSeriesRewriter: base table size is not 1, size=" + baseTables.size());
            return null;
        }
        Table refBaseTable = baseTables.get(0);
        MaterializedView mv = mvContext.getMv();
        OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        if (!isEnableTimeGranularityRollup(refBaseTable, mv, queryExpression)) {
            return null;
        }
        // split predicates for mv rewritten and non-mv-rewritten
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        Map<Table, Column> refBaseTablePartitionCols = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionCols == null || !refBaseTablePartitionCols.containsKey(refBaseTable)) {
            return null;
        }
        Column refPartitionCol = refBaseTablePartitionCols.get(refBaseTable);
        LogicalScanOperator scanOp = scanOperators.get(0);
        // get ref partition column ref from query's scan operator
        Optional<ColumnRefOperator> refPartitionColRefOpt = scanOp.getColRefToColumnMetaMap().keySet().stream()
                .filter(columnRefOperator -> columnRefOperator.getName().equalsIgnoreCase(refPartitionCol.getName()))
                .findFirst();
        // compensate nothing if there is no partition column predicate in the scan node.
        if (!refPartitionColRefOpt.isPresent()) {
            return null;
        }
        ColumnRefOperator refPartitionColRef = refPartitionColRefOpt.get();

        // split partition predicates into mv-rewritten predicates and non-rewritten predicates
        Pair<ScalarOperator, ScalarOperator> splitPartitionPredicates = getSplitPartitionPredicates(mvContext, mv,
                scanOp, refPartitionColRef);

        // why generate push-down aggregate opt rather to use the original aggregate operator?
        // 1. aggregate's predicates/projects cannot be pushed down.
        // 2. if the original aggregate doesn't contain group by keys, add it into projection to be used in the final stage
        AggregatePushDownContext ctx = new AggregatePushDownContext();
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) queryExpression.getOp();
        OptExpression pdOptExpression = buildPushDownOptAggregate(ctx, queryExpression);
        ctx.setAggregator(aggOp);
        OptExpression pdAggOptExpression = doPushDownAggregateRewrite(pdOptExpression, ctx, splitPartitionPredicates);
        if (pdAggOptExpression == null) {
            return null;
        }

        // add final state aggregation above union opt
        OptExpression result = getPushDownRollupFinalAggregateOpt(mvRewriteContext, ctx, remapping,
                queryExpression, Lists.newArrayList(pdAggOptExpression));
        return result;
    }

    private OptExpression doPushDownAggregateRewrite(OptExpression pdOptExpression,
                                                     AggregatePushDownContext ctx,
                                                     Pair<ScalarOperator, ScalarOperator> splitPartitionPredicates) {
        List<ColumnRefOperator> origOutputColumns = getOutputColumns(pdOptExpression);

        // get push down mv expression of an input query opt
        Pair<OptExpression, List<ColumnRefOperator>> mvRewrittenResult = getPushDownMVOptExpression(mvRewriteContext, ctx,
                pdOptExpression, splitPartitionPredicates.first, origOutputColumns);
        if (mvRewrittenResult == null) {
            return null;
        }
        OptExpression mvRewrittenOptExpression = mvRewrittenResult.first;
        List<ColumnRefOperator> mvRewrittenOutputCols = mvRewrittenResult.second;

        // get push down query expression of left query opt
        Pair<OptExpression, List<ColumnRefOperator>> queryLeftResult = getPushDownQueryOptExpression(mvRewriteContext, ctx,
                pdOptExpression, splitPartitionPredicates.second, origOutputColumns);
        if (queryLeftResult == null) {
            return null;
        }
        OptExpression queryLeftOptExpression = queryLeftResult.first;
        List<ColumnRefOperator> queryLeftOutputCols = queryLeftResult.second;

        // union all: mv rewritten push-down expression and query left push-down expression
        OptExpression unionQueryMVOptExpression = getPushDownUnionOptExpression(mvRewrittenOptExpression,
                mvRewrittenOutputCols, queryLeftOptExpression, queryLeftOutputCols, origOutputColumns);
        if (unionQueryMVOptExpression == null) {
            return null;
        }

        return unionQueryMVOptExpression;
    }

    /**
     * Build push-down aggregate opt expression which only contains aggregate+scan operators and aggregate operator only
     * contains the necessary group by keys and aggregates.
     */
    private OptExpression buildPushDownOptAggregate(AggregatePushDownContext ctx,
                                                    OptExpression queryExpression) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) queryExpression.getOp();
        Map<ColumnRefOperator, CallOperator>  aggregations = aggOperator.getAggregations();
        Map<CallOperator, ColumnRefOperator> uniqueAggregations = Maps.newHashMap();
        // agg remappings
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
            ColumnRefOperator aggColRef = entry.getKey();
            CallOperator aggCall = entry.getValue();
            Preconditions.checkArgument(aggCall.getChildren().size() >= 1);
            if (uniqueAggregations.containsKey(aggCall)) {
                ctx.aggColRefToPushDownAggMap.put(aggColRef, aggCall);
                continue;
            }
            // NOTE: This new aggregate type is final stage's type, not the immediate/partial stage type.
            ColumnRefOperator newColRef = queryColumnRefFactory.create(aggCall, aggCall.getType(), aggCall.isNullable());
            uniqueAggregations.put(aggCall, newColRef);

            // record all aggregate in remapping, original (query) agg col ref -> new(query) agg col ref
            remapping.put(aggColRef, newColRef);

            ctx.aggColRefToPushDownAggMap.put(aggColRef, aggCall);
        }
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        uniqueAggregations.forEach((k, v) -> newAggregations.put(v, k));
        List<ColumnRefOperator> groupBys = aggOperator.getGroupingKeys();
        LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                .setAggregations(newAggregations)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(groupBys)
                .setPartitionByColumns(groupBys)
                .build();
        OptExpression optAggOp = OptExpression.create(newAggOp, queryExpression.inputAt(0));
        return optAggOp;
    }

    /**
     * Get push-down mv opt expression which handles predicates that the specific mv can handle.
     */
    public Pair<OptExpression, List<ColumnRefOperator>> getPushDownMVOptExpression(MvRewriteContext mvRewriteContext,
                                                                                   AggregatePushDownContext ctx,
                                                                                   OptExpression queryExpression,
                                                                                   ScalarOperator newPredicate,
                                                                                   List<ColumnRefOperator> origOutputColumns) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        // remove partition column related predicates
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        LogicalScanOperator scanOp = scanOperators.get(0);
        // reset scan operator's predicate
        scanOp.setPredicate(newPredicate);
        // reset scan operator predicates
        if (!(scanOp instanceof LogicalOlapScanOperator)) {
            try {
                scanOp.setScanOperatorPredicates(new ScanOperatorPredicates());
            } catch (Exception e) {
                logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot set scan operator predicates");
                return null;
            }
        }
        // rewrite the input query by the mv context
        OptExpression rewritten = doRewritePushDownAgg(mvRewriteContext, ctx, queryExpression, rule);
        if (rewritten == null) {
            logMVRewrite(mvRewriteContext, "Rewrite table scan node by mv failed");
            return null;
        }

        // refresh remapping & output column refs after mv rewrite
        Map<ColumnRefOperator, ColumnRefOperator> aggColRefMapping = Maps.newHashMap();
        LogicalAggregationOperator rewrittenAggOp = (LogicalAggregationOperator) rewritten.getOp();
        Projection project = rewrittenAggOp.getProjection();
        Map<ColumnRefOperator, ScalarOperator> rewrittenProjectMapping = project == null ? Maps.newHashMap()
                : project.getColumnRefMap();
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : remapping.entrySet()) {
            ColumnRefOperator origAggColRef = e.getKey();
            CallOperator aggCall = ctx.aggregations.get(origAggColRef);
            if (ctx.aggToFinalAggMap.containsKey(aggCall)) {
                CallOperator partialFn = ctx.aggToPartialAggMap.get(aggCall);
                ColumnRefOperator newAggColRef = e.getValue();
                ColumnRefOperator realPartialColRef = new ColumnRefOperator(newAggColRef.getId(), partialFn.getType(),
                        newAggColRef.getName(), partialFn.isNullable());
                remapping.put(origAggColRef, realPartialColRef);
                aggColRefMapping.put(newAggColRef, realPartialColRef);

                if (rewrittenProjectMapping.containsKey(newAggColRef)) {
                    ScalarOperator oldVal = rewrittenProjectMapping.get(newAggColRef);
                    rewrittenProjectMapping.remove(newAggColRef);
                    rewrittenProjectMapping.put(realPartialColRef, oldVal);
                }
            }
        }
        if (!rewrittenProjectMapping.isEmpty()) {
            rewrittenAggOp.setProjection(new Projection(rewrittenProjectMapping));
        }

        // duplicate query left opt expression to void input query opt duplicating
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression dupRewritten = duplicator.duplicate(rewritten);
        deriveLogicalProperty(dupRewritten);
        List<ColumnRefOperator> newOrigOutputColumns = origOutputColumns.stream()
                .map(col -> aggColRefMapping.getOrDefault(col, col))
                .collect(Collectors.toList());
        List<ColumnRefOperator> newOutputColRefs = duplicator.getMappedColumns(newOrigOutputColumns);

        // refresh remapping since after duplication, the column ref id has been changed
        Map<ColumnRefOperator, ColumnRefOperator> aggColMapping = duplicator.getColumnMapping();
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : remapping.entrySet()) {
            remapping.put(e.getKey(), aggColMapping.get(e.getValue()));
        }
        return  Pair.create(dupRewritten, newOutputColRefs);
    }

    /**
     * Get push-down query opt expression which handles predicates that the specific mv cannot handle.
     */
    public Pair<OptExpression, List<ColumnRefOperator>> getPushDownQueryOptExpression(MvRewriteContext mvRewriteContext,
                                                                                      AggregatePushDownContext ctx,
                                                                                      OptExpression queryExpression,
                                                                                      ScalarOperator newPredicate,
                                                                                      List<ColumnRefOperator> origOutputColumns) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        // TODO: use aggregate push down context to generate related push-down aggregation functions
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        LogicalScanOperator logicalOlapScanOp = scanOperators.get(0);
        logicalOlapScanOp.setPredicate(newPredicate);
        Utils.resetOpAppliedRule(logicalOlapScanOp, Operator.OP_PARTITION_PRUNE_BIT);

        LogicalAggregationOperator aggregateOp = (LogicalAggregationOperator) queryExpression.getOp();
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> aggColRefMapping = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> e : aggregateOp.getAggregations().entrySet()) {
            ColumnRefOperator aggColRef = e.getKey();
            CallOperator aggCall = e.getValue();
            CallOperator newAggCall = getRollupPartialAggregate(mvRewriteContext, ctx, aggCall);
            if (newAggCall == null) {
                logMVRewrite(optimizerContext, rule, "AggTimeSeriesRewriter: cannot find partial agg remapping for " + aggCall);
                return null;
            }

            ColumnRefOperator newAggColRef = queryColumnRefFactory.create(newAggCall,
                    newAggCall.getType(), newAggCall.isNullable());
            newAggregations.put(newAggColRef, newAggCall);
            aggColRefMapping.put(aggColRef, newAggColRef);
        }
        LogicalAggregationOperator.Builder builder = LogicalAggregationOperator.builder()
                .withOperator(aggregateOp)
                .setAggregations(newAggregations);
        OptExpression newQueryOptExpression = OptExpression.create(builder.build(),
                queryExpression.inputAt(0));

        // duplicate query left opt expression to void input query opt duplicating
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression queryDuplicateOptExpression = duplicator.duplicate(newQueryOptExpression);
        deriveLogicalProperty(queryDuplicateOptExpression);
        List<ColumnRefOperator> newOrigOutputColumns = origOutputColumns.stream()
                .map(col -> aggColRefMapping.getOrDefault(col, col))
                .collect(Collectors.toList());
        List<ColumnRefOperator> newQueryOutputCols = duplicator.getMappedColumns(newOrigOutputColumns);

        Utils.setOptScanOpsBit(queryDuplicateOptExpression, Operator.OP_UNION_ALL_BIT);

        return Pair.create(queryDuplicateOptExpression, newQueryOutputCols);
    }

    private List<ColumnRefOperator> getOutputColumns(OptExpression optExpression) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) optExpression.getOp();
        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        outputColumns.addAll(aggOperator.getGroupingKeys());
        outputColumns.addAll(aggOperator.getAggregations().keySet());
        return outputColumns;
    }

    /**
     * Union all rewritten mv opt expression and query left opt expression.
     */
    public OptExpression getPushDownUnionOptExpression(OptExpression mvRewrittenOptExpression,
                                                       List<ColumnRefOperator> mvRewrittenOutputCols,
                                                       OptExpression queryLeftOptExpression,
                                                       List<ColumnRefOperator> queryLeftOutputCols,
                                                       List<ColumnRefOperator> origOutputColumns) {
        List<ColumnRefOperator> unionOutputColumns = Lists.newArrayList();
        // change original output columns' type since it's aggregate's intermediate type.
        for (int i = 0; i < origOutputColumns.size(); i++) {
            ColumnRefOperator origOutputCol = origOutputColumns.get(i);
            ColumnRefOperator newAggColRef = mvRewrittenOutputCols.get(i);
            ColumnRefOperator newOutputCol = new ColumnRefOperator(origOutputCol.getId(), newAggColRef.getType(),
                    origOutputCol.getName(), newAggColRef.isNullable());
            unionOutputColumns.add(newOutputCol);
        }
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(unionOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(mvRewrittenOutputCols, queryLeftOutputCols))
                .isUnionAll(true)
                .build();

        // refresh remapping since it has changed after union
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : remapping.entrySet()) {
            ColumnRefOperator newColRef = unionOutputColumns.get(mvRewrittenOutputCols.indexOf(e.getValue()));
            remapping.put(e.getKey(), newColRef);
        }
        OptExpression result = OptExpression.create(unionOperator, mvRewrittenOptExpression, queryLeftOptExpression);
        deriveLogicalProperty(result);
        return result;
    }

    /**
     * Split the original query's partition predicates into rewritten and non-rewritten predicates.
     * rewritten predicates     : predicates which can be used for specific mv to be rewritten
     * non-rewritten predicates : predicates which are left beside rewritten predicates
     */
    public Pair<ScalarOperator, ScalarOperator> getSplitPartitionPredicates(MaterializationContext mvContext,
                                                                            MaterializedView mv,
                                                                            LogicalScanOperator scanOperator,
                                                                            ColumnRefOperator refPartitionColRef) {
        ScalarOperator queryScanPredicate = scanOperator.getPredicate();
        List<ScalarOperator> queryScanPredicates = Utils.extractConjuncts(queryScanPredicate);
        List<ScalarOperator> queryRewrittenPredicates = Lists.newArrayList();
        List<ScalarOperator> queryNonRewrittenPredicates = Lists.newArrayList(queryScanPredicate);
        for (ScalarOperator predicate : queryScanPredicates) {
            List<ColumnRefOperator> colRefs = Lists.newArrayList();
            predicate.getColumnRefs(colRefs);
            if (colRefs.stream().anyMatch(col -> col.equals(refPartitionColRef))) {
                ScalarOperator rewrittenPartitionPredicate = rewritePartitionPredicateToMVPartitionExpr(mvContext,
                        mv, scanOperator, predicate, refPartitionColRef);
                if (rewrittenPartitionPredicate == null) {
                    return null;
                }
                queryRewrittenPredicates.add(rewrittenPartitionPredicate);
                ScalarOperator rewrittenNotPartitionPredicate = CompoundPredicateOperator.not(rewrittenPartitionPredicate);
                queryNonRewrittenPredicates.add(rewrittenNotPartitionPredicate);
            } else {
                queryRewrittenPredicates.add(predicate);
            }
        }
        return Pair.create(Utils.compoundAnd(queryRewrittenPredicates), Utils.compoundAnd(queryNonRewrittenPredicates));
    }

    /**
     * Rewrite the partition predicate to the partition expression of the specific mv so can be rewritten by mv's outputs.
     * @param mvContext
     * @param mv
     * @param logicalScanOp
     * @param refPartitionColPredicate
     * @return
     */
    private ScalarOperator rewritePartitionPredicateToMVPartitionExpr(MaterializationContext mvContext,
                                                                      MaterializedView mv,
                                                                      LogicalScanOperator logicalScanOp,
                                                                      ScalarOperator refPartitionColPredicate,
                                                                      ColumnRefOperator refPartitionColRef) {
        Map<Table, Expr> refBaseTablePartitionExprs = mv.getRefBaseTablePartitionExprs();
        if (!refBaseTablePartitionExprs.containsKey(logicalScanOp.getTable())) {
            return null;
        }
        Expr mvPartitionExpr = refBaseTablePartitionExprs.get(logicalScanOp.getTable());
        List<SlotRef> slotRefs = Lists.newArrayList();
        mvPartitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        ExpressionMapping mapping =
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
        mapping.put(slotRefs.get(0), refPartitionColRef);
        ScalarOperator queryPartitionPredicate =
                SqlToScalarOperatorTranslator.translate(mvPartitionExpr, mapping, mvContext.getQueryRefFactory());
        // rewrite query's partition predicates into predicates which mv can rewrite
        Map<ScalarOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        rewriteMap.put(refPartitionColRef, queryPartitionPredicate);
        ReplaceShuttle replaceShuttle = new ReplaceShuttle(rewriteMap, false);
        ScalarOperator rewritten = replaceShuttle.rewrite(refPartitionColPredicate);
        return rewritten;
    }
}