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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OpRuleBit;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.ReplaceShuttle;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggregatePushDownContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.FunctionSet.WEEK;
import static com.starrocks.sql.common.TimeUnitUtils.DAY;
import static com.starrocks.sql.common.TimeUnitUtils.HOUR;
import static com.starrocks.sql.common.TimeUnitUtils.MINUTE;
import static com.starrocks.sql.common.TimeUnitUtils.MONTH;
import static com.starrocks.sql.common.TimeUnitUtils.QUARTER;
import static com.starrocks.sql.common.TimeUnitUtils.SECOND;
import static com.starrocks.sql.common.TimeUnitUtils.YEAR;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.isSupportedAggFunctionPushDown;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.doRewritePushDownAgg;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.getPushDownRollupFinalAggregateOpt;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.getRollupPartialAggregate;

/**
 * In time-series business scenarios, metrics are aggregated based on the time dimension, with historical
 * data continuously being archived (rolled up, compressed), while new data flows in real-time.
 * By using Materialized Views (MVs) to speed up, different time granularity of MVs can be constructed,
 * the most common being day, month, and year. However, when querying the base table (original table),
 * it is hoped that the archived year, month, and day data can be used to speed up time slicing.
 *
 * <p> Implementation </p>
 * Place the logic of splitting time predicates in time-series scenarios + the logic of aggregation push-down into a Rule. When
 * it can be rewritten, rewrite the output; otherwise, do not change the original Query.
 * Reuse the aggregation push-down + rewriting capabilities, and implement aggregation push-down + rolling up based on the
 * aggregation status defined by the materialized view (later, general aggregation status can be used).
 * Reuse the capability of nested MV rewriting, recursively call this Rule,
 * and rewrite the scope of granularity as much as possible in one go.
 */
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
        // if mv rewrite generates the table, skip it
        if (refBaseTable instanceof MaterializedView && mv.equals(refBaseTable)) {
            return false;
        }

        // check mv is partitioned table
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!partitionInfo.isRangePartition()) {
            return false;
        }
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return false;
        }
        Expr mvPartitionExpr = mvPartitionExprOpt.get();
        if (mvPartitionExpr == null || !(mvPartitionExpr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) mvPartitionExpr;
        if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return false;
        }

        // check ref base table is partitioned or not
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
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: base table size is not 1, size=" + baseTables.size());
            return null;
        }
        Table refBaseTable = baseTables.get(0);
        MaterializedView mv = mvContext.getMv();
        FunctionCallExpr mvPartitionExpr = getMVPartitionExpr(mv);
        if (mvPartitionExpr == null) {
            return null;
        }
        OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        if (!isEnableTimeGranularityRollup(refBaseTable, mv, queryExpression)) {
            logMVRewrite(mvRewriteContext,
                    "AggTimeSeriesRewriter: cannot enable time granularity rollup for mv: " + mv.getName());
            return null;
        }
        // split predicates for mv rewritten and non-mv-rewritten
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        Map<Table, List<Column>> refBaseTablePartitionCols = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionCols == null || !refBaseTablePartitionCols.containsKey(refBaseTable)) {
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot find partition column for ref base table");
            return null;
        }
        List<Column> refPartitionCols = refBaseTablePartitionCols.get(refBaseTable);
        Preconditions.checkArgument(refPartitionCols.size() == 1);
        Column refPartitionCol = refPartitionCols.get(0);
        LogicalScanOperator scanOp = scanOperators.get(0);
        // get ref partition column ref from query's scan operator
        Optional<ColumnRefOperator> refPartitionColRefOpt = scanOp.getColRefToColumnMetaMap().keySet().stream()
                .filter(columnRefOperator -> columnRefOperator.getName().equalsIgnoreCase(refPartitionCol.getName()))
                .findFirst();
        // compensate nothing if there is no partition column predicate in the scan node.
        if (!refPartitionColRefOpt.isPresent()) {
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot find partition column ref in scan node");
            return null;
        }
        ColumnRefOperator refPartitionColRef = refPartitionColRefOpt.get();

        // split partition predicates into mv-rewritten predicates and non-rewritten predicates
        ScalarOperator queryCompensatePedicate = MvPartitionCompensator.compensateQueryPartitionPredicate(
                mvContext, rule, optimizerContext.getColumnRefFactory(), queryExpression);
        ScalarOperator queryScanPredicate = Utils.compoundAnd(queryCompensatePedicate, scanOp.getPredicate());
        Pair<ScalarOperator, ScalarOperator> splitPartitionPredicates = getSplitPartitionPredicates(mvContext,
                mvPartitionExpr, queryScanPredicate, refPartitionColRef);
        if (splitPartitionPredicates == null) {
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot split partition predicates");
            return null;
        }

        // why generate push-down aggregate opt rather to use the original aggregate operator?
        // 1. aggregate's predicates/projects cannot be pushed down.
        // 2. if the original aggregate doesn't contain group by keys, add it into projection to be used in the final stage
        AggregatePushDownContext ctx = new AggregatePushDownContext();
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) queryExpression.getOp();
        OptExpression pdOptExpression = buildPushDownOptAggregate(ctx, queryExpression);
        ctx.setAggregator(aggOp);
        OptExpression pdAggOptExpression = doPushDownAggregateRewrite(pdOptExpression, ctx, splitPartitionPredicates);
        if (pdAggOptExpression == null) {
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot rewrite push-down aggregate");
            return null;
        }

        // add final state aggregation above union opt
        OptExpression result = getPushDownRollupFinalAggregateOpt(mvRewriteContext, ctx, remapping,
                queryExpression, Lists.newArrayList(pdAggOptExpression));

        return result;
    }

    @Override
    public OptExpression postRewrite(OptimizerContext optimizerContext,
                                     MvRewriteContext mvRewriteContext,
                                     OptExpression candidate) {
        OptExpression result = super.postRewrite(optimizerContext, mvRewriteContext, candidate);
        MvUtils.getScanOperator(result)
                .stream()
                .forEach(op -> op.resetOpRuleBit(OpRuleBit.OP_PARTITION_PRUNED));
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
        // copy scan operator to avoid polluting the original query expression
        LogicalScanOperator scanOperator = (LogicalScanOperator) queryExpression.inputAt(0).getOp();
        LogicalScanOperator newScanOperator = buildLogicalScanOperator(scanOperator);
        if (newScanOperator == null) {
            return null;
        }
        OptExpression optAggOp = OptExpression.create(newAggOp, OptExpression.create(newScanOperator));
        return optAggOp;
    }

    private LogicalScanOperator buildLogicalScanOperator(LogicalScanOperator scanOperator) {
        LogicalScanOperator newScanOperator;
        if (scanOperator instanceof LogicalOlapScanOperator) {
            newScanOperator = new LogicalOlapScanOperator.Builder()
                    .withOperator((LogicalOlapScanOperator) scanOperator)
                    .setPrunedPartitionPredicates(Lists.newArrayList())
                    .build();
        } else if (scanOperator instanceof LogicalHiveScanOperator) {
            newScanOperator = new LogicalHiveScanOperator.Builder()
                    .withOperator((LogicalHiveScanOperator) scanOperator)
                    .build();
        } else if (scanOperator instanceof LogicalIcebergScanOperator) {
            newScanOperator = new LogicalIcebergScanOperator.Builder()
                    .withOperator((LogicalIcebergScanOperator) scanOperator)
                    .build();
        } else if (scanOperator instanceof LogicalPaimonScanOperator) {
            newScanOperator = new LogicalPaimonScanOperator.Builder()
                    .withOperator((LogicalPaimonScanOperator) scanOperator)
                    .build();
        } else if (scanOperator instanceof LogicalJDBCScanOperator) {
            newScanOperator = new LogicalJDBCScanOperator.Builder()
                    .withOperator((LogicalJDBCScanOperator) scanOperator)
                    .build();
        } else if (scanOperator instanceof LogicalHudiScanOperator) {
            newScanOperator = new LogicalHudiScanOperator.Builder()
                    .withOperator((LogicalHudiScanOperator) scanOperator)
                    .build();
        } else {
            return null;
        }

        // reset scan operator predicates
        if (!(newScanOperator instanceof LogicalOlapScanOperator)) {
            try {
                newScanOperator.setScanOperatorPredicates(new ScanOperatorPredicates());
            } catch (Exception e) {
                logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot set scan operator predicates");
                return null;
            }
        }
        return newScanOperator;
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

        // rewrite the input query by the mv context
        OptExpression rewritten = doRewritePushDownAgg(mvRewriteContext, ctx, queryExpression, rule);
        if (rewritten == null) {
            logMVRewrite(mvRewriteContext, "Rewrite table scan node by mv failed");
            return null;
        }
        // if mv contains no rows after rewrite, return it directly.
        rewritten = postRewrite(optimizerContext, mvRewriteContext, rewritten);
        if (rewritten == null) {
            return null;
        }
        List<LogicalScanOperator> rewrittenScanOperators = MvUtils.getScanOperator(rewritten);
        if (rewrittenScanOperators.size() == 1 && rewrittenScanOperators.get(0).isEmptyOutputRows()) {
            logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: mv contains no rows after rewrite");
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
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        LogicalScanOperator logicalOlapScanOp = scanOperators.get(0);
        logicalOlapScanOp.setPredicate(newPredicate);
        LogicalAggregationOperator aggregateOp = (LogicalAggregationOperator) queryExpression.getOp();
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> aggColRefMapping = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> e : aggregateOp.getAggregations().entrySet()) {
            ColumnRefOperator aggColRef = e.getKey();
            CallOperator aggCall = e.getValue();
            // use aggregate push down context to generate related push-down aggregation functions
            CallOperator newAggCall = getRollupPartialAggregate(mvRewriteContext, ctx, aggCall);
            if (newAggCall == null) {
                logMVRewrite(mvRewriteContext, "AggTimeSeriesRewriter: cannot find partial agg remapping for " + aggCall);
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

        return Pair.create(queryDuplicateOptExpression, newQueryOutputCols);
    }

    private List<ColumnRefOperator> getOutputColumns(OptExpression optExpression) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) optExpression.getOp();
        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        outputColumns.addAll(aggOperator.getGroupingKeys());
        outputColumns.addAll(aggOperator.getAggregations().keySet());
        return outputColumns;
    }

    private FunctionCallExpr getMVPartitionExpr(MaterializedView mv) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!partitionInfo.isExprRangePartitioned()) {
            return null;
        }
        Optional<Expr> partitionExprOpt = mv.getRangePartitionFirstExpr();
        if (partitionExprOpt.isEmpty()) {
            return null;
        }
        Expr partitionExpr = partitionExprOpt.get();
        if (partitionExpr == null || !(partitionExpr instanceof FunctionCallExpr)) {
            return null;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
        if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return null;
        }
        if (!(functionCallExpr.getChild(0) instanceof StringLiteral)) {
            return null;
        }
        return functionCallExpr;
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
                                                                            FunctionCallExpr mvPartitionExpr,
                                                                            ScalarOperator queryScanPredicate,
                                                                            ColumnRefOperator refPartitionColRef) {
        logMVRewrite(mvRewriteContext, "split predicate={}", queryScanPredicate);
        List<ScalarOperator> queryScanPredicates = Utils.extractConjuncts(queryScanPredicate);

        // part1 is used  for mv's rewrite, part2 is left for query's rewrite, part1 and part2 should be disjoint and union
        // for all the time-series of input predicate.
        List<ScalarOperator> queryPartitionPredicates = Lists.newArrayList();
        List<ScalarOperator> queryNonPartitionPredicates = Lists.newArrayList();
        List<ScalarOperator> rewrittenPartitionPredicates = Lists.newArrayList();

        StringLiteral timeUnit = (StringLiteral) mvPartitionExpr.getChild(0);
        for (ScalarOperator predicate : queryScanPredicates) {
            // collect all column references in the predicate
            List<ColumnRefOperator> colRefs = Lists.newArrayList();
            predicate.getColumnRefs(colRefs);
            List<ColumnRefOperator> refPartitionColRefs = colRefs.stream()
                    .filter(col -> col.equals(refPartitionColRef))
                    .collect(Collectors.toList());
            // only can push down partition predicate if there is only one partition column reference
            if (refPartitionColRefs.size() > 1) {
                return null;
            }

            if (refPartitionColRefs.size() == 1) {
                // TODO: only can handle binary predicate, maybe we can loose this restriction in the future
                if (!(predicate instanceof BinaryPredicateOperator)) {
                    return null;
                }
                BinaryPredicateOperator binaryPredicate =
                        normalizePartitionPredicate((BinaryPredicateOperator) predicate);
                if (binaryPredicate == null) {
                    return null;
                }
                BinaryType binaryType = binaryPredicate.getBinaryType();
                // for not range predicates, it makes no sense to rewrite, return directly
                if (binaryType == BinaryType.EQ || binaryType == BinaryType.NE) {
                    return null;
                }
                // because date_trunc function will take down the input time to the beginning of the time unit,
                // so it's not safe to rewrite the range predicate.
                // queryNonRewrittenPredicates.add(predicate);
                if (binaryType == BinaryType.LT || binaryType == BinaryType.LE) {
                    binaryPredicate = getLowerPartitionPredicate(binaryPredicate, timeUnit);
                    if (binaryPredicate == null) {
                        return null;
                    }
                }
                ScalarOperator rewrittenPartitionPredicate = rewritePartitionPredicateToMVPartitionExpr(mvContext,
                        mvPartitionExpr, binaryPredicate, refPartitionColRef);
                if (rewrittenPartitionPredicate == null) {
                    return null;
                }
                // rewritten predicates
                rewrittenPartitionPredicates.add(rewrittenPartitionPredicate);
                queryPartitionPredicates.add(predicate);
            } else {
                queryNonPartitionPredicates.add(predicate);
            }
        }
        // if there is no predicate that can be pushed down into mv, return null directly
        if (rewrittenPartitionPredicates.isEmpty()) {
            return null;
        }
        rewrittenPartitionPredicates.addAll(queryNonPartitionPredicates);

        // non-rewritten predicates
        final List<ScalarOperator> leftQueryPartitionPredicates = Lists.newArrayList(queryNonPartitionPredicates);
        final NegateFilterShuttle negateFilterShuttle = NegateFilterShuttle.getInstance();
        final List<ScalarOperator> notQueryPartitionPredicates = rewrittenPartitionPredicates.stream()
                .map(pred -> negateFilterShuttle.negateFilter(pred))
                .collect(Collectors.toList());
        leftQueryPartitionPredicates.add(Utils.compoundOr(notQueryPartitionPredicates));
        leftQueryPartitionPredicates.addAll(queryPartitionPredicates);
        logMVRewrite(mvRewriteContext, "rewritten predicates={}, non-rewritten predicates:{}",
                rewrittenPartitionPredicates, leftQueryPartitionPredicates);

        return Pair.create(Utils.compoundAnd(rewrittenPartitionPredicates), Utils.compoundAnd(leftQueryPartitionPredicates));
    }

    /**
     * Normalize binary predicate to make sure the constant is on the right side.
     */
    private BinaryPredicateOperator normalizePartitionPredicate(BinaryPredicateOperator binaryPredicate) {
        ScalarOperator child0 = binaryPredicate.getChild(0);
        ScalarOperator child1 = binaryPredicate.getChild(1);
        if (child0 instanceof ConstantOperator) {
            binaryPredicate.swap();
        } else if (child1 instanceof ConstantOperator) {
            // do nothing.
        } else {
            return null;
        }
        return binaryPredicate;
    }

    private BinaryPredicateOperator getLowerPartitionPredicate(BinaryPredicateOperator binaryPredicate,
                                                               StringLiteral timeUnit) {
        ScalarOperator child0 = binaryPredicate.getChild(0);
        ScalarOperator child1 = binaryPredicate.getChild(1);
        ConstantOperator constantOp = (ConstantOperator) child1;
        // if the constant is not a date, return null directly
        if (!constantOp.getType().isDatetime()) {
            return null;
        }
        LocalDateTime lowerDateTime = getLowerDateTime(constantOp.getDatetime(), timeUnit.getStringValue());
        ConstantOperator newConstOp = ConstantOperator.createDatetimeOrNull(lowerDateTime);
        BinaryPredicateOperator newBinaryPredicate = new BinaryPredicateOperator(binaryPredicate.getBinaryType(),
                child0, newConstOp);
        return newBinaryPredicate;
    }

    private LocalDateTime getLowerDateTime(LocalDateTime lowerDateTime, String granularity) {
        LocalDateTime truncLowerDateTime;
        switch (granularity) {
            case SECOND:
                truncLowerDateTime = lowerDateTime.minusSeconds(1);
                break;
            case MINUTE:
                truncLowerDateTime = lowerDateTime.minusMinutes(1);
                break;
            case HOUR:
                truncLowerDateTime = lowerDateTime.minusHours(1);
                break;
            case DAY:
                truncLowerDateTime = lowerDateTime.minusDays(1);
                break;
            case WEEK:
                truncLowerDateTime = lowerDateTime.minusWeeks(1);
                break;
            case MONTH:
                truncLowerDateTime = lowerDateTime.minusMonths(1);
                break;
            case QUARTER:
                truncLowerDateTime = lowerDateTime.minusMonths(3);
                break;
            case YEAR:
                truncLowerDateTime = lowerDateTime.minusYears(1);
                break;
            default:
                return null;
        }
        return truncLowerDateTime;
    }

    /**
     * Rewrite the partition predicate to the partition expression of the specific mv so can be rewritten by mv's outputs.
     */
    private ScalarOperator rewritePartitionPredicateToMVPartitionExpr(MaterializationContext mvContext,
                                                                      FunctionCallExpr mvPartitionExpr,
                                                                      ScalarOperator refPartitionColPredicate,
                                                                      ColumnRefOperator refPartitionColRef) {
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