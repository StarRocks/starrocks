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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensationBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.OptCompensator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_AGG_PRUNE_COLUMNS;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_UNION_REWRITE;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;

/**
 * This class represents all partition compensations for partition predicates in a materialized view.
 */
public class MvPartitionCompensator {
    private static final Logger LOG = LogManager.getLogger(MvPartitionCompensator.class);

    // supported external scan types for partition compensate
    public static final ImmutableSet<OperatorType> SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_HIVE_SCAN)
                    .add(OperatorType.LOGICAL_ICEBERG_SCAN)
                    .add(OperatorType.LOGICAL_PAIMON_SCAN)
                    .build();


    public static final ImmutableSet<OperatorType> SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_OLAP_SCAN)
                    .addAll(SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES)
                    .build();

    /**
     * External scan operators could use {@link com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner}
     * to prune partitions, so we need to compensate partition predicates for them.
     */
    public static final ImmutableSet<OperatorType> SUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_HIVE_SCAN)
                    .build();
    public static final ImmutableSet<OperatorType> UNSUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_ICEBERG_SCAN)
                    .build();

    public static boolean isUnSupportedPartitionPruneExternalScanType(OperatorType operatorType) {
        return UNSUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES.contains(operatorType);
    }

    /**
     * Whether the table is supported to compensate extra partition predicates.
     * @param t: input table
     * @return: true if the table is supported to compensate extra partition predicates, otherwise false.
     */
    public static boolean isSupportPartitionCompensate(Table t) {
        if (t == null) {
            return false;
        }
        if (t.isNativeTableOrMaterializedView() || t.isIcebergTable() || t.isHiveTable() || t.isPaimonTable()) {
            return true;
        }
        return false;
    }

    public static boolean isSupportPartitionPruneCompensate(Table t) {
        if (t == null) {
            return false;
        }
        if (t.isNativeTableOrMaterializedView() || t.isHiveTable()) {
            return true;
        }
        return false;

    }
    /**
     * Determine whether to compensate extra partition predicates to query plan for the mv,
     * - if it needs compensate, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - if not compensate, use original pruned partition predicates as the compensated partition
     *  predicates.
     * @param queryPlan : query opt expression
     * @param mvContext : materialized view context
     * @return Optional<Boolean>: if `queryPlan` contains ref table, Optional is set and return whether mv can satisfy
     * query plan's freshness, other Optional.empty() is returned.
     */
    public static MVCompensation getMvCompensation(OptExpression queryPlan,
                                                   MaterializationContext mvContext) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        MvUpdateInfo mvUpdateInfo = mvContext.getMvUpdateInfo();
        Set<String> mvPartitionNameToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
        // If mv contains no partitions to refresh, no need compensate
        if (Objects.isNull(mvPartitionNameToRefresh) || mvPartitionNameToRefresh.isEmpty()) {
            logMVRewrite(mvContext, "MV has no partitions to refresh, no need compensate");
            return MVCompensation.noCompensate(sessionVariable);
        }

        MVCompensationBuilder mvCompensationBuilder = new MVCompensationBuilder(mvContext, mvUpdateInfo);
        MVCompensation mvCompensation = mvCompensationBuilder.buildMvCompensation(Optional.of(queryPlan));
        logMVRewrite(mvContext, "MV compensation:{}", mvCompensation);
        return mvCompensation;
    }

    /**
     * Get all refreshed partitions' plan of the materialized view.
     * @param mvContext: materialized view context
     * @return:  a pair of compensated mv scan plan(refreshed partitions) and its output columns
     */
    private static Pair<OptExpression, List<ColumnRefOperator>> getMVScanPlan(MaterializationContext mvContext) {
        // NOTE: mv's scan operator has already been partition pruned by filtering refreshed partitions,
        // see MvRewritePreprocessor#createScanMvOperator.
        final LogicalOlapScanOperator mvScanOperator = mvContext.getScanMvOperator();
        final MaterializedView mv = mvContext.getMv();
        final LogicalOlapScanOperator.Builder mvScanBuilder = OperatorBuilderFactory.build(mvScanOperator);
        final List<ColumnRefOperator> orgMvScanOutputColumns = MvUtils.getMvScanOutputColumnRefs(mv,
                mvScanOperator);
        mvScanBuilder.withOperator(mvScanOperator);

        final LogicalOlapScanOperator newMvScanOperator = mvScanBuilder.build();
        OptExpression mvScanOptExpression = OptExpression.create(newMvScanOperator);
        deriveLogicalProperty(mvScanOptExpression);

        // duplicate mv's plan and output columns
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvScanPlan = duplicator.duplicate(mvScanOptExpression);
        newMvScanPlan.getOp().setOpRuleBit(OP_MV_UNION_REWRITE);
        // output columns order by mv's columns
        List<ColumnRefOperator> mvScanOutputColumns = duplicator.getMappedColumns(orgMvScanOutputColumns);
        return Pair.create(newMvScanPlan, mvScanOutputColumns);
    }

    /**
     * Get all to-refreshed partitions' plan of the materialized view.
     * @param mvContext: materialized view context
     * @param mvCompensation: materialized view's compensation info
     * @return:  a pair of compensated mv query scan plan(to-refreshed partitions) and its output columns
     */
    private static Pair<OptExpression, List<ColumnRefOperator>> getMVCompensationPlan(
            MaterializationContext mvContext,
            MVCompensation mvCompensation,
            List<ColumnRefOperator> originalOutputColumns,
            boolean isMVRewrite) {
        final OptExpression mvQueryPlan = mvContext.getMvExpression();
        OptExpression compensateMvQueryPlan = getMvCompensateQueryPlan(mvContext, mvCompensation, mvQueryPlan);
        if (compensateMvQueryPlan == null) {
            logMVRewrite(mvContext, "Get mv compensate query plan failed");
            return null;
        }
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvQueryPlan = duplicator.duplicate(compensateMvQueryPlan, mvContext.getMvColumnRefFactory());
        if (newMvQueryPlan == null) {
            logMVRewrite(mvContext, "Duplicate compensate query plan failed");
            return null;
        }
        deriveLogicalProperty(newMvQueryPlan);
        List<ColumnRefOperator> orgMvQueryOutputColumnRefs = mvContext.getMvOutputColumnRefs();
        List<ColumnRefOperator> mvQueryOutputColumnRefs = duplicator.getMappedColumns(orgMvQueryOutputColumnRefs);
        newMvQueryPlan.getOp().setOpRuleBit(OP_MV_UNION_REWRITE);
        if (isMVRewrite) {
            // NOTE: mvScanPlan and mvCompensatePlan will output all columns of the mv's defined query,
            // it may contain more columns than the requiredColumns.
            // 1. For simple non-blocking operators(scan/join), it can be pruned by normal rules, but for
            // aggregate operators, it should be handled in MVColumnPruner.
            // 2. For mv rewrite, it's safe to prune aggregate columns in mv compensate plan, but it cannot determine
            // required columns in the transparent rule.
            List<LogicalAggregationOperator> list = Lists.newArrayList();
            Utils.extractOperator(newMvQueryPlan, list, op -> op instanceof LogicalAggregationOperator);
            list.stream().forEach(op -> op.setOpRuleBit(OP_MV_AGG_PRUNE_COLUMNS));
        }
        // Adjust query output columns to mv's output columns to make sure the output columns are the same as
        // expectOutputColumns which are mv scan operator's output columns.
        return adjustOptExpressionOutputColumnType(mvContext.getQueryRefFactory(),
                newMvQueryPlan, mvQueryOutputColumnRefs, originalOutputColumns);
    }

    public static OptExpression getMvCompensateQueryPlan(MaterializationContext mvContext,
                                                         MVCompensation mvCompensation,
                                                         OptExpression mvQueryPlan) {
        MaterializedView mv = mvContext.getMv();
        if (mv.getRefBaseTablePartitionColumns() == null) {
            return null;
        }
        return OptCompensator.getMVCompensatePlan(mvContext.getOptimizerContext(),
                mv, mvCompensation, mvQueryPlan);
    }

    /**
     * <p> What's the transparent plan of mv? </p>
     *
     * Transparent MV: select * from t(all partition refreshed) union all select * from mv's defined query
     *  (all to-refresh partitions)
     *
     * eg:
     * MV       : select col1, col2 from t;
     * MV refreshed partition: dt = '2023-11-01'
     *
     * Query    : select col1, col2 from t where dt in ('2023-11-01', '2023-11-02')
     * Rewritten: select col1, col2 from <Transparent MV> where dt in ('2023-11-01', '2023-11-02')
     *
     * Transparent MV : Refreshed MV Partitions union all To-Refresh MV Partitions
     * = select * from mv where dt='2023-11-01' union all select * from t where dt = '2023-11-02'
     */
    public static OptExpression getMvTransparentPlan(MaterializationContext mvContext,
                                                     MVCompensation mvCompensation,
                                                     List<ColumnRefOperator> originalOutputColumns,
                                                     boolean isMVRewrite) {
        Preconditions.checkArgument(originalOutputColumns != null);
        Preconditions.checkState(mvCompensation.getState().isCompensate());

        final Pair<OptExpression, List<ColumnRefOperator>> mvScanPlan = getMVScanPlan(mvContext);
        if (mvScanPlan == null) {
            logMVRewrite(mvContext, "Get mv scan transparent plan failed");
            return null;
        }

        final Pair<OptExpression, List<ColumnRefOperator>> mvCompensationPlan = getMVCompensationPlan(mvContext,
                mvCompensation, originalOutputColumns, isMVRewrite);
        if (mvCompensationPlan == null) {
            logMVRewrite(mvContext, "Get mv query transparent plan failed");
            return null;
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(originalOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(mvScanPlan.second, mvCompensationPlan.second))
                .isUnionAll(true)
                .build();
        OptExpression result = OptExpression.create(unionOperator, mvScanPlan.first, mvCompensationPlan.first);
        deriveLogicalProperty(result);
        return result;
    }

    /**
     * In some cases, need add cast project to make sure the output columns are the same as expectOutputColumns.
     * <p>
     * eg: table t1: k1 date, v1 int, v2 char(20)
     * mv: create mv mv1 as select k1, v1, v2 from t1.
     * mv's schema will be: k1 date, v1 int, v2 varchar(20)
     * </p>
     * It needs to add a cast project in generating the union operator, which is the same as
     * {@link com.starrocks.sql.optimizer.transformer.RelationTransformer#processSetOperation(SetOperationRelation)}
     * </p>
     * @param columnRefFactory column ref factory to generate the new query column ref
     * @param optExpression the original opt expression
     * @param curOutputColumns the original output columns of the opt expression
     * @param expectOutputColumns the expected output columns
     * @return the new opt expression and the new output columns if it needs to cast, otherwise return the original
     */
    private static Pair<OptExpression, List<ColumnRefOperator>> adjustOptExpressionOutputColumnType(
            ColumnRefFactory columnRefFactory,
            OptExpression optExpression,
            List<ColumnRefOperator> curOutputColumns,
            List<ColumnRefOperator> expectOutputColumns) {
        Preconditions.checkState(curOutputColumns.size() == expectOutputColumns.size());
        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
        List<ColumnRefOperator> newChildOutputs = new ArrayList<>();
        int len = curOutputColumns.size();
        boolean isNeedCast = false;
        for (int i = 0; i < len; i++) {
            ColumnRefOperator outOp = curOutputColumns.get(i);
            Type outputType = outOp.getType();
            ColumnRefOperator expectOp = expectOutputColumns.get(i);
            Type expectType = expectOp.getType();
            if (!outputType.equals(expectType)) {
                isNeedCast = true;
                ColumnRefOperator newColRef = columnRefFactory.create("cast", expectType, expectOp.isNullable());
                ScalarOperator cast = new CastOperator(outputType, outOp, true);
                projections.put(newColRef, cast);
                newChildOutputs.add(newColRef);
            } else {
                projections.put(outOp, outOp);
                newChildOutputs.add(outOp);
            }
        }
        if (isNeedCast) {
            OptExpression newOptExpression = Utils.mergeProjection(optExpression, projections);
            return Pair.create(newOptExpression, newChildOutputs);
        } else {
            return Pair.create(optExpression, curOutputColumns);
        }
    }

    /**
     * - if `isCompensate` is true, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - otherwise use original pruned partition predicates as the compensated partition
     *  predicates.
     * NOTE: When MV has enough partitions for the query, no need to compensate anymore for both mv and the query's plan.
     *       A query can be rewritten just by the original SQL.
     * NOTE: It's not safe if `isCompensate` is always false:
     *      - partitionPredicate is null if olap scan operator cannot prune partitions.
     *      - partitionPredicate is not exact even if olap scan operator has pruned partitions.
     * eg:
     *      t1:
     *       PARTITION p1 VALUES [("0000-01-01"), ("2020-01-01")), has data
     *       PARTITION p2 VALUES [("2020-01-01"), ("2020-02-01")), has data
     *       PARTITION p3 VALUES [("2020-02-01"), ("2020-03-01")), has data
     *       PARTITION p4 VALUES [("2020-03-01"), ("2020-04-01")), no data
     *       PARTITION p5 VALUES [("2020-04-01"), ("2020-05-01")), no data
     *
     *      query1 : SELECT k1, sum(v1) as sum_v1 FROM t1 group by k1;
     *      `partitionPredicate` : null
     *
     *      query2 : SELECT k1, sum(v1) as sum_v1 FROM t1 where k1>='2020-02-01' group by k1;
     *      `partitionPredicate` : k1>='2020-02-11'
     *      however for mv  we need: k1>='2020-02-11' and k1 < "2020-03-01"
     */
    public static ScalarOperator compensateQueryPartitionPredicate(MaterializationContext mvContext,
                                                                   Rule rule,
                                                                   ColumnRefFactory columnRefFactory,
                                                                   OptExpression queryExpression) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        if (scanOperators.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }

        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        MVCompensation mvCompensation = mvContext.getMvCompensation();
        if (mvCompensation.getState().isNoRewrite()) {
            return null;
        }
        // Compensate partition predicates and add them into query predicate.
        Map<LogicalScanOperator, List<ScalarOperator>> scanOperatorScalarOperatorMap =
                mvContext.getScanOpToPartitionCompensatePredicates();
        MaterializedView mv = mvContext.getMv();
        final Set<Table> baseTables = new HashSet<>(mvContext.getBaseTables());
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES.contains(scanOperator.getOpType())) {
                // If the scan operator is not supported, then return null when compensate type is not NO_COMPENSATE
                // which means the query cannot be rewritten only when all partitions are refreshed.
                // Change query_rewrite_consistency=loose to no check query rewrite consistency.
                if (!mvCompensation.isCompensatePartitionPredicate()) {
                    continue;
                }
                return null;
            }
            List<ScalarOperator> partitionPredicate = scanOperatorScalarOperatorMap
                    .computeIfAbsent(scanOperator, x -> {
                        if (!baseTables.contains(scanOperator.getTable())) {
                            return Collections.emptyList();
                        }
                        return getScanOpPrunedPartitionPredicates(mv, scanOperator);
                    });
            if (partitionPredicate == null) {
                logMVRewrite(mvContext.getMv().getName(), "Compensate partition failed for scan {}",
                        scanOperator.getTable().getName());
                return null;
            }
            partitionPredicates.addAll(partitionPredicate);
        }
        ScalarOperator compensatePredicate = partitionPredicates.isEmpty() ? ConstantOperator.createBoolean(true) :
                Utils.compoundAnd(partitionPredicates);
        logMVRewrite(mvContext.getOptimizerContext(), rule, "Query Compensate partition predicate:{}", compensatePredicate);
        return compensatePredicate;
    }

    // convert varchar date to date type
    @VisibleForTesting
    public static Range<PartitionKey> convertToDateRange(Range<PartitionKey> from) throws AnalysisException {
        if ((from.hasLowerBound() && !(from.lowerEndpoint().getKeys().get(0) instanceof StringLiteral)) ||
                (from.hasUpperBound() && !(from.upperEndpoint().getKeys().get(0) instanceof StringLiteral))) {
            return from;
        }

        if (from.hasLowerBound() && from.hasUpperBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());

            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.range(lowerPartitionKey, from.lowerBoundType(), upperPartitionKey, from.upperBoundType());
        } else if (from.hasUpperBound()) {
            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.upTo(upperPartitionKey, from.upperBoundType());
        } else if (from.hasLowerBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());
            return Range.downTo(lowerPartitionKey, from.lowerBoundType());
        }
        return Range.all();
    }

    private static List<ScalarOperator> getScanOpPrunedPartitionPredicates(MaterializedView mv,
                                                                           LogicalScanOperator scanOperator) {
        if (scanOperator instanceof LogicalOlapScanOperator) {
            return ((LogicalOlapScanOperator) scanOperator).getPrunedPartitionPredicates();
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            try {
                ScanOperatorPredicates scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
                return scanOperatorPredicates.getPrunedPartitionConjuncts();
            } catch (AnalysisException e) {
                logMVRewrite(mv.getName(), "Compensate partition predicate for mv {} failed: {}",
                        mv.getName(), DebugUtil.getStackTrace(e));
                return null;
            }
        } else {
            // Cannot decide whether it has been pruned or not, return null for now.
            logMVRewrite(mv.getName(), "Compensate partition predicate for mv {} failed: unsupported scan " +
                            "operator type {} for {}", mv.getName(),
                    scanOperator.getOpType(), scanOperator.getTable().getName());
            return null;
        }
    }

    /**
     * Get transparent plan if possible.
     * What's the transparent plan?
     * see {@link MvPartitionCompensator#getMvTransparentPlan}
     */
    public static OptExpression getMvTransparentPlan(MaterializationContext mvContext,
                                                     OptExpression input,
                                                     List<ColumnRefOperator> expectOutputColumns) {
        // NOTE: MV's mvSelectPartitionIds is not trusted in transparent since it is targeted for the whole partitions(refresh
        //  and no refreshed).
        // 1. Decide ref base table partition ids to refresh in optimizer.
        // 2. consider partition prunes for the input olap scan operator
        MvUpdateInfo mvUpdateInfo = mvContext.getMvUpdateInfo();
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            logMVRewrite(mvContext, "Failed to get mv to refresh partition info: {}", mvContext.getMv().getName());
            return null;
        }
        MVCompensationBuilder mvCompensationBuilder = new MVCompensationBuilder(mvContext, mvUpdateInfo);
        MVCompensation mvCompensation = mvCompensationBuilder.buildMvCompensation(Optional.empty());
        if (mvCompensation == null) {
            logMVRewrite(mvContext, "Failed to get mv compensation info: {}", mvContext.getMv().getName());
            return null;
        }
        logMVRewrite(mvContext, "Get mv compensation info: {}", mvCompensation);
        if (mvCompensation.isNoCompensate()) {
            return input;
        }
        if (mvCompensation.isUncompensable()) {
            logMVRewrite(mvContext, "Return directly because mv compensation info cannot compensate: {}",
                    mvCompensation);
            return null;
        }
        OptExpression transparentPlan = MvPartitionCompensator.getMvTransparentPlan(mvContext, mvCompensation,
                expectOutputColumns, false);
        return transparentPlan;
    }
}
