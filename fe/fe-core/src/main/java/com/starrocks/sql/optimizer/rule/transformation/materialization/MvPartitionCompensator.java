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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensationBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.OptCompensator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
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
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_AGG_PRUNE_COLUMNS;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_UNION_REWRITE;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.mergeRanges;

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
        if (t.isNativeTableOrMaterializedView() || t.isIcebergTable() || t.isHiveTable()) {
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
        OptExpression newMvQueryPlan = duplicator.duplicate(compensateMvQueryPlan);
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

    private static List<ScalarOperator> getCompensatePartitionPredicates(MaterializationContext mvContext,
                                                                         ColumnRefFactory columnRefFactory,
                                                                         LogicalScanOperator scanOperator) {
        List<ScalarOperator> partitionPredicate = null;
        if (scanOperator instanceof LogicalOlapScanOperator) {
            partitionPredicate = compensatePartitionPredicateForOlapScan((LogicalOlapScanOperator) scanOperator,
                    columnRefFactory);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            partitionPredicate = compensatePartitionPredicateForExternalTables(mvContext, scanOperator);
        } else {
            logMVRewrite(mvContext.getMv().getName(), "Compensate partition failed: unsupported scan " +
                    "operator type {} for {}", scanOperator.getOpType(), scanOperator.getTable().getName());
            return null;
        }
        return partitionPredicate;
    }

    private static boolean supportCompensatePartitionPredicateForHiveScan(List<PartitionKey> partitionKeys) {
        for (PartitionKey partitionKey : partitionKeys) {
            // only support one partition column now.
            if (partitionKey.getKeys().size() != 1) {
                return false;
            }
            LiteralExpr e = partitionKey.getKeys().get(0);
            // Only support date/int type
            if (!(e instanceof DateLiteral || e instanceof IntLiteral)) {
                return false;
            }
        }
        return true;
    }

    private static List<ScalarOperator> compensatePartitionPredicateForExternalTables(MaterializationContext mvContext,
                                                                                      LogicalScanOperator scanOperator) {
        if (!SUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            return Lists.newArrayList();
        }

        ScanOperatorPredicates scanOperatorPredicates = null;
        try {
            scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
        } catch (AnalysisException e) {
            return null;
        }
        if (scanOperatorPredicates == null) {
            return null;
        }

        Table baseTable = scanOperator.getTable();
        if (baseTable.isUnPartitioned()) {
            return Lists.newArrayList();
        }

        OperatorType operatorType = scanOperator.getOpType();
        // only used for hive
        if (operatorType == OperatorType.LOGICAL_HIVE_SCAN && scanOperatorPredicates.getSelectedPartitionIds().size()
                == scanOperatorPredicates.getIdToPartitionKey().size()) {
            return Lists.newArrayList();
        }
        // do not support compensate partition predicate for hive scan if partition key has more than one column,
        // we could return empty list here because queryConjuncts in queryExpression for external table will add all conjuncts
        if (operatorType == OperatorType.LOGICAL_HIVE_SCAN &&
                !supportCompensatePartitionPredicateForHiveScan(scanOperatorPredicates.getSelectedPartitionKeys())) {
            return Lists.newArrayList();
        }

        List<Range<PartitionKey>> ranges = Lists.newArrayList();
        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = getRefBaseTablePartitionColumn(mv);
        if (partitionTableAndColumns == null) {
            return null;
        }
        Optional<Expr> mvPartitionOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionOpt.isEmpty()) {
            return null;
        }
        Expr mvPartitionExpr = mvPartitionOpt.get();
        Column partitionColumn = partitionTableAndColumns.second;
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mvPartitionExpr, partitionColumn);
        for (PartitionKey selectedPartitionKey : scanOperatorPredicates.getSelectedPartitionKeys()) {
            try {
                LiteralExpr literalExpr = selectedPartitionKey.getKeys().get(0);
                if (isConvertToDate) {
                    literalExpr = PartitionUtil.convertToDateLiteral(literalExpr);
                    if (literalExpr == null) {
                        return null;
                    }
                }
                PartitionUtil.DateTimeInterval interval = PartitionUtil.getDateTimeInterval(baseTable,
                        baseTable.getPartitionColumns().get(0));
                LiteralExpr expr = PartitionUtil.addOffsetForLiteral(literalExpr, 1, interval);
                PartitionKey partitionKey = new PartitionKey(ImmutableList.of(expr), selectedPartitionKey.getTypes());
                ranges.add(Range.closedOpen(selectedPartitionKey, partitionKey));
            } catch (AnalysisException e) {
                logMVRewrite(mv.getName(), "Compute partition key range failed:{}", DebugUtil.getStackTrace(e));
                return null;
            }
        }

        List<Range<PartitionKey>> mergedRanges = mergeRanges(ranges);
        ColumnRefOperator partitionColumnRef = scanOperator.getColumnReference(baseTable.getPartitionColumns().get(0));
        ScalarOperator partitionPredicate = convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
        if (partitionPredicate == null) {
            return null;
        }
        return ImmutableList.of(partitionPredicate);
    }

    /**
     * Compensate olap table's partition predicates from olap scan operator which may be pruned by optimizer before or not.
     *
     * @param olapScanOperator   : olap scan operator that needs to compensate partition predicates.
     * @param columnRefFactory   : column ref factory that used to generate new partition predicate epxr.
     * @return
     */
    private static List<ScalarOperator> compensatePartitionPredicateForOlapScan(LogicalOlapScanOperator olapScanOperator,
                                                                                ColumnRefFactory columnRefFactory) {
        Preconditions.checkState(olapScanOperator.getTable().isNativeTableOrMaterializedView());
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        // compensate nothing for single partition table
        if (olapTable.getPartitionInfo() instanceof SinglePartitionInfo) {
            return Collections.emptyList();
        }

        // compensate nothing if selected partitions are the same with the total partitions.
        if (olapScanOperator.getSelectedPartitionId() != null
                && olapScanOperator.getSelectedPartitionId().size() == olapTable.getPartitions().size()) {
            return Collections.emptyList();
        }

        // if no partitions are selected, return pruned partition predicates directly.
        if (olapScanOperator.getSelectedPartitionId().isEmpty()) {
            return olapScanOperator.getPrunedPartitionPredicates();
        }

        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        if (olapTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
            Expr partitionExpr = partitionInfo.getPartitionExprs(olapTable.getIdToColumn()).get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionExpr.collect(SlotRef.class, slotRefs);
            Preconditions.checkState(slotRefs.size() == 1);
            Optional<ColumnRefOperator> partitionColumn =
                    olapScanOperator.getColRefToColumnMetaMap().keySet().stream()
                            .filter(columnRefOperator -> columnRefOperator.getName()
                                    .equals(slotRefs.get(0).getColumnName()))
                            .findFirst();
            // compensate nothing if there is no partition column predicate in the scan node.
            if (!partitionColumn.isPresent()) {
                return null;
            }

            ExpressionMapping mapping =
                    new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
            mapping.put(slotRefs.get(0), partitionColumn.get());
            ScalarOperator partitionScalarOperator =
                    SqlToScalarOperatorTranslator.translate(partitionExpr, mapping, columnRefFactory);
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            // compensate selected partition ranges from selected partition id
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(partitionInfo.getRange(pid));
            }

            // normalize selected partition ranges
            List<Range<PartitionKey>> mergedRanges = mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionScalarOperator, mergedRanges);
            if (partitionPredicate == null) {
                return null;
            }
            partitionPredicates.add(partitionPredicate);
        } else if (olapTable.getPartitionInfo() instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn());
            if (partitionColumns.size() != 1) {
                // now do not support more than one partition columns
                return null;
            }
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(rangePartitionInfo.getRange(pid));
            }
            ColumnRefOperator partitionColumnRef = olapScanOperator.getColumnReference(partitionColumns.get(0));
            List<Range<PartitionKey>> mergedRanges = mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
            if (partitionPredicate == null) {
                return null;
            }
            partitionPredicates.add(partitionPredicate);
        } else {
            return null;
        }

        return partitionPredicates;
    }

    public static ScalarOperator convertPartitionRangesToListPredicate(ScalarOperator partitionColRef,
                                                                       List<Range<PartitionKey>> partitionRanges) {
        List<PartitionKey> keys = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }

            // see `convertToDateRange`
            if (range.hasLowerBound()) {
                // partition range must have lower bound and upper bound
                keys.add(range.lowerEndpoint());
            } else if (range.hasUpperBound()) {
                keys.add(range.upperEndpoint());
            } else {
                return null;
            }
        }
        return MvUtils.convertPartitionKeysToListPredicate(Lists.newArrayList(partitionColRef), keys);
    }

    private static ScalarOperator convertPartitionKeysToPredicate(ScalarOperator partitionColumn,
                                                                  List<Range<PartitionKey>> partitionKeys) {
        // NOTE: For string type partition column, it should be list partition rather than range partition.
        boolean isListPartition = partitionColumn.getType().isStringType();
        if (isListPartition) {
            return MvPartitionCompensator.convertPartitionRangesToListPredicate(partitionColumn, partitionKeys);
        } else {
            List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionColumn, partitionKeys);
            return Utils.compoundOr(rangePredicates);
        }
    }

    /**
     * Materialized View's partition column can only refer one base table's partition column, get the referred
     * base table and its partition column.
     * @return : The materialized view's referred base table and its partition column.
     * TODO: Support multi-column partitions later.
     * TODO: Use getRefBaseTablePartitionColumns instead since SR v3.3 has supported multi-tables partition change tracking.
     */
    @Deprecated
    private static Pair<Table, Column> getRefBaseTablePartitionColumn(MaterializedView mv) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (mv.getPartitionRefTableExprs() == null || !(partitionInfo.isRangePartition() || partitionInfo.isListPartition())) {
            return null;
        }
        Expr partitionExpr = mv.getPartitionRefTableExprs().get(0);
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef partitionSlotRef = slotRefs.get(0);
        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Table table = MvUtils.getTableChecked(baseTableInfo);
            if (partitionSlotRef.getTblNameWithoutAnalyzed().getTbl().equals(table.getName())) {
                return Pair.create(table, table.getColumn(partitionSlotRef.getColumnName()));
            }
        }
        String baseTableNames = mv.getBaseTableInfos().stream()
                .map(tableInfo -> MvUtils.getTableChecked(tableInfo).getName()).collect(Collectors.joining(","));
        throw new RuntimeException(
                String.format("can not find partition info for mv:%s on base tables:%s",
                        mv.getName(), baseTableNames));
    }

    // try to get partial partition predicates of partitioned mv.
    // eg, mv1's base partition table is t1, partition column is k1 and has two partition:
    // p1:[2022-01-01, 2022-01-02), p1 is updated(refreshed),
    // p2:[2022-01-02, 2022-01-03), p2 is outdated,
    // then this function will return predicate:
    // k1 >= "2022-01-01" and k1 < "2022-01-02"
    // NOTE: This method can be only used in query rewrite and cannot be used in insert routine.
    public static ScalarOperator getMvPartialPartitionPredicates(
            MaterializedView mv,
            OptExpression mvPlan,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        Pair<Table, Column> partitionTableAndColumns = getRefBaseTablePartitionColumn(mv);
        if (partitionTableAndColumns == null) {
            return null;
        }

        Table refBaseTable = partitionTableAndColumns.first;
        List<Range<PartitionKey>> refreshedRefBaseTableRanges = getMvRefreshedPartitionRange(refBaseTable,
                partitionTableAndColumns.second, mv, mvPartitionNamesToRefresh);
        if (refreshedRefBaseTableRanges == null) {
            return null;
        }

        if (refreshedRefBaseTableRanges.isEmpty()) {
            // if there isn't an updated partition, do not rewrite
            return ConstantOperator.TRUE;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return null;
        }
        Expr partitionExpr = mvPartitionExprOpt.get();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(mvPlan);
        for (LogicalScanOperator scanOperator : scanOperators) {
            // Since mv's plan disabled partition prune, no need to compensate partition predicate for non ref base tables.
            if (!isRefBaseTable(scanOperator, refBaseTable)) {
                continue;
            }
            final Optional<ColumnRefOperator> columnRefOption;
            if (scanOperator instanceof LogicalViewScanOperator) {
                LogicalViewScanOperator viewScanOperator = scanOperator.cast();
                columnRefOption = Optional.ofNullable(viewScanOperator.getExpressionMapping(partitionExpr));
            } else {
                columnRefOption = Optional.ofNullable(scanOperator.getColumnReference(partitionColumn));
            }
            if (!columnRefOption.isPresent()) {
                continue;
            }
            return convertPartitionKeysToPredicate(columnRefOption.get(), refreshedRefBaseTableRanges);
        }
        return null;
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

    /**
     * Return the refreshed partition key ranges of the ref base table.
     *
     * NOTE: This method can be only used in query rewrite and cannot be used to insert routine.
     * @param partitionByTable          : the base table of the mv
     * @param partitionColumn           : the partition column of the base table
     * @param mv                        : the materialized view
     * @param mvPartitionNamesToRefresh : the updated partition names  of the materialized view
     * @return
     */
    private static List<Range<PartitionKey>> getMvRefreshedPartitionRange(
            Table partitionByTable,
            Column partitionColumn,
            MaterializedView mv,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        // materialized view latest partition ranges except to-refresh partitions
        List<Range<PartitionKey>> mvRanges = getLatestPartitionRangeForNativeTable(mv, mvPartitionNamesToRefresh);
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return Lists.newArrayList();
        }
        Expr mvPartitionExpr = mvPartitionExprOpt.get();
        List<Range<PartitionKey>> refBaseTableRanges;
        try {
            Map<String, Range<PartitionKey>> refBaseTableRangeMap =
                    PartitionUtil.getPartitionKeyRange(partitionByTable, partitionColumn, mvPartitionExpr);
            refBaseTableRanges = refBaseTableRangeMap.values().stream().collect(Collectors.toList());
        } catch (StarRocksException e) {
            LOG.warn("Materialized view Optimizer compute partition range failed.", e);
            return Lists.newArrayList();
        }

        // date to varchar range
        Map<Range<PartitionKey>, Range<PartitionKey>> baseRangeMapping = null;
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mvPartitionExpr, partitionColumn);
        if (isConvertToDate) {
            baseRangeMapping = Maps.newHashMap();
            // convert varchar range to date range
            List<Range<PartitionKey>> baseTableDateRanges = Lists.newArrayList();
            for (Range<PartitionKey> range : refBaseTableRanges) {
                Range<PartitionKey> datePartitionRange = convertToDateRange(range);
                baseTableDateRanges.add(datePartitionRange);
                baseRangeMapping.put(datePartitionRange, range);
            }
            refBaseTableRanges = baseTableDateRanges;
        }

        List<Range<PartitionKey>> latestBaseTableRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : refBaseTableRanges) {
            // if materialized view's partition range can enclose the ref base table range, we think that
            // the materialized view's partition has been refreshed and should be compensated into the materialized
            // view's partition predicate.
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                latestBaseTableRanges.add(range);
            }
        }
        if (isConvertToDate) {
            // treat string type partition as list, so no need merge
            List<Range<PartitionKey>> tmpRangeList = Lists.newArrayList();
            for (Range<PartitionKey> range : latestBaseTableRanges) {
                tmpRangeList.add(baseRangeMapping.get(range));
            }
            return tmpRangeList;
        } else {
            return MvUtils.mergeRanges(latestBaseTableRanges);
        }
    }

    private static List<Range<PartitionKey>> getLatestPartitionRangeForNativeTable(OlapTable partitionTable,
                                                                                   Set<String> modifiedPartitionNames) {
        // partitions that will be excluded
        Set<Long> filteredIds = Sets.newHashSet();
        for (Partition p : partitionTable.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName()) || !p.hasData()) {
                filteredIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionTable.getPartitionInfo();
        return rangePartitionInfo.getRangeList(filteredIds, false);
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
     * Find scan operator which contains the ref-base-table of the materialized view.
     * @param scanOperators: scan operators extracted from query plan.
     * @param refBaseTable: ref-base-table of the materialized view.
     * @return: the first scan operator which contains the ref-base-table of the materialized view if existed,
     * otherwise null is returned.
     */
    private static LogicalScanOperator getRefBaseTableScanOperator(List<LogicalScanOperator> scanOperators,
                                                                   Table refBaseTable) {
        Optional<LogicalScanOperator> optRefScanOperator =
                scanOperators.stream().filter(x -> isRefBaseTable(x, refBaseTable)).findFirst();
        if (!optRefScanOperator.isPresent()) {
            return null;
        }
        return optRefScanOperator.get();
    }

    private static LogicalScanOperator getRefBaseTableScanOperator(OptExpression queryPlan,
                                                                   Table refBaseTable) {

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return null;
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return null;
        }

        LogicalScanOperator refScanOperator = getRefBaseTableScanOperator(scanOperators, refBaseTable);
        if (refScanOperator == null) {
            return null;
        }

        Table table = refScanOperator.getTable();
        // If table's not partitioned, no need compensate
        if (table.isUnPartitioned()) {
            return null;
        }
        return refScanOperator;
    }

    private static boolean isRefBaseTable(LogicalScanOperator scanOperator, Table refBaseTable) {
        Table scanTable = scanOperator.getTable();
        if (scanTable.isNativeTableOrMaterializedView() && !scanTable.equals(refBaseTable)) {
            return false;
        }
        if (scanOperator instanceof LogicalViewScanOperator) {
            return true;
        }
        if (!scanTable.isNativeTableOrMaterializedView() && !scanTable.getTableIdentifier().equals(
                refBaseTable.getTableIdentifier())) {
            return false;
        }
        return true;
    }
}
