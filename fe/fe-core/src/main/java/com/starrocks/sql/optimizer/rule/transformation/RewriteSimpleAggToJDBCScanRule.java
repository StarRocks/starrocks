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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.ScanOptimizeOption;
import com.starrocks.sql.optimizer.ScanOptimizeOption.AggPushdownDesc;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Rewrites simple aggregate-only queries (no GROUP BY) on JDBC scan operators to push
 * the aggregation down to the remote database. Supported functions: COUNT(*), COUNT(col),
 * MIN(col), MAX(col).
 *
 * Instead of fetching all rows and aggregating locally, the remote DB executes e.g.
 * {@code SELECT COUNT(*), MIN(col_a), MAX(col_b)} and returns a single row.
 *
 * Follows the same pattern as {@link RewriteSimpleAggToHDFSScanRule}.
 */
public class RewriteSimpleAggToJDBCScanRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(RewriteSimpleAggToJDBCScanRule.class);

    public static final RewriteSimpleAggToJDBCScanRule SCAN_NO_PROJECT =
            new RewriteSimpleAggToJDBCScanRule(false);

    public static final RewriteSimpleAggToJDBCScanRule SCAN_AND_PROJECT =
            new RewriteSimpleAggToJDBCScanRule();

    private final boolean hasProjectOperator;

    private RewriteSimpleAggToJDBCScanRule(boolean /* unused */ noProject) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG_TO_JDBC_SCAN, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JDBC_SCAN)));
        hasProjectOperator = false;
    }

    private RewriteSimpleAggToJDBCScanRule() {
        super(RuleType.TF_REWRITE_SIMPLE_AGG_TO_JDBC_SCAN, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_JDBC_SCAN))));
        hasProjectOperator = true;
    }

    private LogicalScanOperator getScanOperator(final OptExpression input) {
        if (hasProjectOperator) {
            return (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        } else {
            return (LogicalScanOperator) input.getInputs().get(0).getOp();
        }
    }

    /**
     * Resolves a column ref to its corresponding Column in the scan operator.
     * For SCAN_AND_PROJECT, traces through the project's columnRefMap.
     * Returns null if the column ref cannot be resolved to a direct scan column.
     */
    private Column resolveToScanColumn(ColumnRefOperator ref, OptExpression input) {
        LogicalScanOperator scanOperator = getScanOperator(input);

        if (hasProjectOperator) {
            LogicalProjectOperator projectOp =
                    (LogicalProjectOperator) input.getInputs().get(0).getOp();
            ScalarOperator mapped = projectOp.getColumnRefMap().get(ref);
            if (mapped == null || !mapped.isColumnRef()) {
                return null;
            }
            ref = (ColumnRefOperator) mapped;
        }

        return scanOperator.getColRefToColumnMetaMap().get(ref);
    }

    /**
     * Types safe for MIN/MAX pushdown — consistent semantics across all JDBC databases.
     */
    private static boolean isSafeForMinMaxPushdown(Type type) {
        return type.isNumericType() || type.isDateType();
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableJdbcAggPushdown()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);

        // no limit on scan
        if (scanOperator.getLimit() != -1) {
            return false;
        }

        // no GROUP BY — pushing GROUP BY would return multiple rows from remote
        if (!aggregationOperator.getGroupingKeys().isEmpty()) {
            return false;
        }

        // not applicable if there is no aggregation functions, like `distinct x`.
        if (aggregationOperator.getAggregations().isEmpty()) {
            return false;
        }

        // all agg functions must be pushable: COUNT(*), COUNT(const), COUNT(col), MIN(col), MAX(col)
        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();

                    // no DISTINCT pushdown
                    if (aggregator.isDistinct()) {
                        return false;
                    }

                    if (functionName.equals(FunctionSet.COUNT)) {
                        ColumnRefSet usedColumns = aggregator.getUsedColumns();
                        if (usedColumns.isEmpty()) {
                            // COUNT(*) or COUNT(non-null-constant)
                            List<ScalarOperator> arguments = aggregator.getArguments();
                            if (arguments.isEmpty()) {
                                return true;
                            }
                            return arguments.size() == 1 && !arguments.get(0).isConstantNull();
                        } else {
                            // COUNT(col) — single column ref that resolves to a scan column
                            List<ScalarOperator> arguments = aggregator.getArguments();
                            return arguments.size() == 1 && arguments.get(0).isColumnRef()
                                    && resolveToScanColumn(
                                            (ColumnRefOperator) arguments.get(0), input) != null;
                        }
                    }

                    if (functionName.equals(FunctionSet.MIN) || functionName.equals(FunctionSet.MAX)) {
                        List<ScalarOperator> arguments = aggregator.getArguments();
                        if (arguments.size() != 1 || !arguments.get(0).isColumnRef()) {
                            return false;
                        }
                        ColumnRefOperator colRef = (ColumnRefOperator) arguments.get(0);
                        if (resolveToScanColumn(colRef, input) == null) {
                            return false;
                        }
                        return isSafeForMinMaxPushdown(colRef.getType());
                    }

                    return false;
                }
        );
        return allValid;
    }

    private OptExpression buildAggScanOperator(LogicalAggregationOperator aggregationOperator,
                                               LogicalScanOperator scanOperator,
                                               OptExpression input,
                                               OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        Map<ColumnRefOperator, CallOperator> aggs = aggregationOperator.getAggregations();

        // Find the table relation id from existing scan columns
        int tableRelationId = -1;
        for (ColumnRefOperator c : scanOperator.getColRefToColumnMetaMap().keySet()) {
            int relationId = columnRefFactory.getRelationId(c.getId());
            if (tableRelationId == -1) {
                tableRelationId = relationId;
            } else if (tableRelationId != relationId) {
                LOG.warn("Table relationIds are different in columns, tableRelationId = {}, relationId = {}",
                        tableRelationId, relationId);
                return null;
            }
        }

        if (tableRelationId == -1) {
            LOG.warn("Can not find table relation id in scan operator");
            return null;
        }

        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newLinkedHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newLinkedHashMap();
        Map<ColumnRefOperator, ScalarOperator> aggResultProjections = Maps.newLinkedHashMap();
        List<AggPushdownDesc> aggDescriptors = new ArrayList<>();
        int aggIndex = 0;

        // aggs is ImmutableMap (from LogicalAggregationOperator) — insertion-order iteration guaranteed.
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggs.entrySet()) {
            ColumnRefOperator aggColumnRef = entry.getKey();
            CallOperator aggCall = entry.getValue();
            String fnName = aggCall.getFnName();

            String metaColumnName = "___agg_" + aggIndex + "___";
            aggIndex++;

            if (fnName.equals(FunctionSet.COUNT)) {
                // COUNT(*), COUNT(constant), or COUNT(col) — result is always BIGINT
                Column c = new Column(metaColumnName, Type.BIGINT);
                c.setIsAllowNull(true);
                ColumnRefOperator placeholder =
                        columnRefFactory.create(metaColumnName, Type.BIGINT, true);
                columnRefFactory.updateColumnToRelationIds(placeholder.getId(), tableRelationId);
                columnRefFactory.updateColumnRefToColumns(placeholder, c, scanOperator.getTable());
                newScanColumnRefs.put(placeholder, c);

                // Upper agg: SUM(placeholder) to merge (single row for JDBC, but keeps plan structure consistent)
                ColumnRefOperator sumOutput =
                        columnRefFactory.create("sum_count_" + aggIndex, Type.BIGINT, true);
                CallOperator sumCall = new CallOperator(FunctionSet.SUM, Type.BIGINT,
                        Collections.singletonList(placeholder),
                        Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT},
                                Function.CompareMode.IS_IDENTICAL));
                newAggCalls.put(sumOutput, sumCall);

                // Project: IFNULL(SUM(...), 0) to avoid null result
                CallOperator ifNullCall = new CallOperator(FunctionSet.IFNULL, Type.BIGINT,
                        Lists.newArrayList(sumOutput, ConstantOperator.createBigint(0)),
                        Expr.getBuiltinFunction(FunctionSet.IFNULL,
                                new Type[] {Type.BIGINT, Type.BIGINT},
                                Function.CompareMode.IS_IDENTICAL));
                aggResultProjections.put(aggColumnRef, ifNullCall);

                // Determine remote column name for the descriptor
                String remoteColName = null;
                if (!aggCall.getUsedColumns().isEmpty()) {
                    ColumnRefOperator colRef = (ColumnRefOperator) aggCall.getArguments().get(0);
                    Column scanCol = resolveToScanColumn(colRef, input);
                    if (scanCol != null) {
                        remoteColName = scanCol.getName();
                    }
                }
                aggDescriptors.add(new AggPushdownDesc(FunctionSet.COUNT.toUpperCase(), remoteColName));

            } else if (fnName.equals(FunctionSet.MIN) || fnName.equals(FunctionSet.MAX)) {
                // MIN/MAX — result type matches the column type
                ColumnRefOperator colRef = (ColumnRefOperator) aggCall.getArguments().get(0);
                Column scanCol = resolveToScanColumn(colRef, input);
                if (scanCol == null) {
                    LOG.warn("Cannot resolve column ref {} to scan column", colRef);
                    return null;
                }
                // Use StarRocks aggregate return type — for numeric/date types this aligns with
                // JDBC driver type mapping. (String/boolean types are excluded by isSafeForMinMaxPushdown.)
                Type colType = aggCall.getType();

                Column c = new Column(metaColumnName, colType);
                c.setIsAllowNull(true);
                ColumnRefOperator placeholder =
                        columnRefFactory.create(metaColumnName, colType, true);
                columnRefFactory.updateColumnToRelationIds(placeholder.getId(), tableRelationId);
                columnRefFactory.updateColumnRefToColumns(placeholder, c, scanOperator.getTable());
                newScanColumnRefs.put(placeholder, c);

                // Upper agg: MIN/MAX(placeholder) — idempotent for single-row JDBC result
                ColumnRefOperator aggOutput =
                        columnRefFactory.create(fnName.toLowerCase() + "_" + aggIndex, colType, true);
                CallOperator newAggCall = new CallOperator(fnName, colType,
                        Collections.singletonList(placeholder),
                        Expr.getBuiltinFunction(fnName, new Type[] {colType},
                                Function.CompareMode.IS_IDENTICAL));
                newAggCalls.put(aggOutput, newAggCall);

                // No IFNULL for MIN/MAX — null is a valid result for empty tables
                aggResultProjections.put(aggColumnRef, aggOutput);

                aggDescriptors.add(new AggPushdownDesc(fnName.toUpperCase(), scanCol.getName()));
            }
        }

        // Build reverse column map for the new scan
        Map<Column, ColumnRefOperator> newScanColumnMeta = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Column> c : newScanColumnRefs.entrySet()) {
            newScanColumnMeta.put(c.getValue(), c.getKey());
        }

        LogicalJDBCScanOperator newScan = new LogicalJDBCScanOperator(scanOperator.getTable(),
                newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate(), null);
        ScanOptimizeOption newOption = scanOperator.getScanOptimizeOption() != null
                ? scanOperator.getScanOptimizeOption().copy()
                : new ScanOptimizeOption();
        newOption.setPushedAggDescriptors(aggDescriptors);
        newScan.setScanOptimizeOption(newOption);
        // Note: JDBC scans don't support getScanOperatorPredicates() (no partition pruning),
        // so we skip copying them — unlike the HDFS variant which needs partition predicates.

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggCalls);
        newAggOperator.setProjection(aggregationOperator.getProjection());

        // Build final project: start with any extra projection expressions from original agg operator,
        // then override with our computed aggregate result mappings (IFNULL for COUNT, pass-through for MIN/MAX).
        Map<ColumnRefOperator, ScalarOperator> finalProjectMap = Maps.newLinkedHashMap();
        finalProjectMap.putAll(newAggOperator.getColumnRefMap());
        finalProjectMap.putAll(aggResultProjections);
        // Remove intermediate agg output refs (sumOutput, aggOutput) that are not original agg column refs
        for (ColumnRefOperator newAggKey : newAggCalls.keySet()) {
            if (!aggs.containsKey(newAggKey)) {
                finalProjectMap.remove(newAggKey);
            }
        }
        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(finalProjectMap);

        // project -> agg -> scan
        OptExpression optExpression = OptExpression.create(newProjectOperator);
        OptExpression aggExpression = OptExpression.create(newAggOperator);
        optExpression.getInputs().add(aggExpression);
        aggExpression.getInputs().add(OptExpression.create(newScan));
        return optExpression;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);
        OptExpression result = buildAggScanOperator(aggregationOperator, scanOperator, input, context);
        if (result == null) {
            return Lists.newArrayList(input);
        }
        return Lists.newArrayList(result);
    }
}
