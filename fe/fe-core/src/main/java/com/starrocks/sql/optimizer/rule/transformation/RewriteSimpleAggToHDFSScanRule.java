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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RewriteSimpleAggToHDFSScanRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(RewriteSimpleAggToHDFSScanRule.class);

    private static final Set<OperatorType> SUPPORTED = Set.of(OperatorType.LOGICAL_HIVE_SCAN,
            OperatorType.LOGICAL_ICEBERG_SCAN,
            OperatorType.LOGICAL_FILE_SCAN
    );

    public static final RewriteSimpleAggToHDFSScanRule SCAN_NO_PROJECT =
            new RewriteSimpleAggToHDFSScanRule(false);

    public static final RewriteSimpleAggToHDFSScanRule SCAN_AND_PROJECT =
            new RewriteSimpleAggToHDFSScanRule();

    private final boolean hasProjectOperator;

    private RewriteSimpleAggToHDFSScanRule(boolean /* unused */ noProject) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(MultiOpPattern.of(SUPPORTED)));
        hasProjectOperator = false;
    }

    private RewriteSimpleAggToHDFSScanRule() {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(MultiOpPattern.of(SUPPORTED))));
        hasProjectOperator = true;
    }

    private OptExpression buildAggScanOperator(LogicalAggregationOperator aggregationOperator,
                                               LogicalScanOperator scanOperator,
                                               OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        // only need to handle count(*)
        Map<ColumnRefOperator, CallOperator> aggs = aggregationOperator.getAggregations();
        Preconditions.checkArgument(aggs.entrySet().size() == 1);
        ColumnRefOperator aggColumnRef = aggs.entrySet().iterator().next().getKey();
        CallOperator aggCall = aggs.entrySet().iterator().next().getValue();
        Preconditions.checkArgument(aggCall.getFnName().equals(FunctionSet.COUNT) && !aggCall.isDistinct());

        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        // select out partition columns.
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
            if (scanOperator.getPartitionColumns().contains(c.getName())) {
                newScanColumnRefs.put(c, scanOperator.getColRefToColumnMetaMap().get(c));
            }
        }

        if (tableRelationId == -1) {
            LOG.warn("Can not find table relation id in scan operator");
            return null;
        }

        ColumnRefOperator sumOutputColumnRef =
                columnRefFactory.create("sum_" + aggCall.getFnName(), aggCall.getType(), aggCall.isNullable());
        {
            // generate a placeholder column for scan node.
            // ___count___ must be the column name for backend code.
            String metaColumnName = "___" + aggCall.getFnName() + "___";
            Column c = new Column(metaColumnName, Type.NULL);
            c.setIsAllowNull(true);
            ColumnRefOperator placeholderColumn =
                    columnRefFactory.create(metaColumnName, aggCall.getType(), aggCall.isNullable());
            columnRefFactory.updateColumnToRelationIds(placeholderColumn.getId(), tableRelationId);
            columnRefFactory.updateColumnRefToColumns(placeholderColumn, c, scanOperator.getTable());
            newScanColumnRefs.put(placeholderColumn, c);

            CallOperator sumCall = new CallOperator(FunctionSet.SUM, Type.BIGINT,
                    Collections.singletonList(placeholderColumn),
                    Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_IDENTICAL));
            newAggCalls.put(sumOutputColumnRef, sumCall);
        }

        Map<Column, ColumnRefOperator> newScanColumnMeta = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Column> c : newScanColumnRefs.entrySet()) {
            newScanColumnMeta.put(c.getValue(), c.getKey());
        }

        LogicalScanOperator newMetaScan = null;

        if (scanOperator instanceof LogicalHiveScanOperator) {
            newMetaScan = new LogicalHiveScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else if (scanOperator instanceof LogicalIcebergScanOperator) {
            newMetaScan = new LogicalIcebergScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate(),
                    scanOperator.getTvrVersionRange());
        } else if (scanOperator instanceof LogicalFileScanOperator) {
            newMetaScan = new LogicalFileScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else {
            LOG.warn("Unexpected scan operator: " + scanOperator);
            return null;
        }
        newMetaScan.setScanOptimizeOption(scanOperator.getScanOptimizeOption());
        newMetaScan.getScanOptimizeOption().setCanUseCountOpt(true);
        try {
            newMetaScan.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());
        } catch (AnalysisException e) {
            LOG.warn("Exception caught when set scan operator predicates", e);
            return null;
        }

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggCalls);
        newAggOperator.setProjection(aggregationOperator.getProjection());

        // ifnull(sum(__count__)), 0) to avoid null result
        CallOperator ifNullCall = new CallOperator(FunctionSet.IFNULL, Type.BIGINT,
                Lists.newArrayList(sumOutputColumnRef, ConstantOperator.createBigint(0)),
                Expr.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {Type.BIGINT, Type.BIGINT},
                        Function.CompareMode.IS_IDENTICAL));
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        newProjectMap.putAll(newAggOperator.getColumnRefMap());
        newProjectMap.remove(sumOutputColumnRef);
        newProjectMap.put(aggColumnRef, ifNullCall);
        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(newProjectMap);

        // project(ifnull) -> agg(sum(__count__)) -> scan
        OptExpression optExpression = OptExpression.create(newProjectOperator);
        OptExpression aggExpression = OptExpression.create(newAggOperator);
        optExpression.getInputs().add(aggExpression);
        aggExpression.getInputs().add(OptExpression.create(newMetaScan));
        return optExpression;
    }

    private LogicalScanOperator getScanOperator(final OptExpression input) {
        LogicalScanOperator scanOperator = null;
        if (hasProjectOperator) {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        } else {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getOp();
        }
        return scanOperator;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);

        // no limit
        if (scanOperator.getLimit() != -1) {
            return false;
        }

        // no materialized column in predicate of scan
        if (hasMaterializedColumnInPredicate(scanOperator, scanOperator.getPredicate())) {
            return false;
        }

        // all group by keys are partition keys.
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        if (!scanOperator.getPartitionColumns()
                .containsAll(groupingKeys.stream().map(x -> x.getName()).collect(Collectors.toList()))) {
            return false;
        }

        // no materialized column in predicate of aggregation
        if (hasMaterializedColumnInPredicate(scanOperator, aggregationOperator.getPredicate())) {
            return false;
        }

        // not applicable if there is no aggregation functions, like `distinct x`.
        if (aggregationOperator.getAggregations().isEmpty()) {
            return false;
        }

        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();
                    ColumnRefSet usedColumns = aggregator.getUsedColumns();

                    if (functionName.equals(FunctionSet.COUNT) && !aggregator.isDistinct() && usedColumns.isEmpty()) {
                        List<ScalarOperator> arguments = aggregator.getArguments();
                        if (arguments.isEmpty()) {
                            // count()/count(*)
                            return true;
                        } else if (arguments.size() == 1 && !arguments.get(0).isConstantNull()) {
                            // count(non-null constant)
                            return true;
                        }
                        return false;
                    }
                    return false;
                }
        );
        return allValid;
    }

    private static boolean hasMaterializedColumnInPredicate(LogicalScanOperator scanOperator, ScalarOperator predicate) {
        if (predicate == null) {
            return false;
        }
        List<ColumnRefOperator> columnRefOperators = predicate.getColumnRefs();
        Set<String> partitionColumns = scanOperator.getPartitionColumns();
        for (ColumnRefOperator c : columnRefOperators) {
            if (!partitionColumns.contains(c.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);
        OptExpression result = buildAggScanOperator(aggregationOperator, scanOperator, context);
        if (result == null) {
            // Fail to rewrite
            return Lists.newArrayList(input);
        }
        return Lists.newArrayList(result);
    }
}
