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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
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
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
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

        CountRewrite countRewrite = new CountRewrite(scanOperator, columnRefFactory, tableRelationId, newScanColumnRefs);
        Map<ColumnRefOperator, ScalarOperator> counts = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> agg : aggregationOperator.getAggregations().entrySet()) {
            counts.put(agg.getKey(), countRewrite.rewrite(agg.getValue()));
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
        newMetaScan.setScanOptimizeOption(scanOperator.getScanOptimizeOption().copy());
        newMetaScan.getScanOptimizeOption().setCanUseCountOpt(true);
        newMetaScan.getScanOptimizeOption().setNonNullCountColumns(countRewrite.nonNullCountColumns);
        try {
            newMetaScan.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());
        } catch (AnalysisException e) {
            LOG.warn("Exception caught when set scan operator predicates", e);
            return null;
        }

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), countRewrite.aggCalls);
        newAggOperator.setProjection(aggregationOperator.getProjection());

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        newProjectMap.putAll(newAggOperator.getColumnRefMap());
        countRewrite.aggCalls.keySet().forEach(newProjectMap::remove);
        newProjectMap.putAll(counts);
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

        // add check for column mapping
        if (!scanOperator.getColRefToColumnMetaMap().keySet()
                .containsAll(groupingKeys)) {
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

        return aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> isCountOfRows(aggregator) || isCountOfDataColumn(aggregator, scanOperator));
    }

    /**
     * COUNT(), COUNT(*) or COUNT of a non-null constant, answered from each data file's record count.
     */
    private static boolean isCountOfRows(CallOperator aggregator) {
        if (!isPlainCount(aggregator) || !aggregator.getUsedColumns().isEmpty()) {
            return false;
        }
        List<ScalarOperator> arguments = aggregator.getArguments();
        return arguments.isEmpty() || (arguments.size() == 1 && !arguments.get(0).isConstantNull());
    }

    /**
     * COUNT(col) is answered from Iceberg manifest null counts, so it qualifies only on an Iceberg scan, for a
     * scalar column that is read from the data files rather than being a partition column.
     */
    private static boolean isCountOfDataColumn(CallOperator aggregator, LogicalScanOperator scanOperator) {
        if (!(scanOperator instanceof LogicalIcebergScanOperator) || !isPlainCount(aggregator)
                || aggregator.getArguments().size() != 1 || !aggregator.getArguments().get(0).isColumnRef()) {
            return false;
        }
        Column column = scanOperator.getColRefToColumnMetaMap().get((ColumnRefOperator) aggregator.getArguments().get(0));
        return column != null && column.getType().isScalarType()
                && !scanOperator.getPartitionColumns().contains(column.getName());
    }

    private static boolean isPlainCount(CallOperator aggregator) {
        AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
        return aggregateFunction.functionName().equals(FunctionSet.COUNT) && !aggregator.isDistinct();
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

    /**
     * Rewrites each count into a sum of per-file counts that the backend writes into a placeholder scan column,
     * collecting the aggregate calls and scan columns that needs. Counts sharing a placeholder share one sum.
     */
    private static final class CountRewrite {
        /**
         * The COUNT(*) placeholder, which the backend recognizes by this name. A COUNT(col) placeholder appends the
         * counted column's name for readability and is recognized by slot id instead.
         */
        private static final String COUNT_PLACEHOLDER = "___count___";

        private final LogicalScanOperator scanOperator;
        private final ColumnRefFactory columnRefFactory;
        private final int tableRelationId;
        private final Map<ColumnRefOperator, Column> scanColumns;
        private final Map<ColumnRefOperator, CallOperator> aggCalls = Maps.newHashMap();
        private final Map<Integer, String> nonNullCountColumns = Maps.newHashMap();
        private final Map<String, ScalarOperator> countByPlaceholder = Maps.newHashMap();

        private CountRewrite(LogicalScanOperator scanOperator, ColumnRefFactory columnRefFactory, int tableRelationId,
                             Map<ColumnRefOperator, Column> scanColumns) {
            this.scanOperator = scanOperator;
            this.columnRefFactory = columnRefFactory;
            this.tableRelationId = tableRelationId;
            this.scanColumns = scanColumns;
        }

        private ScalarOperator rewrite(CallOperator count) {
            if (count.getUsedColumns().isEmpty()) {
                return countByPlaceholder.computeIfAbsent(COUNT_PLACEHOLDER,
                        name -> sumOfFileCounts(addPlaceholder(name, count), count));
            }
            ColumnRefOperator counted = (ColumnRefOperator) count.getArguments().get(0);
            Column countedColumn = scanOperator.getColRefToColumnMetaMap().get(counted);
            return countByPlaceholder.computeIfAbsent(COUNT_PLACEHOLDER + countedColumn.getName(),
                    name -> countOfColumn(name, count, counted, countedColumn));
        }

        /**
         * ifnull(sum(placeholder), 0) + count(col). A file answered from its statistics adds its non-null count
         * through the placeholder and leaves the counted column NULL, while any other file is read with the
         * placeholder at zero and counted by count(col).
         */
        private ScalarOperator countOfColumn(String placeholderName, CallOperator count, ColumnRefOperator counted,
                                             Column countedColumn) {
            ColumnRefOperator placeholder = addPlaceholder(placeholderName, count);
            nonNullCountColumns.put(placeholder.getId(), countedColumn.getName());
            scanColumns.put(counted, countedColumn);
            ColumnRefOperator countOfRowsRead = columnRefFactory.create(count, count.getType(), count.isNullable());
            aggCalls.put(countOfRowsRead, count);
            return new CallOperator(FunctionSet.ADD, IntegerType.BIGINT,
                    Lists.newArrayList(sumOfFileCounts(placeholder, count), countOfRowsRead),
                    ExprUtils.getBuiltinFunction(FunctionSet.ADD, new Type[] {IntegerType.BIGINT, IntegerType.BIGINT},
                            Function.CompareMode.IS_IDENTICAL));
        }

        private ColumnRefOperator addPlaceholder(String name, CallOperator count) {
            Column column = new Column(name, IntegerType.BIGINT);
            column.setIsAllowNull(true);
            ColumnRefOperator placeholder = columnRefFactory.create(name, count.getType(), count.isNullable());
            columnRefFactory.updateColumnToRelationIds(placeholder.getId(), tableRelationId);
            columnRefFactory.updateColumnRefToColumns(placeholder, column, scanOperator.getTable());
            scanColumns.put(placeholder, column);
            return placeholder;
        }

        /**
         * ifnull(sum(placeholder), 0), so that a scan over no files still counts zero.
         */
        private ScalarOperator sumOfFileCounts(ColumnRefOperator placeholder, CallOperator count) {
            ColumnRefOperator sum = columnRefFactory.create("sum_" + count.getFnName(), count.getType(),
                    count.isNullable());
            aggCalls.put(sum, new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                    Collections.singletonList(placeholder),
                    ExprUtils.getBuiltinFunction(FunctionSet.SUM, new Type[] {IntegerType.BIGINT},
                            Function.CompareMode.IS_IDENTICAL)));
            return new CallOperator(FunctionSet.IFNULL, IntegerType.BIGINT,
                    Lists.newArrayList(sum, ConstantOperator.createBigint(0)),
                    ExprUtils.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {IntegerType.BIGINT, IntegerType.BIGINT},
                            Function.CompareMode.IS_IDENTICAL));
        }
    }
}
