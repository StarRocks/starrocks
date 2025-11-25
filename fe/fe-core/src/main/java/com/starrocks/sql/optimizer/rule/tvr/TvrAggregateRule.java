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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptContext;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import org.apache.hadoop.util.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TvrAggregateRule extends TvrTransformationRule {

    public TvrAggregateRule() {
        super(RuleType.TF_TVR_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!isSupportedTvr(input)) {
            return false;
        }
        LogicalAggregationOperator aggOp = input.getOp().cast();
        List<ColumnRefOperator> groupBys = aggOp.getGroupingKeys();
        if (groupBys.isEmpty()) {
            return false;
        }
        // TODO: aggregate distinct
        if (aggOp.getAggregations().values().stream().anyMatch(call -> call.isDistinct())) {
            return false;
        }
        // TODO: aggregate having
        if (aggOp.getPredicate() != null) {
            return false;
        }
        return true;
    }

    protected OptExpression doTransformWithMonotonic(OptimizerContext optimizerContext,
                                                     MaterializedView aggStateTable,
                                                     LogicalAggregationOperator inputAggOperator,
                                                     OptExpression input) {
        Preconditions.checkArgument(aggStateTable != null,
                "Aggregate state table must not be null for TVR aggregate rule");
        final Column tvrColumnRowId = aggStateTable.getColumn(TvrOpUtils.COLUMN_ROW_ID);
        Preconditions.checkArgument(tvrColumnRowId != null,
                "TVR column row id must exist in agg state table");
        final ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
        final LogicalOlapScanOperator aggStateOlapScanOperator = MvRewritePreprocessor.createScanMvOperator(
                aggStateTable, columnRefFactory, PCellSortedSet.of(), true);
        ColumnRefOperator aggStateRowIdColumnRef =
                aggStateOlapScanOperator.getColumnMetaToColRefMap().get(tvrColumnRowId);
        List<Column> aggStateTableFullColumns = aggStateTable.getFullSchema();
        List<Column> aggStateTableColumns = aggStateTableFullColumns.stream()
                .filter(col -> col.getName().startsWith(TvrOpUtils.COLUMN_AGG_STATE_PREFIX))
                .collect(Collectors.toList());
        // collect agg state table's aggregate agg state columns
        Map<Column, ColumnRefOperator> aggStateTableColumnMetaToColRefMap =
                aggStateOlapScanOperator.getColumnMetaToColRefMap();
        List<ColumnRefOperator> aggStateTableColumnRefs = aggStateTableColumns.stream()
                .map(aggStateTableColumnMetaToColRefMap::get)
                .collect(Collectors.toList());

        // collect input aggregator's grouping keys and aggregations
        final List<ColumnRefOperator> groupingKeys = inputAggOperator.getGroupingKeys();
        final Map<ColumnRefOperator, CallOperator> inputAggMap = inputAggOperator.getAggregations();
        Preconditions.checkArgument(aggStateTableColumns.size() == inputAggMap.size(),
                String.format("Aggregate state table columns size %s must match input aggregate map size %s",
                        aggStateTableColumns.size(), inputAggMap.size()));

        // build eq predicate for delta changes by row id
        List<ScalarOperator> inputAggUniqueKeys = inputAggOperator.getGroupingKeys()
                .stream()
                .map(col -> (ScalarOperator) col)
                .collect(Collectors.toList());
        final int encodeRowIdVersion = aggStateTable.getEncodeRowIdVersion();
        ScalarOperator eqRowIdOperator = TvrOpUtils.buildRowIdEqBinaryPredicateOp(encodeRowIdVersion,
                aggStateRowIdColumnRef, inputAggUniqueKeys);

        // old aggregate function to new column ref operator map
        Map<ScalarOperator, ColumnRefOperator> oldToNewColumnRefMap = Maps.newHashMap();
        // build input aggregator intermediate aggregation operator
        LogicalAggregationOperator intermediateAggOperator = buildIntermediateAggOperator(columnRefFactory,
                groupingKeys, inputAggMap, oldToNewColumnRefMap);

        // delta changes left join agg state scan operator
        LogicalJoinOperator deltaJoinOperator = new LogicalJoinOperator(
                JoinOperator.LEFT_OUTER_JOIN, eqRowIdOperator);
        OptExpression deltaJoinOptExpression = OptExpression.createWithoutTvr(deltaJoinOperator,
                OptExpression.createWithoutTvr(intermediateAggOperator, input.getInputs()),
                OptExpression.createWithoutTvr(aggStateOlapScanOperator));

        // change the aggregation operator into project operator
        Map<ColumnRefOperator, ScalarOperator> projColumnRefMap  = Maps.newHashMap();
        if (inputAggOperator.getProjection() != null) {
            projColumnRefMap.putAll(inputAggOperator.getProjection().getColumnRefMap());
        }

        // TODO: We may align the agg state table's output columns with the insert planner's
        //  output column refs in order to get the correct order.
        // We can assume that the agg state table's columns are in the same order
        // as the input aggregate map, but this is a bit tricky.
        List<ColumnRefOperator> inputAggCallColumnRef = inputAggMap.keySet().stream()
                .sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                .collect(Collectors.toList());
        Preconditions.checkArgument(inputAggCallColumnRef.size() == aggStateTableColumnRefs.size(),
                "Input aggregate call column ref operators size %s must match " +
                        "agg state table column ref size %s", inputAggCallColumnRef.size(), aggStateTableColumnRefs.size());
        for (int i = 0; i < inputAggCallColumnRef.size(); i++) {
            ColumnRefOperator oldColumnRef = inputAggCallColumnRef.get(i);
            CallOperator callOperator = inputAggMap.get(oldColumnRef);
            ColumnRefOperator intermediateAggColumnRef = oldToNewColumnRefMap.get(callOperator);
            Preconditions.checkState(intermediateAggColumnRef != null,
                    "New column ref operator should not be null for: %s", callOperator);
            ColumnRefOperator aggStateAggStateColumnRef = aggStateTableColumnRefs.get(i);
            ScalarOperator stateUnionScalarOperator = TvrOpUtils.buildStateUnionScalarOperator(
                    callOperator, intermediateAggColumnRef, aggStateAggStateColumnRef);
            projColumnRefMap.put(oldColumnRef, stateUnionScalarOperator);
        }
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projColumnRefMap);
        return OptExpression.createWithoutTvr(logicalProjectOperator, deltaJoinOptExpression);
    }

    private LogicalAggregationOperator buildIntermediateAggOperator(
            ColumnRefFactory columnRefFactory,
            List<ColumnRefOperator> groupingKeys,
            Map<ColumnRefOperator, CallOperator> inputAggMap,
            Map<ScalarOperator, ColumnRefOperator> oldToNewColumnRefMap) {
        // output intermediate results rather than final result
        Map<ColumnRefOperator, CallOperator> intermediateAggMap = inputAggMap.entrySet()
                .stream()
                .map(e -> {
                    ColumnRefOperator origColumnRef = e.getKey();
                    CallOperator origCall = e.getValue();
                    CallOperator intermediateFunc = AggregateFunctionRollupUtils.getIntermediateStateAggregateFunc(origCall);
                    Preconditions.checkArgument(intermediateFunc != null,
                            "Intermediate state aggregate function should not be null for: %s", origCall);
                    // create a new column ref for the intermediate state aggregate function
                    ColumnRefOperator newColumnRefOperator =
                            columnRefFactory.create(origColumnRef.getName(),
                                    origColumnRef.getType(), origColumnRef.isNullable());
                    // map old column ref operator to new column ref operator
                    oldToNewColumnRefMap.put(origCall, newColumnRefOperator);
                    return Map.entry(newColumnRefOperator, intermediateFunc);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        LogicalAggregationOperator newAggOp = new LogicalAggregationOperator(AggType.GLOBAL, groupingKeys, intermediateAggMap);
        return newAggOp;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        TvrOptMeta childTvrOptMeta = input.inputAt(0).getTvrMeta();
        Preconditions.checkArgument(childTvrOptMeta != null,
                "TVR meta should not be null for aggregate input");
        LogicalAggregationOperator aggOp = input.getOp().cast();
        TvrOptContext tvrOptContext = context.getTvrOptContext();
        // find the agg state table
        MaterializedView aggStateTable = tvrOptContext.getTvrTargetMV();

        OptExpression deltaAggregate;
        if (childTvrOptMeta.isAppendOnly()) {
            // Append-only (MONOTONIC) input: use state union approach
            deltaAggregate = doTransformWithMonotonic(context, aggStateTable, aggOp, input);
        } else {
            // Retractable input: use StreamAggregator with update/retract dispatch
            // based on StreamRowOp (OP_INSERT -> update, OP_DELETE -> retract)
            deltaAggregate = doTransformWithRetractable(context, aggStateTable, aggOp, input, childTvrOptMeta);
        }

        return Lists.newArrayList(deltaAggregate);
    }

    /**
     * Transform aggregate operator for retractable inputs (inputs with DELETE/UPDATE operations).
     *
     * For retractable inputs, the aggregation needs to:
     * 1. Read the StreamRowOp column from the input to determine if each row is INSERT or DELETE
     * 2. For INSERT rows: call aggregate function's update() method
     * 3. For DELETE rows: call aggregate function's retract() method
     *
     * The StreamAggregator in BE layer handles this via process_chunk() which dispatches
     * based on StreamRowOp:
     * - OP_INSERT, OP_UPDATE_BEFORE -> update()
     * - OP_DELETE, OP_UPDATE_AFTER -> retract()
     *
     * This method builds the plan that:
     * 1. Performs incremental aggregation on the delta (with proper op handling)
     * 2. Joins with existing aggregate state
     * 3. Computes the updated aggregate state
     */
    protected OptExpression doTransformWithRetractable(OptimizerContext optimizerContext,
                                                        MaterializedView aggStateTable,
                                                        LogicalAggregationOperator inputAggOperator,
                                                        OptExpression input,
                                                        TvrOptMeta childTvrOptMeta) {
        Preconditions.checkArgument(aggStateTable != null,
                "Aggregate state table must not be null for TVR aggregate rule");
        final Column tvrColumnRowId = aggStateTable.getColumn(TvrOpUtils.COLUMN_ROW_ID);
        Preconditions.checkArgument(tvrColumnRowId != null,
                "TVR column row id must exist in agg state table");
        final ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
        final LogicalOlapScanOperator aggStateOlapScanOperator = MvRewritePreprocessor.createScanMvOperator(
                aggStateTable, columnRefFactory, PCellSortedSet.of(), true);
        ColumnRefOperator aggStateRowIdColumnRef =
                aggStateOlapScanOperator.getColumnMetaToColRefMap().get(tvrColumnRowId);
        List<Column> aggStateTableFullColumns = aggStateTable.getFullSchema();
        List<Column> aggStateTableColumns = aggStateTableFullColumns.stream()
                .filter(col -> col.getName().startsWith(TvrOpUtils.COLUMN_AGG_STATE_PREFIX))
                .collect(Collectors.toList());
        // collect agg state table's aggregate agg state columns
        Map<Column, ColumnRefOperator> aggStateTableColumnMetaToColRefMap =
                aggStateOlapScanOperator.getColumnMetaToColRefMap();
        List<ColumnRefOperator> aggStateTableColumnRefs = aggStateTableColumns.stream()
                .map(aggStateTableColumnMetaToColRefMap::get)
                .collect(Collectors.toList());

        // collect input aggregator's grouping keys and aggregations
        final List<ColumnRefOperator> groupingKeys = inputAggOperator.getGroupingKeys();
        final Map<ColumnRefOperator, CallOperator> inputAggMap = inputAggOperator.getAggregations();
        Preconditions.checkArgument(aggStateTableColumns.size() == inputAggMap.size(),
                String.format("Aggregate state table columns size %s must match input aggregate map size %s",
                        aggStateTableColumns.size(), inputAggMap.size()));

        // build eq predicate for delta changes by row id
        List<ScalarOperator> inputAggUniqueKeys = inputAggOperator.getGroupingKeys()
                .stream()
                .map(col -> (ScalarOperator) col)
                .collect(Collectors.toList());
        final int encodeRowIdVersion = aggStateTable.getEncodeRowIdVersion();
        ScalarOperator eqRowIdOperator = TvrOpUtils.buildRowIdEqBinaryPredicateOp(encodeRowIdVersion,
                aggStateRowIdColumnRef, inputAggUniqueKeys);

        // old aggregate function to new column ref operator map
        Map<ScalarOperator, ColumnRefOperator> oldToNewColumnRefMap = Maps.newHashMap();
        // build input aggregator intermediate aggregation operator
        // For retractable inputs, the intermediate aggregation should preserve
        // the StreamRowOp information for the BE StreamAggregator to dispatch
        LogicalAggregationOperator intermediateAggOperator = buildIntermediateAggOperatorForRetractable(
                columnRefFactory, groupingKeys, inputAggMap, oldToNewColumnRefMap, childTvrOptMeta);

        // delta changes left join agg state scan operator
        LogicalJoinOperator deltaJoinOperator = new LogicalJoinOperator(
                JoinOperator.LEFT_OUTER_JOIN, eqRowIdOperator);
        OptExpression deltaJoinOptExpression = OptExpression.createWithoutTvr(deltaJoinOperator,
                OptExpression.createWithoutTvr(intermediateAggOperator, input.getInputs()),
                OptExpression.createWithoutTvr(aggStateOlapScanOperator));

        // change the aggregation operator into project operator
        Map<ColumnRefOperator, ScalarOperator> projColumnRefMap = Maps.newHashMap();
        if (inputAggOperator.getProjection() != null) {
            projColumnRefMap.putAll(inputAggOperator.getProjection().getColumnRefMap());
        }

        // Build the state combine expression for retractable aggregation
        // For retractable inputs, we use agg_state_combine instead of state_union
        // to properly handle retraction semantics
        List<ColumnRefOperator> inputAggCallColumnRef = inputAggMap.keySet().stream()
                .sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                .collect(Collectors.toList());
        Preconditions.checkArgument(inputAggCallColumnRef.size() == aggStateTableColumnRefs.size(),
                "Input aggregate call column ref operators size %s must match " +
                        "agg state table column ref size %s", inputAggCallColumnRef.size(), aggStateTableColumnRefs.size());
        for (int i = 0; i < inputAggCallColumnRef.size(); i++) {
            ColumnRefOperator oldColumnRef = inputAggCallColumnRef.get(i);
            CallOperator callOperator = inputAggMap.get(oldColumnRef);
            ColumnRefOperator intermediateAggColumnRef = oldToNewColumnRefMap.get(callOperator);
            Preconditions.checkState(intermediateAggColumnRef != null,
                    "New column ref operator should not be null for: %s", callOperator);
            ColumnRefOperator aggStateAggStateColumnRef = aggStateTableColumnRefs.get(i);
            // For retractable inputs, we can still use state_union since the intermediate
            // aggregate state already incorporates the retraction semantics from the
            // StreamAggregator's update/retract dispatch
            ScalarOperator stateUnionScalarOperator = TvrOpUtils.buildStateUnionScalarOperator(
                    callOperator, intermediateAggColumnRef, aggStateAggStateColumnRef);
            projColumnRefMap.put(oldColumnRef, stateUnionScalarOperator);
        }
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projColumnRefMap);
        return OptExpression.createWithoutTvr(logicalProjectOperator, deltaJoinOptExpression);
    }

    /**
     * Build intermediate aggregation operator for retractable inputs.
     *
     * The key difference from monotonic inputs is that the aggregation must be
     * aware of the StreamRowOp to dispatch between update() and retract().
     * This is handled by the StreamAggregator in the BE layer.
     *
     * For the FE layer, we need to ensure:
     * 1. The aggregation preserves the ability to handle retractions
     * 2. The output trait is marked as RETRACTABLE
     */
    private LogicalAggregationOperator buildIntermediateAggOperatorForRetractable(
            ColumnRefFactory columnRefFactory,
            List<ColumnRefOperator> groupingKeys,
            Map<ColumnRefOperator, CallOperator> inputAggMap,
            Map<ScalarOperator, ColumnRefOperator> oldToNewColumnRefMap,
            TvrOptMeta childTvrOptMeta) {
        // For retractable inputs, we still produce intermediate state results
        // The retraction is handled at runtime by the StreamAggregator based on StreamRowOp
        Map<ColumnRefOperator, CallOperator> intermediateAggMap = inputAggMap.entrySet()
                .stream()
                .map(e -> {
                    ColumnRefOperator origColumnRef = e.getKey();
                    CallOperator origCall = e.getValue();
                    // For retractable aggregation, we need aggregate functions that support retraction
                    // Most numeric aggregates (SUM, COUNT, AVG) support retraction
                    // MIN/MAX require additional state tracking
                    CallOperator intermediateFunc = AggregateFunctionRollupUtils.getIntermediateStateAggregateFunc(origCall);
                    Preconditions.checkArgument(intermediateFunc != null,
                            "Intermediate state aggregate function should not be null for: %s", origCall);
                    // create a new column ref for the intermediate state aggregate function
                    ColumnRefOperator newColumnRefOperator =
                            columnRefFactory.create(origColumnRef.getName(),
                                    origColumnRef.getType(), origColumnRef.isNullable());
                    // map old column ref operator to new column ref operator
                    oldToNewColumnRefMap.put(origCall, newColumnRefOperator);
                    return Map.entry(newColumnRefOperator, intermediateFunc);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Mark this aggregation as handling retractable input
        // The AggType.GLOBAL will be executed by StreamAggregator which handles
        // update/retract dispatch based on StreamRowOp
        LogicalAggregationOperator newAggOp = new LogicalAggregationOperator(AggType.GLOBAL, groupingKeys, intermediateAggMap);
        return newAggOp;
    }
}