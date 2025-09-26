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

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.expression.JoinOperator;
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
                                                     OlapTable aggStateTable,
                                                     LogicalAggregationOperator inputAggOperator,
                                                     OptExpression input) {
        Preconditions.checkArgument(aggStateTable != null,
                "Aggregate state table must not be null for TVR aggregate rule");
        final Column tvrColumnRowId = aggStateTable.getColumn(TvrOpUtils.COLUMN_ROW_ID);
        Preconditions.checkArgument(tvrColumnRowId != null,
                "TVR column row id must exist in agg state table");
        final ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
        final LogicalOlapScanOperator aggStateOlapScanOperator = MvRewritePreprocessor.createScanMvOperator(
                aggStateTable, columnRefFactory, Sets.newHashSet(), true);
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
        ScalarOperator eqRowIdOperator = TvrOpUtils.buildRowIdEqBinaryPredicateOp(aggStateRowIdColumnRef,
                inputAggUniqueKeys);

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
        // handle append only aggregate state table
        TvrOptMeta childTvrOptMeta = input.inputAt(0).getTvrMeta();
        Preconditions.checkArgument(childTvrOptMeta != null,
                "TVR meta should not be null for aggregate input");
        if (!childTvrOptMeta.isAppendOnly()) {
            throw new IllegalStateException("Only Append TVR change type is supported for aggregate rule.");
        }
        LogicalAggregationOperator aggOp = input.getOp().cast();
        TvrOptContext tvrOptContext = context.getTvrOptContext();
        // find the agg state table
        MaterializedView aggStateTable = tvrOptContext.getTvrTargetMV();
        OptExpression deltaAggregate = doTransformWithMonotonic(context, aggStateTable, aggOp, input);

        return Lists.newArrayList(deltaAggregate);
    }
}