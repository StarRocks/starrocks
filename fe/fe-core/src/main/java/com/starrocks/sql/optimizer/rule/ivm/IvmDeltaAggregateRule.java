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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves a {@link LogicalDeltaOperator} wrapping a {@link LogicalAggregationOperator} by building
 * an incremental aggregate plan that merges delta changes with the existing MV aggregate state.
 *
 * <p>Pattern: {@code LogicalDeltaOperator -> LogicalAggregationOperator -> child}
 *
 * <p>Only matches when {@code isRootDelta=true} (IVMAnalyzer guarantees aggregate is at the root).
 *
 * <p>This rule is a "terminal" rule — it consumes the Delta marker at the aggregate level and
 * restructures the plan tree. However, it preserves the Delta marker on the aggregate's child
 * so that subsequent iterations can push it down through filter/project to the scan.
 *
 * <p>The output plan structure:
 * <pre>
 *   Project(state_union(intermediate, mv_state), original_projection...)
 *     └── LeftOuterJoin(encode_row_id(group_keys) = mv.__ROW_ID__)
 *           ├── Aggregate(intermediate _combine)
 *           │     └── Delta(...)       ← preserved for subsequent iterations
 *           │           └── child...
 *           └── OlapScan(MV)           ← scan existing MV state
 * </pre>
 */
public class IvmDeltaAggregateRule extends TransformationRule {
    public IvmDeltaAggregateRule() {
        super(RuleType.TF_IVM_DELTA_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        if (!delta.isRootDelta()) {
            return false;
        }
        LogicalAggregationOperator aggOp = input.inputAt(0).getOp().cast();
        if (aggOp.getGroupingKeys().isEmpty()) {
            return false;
        }
        if (aggOp.getAggregations().values().stream().anyMatch(CallOperator::isDistinct)) {
            return false;
        }
        if (aggOp.getPredicate() != null) {
            return false;
        }
        // Must be able to load the target MV for state merge
        MaterializedView mv = IvmRewriter.loadTargetMv(context);
        return mv != null && mv.getColumn(IvmOpUtils.COLUMN_ROW_ID) != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalAggregationOperator inputAggOp = input.inputAt(0).getOp().cast();
        OptExpression aggChild = input.inputAt(0).inputAt(0);

        MaterializedView mv = IvmRewriter.loadTargetMv(context);
        Preconditions.checkState(mv != null, "Target MV must not be null");

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        // Step 1: Get MV schema info
        Column rowIdColumn = mv.getColumn(IvmOpUtils.COLUMN_ROW_ID);
        Preconditions.checkState(rowIdColumn != null, "__ROW_ID__ column must exist in MV");
        int encodeRowIdVersion = mv.getEncodeRowIdVersion();

        List<Column> aggStateColumns = mv.getFullSchema().stream()
                .filter(col -> col.getName().startsWith(IvmOpUtils.COLUMN_AGG_STATE_PREFIX))
                .collect(Collectors.toList());

        // Step 2: Create MV scan
        LogicalOlapScanOperator mvScan = MvRewritePreprocessor.createScanMvOperator(
                mv, columnRefFactory, PCellSortedSet.of(), true);
        ColumnRefOperator mvRowIdRef = mvScan.getColumnMetaToColRefMap().get(rowIdColumn);
        Map<Column, ColumnRefOperator> mvColMetaToRefMap = mvScan.getColumnMetaToColRefMap();
        List<ColumnRefOperator> mvAggStateRefs = aggStateColumns.stream()
                .map(mvColMetaToRefMap::get)
                .collect(Collectors.toList());

        // Step 3: Collect input aggregate info
        List<ColumnRefOperator> groupingKeys = inputAggOp.getGroupingKeys();
        Map<ColumnRefOperator, CallOperator> inputAggMap = inputAggOp.getAggregations();
        Preconditions.checkState(aggStateColumns.size() == inputAggMap.size(),
                "MV __AGG_STATE__ columns size %s must match aggregate map size %s",
                aggStateColumns.size(), inputAggMap.size());

        // Step 4: Build intermediate aggregate operator (same _combine funcs, new ColumnRefs)
        Map<ScalarOperator, ColumnRefOperator> oldToNewRefMap = Maps.newHashMap();
        LogicalAggregationOperator intermediateAgg = buildIntermediateAggOperator(
                columnRefFactory, groupingKeys, inputAggMap, oldToNewRefMap);

        // Step 5: Build __ROW_ID__ equality predicate
        List<ScalarOperator> uniqueKeys = groupingKeys.stream()
                .map(col -> (ScalarOperator) col)
                .collect(Collectors.toList());
        ScalarOperator eqPredicate = IvmOpUtils.buildRowIdEqBinaryPredicateOp(
                encodeRowIdVersion, mvRowIdRef, uniqueKeys);

        // Step 6: Build LEFT OUTER JOIN (intermediate agg ⋈ MV scan)
        // Delta is preserved on the aggregate's child for subsequent iterations.
        LogicalDeltaOperator childDelta = new LogicalDeltaOperator(false, delta.getActionColumn());
        OptExpression intermediateAggExpr = OptExpression.create(intermediateAgg,
                OptExpression.create(childDelta, aggChild));
        LogicalJoinOperator joinOp = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, eqPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp,
                intermediateAggExpr,
                OptExpression.create(mvScan));

        // Step 7: Build state_union project
        Map<ColumnRefOperator, ScalarOperator> projMap = Maps.newHashMap();
        if (inputAggOp.getProjection() != null) {
            projMap.putAll(inputAggOp.getProjection().getColumnRefMap());
        }

        List<ColumnRefOperator> inputAggCallRefs = inputAggMap.keySet().stream()
                .sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                .collect(Collectors.toList());
        Preconditions.checkState(inputAggCallRefs.size() == mvAggStateRefs.size(),
                "Input agg call refs size %s must match MV agg state refs size %s",
                inputAggCallRefs.size(), mvAggStateRefs.size());
        for (int i = 0; i < inputAggCallRefs.size(); i++) {
            ColumnRefOperator origRef = inputAggCallRefs.get(i);
            CallOperator origCall = inputAggMap.get(origRef);
            ColumnRefOperator intermediateRef = oldToNewRefMap.get(origCall);
            Preconditions.checkState(intermediateRef != null,
                    "Intermediate ref should not be null for: %s", origCall);
            ColumnRefOperator mvStateRef = mvAggStateRefs.get(i);
            ScalarOperator stateUnion = IvmOpUtils.buildStateUnionScalarOperator(
                    origCall, intermediateRef, mvStateRef);
            projMap.put(origRef, stateUnion);
        }

        // Aggregate MV outputs are all UPSERTs. __ACTION__ = 0 (= __op UPSERT).
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            projMap.put(actionColumn, ConstantOperator.createTinyInt((byte) 0));
        }

        OptExpression result = OptExpression.create(new LogicalProjectOperator(projMap), joinExpr);
        return List.of(result);
    }

    private LogicalAggregationOperator buildIntermediateAggOperator(
            ColumnRefFactory columnRefFactory,
            List<ColumnRefOperator> groupingKeys,
            Map<ColumnRefOperator, CallOperator> inputAggMap,
            Map<ScalarOperator, ColumnRefOperator> oldToNewRefMap) {
        Map<ColumnRefOperator, CallOperator> intermediateAggMap = inputAggMap.entrySet().stream()
                .map(e -> {
                    ColumnRefOperator origRef = e.getKey();
                    CallOperator origCall = e.getValue();
                    CallOperator intermediateFunc =
                            AggregateFunctionRollupUtils.getIntermediateStateAggregateFunc(origCall);
                    Preconditions.checkArgument(intermediateFunc != null,
                            "Intermediate state agg func should not be null for: %s", origCall);
                    ColumnRefOperator newRef = columnRefFactory.create(
                            origRef.getName(), origRef.getType(), origRef.isNullable());
                    oldToNewRefMap.put(origCall, newRef);
                    return Map.entry(newRef, intermediateFunc);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new LogicalAggregationOperator(AggType.GLOBAL, groupingKeys, intermediateAggMap);
    }
}
