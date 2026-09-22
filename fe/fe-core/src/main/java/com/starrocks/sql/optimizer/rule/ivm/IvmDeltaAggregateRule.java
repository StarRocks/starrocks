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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
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
 *     └── LeftOuterJoin(encode(group_keys) = mv.__ROW_ID__)
 *           ├── Aggregate(intermediate _combine)
 *           │     └── Delta(...)       ← preserved for subsequent iterations
 *           │           └── child...
 *           └── OlapScan(MV)           ← scan existing MV state
 * </pre>
 */
public class IvmDeltaAggregateRule extends TransformationRule {
    // Measured to saturate here: past the second column the delta spans the whole domain of every later
    // one, so its filter is always true and only the extra column read is left.
    private static final int MAX_SORT_KEY_JOIN_COLUMNS = 2;

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
        // The retractable cloud-native PK path is handled by IvmDeltaRetractableAggregateRule; the two
        // rules' checks are mutually exclusive on subtreeHasRetractablePkScan so exactly one fires.
        if (IvmDeltaRetractableAggregateRule.subtreeHasRetractablePkScan(input.inputAt(0).inputAt(0))) {
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

        // Step 2: Create MV scan
        LogicalOlapScanOperator mvScan = MvRewritePreprocessor.createScanMvOperator(
                mv, columnRefFactory, context.getTvrOptContext().getIvmExcludedMvPartitions(), true);
        ColumnRefOperator mvRowIdRef = mvScan.getColumnMetaToColRefMap().get(rowIdColumn);
        Map<Column, ColumnRefOperator> mvColMetaToRefMap = mvScan.getColumnMetaToColRefMap();

        // Step 3: Collect input aggregate info and the aggregate -> state-column binding
        // (built by IvmRewriter.bindMvColumnsForAggregate; keyed by ref id so the pairing
        // holds for any column layout).
        List<ColumnRefOperator> groupingKeys = inputAggOp.getGroupingKeys();
        Map<ColumnRefOperator, CallOperator> inputAggMap = inputAggOp.getAggregations();
        Map<Integer, String> stateColumnByAggRefId =
                context.getTvrOptContext().getIvmStateColumnNameByAggRefId();
        Preconditions.checkState(stateColumnByAggRefId != null,
                "IVM aggregate state-column binding is missing for MV %s", mv.getName());
        Preconditions.checkState(stateColumnByAggRefId.size() == inputAggMap.size(),
                "state-column binding size %s must match aggregate map size %s",
                stateColumnByAggRefId.size(), inputAggMap.size());

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
        List<ScalarOperator> onConjuncts = Lists.newArrayList(eqPredicate);
        if (context.getSessionVariable().isEnableIvmMvScanSortKeyJoinKeys()) {
            onConjuncts.addAll(buildSortKeyPrefixConjuncts(mv, mvColMetaToRefMap, groupingKeys,
                    context.getTvrOptContext().getIvmGroupKeyRefIdByMvColumn()));
        }

        // Step 6: Build LEFT OUTER JOIN (intermediate agg ⋈ MV scan)
        // Delta is preserved on the aggregate's child for subsequent iterations.
        LogicalDeltaOperator childDelta = new LogicalDeltaOperator(false, delta.getActionColumn());
        OptExpression intermediateAggExpr = OptExpression.create(intermediateAgg,
                OptExpression.create(childDelta, aggChild));
        LogicalJoinOperator joinOp =
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, Utils.compoundAnd(onConjuncts));
        OptExpression joinExpr = OptExpression.create(joinOp,
                intermediateAggExpr,
                OptExpression.create(mvScan));

        // Step 7: Build state_union project
        Map<ColumnRefOperator, ScalarOperator> projMap = Maps.newHashMap();
        if (inputAggOp.getProjection() != null) {
            projMap.putAll(inputAggOp.getProjection().getColumnRefMap());
        }

        // Iteration is id-sorted only to keep plans deterministic; the pairing itself is by ref id.
        List<ColumnRefOperator> inputAggCallRefs = inputAggMap.keySet().stream()
                .sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                .collect(Collectors.toList());
        for (ColumnRefOperator origRef : inputAggCallRefs) {
            CallOperator origCall = inputAggMap.get(origRef);
            ColumnRefOperator intermediateRef = oldToNewRefMap.get(origCall);
            Preconditions.checkState(intermediateRef != null,
                    "Intermediate ref should not be null for: %s", origCall);
            String stateColumnName = stateColumnByAggRefId.get(origRef.getId());
            Preconditions.checkState(stateColumnName != null,
                    "no MV state column bound for aggregate %s (%s)", origCall, origRef);
            Column stateColumn = mv.getColumn(stateColumnName);
            Preconditions.checkState(stateColumn != null,
                    "MV %s has no column '%s' bound for aggregate %s", mv.getName(), stateColumnName, origCall);
            ColumnRefOperator mvStateRef = mvColMetaToRefMap.get(stateColumn);
            Preconditions.checkState(mvStateRef != null,
                    "MV scan lost column '%s' of MV %s", stateColumnName, mv.getName());
            Preconditions.checkState(stateColumn.getType().matchesType(origRef.getType()),
                    "state column '%s' type %s does not match aggregate %s output type %s",
                    stateColumnName, stateColumn.getType(), origCall, origRef.getType());
            ScalarOperator stateUnion = IvmOpUtils.buildStateUnionScalarOperator(
                    origCall, intermediateRef, mvStateRef);
            projMap.put(origRef, stateUnion);
        }

        // Aggregate MV outputs are all UPSERTs. __ACTION__ = INSERT_ACTION (= __op UPSERT).
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            projMap.put(actionColumn, ConstantOperator.createTinyInt(IvmRuleUtils.INSERT_ACTION));
        }

        OptExpression result = OptExpression.create(new LogicalProjectOperator(projMap), joinExpr);
        return List.of(result);
    }

    /**
     * Equalities between the mv's leading sort-key columns and the delta's matching group keys.
     *
     * <p>The row-id equality already implies them — {@code __ROW_ID__} encodes the whole group key tuple —
     * so the join matches the same rows either way. What they add is a runtime filter on a column the mv is
     * physically ordered by, which the storage layer turns into a row-range seek. A null
     * {@code getSortKeyIdxes()} means the sort key IS {@code __ROW_ID__}, which the join already carries.</p>
     *
     * <p>A partition column is deliberately not excluded. What a partition holds constant is its partition
     * <em>key</em>: {@code PARTITION BY date_trunc('day', dt)} leaves {@code dt} itself spanning the day,
     * measured at 86400 distinct values in one partition, where filtering on it read 7.6% of the mv and
     * filtering on the next sort column read all of it.</p>
     */
    private static List<ScalarOperator> buildSortKeyPrefixConjuncts(MaterializedView mv,
                                                                    Map<Column, ColumnRefOperator> mvColMetaToRefMap,
                                                                    List<ColumnRefOperator> groupingKeys,
                                                                    Map<String, Integer> groupKeyRefIdByMvColumn) {
        MaterializedIndexMeta indexMeta = mv.getIndexMetaByMetaId(mv.getBaseIndexMetaId());
        if (indexMeta == null || indexMeta.getSortKeyIdxes() == null) {
            return List.of();
        }
        Map<Integer, ColumnRefOperator> groupKeyByRefId = Maps.newHashMap();
        groupingKeys.forEach(groupingKey -> groupKeyByRefId.put(groupingKey.getId(), groupingKey));
        Map<String, ColumnRefOperator> groupKeyByMvColumnName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        groupKeyRefIdByMvColumn.forEach((mvColumnName, refId) -> {
            ColumnRefOperator groupingKey = groupKeyByRefId.get(refId);
            if (groupingKey != null) {
                groupKeyByMvColumnName.put(mvColumnName, groupingKey);
            }
        });
        Map<String, ColumnRefOperator> mvRefByColumnName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        mvColMetaToRefMap.forEach((column, columnRef) -> mvRefByColumnName.put(column.getName(), columnRef));

        List<Column> schema = indexMeta.getSchema();
        List<ScalarOperator> conjuncts = Lists.newArrayList();
        for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
            if (conjuncts.size() >= MAX_SORT_KEY_JOIN_COLUMNS) {
                break;
            }
            if (sortKeyIdx == null || sortKeyIdx < 0 || sortKeyIdx >= schema.size()) {
                break;
            }
            String columnName = schema.get(sortKeyIdx).getName();
            ColumnRefOperator groupKey = groupKeyByMvColumnName.get(columnName);
            ColumnRefOperator mvColumnRef = mvRefByColumnName.get(columnName);
            // A sort key that is not a group key is an aggregate's output: the mv holds the merged value and
            // the delta a partial one, so equating them would drop the row; a mismatched type would go
            // through a cast no filter is built from. Stop rather than skip: every later sort column is
            // clustered only within this one.
            if (groupKey == null || mvColumnRef == null || !groupKey.getType().equals(mvColumnRef.getType())) {
                break;
            }
            // Null-safe: a NULL group key is a real group, and = would leave its mv row unmatched, which
            // state_union reads as "no prior state" and overwrites that group's accumulated value.
            conjuncts.add(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, mvColumnRef, groupKey));
        }
        return conjuncts;
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
