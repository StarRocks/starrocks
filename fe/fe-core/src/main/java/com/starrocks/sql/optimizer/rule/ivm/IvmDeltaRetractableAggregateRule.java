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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enterprise-only IVM rewrite rule that maintains an aggregate materialized view over a cloud-native
 * PRIMARY KEY base under delete/update by recomputing every affected group from the FROM/TO snapshots,
 * rather than the additive {@code state_union} merge (which cannot subtract a delete).
 *
 * <p>It shares the {@code Delta -> Aggregate} pattern with the append-only {@link IvmDeltaAggregateRule};
 * the two checks are mutually exclusive on {@link #subtreeHasRetractablePkScan} (this rule matches a
 * retractable PK base, that rule matches everything else). Kept as a separate enterprise file so the
 * retraction logic (delete-CDC is enterprise-only) stays off the community sync surface.
 */
public class IvmDeltaRetractableAggregateRule extends TransformationRule {
    public IvmDeltaRetractableAggregateRule() {
        super(RuleType.TF_IVM_DELTA_RETRACTABLE_AGGREGATE,
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
        return subtreeHasRetractablePkScan(input.inputAt(0).inputAt(0));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        OptExpression child = input.inputAt(0).inputAt(0);
        return transformRetractable(context, delta, agg, child);
    }

    private static boolean isRetractablePkScan(OptExpression root) {
        if (root.getOp() instanceof LogicalOlapScanOperator scan) {
            Table table = scan.getTable();
            return table instanceof OlapTable ot
                    && ot.isCloudNativeTableOrMaterializedView()
                    && ot.getKeysType() == KeysType.PRIMARY_KEYS;
        }
        return false;
    }

    /**
     * True iff the subtree reads from a cloud-native PRIMARY KEY base -- the one base kind whose delta
     * may carry deletes/updates. Decided by the scan's TABLE (not its delta trait) so the answer is
     * stable in both the CREATE-time trial and the real refresh, and so this rule's check is the exact
     * complement of {@link IvmDeltaAggregateRule}'s.
     */
    static boolean subtreeHasRetractablePkScan(OptExpression root) {
        if (isRetractablePkScan(root)) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (subtreeHasRetractablePkScan(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True iff every leaf (input-less) operator in the subtree is a retractable cloud-native PRIMARY KEY
     * scan. The recompute-over-join path reads FROM/TO version snapshots of the whole child and assumes
     * every base supports retractable versioning, so a mixed join (a PK base joined to a non-PK / iceberg
     * base) is out of scope and must be rejected rather than read a snapshot the non-PK side can't honor.
     */
    static boolean allLeafScansAreRetractablePk(OptExpression root) {
        if (root.getInputs().isEmpty()) {
            return isRetractablePkScan(root);
        }
        for (OptExpression child : root.getInputs()) {
            if (!allLeafScansAreRetractablePk(child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Recompute every affected group from scratch and express the change as a retract of the old
     * aggregate plus an insert of the new one.
     *
     * <pre>
     *   CTEAnchor
     *     ├── affectedKeys = DISTINCT(group keys) over Delta(child)        which groups changed
     *     └── Aggregate(group keys, __ACTION__) partitionBy group keys
     *           └── UNION ALL
     *                 ├── Version(TO,  child) ⋉ affectedKeys, __ACTION__=UPSERT    new aggregate
     *                 └── Version(FROM, child) ⋉ affectedKeys, __ACTION__=DELETE    old aggregate
     * </pre>
     *
     * <p>An emptied group yields only the DELETE row; a new group only the UPSERT row; a changed group
     * both (the PK sink applies DELETE before UPSERT, netting the new value). Works for every aggregate
     * function — including MIN/MAX — because each group's state is rebuilt by re-running the original
     * {@code _combine} aggregations over the snapshot rather than merged from the prior state.
     */
    private static List<OptExpression> transformRetractable(OptimizerContext context, LogicalDeltaOperator delta,
                                                            LogicalAggregationOperator agg, OptExpression child) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        // A mixed join (a PK base joined to a non-PK base) can't honor the whole-child FROM/TO snapshot
        // recompute, so reject it rather than silently mis-maintain (see allLeafScansAreRetractablePk).
        if (!allLeafScansAreRetractablePk(child)) {
            throw new SemanticException(
                    "IVM retractable aggregate requires every base to be a cloud-native PRIMARY KEY table");
        }
        List<ColumnRefOperator> childOutputs = child.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> groupingKeys = agg.getGroupingKeys();
        if (groupingKeys.stream().anyMatch(k -> !childOutputs.contains(k))) {
            return List.of();
        }

        List<OptExpression> singleSnapshot = buildSingleSnapshotPlan(context, delta, agg, child, childOutputs);
        if (singleSnapshot != null) {
            return singleSnapshot;
        }
        return buildTwoSnapshotPlan(context, delta, agg, child, childOutputs);
    }

    /**
     * Recompute the affected groups from the TO snapshot alone, and read each group's action off whether that
     * recompute still produced a row for it:
     *
     * <pre>
     *   CTEAnchor
     *     ├── affectedKeys = DISTINCT(group keys) over Delta(child)
     *     └── Project(__ACTION__ = survived ? UPSERT : DELETE)
     *           └── affectedKeys ⟕ Aggregate(group keys) over (Version(TO, child) ⋉ affectedKeys)
     * </pre>
     *
     * <p>One row per affected group instead of the UPSERT/DELETE pair {@link #buildTwoSnapshotPlan} emits: a
     * surviving group's UPSERT already overwrites its MV row, so the paired DELETE is redundant -- net-collapse
     * discards it. Only an emptied group still needs a DELETE, and a DELETE needs the key alone, which the
     * affected-keys side already carries.
     *
     * <p>Dropping the old aggregate values is sound only while nothing above consumes this delta: net-collapse
     * and the PK sink key on {@code __ROW_ID__} and ignore the rest. {@link #check} enforces that through
     * {@code isRootDelta}, and IVMAnalyzer rejects both routes that would break it (an MV as an IVM base, and
     * a join over an aggregate). Lift either gate and this branch has to start carrying values again.
     */
    private static List<OptExpression> buildSingleSnapshotPlan(OptimizerContext context, LogicalDeltaOperator delta,
                                                               LogicalAggregationOperator agg, OptExpression child,
                                                               List<ColumnRefOperator> childOutputs) {
        // buildTwoSnapshotPlan carries the aggregate over verbatim; this plan rebuilds it, so whatever the
        // rebuild cannot express keeps the old plan: a LIMIT would be dropped, and a projection's common
        // sub-expressions have nowhere to live (LogicalProjectOperator takes only a columnRefMap).
        if (agg.hasLimit() || (agg.getProjection() != null
                && !agg.getProjection().getCommonSubOperatorMap().isEmpty())) {
            return null;
        }
        ColumnRefFactory factory = context.getColumnRefFactory();
        List<ColumnRefOperator> groupingKeys = agg.getGroupingKeys();
        List<ColumnRefOperator> aggRefs = agg.getAggregations().keySet().stream()
                .sorted(Comparator.comparingInt(ColumnRefOperator::getId))
                .collect(Collectors.toList());

        BranchPlan affectedChild = cloneChild(context, groupingKeys, child, childOutputs);
        int cteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeysProducer = createAffectedKeysProducer(factory, cteId, affectedChild);

        // Aggregating below the outer join, not above it, is what keeps the join at one row per affected group
        // instead of every raw row of every affected group.
        OptExpressionDuplicator toDuplicator = new OptExpressionDuplicator(factory, context);
        OptExpression toChild = toDuplicator.duplicate(child);
        List<ColumnRefOperator> toGroupingKeys = toDuplicator.getMappedColumns(groupingKeys);
        OptExpression toSnapshot = OptExpression.create(
                new LogicalVersionOperator(LogicalVersionOperator.VersionRefType.TO_VERSION), toChild);
        OptExpression toJoin = createLeftSemiJoin(factory, cteId, affectedChild.groupingKeys,
                new BranchPlan(toSnapshot, toDuplicator.getMappedColumns(childOutputs), toGroupingKeys));
        if (toJoin == null) {
            return null;
        }
        List<ColumnRefOperator> toAggRefs = Lists.newArrayList();
        Map<ColumnRefOperator, CallOperator> toAggregations = Maps.newHashMap();
        for (ColumnRefOperator aggRef : aggRefs) {
            ScalarOperator rewritten = toDuplicator.rewriteAfterDuplicate(agg.getAggregations().get(aggRef));
            if (!(rewritten instanceof CallOperator call)) {
                return null;
            }
            ColumnRefOperator newRef = factory.create(aggRef.getName(), aggRef.getType(), aggRef.isNullable());
            toAggregations.put(newRef, call);
            toAggRefs.add(newRef);
        }
        // A NULL group key is a real group that the EQ_FOR_NULL join matches, so a null-valued key on the
        // recompute side cannot distinguish "no match" from "matched the NULL group" -- carry a marker.
        ColumnRefOperator survivedMarker =
                factory.create("__SURVIVED__", IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        Map<ColumnRefOperator, ScalarOperator> recomputeMap = Maps.newHashMap();
        toGroupingKeys.forEach(col -> recomputeMap.put(col, col));
        toAggRefs.forEach(col -> recomputeMap.put(col, col));
        recomputeMap.put(survivedMarker, ConstantOperator.createTinyInt((byte) 1));
        OptExpression recompute = OptExpression.create(new LogicalProjectOperator(recomputeMap),
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, toGroupingKeys, toAggregations),
                        toJoin));

        List<ColumnRefOperator> consumerOutputs = Lists.newArrayList();
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newHashMap();
        for (ColumnRefOperator producerCol : affectedChild.groupingKeys) {
            ColumnRefOperator consumerCol =
                    factory.create(producerCol.getName(), producerCol.getType(), producerCol.isNullable());
            consumerMap.put(consumerCol, producerCol);
            consumerOutputs.add(consumerCol);
        }
        ScalarOperator onPredicate = buildSemiJoinPredicate(consumerOutputs, toGroupingKeys);
        if (onPredicate == null) {
            return null;
        }
        OptExpression outerJoin = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, onPredicate),
                OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap)), recompute);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (int i = 0; i < groupingKeys.size(); i++) {
            projectMap.put(groupingKeys.get(i), consumerOutputs.get(i));
        }
        for (int i = 0; i < aggRefs.size(); i++) {
            ScalarOperator emptyValue =
                    emptyGroupValue(aggRefs.get(i), agg.getAggregations().get(aggRefs.get(i)));
            if (emptyValue == null) {
                return null;
            }
            projectMap.put(aggRefs.get(i), new CaseWhenOperator(aggRefs.get(i).getType(), null, toAggRefs.get(i),
                    List.of(new IsNullPredicateOperator(survivedMarker), emptyValue)));
        }
        projectMap.put(delta.getActionColumn(), new CaseWhenOperator(IvmRuleUtils.ACTION_COLUMN_TYPE, null,
                ConstantOperator.createTinyInt(IvmRuleUtils.INSERT_ACTION),
                List.of(new IsNullPredicateOperator(survivedMarker),
                        ConstantOperator.createTinyInt(IvmRuleUtils.DELETE_ACTION))));
        OptExpression root = OptExpression.create(new LogicalProjectOperator(projectMap), outerJoin);

        // The original aggregate's projection (the MV's SELECT list over the aggregate outputs) rode on the
        // aggregate; this project now stands where that aggregate was, so replay it or the sink loses those refs.
        if (agg.getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> replayMap =
                    Maps.newHashMap(agg.getProjection().getColumnRefMap());
            replayMap.put(delta.getActionColumn(), delta.getActionColumn());
            root = OptExpression.create(new LogicalProjectOperator(replayMap), root);
        }
        return List.of(OptExpression.create(new LogicalCTEAnchorOperator(cteId), affectedKeysProducer, root));
    }

    /**
     * What an aggregate returns over the group that just went empty. NULL for most, but the MV's hidden
     * {@code __AGG_STATE_count(*)} column is declared NOT NULL -- a count over no rows is zero, not null, and
     * the sink rejects the row otherwise. Returns null for any other non-nullable state, whose empty value
     * this rule cannot name, so the caller falls back to the two-snapshot plan.
     */
    private static ScalarOperator emptyGroupValue(ColumnRefOperator aggRef, CallOperator aggCall) {
        if (aggRef.isNullable()) {
            return ConstantOperator.createNull(aggRef.getType());
        }
        String fnName = aggCall.getFnName();
        String baseName = fnName.endsWith(FunctionSet.AGG_STATE_COMBINE_SUFFIX)
                ? fnName.substring(0, fnName.length() - FunctionSet.AGG_STATE_COMBINE_SUFFIX.length())
                : fnName;
        if (FunctionSet.COUNT.equalsIgnoreCase(baseName) && aggRef.getType().matchesType(IntegerType.BIGINT)) {
            return ConstantOperator.createBigint(0);
        }
        return null;
    }

    private static OptExpression createAffectedKeysProducer(ColumnRefFactory factory, int cteId,
                                                            BranchPlan affectedChild) {
        ColumnRefOperator affectedAction =
                factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        OptExpression affectedKeysExpr = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, affectedChild.groupingKeys, Maps.newHashMap()),
                OptExpression.create(new LogicalDeltaOperator(false, affectedAction), affectedChild.optExpression));
        return OptExpression.create(new LogicalCTEProduceOperator(cteId), affectedKeysExpr);
    }

    private static List<OptExpression> buildTwoSnapshotPlan(OptimizerContext context, LogicalDeltaOperator delta,
                                                            LogicalAggregationOperator agg, OptExpression child,
                                                            List<ColumnRefOperator> childOutputs) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        List<ColumnRefOperator> groupingKeys = agg.getGroupingKeys();
        // Full FROM/TO snapshots of the child, each tagged with a constant UPSERT/DELETE action.
        BranchPlan toSnapshot = createSnapshotChild(context, groupingKeys, child, childOutputs,
                LogicalVersionOperator.VersionRefType.TO_VERSION, IvmRuleUtils.INSERT_ACTION);
        BranchPlan fromSnapshot = createSnapshotChild(context, groupingKeys, child, childOutputs,
                LogicalVersionOperator.VersionRefType.FROM_VERSION, IvmRuleUtils.DELETE_ACTION);

        // The group keys touched by the delta, published once as a CTE and semi-joined into both
        // snapshots so only affected groups are recomputed. The per-row delta action is irrelevant here.
        BranchPlan affectedChild = cloneChild(context, groupingKeys, child, childOutputs);
        ColumnRefOperator affectedAction =
                factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        OptExpression affectedKeysExpr = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, affectedChild.groupingKeys, Maps.newHashMap()),
                OptExpression.create(new LogicalDeltaOperator(false, affectedAction), affectedChild.optExpression));
        int cteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeysProducer =
                OptExpression.create(new LogicalCTEProduceOperator(cteId), affectedKeysExpr);

        OptExpression toJoin = createLeftSemiJoin(factory, cteId, affectedChild.groupingKeys, toSnapshot);
        OptExpression fromJoin = createLeftSemiJoin(factory, cteId, affectedChild.groupingKeys, fromSnapshot);
        if (toJoin == null || fromJoin == null) {
            return List.of();
        }

        List<ColumnRefOperator> unionOutputs = Lists.newArrayList(childOutputs);
        unionOutputs.add(delta.getActionColumn());
        OptExpression union = OptExpression.create(
                new LogicalUnionOperator(unionOutputs,
                        List.of(toSnapshot.outputColumns, fromSnapshot.outputColumns), true),
                toJoin, fromJoin);

        // Re-aggregate per (group keys, action): the UPSERT row carries the new group state, the DELETE
        // row the old. Reuse the original aggregate so the output refs match what the row-id project expects.
        List<ColumnRefOperator> newGroupingKeys = Lists.newArrayList(groupingKeys);
        newGroupingKeys.add(delta.getActionColumn());
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(newGroupingKeys)
                .setPartitionByColumns(groupingKeys)
                .build();
        OptExpression newAggExpr = OptExpression.create(newAgg, union);

        return List.of(OptExpression.create(
                new LogicalCTEAnchorOperator(cteId), affectedKeysProducer, newAggExpr));
    }

    private static BranchPlan cloneChild(OptimizerContext context, List<ColumnRefOperator> oldGroupingKeys,
                                           OptExpression child, List<ColumnRefOperator> oldOutputs) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        OptExpression newChild = duplicator.duplicate(child);
        return new BranchPlan(newChild, duplicator.getMappedColumns(oldOutputs),
                duplicator.getMappedColumns(oldGroupingKeys));
    }

    private static BranchPlan createSnapshotChild(OptimizerContext context, List<ColumnRefOperator> groupingKeys,
                                                    OptExpression child, List<ColumnRefOperator> oldOutputs,
                                                    LogicalVersionOperator.VersionRefType versionRefType,
                                                    byte actionValue) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        BranchPlan cloned = cloneChild(context, groupingKeys, child, oldOutputs);
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator out : cloned.outputColumns) {
            projectMap.put(out, out);
        }
        ColumnRefOperator actionColumn =
                factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        projectMap.put(actionColumn, ConstantOperator.createTinyInt(actionValue));
        OptExpression optExpr = OptExpression.create(new LogicalVersionOperator(versionRefType),
                OptExpression.create(new LogicalProjectOperator(projectMap), cloned.optExpression));
        List<ColumnRefOperator> outputs = Lists.newArrayList(cloned.outputColumns);
        outputs.add(actionColumn);
        return new BranchPlan(optExpr, outputs, cloned.groupingKeys);
    }

    private static OptExpression createLeftSemiJoin(ColumnRefFactory factory, int cteId,
                                                    List<ColumnRefOperator> producerOutputColumns,
                                                    BranchPlan leftSnapshot) {
        List<ColumnRefOperator> consumerOutputs = Lists.newArrayList();
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newHashMap();
        for (ColumnRefOperator producerCol : producerOutputColumns) {
            ColumnRefOperator consumerCol =
                    factory.create(producerCol.getName(), producerCol.getType(), producerCol.isNullable());
            consumerMap.put(consumerCol, producerCol);
            consumerOutputs.add(consumerCol);
        }
        OptExpression consumer = OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap));
        ScalarOperator onPredicate = buildSemiJoinPredicate(leftSnapshot.groupingKeys, consumerOutputs);
        if (onPredicate == null) {
            return null;
        }
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, onPredicate),
                leftSnapshot.optExpression, consumer);
    }

    private static ScalarOperator buildSemiJoinPredicate(List<ColumnRefOperator> leftKeys,
                                                         List<ColumnRefOperator> rightKeys) {
        if (leftKeys.size() != rightKeys.size()) {
            return null;
        }
        List<ScalarOperator> conjuncts = Lists.newArrayListWithCapacity(leftKeys.size());
        for (int i = 0; i < leftKeys.size(); i++) {
            // EQ_FOR_NULL (<=>): a NULL group key is a real group and must match its affected-keys row,
            // otherwise the NULL group is never recomputed and its MV row goes stale.
            conjuncts.add(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, leftKeys.get(i), rightKeys.get(i)));
        }
        return Utils.compoundAnd(conjuncts);
    }

    private record BranchPlan(OptExpression optExpression, List<ColumnRefOperator> outputColumns,
                                List<ColumnRefOperator> groupingKeys) {
    }
}
