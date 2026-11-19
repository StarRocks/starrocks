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
import com.google.common.collect.Sets;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Pre-filter the aggregation under the preserved side of a chain of LEFT OUTER joins when the
 * predicate above the chain is an OR whose every branch null-rejects one right side.
 *
 * <p>For example (TPC-DS Q78, ss / ws / cs each aggregated by k):
 * <pre>
 * SELECT ... FROM ss LEFT JOIN ws ON ss.k = ws.k LEFT JOIN cs ON ss.k = cs.k
 * WHERE coalesce(ws.qty, 0) > 0 OR coalesce(cs.qty, 0) > 0
 * </pre>
 * is planned as if it were written
 * <pre>
 * WITH ws AS (...), cs AS (...)
 * SELECT ... FROM (
 *     SELECT k, sum(...) FROM store_sales ...
 *     WHERE k IN (SELECT k FROM ws WHERE coalesce(qty, 0) > 0
 *                 UNION ALL SELECT k FROM cs WHERE coalesce(qty, 0) > 0)
 *     GROUP BY k) ss
 * LEFT JOIN ws ON ss.k = ws.k LEFT JOIN cs ON ss.k = cs.k
 * WHERE coalesce(ws.qty, 0) > 0 OR coalesce(cs.qty, 0) > 0
 * </pre>
 * The key set is computed from the same (forced) CTEs the joins read, so ws / cs are evaluated once.
 *
 * Soundness: an output row survives only if some D_k is true on it. D_k null-rejects some B_j, so the
 * row's B_j columns come from a real match b, which means the equality conjuncts of J_j's ON hold and
 * the B_j-only conjuncts of D_k hold on b. The row's A-side keys are therefore in π_K σ(B_j). K are
 * group keys of the aggregation, so filtering on them commutes with it. The original predicate stays
 * in place; the semi join only removes rows that could never pass it.
 *
 * Every B_j becomes a forced CTE: one consumer keeps B_j's original column refs at its old position,
 * the other feeds the key set.
 */
public class OuterJoinSemiJoinReductionRule extends TransformationRule {
    // RewriteTreeTask descends into the tree this rule returns, and the rewritten spine reuses the top
    // join operator, so the rule would otherwise fire again on its own output.
    private final Set<Operator> rewrittenJoins = Sets.newIdentityHashSet();

    public OuterJoinSemiJoinReductionRule() {
        super(RuleType.TF_OUTER_JOIN_SEMI_JOIN_REDUCTION,
                Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableOuterJoinSemiJoinReduction()) {
            return false;
        }
        LogicalJoinOperator join = input.getOp().cast();
        return join.getJoinType() == JoinOperator.LEFT_OUTER_JOIN && join.getPredicate() != null
                && !rewrittenJoins.contains(join);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Spine spine = Spine.collect(input);
        if (spine == null) {
            return Collections.emptyList();
        }
        LogicalJoinOperator topJoin = input.getOp().cast();
        for (ScalarOperator conjunct : Utils.extractConjuncts(topJoin.getPredicate())) {
            Reduction reduction = Reduction.derive(spine, conjunct, context.getColumnRefFactory());
            if (reduction == null) {
                continue;
            }
            if (!isWorthRewriting(spine, reduction, context)) {
                return Collections.emptyList();
            }
            OptExpression result = rewrite(spine, reduction, context);
            if (result != null) {
                rewrittenJoins.add(topJoin);
                return Lists.newArrayList(result);
            }
        }
        return Collections.emptyList();
    }

    /**
     * The left spine from the matched join down to the aggregation. Only LEFT OUTER joins, projects and
     * filters may sit on it; anything else could change which A rows reach the joins.
     */
    private static final class Spine {
        // top-down, the aggregation excluded; every node's spine child is inputAt(0)
        final List<OptExpression> nodes = Lists.newArrayList();
        final List<Integer> joinIndexes = Lists.newArrayList();
        OptExpression agg;

        static Spine collect(OptExpression top) {
            Spine spine = new Spine();
            OptExpression cur = top;
            while (true) {
                Operator op = cur.getOp();
                if (op.getProjection() != null) {
                    return null;
                }
                // A limit below the top would pick its rows before the new filter. The top join's limit is
                // applied after its own predicate, which the semi join never changes.
                if (cur != top && op.hasLimit()) {
                    return null;
                }
                // filtering early changes which rows a non-deterministic expression is evaluated on
                if (hasNonDeterministicExpr(op)) {
                    return null;
                }
                if (op instanceof LogicalJoinOperator) {
                    if (((LogicalJoinOperator) op).getJoinType() != JoinOperator.LEFT_OUTER_JOIN) {
                        return null;
                    }
                    spine.joinIndexes.add(spine.nodes.size());
                } else if (op instanceof LogicalAggregationOperator) {
                    LogicalAggregationOperator aggOp = (LogicalAggregationOperator) op;
                    if (aggOp.getType() != AggType.GLOBAL || aggOp.isSplit() || aggOp.getGroupingKeys().isEmpty()
                            || cur.inputAt(0).getOp().getOpType() == OperatorType.LOGICAL_REPEAT) {
                        return null;
                    }
                    spine.agg = cur;
                    return spine;
                } else if (!(op instanceof LogicalProjectOperator) && !(op instanceof LogicalFilterOperator)) {
                    return null;
                }
                spine.nodes.add(cur);
                cur = cur.inputAt(0);
            }
        }

        private static boolean hasNonDeterministicExpr(Operator op) {
            List<ScalarOperator> exprs = Lists.newArrayList();
            if (op.getPredicate() != null) {
                exprs.add(op.getPredicate());
            }
            if (op instanceof LogicalJoinOperator && ((LogicalJoinOperator) op).getOnPredicate() != null) {
                exprs.add(((LogicalJoinOperator) op).getOnPredicate());
            } else if (op instanceof LogicalProjectOperator) {
                exprs.addAll(((LogicalProjectOperator) op).getColumnRefMap().values());
            }
            return exprs.stream().anyMatch(Utils::hasNonDeterministicFunc);
        }

        OptExpression rightChild(int joinIndex) {
            return nodes.get(joinIndex).inputAt(1);
        }

        /**
         * Resolve a column seen at the node {@code nodeIndex} back to the column the aggregation outputs,
         * following plain column renames of the projects below that node.
         */
        ColumnRefOperator resolveToAggOutput(ColumnRefOperator column, int nodeIndex) {
            ColumnRefOperator cur = column;
            for (int i = nodeIndex + 1; i < nodes.size(); i++) {
                Operator op = nodes.get(i).getOp();
                if (op instanceof LogicalProjectOperator) {
                    ScalarOperator mapped = ((LogicalProjectOperator) op).getColumnRefMap().get(cur);
                    if (!(mapped instanceof ColumnRefOperator)) {
                        return null;
                    }
                    cur = (ColumnRefOperator) mapped;
                }
            }
            return cur;
        }
    }

    /**
     * Which right sides the OR needs, the key-set filter for each, and the aggregation keys to semi join on.
     */
    private static final class Reduction {
        // join index -> the OR of the key-set filters of the disjuncts assigned to it; null means no filter
        final Map<Integer, ScalarOperator> filters = new LinkedHashMap<>();
        // join index -> aggregation group key -> column of that join's right side
        final Map<Integer, Map<ColumnRefOperator, ColumnRefOperator>> keyColumns = new LinkedHashMap<>();
        final List<ColumnRefOperator> keys = Lists.newArrayList();

        static Reduction derive(Spine spine, ScalarOperator conjunct, ColumnRefFactory factory) {
            List<ScalarOperator> disjuncts = Utils.extractDisjunctive(conjunct);
            if (disjuncts.size() < 2 || Utils.hasNonDeterministicFunc(conjunct)) {
                return null;
            }

            Reduction reduction = new Reduction();
            Map<Integer, List<ScalarOperator>> disjunctFilters = new LinkedHashMap<>();
            Set<Integer> unfiltered = Sets.newHashSet();
            for (ScalarOperator disjunct : disjuncts) {
                Integer joinIndex = findNullRejectedJoin(spine, disjunct, factory);
                if (joinIndex == null) {
                    return null;
                }
                ColumnRefSet rightColumns = spine.rightChild(joinIndex).getOutputColumns();
                List<ScalarOperator> rightOnly = Lists.newArrayList();
                for (ScalarOperator c : Utils.extractConjuncts(disjunct)) {
                    ColumnRefSet used = c.getUsedColumns();
                    if (!used.isEmpty() && rightColumns.containsAll(used)) {
                        rightOnly.add(c);
                    }
                }
                List<ScalarOperator> filters = disjunctFilters.computeIfAbsent(joinIndex, k -> Lists.newArrayList());
                if (rightOnly.isEmpty()) {
                    unfiltered.add(joinIndex);
                } else {
                    filters.add(Utils.compoundAnd(rightOnly));
                }
            }
            for (Map.Entry<Integer, List<ScalarOperator>> entry : disjunctFilters.entrySet()) {
                int joinIndex = entry.getKey();
                reduction.filters.put(joinIndex, unfiltered.contains(joinIndex) ? null :
                        Utils.compoundOr(entry.getValue()));
            }

            List<ColumnRefOperator> groupingKeys =
                    ((LogicalAggregationOperator) spine.agg.getOp()).getGroupingKeys();
            Set<ColumnRefOperator> common = null;
            for (int joinIndex : reduction.filters.keySet()) {
                Map<ColumnRefOperator, ColumnRefOperator> keyMap = equalityKeys(spine, joinIndex, groupingKeys);
                reduction.keyColumns.put(joinIndex, keyMap);
                if (common == null) {
                    common = Sets.newHashSet(keyMap.keySet());
                } else {
                    common.retainAll(keyMap.keySet());
                }
            }
            for (ColumnRefOperator key : groupingKeys) {
                if (common.contains(key)) {
                    reduction.keys.add(key);
                }
            }
            if (reduction.keys.isEmpty()) {
                return null;
            }
            // each branch of the key set must output distinct columns
            for (Map<ColumnRefOperator, ColumnRefOperator> keyMap : reduction.keyColumns.values()) {
                if (reduction.keys.stream().map(keyMap::get).distinct().count() != reduction.keys.size()) {
                    return null;
                }
            }
            return reduction;
        }

        private static Integer findNullRejectedJoin(Spine spine, ScalarOperator disjunct, ColumnRefFactory factory) {
            ColumnRefSet used = disjunct.getUsedColumns();
            for (int joinIndex : spine.joinIndexes) {
                ColumnRefSet rightColumns = spine.rightChild(joinIndex).getOutputColumns();
                if (rightColumns.isIntersect(used)
                        && Utils.canEliminateNull(factory.getColumnRefs(rightColumns), disjunct.clone())) {
                    return joinIndex;
                }
            }
            return null;
        }

        /**
         * Plain {@code a = b} conjuncts of the join's ON, where a resolves to a group key of the aggregation
         * and b is a column of the right side with the same type. Other conjuncts only restrict matches
         * further, so ignoring them keeps the key set a superset.
         */
        private static Map<ColumnRefOperator, ColumnRefOperator> equalityKeys(Spine spine, int joinIndex,
                                                                              List<ColumnRefOperator> groupingKeys) {
            LogicalJoinOperator join = spine.nodes.get(joinIndex).getOp().cast();
            ColumnRefSet rightColumns = spine.rightChild(joinIndex).getOutputColumns();
            Map<ColumnRefOperator, ColumnRefOperator> result = Maps.newHashMap();
            for (ScalarOperator c : Utils.extractConjuncts(join.getOnPredicate())) {
                if (!(c instanceof BinaryPredicateOperator)
                        || ((BinaryPredicateOperator) c).getBinaryType() != BinaryType.EQ
                        || !c.getChild(0).isColumnRef() || !c.getChild(1).isColumnRef()) {
                    continue;
                }
                ColumnRefOperator left = (ColumnRefOperator) c.getChild(0);
                ColumnRefOperator right = (ColumnRefOperator) c.getChild(1);
                if (rightColumns.contains(left)) {
                    ColumnRefOperator tmp = left;
                    left = right;
                    right = tmp;
                }
                if (!rightColumns.contains(right) || rightColumns.contains(left)) {
                    continue;
                }
                ColumnRefOperator key = spine.resolveToAggOutput(left, joinIndex);
                if (key == null || !groupingKeys.contains(key) || !isSupportedKeyType(key.getType())
                        || !key.getType().equals(right.getType())) {
                    continue;
                }
                result.putIfAbsent(key, right);
            }
            return result;
        }

        // String keys may be dictionary encoded on the aggregation side but not on the key-set side.
        private static boolean isSupportedKeyType(Type type) {
            return type.isNumericType() || type.isDecimalOfAnyVersion() || type.isDateType() || type.isBoolean();
        }
    }

    /**
     * The rewrite is always correct but not always profitable, and how many aggregation keys the right sides
     * cover is exactly what statistics cannot tell (Q78's channels share almost no (item, customer) keys, which
     * no single-column statistic shows). So only bound the cost: skip when the aggregation is too small to be
     * worth the CTEs, the key set and its shuffle, or when the key set is far larger than the rows it filters.
     * The factor allows for the preserved side's input being underestimated: in Q78 it comes out of a
     * `LEFT JOIN returns ... IS NULL` anti join whose estimate is floored at 10% while ~90% of the rows survive.
     */
    private static boolean isWorthRewriting(Spine spine, Reduction reduction, OptimizerContext context) {
        Utils.calculateStatistics(spine.agg, context);
        Statistics aggStats = spine.agg.getStatistics();
        Statistics inputStats = spine.agg.inputAt(0).getStatistics();
        if (aggStats == null || inputStats == null) {
            return false;
        }
        // Without key statistics (no ANALYZE yet, or a cold connector statistics cache) the aggregation estimate
        // says nothing about its size, so keep today's plan. A capped estimate (aggregation rows == input rows)
        // is not rejected: it is what real 1T statistics give for Q78 itself, whose three-key NDV product exceeds
        // the input, and over-estimating the aggregation only errs toward the rewrite the key-set bound still
        // checks.
        for (ColumnRefOperator key : reduction.keys) {
            if (inputStats.getColumnStatistic(key).isUnknown()) {
                return false;
            }
        }
        long minAggRows = context.getSessionVariable().getOuterJoinSemiJoinReductionMinAggRows();
        if (minAggRows > 0 && aggStats.getOutputRowCount() < minAggRows) {
            return false;
        }
        // an upper bound of the key set: every row of every right side it reads
        double keySetRows = 0;
        for (int joinIndex : reduction.filters.keySet()) {
            OptExpression right = spine.rightChild(joinIndex);
            Utils.calculateStatistics(right, context);
            if (right.getStatistics() == null) {
                return false;
            }
            keySetRows += right.getStatistics().getOutputRowCount();
        }
        double maxRatio = context.getSessionVariable().getOuterJoinSemiJoinReductionMaxKeySetRatio();
        return maxRatio <= 0 || keySetRows <= maxRatio * inputStats.getOutputRowCount();
    }

    private OptExpression rewrite(Spine spine, Reduction reduction, OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<Integer, OptExpression> consumes = Maps.newHashMap();
        List<OptExpression> produces = Lists.newArrayList();
        List<Integer> cteIds = Lists.newArrayList();
        List<OptExpression> branches = Lists.newArrayList();
        List<List<ColumnRefOperator>> branchKeys = Lists.newArrayList();

        for (Map.Entry<Integer, ScalarOperator> entry : reduction.filters.entrySet()) {
            int joinIndex = entry.getKey();
            OptExpression right = spine.rightChild(joinIndex);

            OptExpressionDuplicator duplicator = new OptExpressionDuplicator(factory, context);
            OptExpression duplicated = duplicator.duplicate(right);
            if (!isFullyDuplicated(duplicated)) {
                return null;
            }
            Map<ColumnRefOperator, ColumnRefOperator> mapping = duplicator.getColumnMapping();

            int cteId = context.getCteContext().getNextCteId();
            // the consumer at the old position keeps the original column refs, so nothing above changes
            Map<ColumnRefOperator, ColumnRefOperator> joinSideMap = Maps.newHashMap();
            for (ColumnRefOperator column : factory.getColumnRefs(right.getOutputColumns())) {
                ColumnRefOperator produced = mapping.get(column);
                if (produced == null) {
                    return null;
                }
                joinSideMap.put(column, produced);
            }

            ScalarOperator filter = entry.getValue();
            Map<ColumnRefOperator, ColumnRefOperator> keyMap = reduction.keyColumns.get(joinIndex);
            ColumnRefSet keySideColumns = new ColumnRefSet();
            reduction.keys.forEach(key -> keySideColumns.union(keyMap.get(key)));
            if (filter != null) {
                keySideColumns.union(filter.getUsedColumns());
            }
            Map<ColumnRefOperator, ColumnRefOperator> keySideMap = Maps.newHashMap();
            Map<ColumnRefOperator, ScalarOperator> renames = Maps.newHashMap();
            for (ColumnRefOperator column : factory.getColumnRefs(keySideColumns)) {
                ColumnRefOperator fresh = factory.create(column, column.getType(), column.isNullable());
                keySideMap.put(fresh, joinSideMap.get(column));
                renames.put(column, fresh);
            }
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(renames);

            OptExpression branch = OptExpression.create(new LogicalCTEConsumeOperator(cteId, keySideMap));
            if (filter != null) {
                branch = OptExpression.create(new LogicalFilterOperator(rewriter.rewrite(filter)), branch);
            }
            List<ColumnRefOperator> outputKeys = Lists.newArrayList();
            for (ColumnRefOperator key : reduction.keys) {
                outputKeys.add((ColumnRefOperator) renames.get(keyMap.get(key)));
            }
            branches.add(branch);
            branchKeys.add(outputKeys);

            consumes.put(joinIndex, OptExpression.create(new LogicalCTEConsumeOperator(cteId, joinSideMap)));
            produces.add(OptExpression.create(new LogicalCTEProduceOperator(cteId), duplicated));
            cteIds.add(cteId);
        }

        // The key set is not deduplicated: a semi join ignores duplicates on its build side, and each branch reads
        // an aggregation already grouped by these keys (Q78: 387.2M rows in, 387.19M out of a DISTINCT that cost
        // ~6.9 s per instance).
        List<ColumnRefOperator> keySetColumns;
        OptExpression keySetInput;
        if (branches.size() == 1) {
            keySetColumns = branchKeys.get(0);
            keySetInput = branches.get(0);
        } else {
            keySetColumns = Lists.newArrayList();
            for (ColumnRefOperator key : reduction.keys) {
                keySetColumns.add(factory.create(key, key.getType(), true));
            }
            keySetInput = OptExpression.create(new LogicalUnionOperator(keySetColumns, branchKeys, true), branches);
        }
        OptExpression keySet = keySetInput;

        List<ScalarOperator> semiOn = Lists.newArrayList();
        for (int i = 0; i < reduction.keys.size(); i++) {
            semiOn.add(BinaryPredicateOperator.eq(reduction.keys.get(i), keySetColumns.get(i)));
        }
        ScalarOperator semiOnPredicate = Utils.compoundAnd(semiOn);
        ColumnRefSet keyColumns = new ColumnRefSet(reduction.keys);
        OptExpression semiJoin = pushDownSemiJoin(spine.agg.inputAt(0),
                input -> createSemiJoin(input, keySet, semiOnPredicate, factory), keyColumns, factory);

        // rebuild the spine bottom-up
        OptExpression child = OptExpression.create(spine.agg.getOp(), semiJoin);
        for (int i = spine.nodes.size() - 1; i >= 0; i--) {
            OptExpression node = spine.nodes.get(i);
            List<OptExpression> inputs = Lists.newArrayList(node.getInputs());
            inputs.set(0, child);
            if (consumes.containsKey(i)) {
                inputs.set(1, consumes.get(i));
            }
            child = OptExpression.create(node.getOp(), inputs);
        }

        for (int i = produces.size() - 1; i >= 0; i--) {
            int cteId = cteIds.get(i);
            // the key-set consumer has no inlined subtree, and inlining it would lose the reuse anyway
            context.getCteContext().addForceCTE(cteId);
            child = OptExpression.create(new LogicalCTEAnchorOperator(cteId), produces.get(i), child);
        }
        return child;
    }

    // The join is built after column pruning, and without a projection a semi join still reports both sides'
    // columns as its output. The SHUFFLE hint does two things:
    // - it keeps the join where pushDownSemiJoin put it: the estimates think the `LEFT JOIN ... IS NULL` above it
    //   keeps 10% of its input (it keeps ~90%), so the join reorder rules would otherwise move it back above;
    // - the build side is now always the key set (a hinted join is not commuted), and the key set must never be
    //   broadcast: its estimate can be far too low (Q78: estimated 120M, actual 387M keys to every BE).
    private static OptExpression createSemiJoin(OptExpression input, OptExpression keySet, ScalarOperator onPredicate,
                                                ColumnRefFactory factory) {
        Map<ColumnRefOperator, ScalarOperator> output = Maps.newHashMap();
        factory.getColumnRefs(input.getOutputColumns()).forEach(c -> output.put(c, c));
        return OptExpression.create(
                LogicalJoinOperator.builder().setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                        .setOnPredicate(onPredicate).setJoinHint(HintNode.HINT_JOIN_SHUFFLE)
                        .setProjection(new Projection(output)).build(),
                input, keySet);
    }

    /**
     * Put the semi join as low under the aggregation as its keys allow: a filter on columns of the preserved side
     * commutes with projects that pass the keys through, with deterministic filters, with a LEFT OUTER JOIN (and its
     * IS NULL predicate) whose preserved side has every key, and with either side of an inner join holding every key.
     *
     * <p>Q78's keys span an inner join: d_year comes from date_dim, joined above
     * {@code store_sales LEFT JOIN store_returns ... WHERE sr_ticket_number IS NULL}. That inner join only reads
     * columns of store_sales and date_dim, so it is moved below the outer join first:
     * {@code (X LEFT JOIN B) JOIN R  =>  (X JOIN R) LEFT JOIN B}, and the semi join goes right above X JOIN R. The
     * store side is then filtered before the anti join and its shuffles instead of after them.
     */
    private static OptExpression pushDownSemiJoin(OptExpression node, Function<OptExpression, OptExpression> semiJoin,
                                                  ColumnRefSet keys, ColumnRefFactory factory) {
        Operator op = node.getOp();
        if (op.getProjection() != null || op.hasLimit() || Spine.hasNonDeterministicExpr(op)) {
            return semiJoin.apply(node);
        }
        if (op instanceof LogicalProjectOperator) {
            if (!passesThrough((LogicalProjectOperator) op, keys)) {
                return semiJoin.apply(node);
            }
            return OptExpression.create(op, pushDownSemiJoin(node.inputAt(0), semiJoin, keys, factory));
        }
        if (op instanceof LogicalFilterOperator) {
            return OptExpression.create(op, pushDownSemiJoin(node.inputAt(0), semiJoin, keys, factory));
        }
        if (!(op instanceof LogicalJoinOperator)) {
            return semiJoin.apply(node);
        }
        JoinOperator joinType = ((LogicalJoinOperator) op).getJoinType();
        OptExpression left = node.inputAt(0);
        OptExpression right = node.inputAt(1);
        if (joinType == JoinOperator.LEFT_OUTER_JOIN && left.getOutputColumns().containsAll(keys)) {
            return OptExpression.create(op, pushDownSemiJoin(left, semiJoin, keys, factory), right);
        }
        if (joinType != JoinOperator.INNER_JOIN) {
            return semiJoin.apply(node);
        }
        if (left.getOutputColumns().containsAll(keys)) {
            return OptExpression.create(op, pushDownSemiJoin(left, semiJoin, keys, factory), right);
        }
        if (right.getOutputColumns().containsAll(keys)) {
            return OptExpression.create(op, left, pushDownSemiJoin(right, semiJoin, keys, factory));
        }
        OptExpression reordered = innerJoinBelowOuterJoin(node, keys, factory);
        if (reordered == null) {
            return semiJoin.apply(node);
        }
        // reordered: (X JOIN R) LEFT JOIN B
        return OptExpression.create(reordered.getOp(), semiJoin.apply(reordered.inputAt(0)), reordered.inputAt(1));
    }

    /**
     * {@code (X LEFT JOIN B) JOIN R  =>  (X JOIN R) LEFT JOIN B}, valid when the inner join reads no column of B:
     * every row of the outer join is an X row with its matching B row or NULLs, and joining it to R depends only on
     * the X part, while the outer join's ON and predicate read no column of R. A project between the two joins is
     * only allowed to prune columns. Returns null when the shape does not hold.
     */
    private static OptExpression innerJoinBelowOuterJoin(OptExpression innerJoin, ColumnRefSet keys,
                                                         ColumnRefFactory factory) {
        OptExpression outerJoin = innerJoin.inputAt(0);
        if (outerJoin.getOp() instanceof LogicalProjectOperator) {
            if (!isColumnPruning((LogicalProjectOperator) outerJoin.getOp())) {
                return null;
            }
            outerJoin = outerJoin.inputAt(0);
        }
        Operator outerOp = outerJoin.getOp();
        if (!(outerOp instanceof LogicalJoinOperator)
                || ((LogicalJoinOperator) outerOp).getJoinType() != JoinOperator.LEFT_OUTER_JOIN
                || outerOp.getProjection() != null || outerOp.hasLimit() || Spine.hasNonDeterministicExpr(outerOp)) {
            return null;
        }
        OptExpression preserved = outerJoin.inputAt(0);
        OptExpression r = innerJoin.inputAt(1);
        ColumnRefSet newInnerColumns = new ColumnRefSet();
        newInnerColumns.union(preserved.getOutputColumns());
        newInnerColumns.union(r.getOutputColumns());
        LogicalJoinOperator innerOp = innerJoin.getOp().cast();
        ColumnRefSet innerUsed = new ColumnRefSet();
        if (innerOp.getOnPredicate() != null) {
            innerUsed.union(innerOp.getOnPredicate().getUsedColumns());
        }
        if (innerOp.getPredicate() != null) {
            innerUsed.union(innerOp.getPredicate().getUsedColumns());
        }
        if (!newInnerColumns.containsAll(innerUsed) || !newInnerColumns.containsAll(keys)) {
            return null;
        }
        OptExpression newInner = OptExpression.create(innerOp, preserved, r);
        newInner.deriveLogicalPropertyItself();
        return OptExpression.create(outerOp, newInner, outerJoin.inputAt(1));
    }

    private static boolean passesThrough(LogicalProjectOperator project, ColumnRefSet keys) {
        for (int id : keys.getColumnIds()) {
            ScalarOperator mapped = project.getColumnRefMap().entrySet().stream()
                    .filter(e -> e.getKey().getId() == id).map(Map.Entry::getValue).findFirst().orElse(null);
            if (!(mapped instanceof ColumnRefOperator) || ((ColumnRefOperator) mapped).getId() != id) {
                return false;
            }
        }
        return true;
    }

    private static boolean isColumnPruning(LogicalProjectOperator project) {
        return project.getColumnRefMap().entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue()));
    }

    // OptExpressionDuplicator returns null for an operator it does not support, at any depth
    private static boolean isFullyDuplicated(OptExpression expression) {
        if (expression == null) {
            return false;
        }
        for (OptExpression input : expression.getInputs()) {
            if (!isFullyDuplicated(input)) {
                return false;
            }
        }
        return true;
    }
}
