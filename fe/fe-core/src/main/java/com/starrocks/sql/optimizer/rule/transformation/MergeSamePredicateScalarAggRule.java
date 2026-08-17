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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Merge sibling branches of a CROSS JOIN chain that compute scalar (grouping-key-less) aggregations over the same
 * table guarded by <b>identical</b> predicates.
 *
 * <pre>
 * Project(...)                                          Project(...)
 *   CrossJoin                                             CrossJoin
 *     CrossJoin                                             residual
 *       CrossJoin                                    ==&gt;    Agg{o1 -&gt; count(*),
 *         residual                                               o2 -&gt; avg(a),
 *         Agg{o1 -&gt; count(*)} / Filter(p) / Scan(T)              o3 -&gt; avg(b)}
 *       Agg{o2 -&gt; avg(a)} / Filter(p) / Scan(T)              Filter(p)
 *     Agg{o3 -&gt; avg(b)} / Filter(p) / Scan(T)                Scan(T)
 * </pre>
 *
 * <p>This is the conservative half of "conditional aggregate merging": because every merged branch carries the
 * <b>same</b> predicate, the surviving scan's predicate is byte-for-byte what each branch had before, so partition
 * pruning, zone maps, late materialization and the meta-scan fast path all behave exactly as they did. No
 * disjunction is produced and no {@code if()} wrapping is needed, which is what separates this rule from the
 * (much riskier) OR-merging variant.
 *
 * <p>TPC-DS q09 is the motivating case: its 15 uncorrelated scalar subqueries carry only 5 distinct predicates
 * (each {@code ss_quantity BETWEEN a AND b} is shared by a count and two avgs), so the table is scanned 5 times
 * instead of 15.
 *
 * <p>Branches are compared after <b>normalization</b> rather than by walking their operator spines in lockstep: the
 * projections between the aggregation and the scan are inlined away, so a branch becomes the triple
 * {@code (scan, predicate over scan columns, aggregations over scan columns)}. Two branches that read different
 * measure columns produce differently shaped spines for the very same logical filter, which a structural walk
 * would reject; normalization sees straight through that.
 *
 * <p>The rule is scheduled once, right after the Apply -&gt; Join rewrite, where predicates still live in their own
 * {@link LogicalFilterOperator}, projections have not been folded into {@link Operator#getProjection()}, and scans
 * have not been specialized by partition/tablet pruning.
 */
public class MergeSamePredicateScalarAggRule extends TransformationRule {
    private static final int MIN_MERGE_BRANCHES = 2;

    public MergeSamePredicateScalarAggRule() {
        super(RuleType.TF_MERGE_SAME_PREDICATE_SCALAR_AGG,
                Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableMergeSamePredicateScalarAgg()) {
            return false;
        }
        if (!isChainJoin(input.getOp())) {
            return false;
        }
        // Cheap pre-filter: unless the right input already looks like a mergeable branch there is nothing to do.
        return matchBranch(input.inputAt(1)) != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ChainInfo chain = new ChainInfo();
        if (!collectChain(input, chain) || chain.inputs.size() < MIN_MERGE_BRANCHES) {
            return Collections.emptyList();
        }

        List<Branch> branches = new ArrayList<>(chain.inputs.size());
        for (OptExpression child : chain.inputs) {
            branches.add(matchBranch(child));
        }

        // Pairwise grouping. N is the number of cross-joined inputs, small in practice (15 for TPC-DS q09).
        Map<Integer, Map<ColumnRefOperator, ScalarOperator>> renames = new HashMap<>();
        List<List<Integer>> groups = new ArrayList<>();
        boolean[] taken = new boolean[branches.size()];
        for (int i = 0; i < branches.size(); i++) {
            if (branches.get(i) == null || taken[i]) {
                continue;
            }
            List<Integer> group = Lists.newArrayList(i);
            taken[i] = true;
            for (int j = i + 1; j < branches.size(); j++) {
                if (branches.get(j) == null || taken[j]) {
                    continue;
                }
                Map<ColumnRefOperator, ScalarOperator> rename = matchAgainstLeader(branches.get(i), branches.get(j));
                if (rename != null) {
                    group.add(j);
                    taken[j] = true;
                    renames.put(j, rename);
                }
            }
            if (group.size() >= MIN_MERGE_BRANCHES) {
                groups.add(group);
            }
        }
        if (groups.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Integer, OptExpression> merged = new HashMap<>();
        Set<Integer> dropped = new HashSet<>();
        for (List<Integer> group : groups) {
            OptExpression mergedBranch = buildMergedBranch(group, branches, renames);
            if (mergedBranch == null) {
                // A single unmergeable group must not sink the whole rewrite; the other groups still merge.
                continue;
            }
            merged.put(group.get(0), mergedBranch);
            for (int k = 1; k < group.size(); k++) {
                dropped.add(group.get(k));
            }
        }
        if (dropped.isEmpty()) {
            return Collections.emptyList();
        }

        List<OptExpression> newInputs = new ArrayList<>();
        List<LogicalJoinOperator> newJoins = new ArrayList<>();
        ColumnRefSet available = new ColumnRefSet();
        for (int i = 0; i < chain.inputs.size(); i++) {
            if (dropped.contains(i)) {
                continue;
            }
            OptExpression child = merged.get(i);
            if (child == null) {
                child = chain.inputs.get(i);
                available.union(child.getOutputColumns());
            } else {
                // freshly built, no logical property derived yet
                outputColumnsOf(child).forEach(available::union);
            }
            if (!newInputs.isEmpty()) {
                // every input except the leftmost keeps the very edge that attached it
                newJoins.add(chain.joins.get(i - 1));
            }
            newInputs.add(child);
        }

        Map<ColumnRefOperator, ScalarOperator> topProjectMap = composeProjects(chain.projects);
        OptExpression newChain = buildJoinChain(newInputs, newJoins);
        if (topProjectMap == null) {
            return Lists.newArrayList(newChain);
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        for (int id : input.getOutputColumns().getColumnIds()) {
            ColumnRefOperator ref = factory.getColumnRef(id);
            topProjectMap.putIfAbsent(ref, ref);
        }
        // Safety net: everything the surviving projection reads must still be produced by the new join chain.
        for (ScalarOperator value : topProjectMap.values()) {
            if (!available.containsAll(new ColumnRefSet(Utils.extractColumnRef(value)))) {
                return Collections.emptyList();
            }
        }
        return Lists.newArrayList(OptExpression.create(new LogicalProjectOperator(topProjectMap), newChain));
    }

    private static List<ColumnRefOperator> outputColumnsOf(OptExpression expr) {
        Operator op = expr.getOp();
        if (op instanceof LogicalProjectOperator) {
            return new ArrayList<>(((LogicalProjectOperator) op).getColumnRefMap().keySet());
        }
        return new ArrayList<>(((LogicalAggregationOperator) op).getAggregations().keySet());
    }

    // ------------------------------------------------------------------------------------------------------
    // chain collection
    // ------------------------------------------------------------------------------------------------------

    private static class ChainInfo {
        // cross-joined inputs, left to right
        private final List<OptExpression> inputs = new ArrayList<>();
        // the join operator that attached inputs.get(i + 1), so this list is always one shorter than inputs.
        // Each edge is kept separately rather than cloning one template: a chain can mix joins from different
        // sources (the transformer's own cross joins carry no hint, ScalarApply2JoinRule's carry BROADCAST), and
        // copying one edge's hint onto all of them could force a large residual relation to be broadcast.
        private final List<LogicalJoinOperator> joins = new ArrayList<>();
        // LogicalProjectOperators interleaved in the left spine, shallowest first
        private final List<LogicalProjectOperator> projects = new ArrayList<>();
    }

    private boolean collectChain(OptExpression node, ChainInfo chain) {
        Operator op = node.getOp();
        if (op instanceof LogicalProjectOperator && isPassThroughNode(op)) {
            LogicalProjectOperator project = (LogicalProjectOperator) op;
            // An interleaved projection is recreated above the *whole* rebuilt chain. For a deterministic
            // expression that is harmless, but a non-deterministic one would go from being evaluated once per row
            // at its original depth to once per final joined row - observably different as soon as a multi-row
            // input sits above its original position, where before one value was replicated across those rows.
            if (project.getColumnRefMap().values().stream().anyMatch(Utils::hasNonDeterministicFunc)) {
                return false;
            }
            chain.projects.add(project);
            return collectChain(node.inputAt(0), chain);
        }
        if (isChainJoin(op)) {
            if (!collectChain(node.inputAt(0), chain)) {
                return false;
            }
            chain.inputs.add(node.inputAt(1));
            chain.joins.add((LogicalJoinOperator) op);
            return true;
        }
        chain.inputs.add(node);
        return true;
    }

    private static boolean isChainJoin(Operator op) {
        if (!(op instanceof LogicalJoinOperator) || !isPassThroughNode(op)) {
            return false;
        }
        LogicalJoinOperator join = (LogicalJoinOperator) op;
        return (join.getJoinType().isCrossJoin() || join.getJoinType().isInnerJoin())
                && join.getOnPredicate() == null
                && join.getSkewColumn() == null;
    }

    /**
     * No predicate, no folded projection, no limit. Deliberately not applicable to {@link LogicalFilterOperator},
     * whose whole content lives in {@link Operator#getPredicate()}.
     */
    private static boolean isPassThroughNode(Operator op) {
        return op.getPredicate() == null && op.getProjection() == null && !op.hasLimit();
    }

    /** Fold a stack of projections (shallowest first) into a single map. Returns null for an empty stack. */
    private static Map<ColumnRefOperator, ScalarOperator> composeProjects(List<LogicalProjectOperator> projects) {
        if (projects.isEmpty()) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> composed = null;
        for (int i = projects.size() - 1; i >= 0; i--) {
            Map<ColumnRefOperator, ScalarOperator> current = projects.get(i).getColumnRefMap();
            if (composed == null) {
                composed = new LinkedHashMap<>(current);
                continue;
            }
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(composed);
            Map<ColumnRefOperator, ScalarOperator> next = new LinkedHashMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : current.entrySet()) {
                next.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }
            composed = next;
        }
        return composed;
    }

    /** Rebuild the left-deep chain, each surviving edge keeping the join operator it originally had. */
    private OptExpression buildJoinChain(List<OptExpression> inputs, List<LogicalJoinOperator> joins) {
        OptExpression current = inputs.get(0);
        for (int i = 1; i < inputs.size(); i++) {
            LogicalJoinOperator join = LogicalJoinOperator.builder().withOperator(joins.get(i - 1)).build();
            current = OptExpression.create(join, current, inputs.get(i));
        }
        return current;
    }

    // ------------------------------------------------------------------------------------------------------
    // branch matching
    // ------------------------------------------------------------------------------------------------------

    /**
     * A cross-join input of the shape {@code Project* / Agg / (Project|Filter)* / Scan} whose aggregation has no
     * grouping keys, normalized so that both the predicate and the aggregate arguments are expressed directly in
     * terms of the scan's column refs. Anything else (a join, a window, a second aggregation, an AssertOneRow, a
     * CTE consume, ...) yields null and simply stays an ordinary cross-join input.
     */
    private static class Branch {
        private final LogicalAggregationOperator agg;
        private final LogicalScanOperator scan;
        private final ScalarOperator predicate;
        private final Map<ColumnRefOperator, CallOperator> aggregations;
        private final Map<ColumnRefOperator, ScalarOperator> outputMap;

        private Branch(LogicalAggregationOperator agg, LogicalScanOperator scan, ScalarOperator predicate,
                       Map<ColumnRefOperator, CallOperator> aggregations,
                       Map<ColumnRefOperator, ScalarOperator> outputMap) {
            this.agg = agg;
            this.scan = scan;
            this.predicate = predicate;
            this.aggregations = aggregations;
            this.outputMap = outputMap;
        }
    }

    private Branch matchBranch(OptExpression node) {
        List<LogicalProjectOperator> topProjects = new ArrayList<>();
        OptExpression current = node;
        while (current.getOp() instanceof LogicalProjectOperator && isPassThroughNode(current.getOp())
                && current.arity() == 1) {
            topProjects.add((LogicalProjectOperator) current.getOp());
            current = current.inputAt(0);
        }

        Operator op = current.getOp();
        if (!(op instanceof LogicalAggregationOperator) || !isPassThroughNode(op)) {
            return null;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
        if (!agg.getGroupingKeys().isEmpty() || agg.getType() != AggType.GLOBAL || agg.isSplit()
                || agg.getAggregations().isEmpty()) {
            return null;
        }

        // collect the nodes between the aggregation and the scan, top-down
        List<Operator> spine = new ArrayList<>();
        current = current.inputAt(0);
        LogicalScanOperator scan;
        while (true) {
            Operator spineOp = current.getOp();
            if (spineOp instanceof LogicalScanOperator) {
                if (!isMergeableScan((LogicalScanOperator) spineOp)) {
                    return null;
                }
                scan = (LogicalScanOperator) spineOp;
                break;
            }
            if (current.arity() != 1 || spineOp.getProjection() != null || spineOp.hasLimit()) {
                return null;
            }
            if (spineOp instanceof LogicalFilterOperator) {
                if (Utils.hasNonDeterministicFunc(spineOp.getPredicate())) {
                    return null;
                }
            } else if (!(spineOp instanceof LogicalProjectOperator) || spineOp.getPredicate() != null) {
                return null;
            }
            spine.add(spineOp);
            current = current.inputAt(0);
        }

        // normalize bottom-up: inline the projections so everything ends up expressed over scan columns
        Map<ColumnRefOperator, ScalarOperator> inline = new HashMap<>();
        List<ScalarOperator> conjuncts = new ArrayList<>();
        for (int i = spine.size() - 1; i >= 0; i--) {
            Operator spineOp = spine.get(i);
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(inline);
            if (spineOp instanceof LogicalFilterOperator) {
                conjuncts.addAll(Utils.extractConjuncts(rewriter.rewrite(spineOp.getPredicate())));
                continue;
            }
            Map<ColumnRefOperator, ScalarOperator> next = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                    ((LogicalProjectOperator) spineOp).getColumnRefMap().entrySet()) {
                next.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }
            inline = next;
        }

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(inline);
        Map<ColumnRefOperator, CallOperator> aggregations = new LinkedHashMap<>();
        Set<ColumnRefOperator> scanColumns = scan.getColRefToColumnMetaMap().keySet();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
            if (!(rewritten instanceof CallOperator) || Utils.hasNonDeterministicFunc(rewritten)) {
                return null;
            }
            // Keep v1 to aggregates whose arguments are plain columns or constants: anything else would have to be
            // materialized by a projection that the merged branch no longer has.
            for (ScalarOperator arg : rewritten.getChildren()) {
                if (!(arg instanceof ColumnRefOperator) && !(arg instanceof ConstantOperator)) {
                    return null;
                }
            }
            if (!scanColumns.containsAll(Utils.extractColumnRef(rewritten))) {
                return null;
            }
            aggregations.put(entry.getKey(), (CallOperator) rewritten);
        }

        ScalarOperator predicate = Utils.compoundAnd(conjuncts);
        if (predicate != null && (Utils.hasNonDeterministicFunc(predicate)
                || !scanColumns.containsAll(Utils.extractColumnRef(predicate)))) {
            return null;
        }

        Map<ColumnRefOperator, ScalarOperator> composed = composeProjects(topProjects);
        if (composed == null) {
            Map<ColumnRefOperator, ScalarOperator> identity = new LinkedHashMap<>();
            agg.getAggregations().keySet().forEach(ref -> identity.put(ref, ref));
            composed = identity;
        }
        return new Branch(agg, scan, predicate, aggregations, composed);
    }

    /**
     * The scan must be over a table this rule understands, and it must be unspecialized: no predicate, projection
     * or limit of its own, no pushed-down subfield paths, and (for OLAP) none of the partition/tablet/index state
     * that later rules attach nor any per-relation access-path feature. Connector-side pruning state lives in
     * {@link ScanOperatorPredicates} and is compared between the two branches in {@link #matchAgainstLeader}, so it
     * is verified rather than assumed.
     *
     * <p>Table scope is the internal OLAP/cloud-native table plus {@code Table.IS_ANALYZABLE_EXTERNAL_TABLE}
     * (Hive, Iceberg, Hudi, Odps, Delta Lake, Paimon). Reusing that existing set rather than a hand-rolled one
     * means a newly supported lake format is picked up automatically. Everything else - view scans, meta scans,
     * table-function scans, JDBC/MySQL/ES scans - is left alone.
     */
    private static boolean isMergeableScan(LogicalScanOperator scan) {
        Table table = scan.getTable();
        if (!table.isOlapOrCloudNativeTable() && !table.isAnalyzableExternalTable()) {
            return false;
        }
        if (scan.getPredicate() != null || scan.getProjection() != null || scan.hasLimit()) {
            return false;
        }
        if (!scan.getColumnAccessPaths().isEmpty()) {
            return false;
        }
        if (scan instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olap = (LogicalOlapScanOperator) scan;
            VectorSearchOptions vector = olap.getVectorSearchOptions();
            return olap.getSelectedPartitionId() == null
                    && CollectionUtils.isEmpty(olap.getSelectedTabletId())
                    && CollectionUtils.isEmpty(olap.getPrunedPartitionPredicates())
                    && CollectionUtils.isEmpty(olap.getHintsTabletIds())
                    && CollectionUtils.isEmpty(olap.getHintsReplicaIds())
                    && !olap.hasTableHints()
                    // SAMPLE draws an independent subset per relation: merging would force the branches to agree
                    // on one draw, and TableSampleClause has no equals() to compare them with anyway
                    && olap.getSample() == null
                    // an explicit PARTITION(...) is per-relation, and PartitionNames has no equals()
                    && olap.getPartitionNames() == null
                    // per-relation access path, and VectorSearchOptions has no equals() to compare two with
                    && (vector == null || !vector.isEnableUseANN())
                    && !olap.isFromSplitOR();
        }
        return true;
    }

    /** Connector pruning state, or null for scan types that do not carry any. */
    private static ScanOperatorPredicates scanPredicatesOf(LogicalScanOperator scan) {
        try {
            return scan.getScanOperatorPredicates();
        } catch (AnalysisException e) {
            // the base implementation throws for scan types without connector-side pruning (OLAP, JDBC, schema, ...)
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // branch equivalence
    // ------------------------------------------------------------------------------------------------------

    /**
     * Verify that {@code other} scans the same table as {@code leader} and, crucially, is guarded by the very same
     * predicate. Returns the renaming of {@code other}'s scan column refs onto {@code leader}'s, or null.
     *
     * <p>The scans' distribution specs are deliberately <b>not</b> compared: a {@code HashDistributionSpec} carries
     * this scan's own column ref ids, so two scans of one table never compare equal. Distribution is a property of
     * the table, and the merged branch keeps the leader's scan verbatim.
     */
    private Map<ColumnRefOperator, ScalarOperator> matchAgainstLeader(Branch leader, Branch other) {
        // Compared with equals(), never by instance and never by id. External catalogs build a fresh Table
        // wrapper per resolution (IcebergApiConverter.toIcebergTable even burns a fresh CONNECTOR_ID_GENERATOR
        // id each time), so both identity and getId() reject every external-table merge. IcebergTable.equals
        // compares catalog + database + getTableIdentifier(), which embeds the native table uuid and is
        // therefore stable across instances while still discriminating a drop/recreate.
        if (leader.scan.getOpType() != other.scan.getOpType()
                || !Objects.equals(leader.scan.getTable(), other.scan.getTable())
                // two relations over one table may be pinned to different snapshots by FOR VERSION AS OF
                || !Objects.equals(leader.scan.getTvrVersionRange(), other.scan.getTvrVersionRange())
                // connector-side partition pruning state; both are still at their defaults here, but compare
                // rather than assume, so a pruned scan can never be folded into an unpruned one
                || !Objects.equals(scanPredicatesOf(leader.scan), scanPredicatesOf(other.scan))
                || !sameScanTypeSpecificState(leader.scan, other.scan)) {
            return null;
        }

        Map<ColumnRefOperator, ScalarOperator> rename = new HashMap<>();
        Map<Column, ColumnRefOperator> leaderColumns = leader.scan.getColumnMetaToColRefMap();
        // The keyed lookup works across Table instances because Column.equals/hashCode are value-based (name +
        // type), not identity - external connectors do rebuild the Column objects per resolution. The name-keyed
        // map is a fallback for connectors whose Column carries extra state that breaks that equality.
        Map<String, ColumnRefOperator> leaderColumnsByName = new HashMap<>();
        leaderColumns.forEach((column, ref) -> leaderColumnsByName.put(column.getName().toLowerCase(), ref));
        for (Map.Entry<ColumnRefOperator, Column> entry : other.scan.getColRefToColumnMetaMap().entrySet()) {
            ColumnRefOperator leaderRef = leaderColumns.get(entry.getValue());
            if (leaderRef == null) {
                leaderRef = leaderColumnsByName.get(entry.getValue().getName().toLowerCase());
            }
            if (leaderRef == null || !leaderRef.getType().equals(entry.getKey().getType())) {
                return null;
            }
            rename.put(entry.getKey(), leaderRef);
        }

        ScalarOperator renamed = other.predicate == null
                ? null : new ReplaceColumnRefRewriter(rename).rewrite(other.predicate);
        if (!sameConjuncts(leader.predicate, renamed)) {
            return null;
        }
        return rename;
    }

    /**
     * The remaining per-subclass state that is not covered by the table, the column bijection or
     * {@link ScanOperatorPredicates}. The lake-format scans carry nothing else, so only OLAP needs a case here -
     * and only for the two fields that are plain values; everything else is rejected outright by
     * {@link #isMergeableScan} because the classes involved do not implement equals().
     */
    private static boolean sameScanTypeSpecificState(LogicalScanOperator leader, LogicalScanOperator other) {
        if (leader instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator a = (LogicalOlapScanOperator) leader;
            LogicalOlapScanOperator b = (LogicalOlapScanOperator) other;
            // materialized-index choice and the transaction snapshot are per-relation.
            // usePkIndex is the user's [_USE_PK_INDEX_] table hint, which TableRelation.hasTableHints() does NOT
            // cover; merging branches that disagree on it would silently drop or silently extend an explicit hint,
            // so require them to agree rather than rejecting the hint outright.
            return a.getSelectedIndexMetaId() == b.getSelectedIndexMetaId()
                    && a.getGtid() == b.getGtid()
                    && a.isUsePkIndex() == b.isUsePkIndex();
        }
        return true;
    }

    private static boolean sameConjuncts(ScalarOperator left, ScalarOperator right) {
        if (left == null || right == null) {
            return left == null && right == null;
        }
        return new HashSet<>(Utils.extractConjuncts(left)).equals(new HashSet<>(Utils.extractConjuncts(right)));
    }

    // ------------------------------------------------------------------------------------------------------
    // merging
    // ------------------------------------------------------------------------------------------------------

    private OptExpression buildMergedBranch(List<Integer> group, List<Branch> branches,
                                            Map<Integer, Map<ColumnRefOperator, ScalarOperator>> renames) {
        Branch leader = branches.get(group.get(0));
        Map<ColumnRefOperator, CallOperator> aggregations = new LinkedHashMap<>(leader.aggregations);
        Map<ColumnRefOperator, ScalarOperator> outputMap = new LinkedHashMap<>(leader.outputMap);

        for (int k = 1; k < group.size(); k++) {
            int index = group.get(k);
            Branch branch = branches.get(index);
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(renames.get(index));
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : branch.aggregations.entrySet()) {
                if (aggregations.containsKey(entry.getKey())) {
                    return null;
                }
                ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                if (!(rewritten instanceof CallOperator)) {
                    return null;
                }
                aggregations.put(entry.getKey(), (CallOperator) rewritten);
            }
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : branch.outputMap.entrySet()) {
                if (outputMap.containsKey(entry.getKey())) {
                    return null;
                }
                outputMap.put(entry.getKey(), entry.getValue());
            }
        }

        // Merging several DISTINCT aggregates over *different* columns into one aggregation hands the plan to
        // RewriteMultiDistinctRule, which may fan it back out through a CTE. Not wrong, but it can undo the win,
        // so keep v1 out of that territory.
        Set<ScalarOperator> distinctArgs = new HashSet<>();
        for (CallOperator call : aggregations.values()) {
            if (call.isDistinct()) {
                distinctArgs.addAll(call.getChildren());
            }
        }
        if (distinctArgs.size() > 1) {
            return null;
        }

        OptExpression current = widenScan(leader, aggregations);
        if (current == null) {
            return null;
        }
        if (leader.predicate != null) {
            current = OptExpression.create(new LogicalFilterOperator(leader.predicate), current);
        }
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(leader.agg)
                .setAggregations(aggregations)
                .build();
        current = OptExpression.create(newAgg, current);
        if (outputMap.keySet().equals(aggregations.keySet())
                && outputMap.entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue()))) {
            return current;
        }
        return OptExpression.create(new LogicalProjectOperator(outputMap), current);
    }

    /**
     * The merged aggregation may read columns the leader's own scan did not output. The column ref it now reads is
     * already registered against the leader's relation (it came out of that scan's column &lt;-&gt; colref
     * bijection), so widening is a pure addition to {@code colRefToColumnMetaMap}; no column ref is ever minted.
     */
    private OptExpression widenScan(Branch leader, Map<ColumnRefOperator, CallOperator> aggregations) {
        Map<ColumnRefOperator, Column> scanColumns = leader.scan.getColRefToColumnMetaMap();
        Map<ColumnRefOperator, Column> inverse = new HashMap<>();
        leader.scan.getColumnMetaToColRefMap().forEach((column, ref) -> inverse.put(ref, column));

        Map<ColumnRefOperator, Column> missing = new LinkedHashMap<>();
        List<ScalarOperator> used = new ArrayList<>(aggregations.values());
        if (leader.predicate != null) {
            used.add(leader.predicate);
        }
        for (ScalarOperator expr : used) {
            for (ColumnRefOperator ref : Utils.extractColumnRef(expr)) {
                if (scanColumns.containsKey(ref) || missing.containsKey(ref)) {
                    continue;
                }
                Column column = inverse.get(ref);
                if (column == null) {
                    return null;
                }
                missing.put(ref, column);
            }
        }
        if (missing.isEmpty()) {
            return OptExpression.create(leader.scan);
        }
        Map<ColumnRefOperator, Column> widened = new LinkedHashMap<>(scanColumns);
        widened.putAll(missing);
        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(leader.scan);
        Operator newScan = builder.withOperator(leader.scan).setColRefToColumnMetaMap(widened).build();
        return OptExpression.create(newScan);
    }
}
