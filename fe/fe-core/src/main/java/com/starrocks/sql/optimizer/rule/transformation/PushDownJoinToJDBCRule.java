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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.join.MultiJoinNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Push down multi-table INNER JOINs to JDBC external database when all tables
 * belong to the same JDBC catalog and connection.
 *
 * <p>Each output column is aliased by its ColumnRefOperator ID (sr_c{id}) so the
 * outer wrapping query (composed by the BE from JDBCScanNode's column list) can
 * reference them by name without relying on positional ordering.
 *
 * <p>Before:
 * <pre>
 *   HashJoin(a.id = b.id)
 *     JDBC_SCAN(a)
 *     JDBC_SCAN(b)
 * </pre>
 *
 * <p>After:
 * <pre>
 *   JDBC_SCAN("SELECT sr_t0.`id` AS sr_c1, sr_t1.`id` AS sr_c2
 *              FROM a sr_t0
 *              INNER JOIN b sr_t1
 *              ON (sr_t0.`id` = sr_t1.`id`)")
 * </pre>
 *
 * <p>The rule is self-contained: it computes the columns the merged scan must expose
 * (the original root's output expressions plus the predicates that stay in the rebuilt
 * local join) and prunes everything else from the merged SELECT, and it re-attaches the
 * original output projection itself. It is meant to run as part of
 * {@code RuleSet.JDBC_PUSHDOWN_RULES} — after CTE inlining and the final
 * MergeProjectWithChildRule pass, where standalone projections have been merged into
 * operators (which is what lets MultiJoinNode flatten through join projections) and where
 * an operator-attached projection is the canonical output form. Applied iteratively,
 * the companion rules of the set then fold aggregations and limits onto the merged scan.
 *
 * <p><b>Known limitation — statistics:</b> the merged scan inherits the same
 * default statistics path as any single-table JDBC scan
 * ({@code Config.default_statistics_output_row_count} for rows, {@code ColumnStatistic.unknown()}
 * for columns). The join output cardinality is therefore not estimated — downstream operators
 * see the merged scan as if it were one table of default size. This is consistent with the
 * broader JDBC stats gap (JDBC tables are not auto-analyzed, and column stats go through the
 * internal OlapTable cache loader which returns empty for non-OlapTables), and is not a
 * regression introduced by the merge. Improving this requires a separate effort to collect
 * real JDBC statistics from the external DB.
 */
public class PushDownJoinToJDBCRule extends TransformationRule {

    public PushDownJoinToJDBCRule() {
        super(RuleType.TF_PUSH_DOWN_JOIN_TO_JDBC,
                Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                                Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return context.getSessionVariable().isEnableJdbcJoinPushDown();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // Step 1: Flatten the join tree into atoms and join-level predicates.
        MultiJoinNode multiJoin = MultiJoinNode.toMultiJoinNode(input);
        LinkedHashSet<OptExpression> atoms = multiJoin.getAtoms();
        if (atoms.size() < 2) {
            return Lists.newArrayList();
        }

        // Flattened predicates may reference refs that no atom outputs — refs defined by
        // projections MultiJoinNode absorbed into expressionMap (e.g. a + b AS c). Re-expand
        // them down to atom columns first (the same compensation computeOutputProjection
        // applies to the output): grouping and predicate ownership only see atom columns, so
        // an unexpanded predicate can never connect or be owned by a group and the tables it
        // bridges are left out of the merge.
        List<ScalarOperator> flatPredicates = multiJoin.getPredicates();
        if (!multiJoin.getExpressionMap().isEmpty()) {
            ReplaceColumnRefRewriter expressionRewriter =
                    new ReplaceColumnRefRewriter(multiJoin.getExpressionMap(), true);
            List<ScalarOperator> rewritten = new ArrayList<>(flatPredicates.size());
            for (ScalarOperator pred : flatPredicates) {
                rewritten.add(expressionRewriter.rewrite(pred));
            }
            flatPredicates = rewritten;
        }

        // Step 2: Collect the candidate groups — same-catalog plain JDBC scans, >= 2 per group.
        List<MergeGroup> groups = collectMergeGroups(atoms, flatPredicates);
        if (groups.isEmpty()) {
            return Lists.newArrayList();
        }

        // Step 3: Route every predicate either to the group that owns it or to the rebuilt
        // local join, and decide per group whether it actually merges.
        List<ScalarOperator> remainingPredicates = new ArrayList<>();
        routePredicates(flatPredicates, groups, remainingPredicates);
        groups.removeIf(g -> !g.shouldMerge);
        if (groups.isEmpty()) {
            return Lists.newArrayList();
        }

        // Step 4: Compute the projection the rewritten subtree must expose to upper
        // operators. Its value refs — or, when the original root exposed its raw child
        // outputs unchanged (null projection), the root's visible output columns, which
        // exclude columns a scan's pruning projection hides — plus the predicates that
        // stay in the rebuilt local join, are exactly the columns the merged scans need
        // to output. Join keys and filter columns consumed inside the pushdown SQL are
        // pruned from the SELECT. A null projection is deliberately left unattached in
        // Step 7 so a fully merged scan stays projection-free and the companion aggregate
        // rule can still fold onto it.
        Map<ColumnRefOperator, ScalarOperator> outputProjection =
                computeOutputProjection(input, atoms, multiJoin.getExpressionMap(), context);
        ColumnRefSet neededColumns = new ColumnRefSet();
        if (outputProjection != null) {
            for (ScalarOperator value : outputProjection.values()) {
                neededColumns.union(value.getUsedColumns());
            }
        } else {
            neededColumns.union(input.getOutputColumns());
        }
        for (ScalarOperator pred : remainingPredicates) {
            neededColumns.union(pred.getUsedColumns());
        }

        // Step 5: Build one merged scan per group. The group's first atom is substituted by
        // the merged scan; its remaining atoms are dropped from the join rebuild.
        Map<OptExpression, OptExpression> atomSubstitution = new HashMap<>();
        Set<OptExpression> droppedAtoms = new HashSet<>();
        for (MergeGroup group : groups) {
            atomSubstitution.put(group.entries.get(0).atom, buildMergedScan(group, neededColumns));
            for (int i = 1; i < group.entries.size(); i++) {
                droppedAtoms.add(group.entries.get(i).atom);
            }
        }

        // Step 6: Rebuild the join tree in the original atom order. This preserves the join
        // order that the earlier RBO join-reorder pass established for non-JDBC atoms and
        // unmerged JDBC atoms.
        List<OptExpression> toJoin = new ArrayList<>();
        for (OptExpression atom : atoms) {
            if (droppedAtoms.contains(atom)) {
                continue;
            }
            toJoin.add(atomSubstitution.getOrDefault(atom, atom));
        }

        OptExpression result = toJoin.get(0);
        for (int i = 1; i < toJoin.size(); i++) {
            result = appendJoin(result, toJoin.get(i), remainingPredicates);
        }

        // Attach any leftover predicates.
        // If the top node is a Join, set its filter predicate (== WHERE above the join).
        // Otherwise (single merged scan with no other atoms), wrap with a LogicalFilter so
        // the predicate is evaluated locally after the scan.
        if (!remainingPredicates.isEmpty()) {
            ScalarOperator remainPred = Utils.compoundAnd(remainingPredicates);
            if (result.getOp() instanceof LogicalJoinOperator topJoin) {
                LogicalJoinOperator newTopJoin = new LogicalJoinOperator.Builder()
                        .withOperator(topJoin)
                        .setPredicate(remainPred)
                        .build();
                OptExpression newResult = new OptExpression(newTopJoin);
                newResult.getInputs().addAll(result.getInputs());
                result = newResult;
            } else {
                OptExpression filterExpr = new OptExpression(new LogicalFilterOperator(remainPred));
                filterExpr.getInputs().add(result);
                result = filterExpr;
            }
        }

        // Step 7: Re-expose the original root's output on top of the rewritten result, so
        // upper operators keep seeing the same ColumnRefs.
        if (outputProjection != null) {
            result.getOp().setProjection(new Projection(outputProjection));
        }

        // Carry the join's row limit onto the rewritten root. PushDownLimitJoinRule folded the
        // LOCAL-phase limit (= original limit + offset, offset-less) onto the matched join; re-exposing
        // it lets a fully merged scan push a row cap to the remote DB. The GLOBAL limit above stays
        // local as the authoritative trim (it applies the real offset+limit), so leaving it un-pushed
        // only risks the remote over-fetching `offset` extra rows. Set it on the rewritten root, not
        // the inner merged scan: on a partial merge the root is a rebuilt join and the limit must sit
        // on the join output, never on one of its inputs (which would under-fetch).
        if (input.getOp().getLimit() != Operator.DEFAULT_LIMIT) {
            result.getOp().setLimit(input.getOp().getLimit());
        }

        return Lists.newArrayList(result);
    }

    /**
     * Group the JDBC scan atoms by catalog name — the catalog identifies the JDBC connection
     * (URI + credentials + driver) that BE uses to talk to the external DB — split each catalog
     * bucket into join-connected components ({@link #splitConnectedComponents}), and keep the
     * components that are candidates for merging: at least 2 plain base-table scans. Splitting per
     * component is what keeps a cross-joined atom (or a disconnected sub-join) out of the merge so
     * it never becomes a remote Cartesian product.
     *
     * <p>Scans with an expression projection are excluded because the merged SQL cannot
     * express projection expressions; a pure column-pruning projection (every value a bare
     * ref of itself — typically hiding a predicate-only column from upstream) is fine, since
     * the merged SELECT prunes to the needed columns anyway.
     *
     * <p>Inline-table scans — a native_query pass-through ({@code table(jdbc.native_query(...))})
     * or a derived table produced by a previous pushdown — ARE merged: each is emitted as its
     * own parenthesized derived subquery {@code (<body>) sr_t{i}} (see
     * {@link JDBCPushDownSQLBuilder#buildTableExpression}), which isolates any inner structure
     * (ORDER BY / UNION / aggregates / LIMIT) the way SQL scopes a derived table, so joining it
     * is semantically safe. Termination still holds: every merge strictly reduces the atom count,
     * and a single merged scan cannot form a >= 2 atom group by itself.
     */
    private List<MergeGroup> collectMergeGroups(LinkedHashSet<OptExpression> atoms,
                                                List<ScalarOperator> predicates) {
        Map<String, MergeGroup> byCatalog = new LinkedHashMap<>();
        for (OptExpression atom : atoms) {
            if (atom.getOp() instanceof LogicalJDBCScanOperator scanOp) {
                JDBCTable table = (JDBCTable) scanOp.getTable();
                if (!Strings.isNullOrEmpty(table.getResourceName())) {
                    // Resource-based external tables (CREATE EXTERNAL TABLE ... ENGINE=jdbc) are
                    // deprecated and lack a catalog name; keep them out of join merging.
                    continue;
                }
                byCatalog.computeIfAbsent(table.getCatalogName(), k -> new MergeGroup())
                        .add(new AtomEntry(atom, scanOp, table));
            }
        }

        List<MergeGroup> groups = new ArrayList<>();
        for (MergeGroup bucket : byCatalog.values()) {
            for (MergeGroup component : splitConnectedComponents(bucket, predicates)) {
                boolean eligible = component.entries.size() >= 2 && component.entries.stream()
                        .allMatch(e -> JDBCPushDownRuleUtils.isColumnPruningOnly(e.scanOp.getProjection()));
                if (eligible) {
                    groups.add(component);
                }
            }
        }
        return groups;
    }

    /**
     * Partition a same-catalog bucket into connected components, where two atoms are connected when
     * an intra-bucket join predicate — one with >= 2 columns, all inside the bucket — touches both.
     * Join predicates are the only edges; an atom no such predicate reaches becomes a component of one.
     *
     * <p>Only a component of >= 2 atoms is a merge candidate (the caller filters on this). A lone
     * atom — standalone, or cross-joined to the rest with no connecting predicate — is its own
     * single-atom component and stays an individual local scan; likewise two components with no
     * predicate between them (e.g. {@code (a JOIN b) CROSS JOIN (c JOIN d)} in one catalog) are
     * returned separately and merge into their own pushdowns, joined locally. Either way a remote
     * Cartesian product is never pushed down — the cross join is evaluated locally, not by the
     * external DB. The rule is idempotent on the result: the local cross join of two merged scans
     * re-flattens to two predicate-free single-atom components, so nothing re-merges.
     */
    private List<MergeGroup> splitConnectedComponents(MergeGroup bucket, List<ScalarOperator> predicates) {
        int n = bucket.entries.size();
        int[] parent = new int[n];
        for (int i = 0; i < n; i++) {
            parent[i] = i;
        }
        for (ScalarOperator pred : predicates) {
            ColumnRefSet used = pred.getUsedColumns();
            if (used.cardinality() < 2 || !bucket.columns.containsAll(used)) {
                // A single-column predicate is a filter, never a join; one referencing columns
                // outside the bucket cannot connect two of its atoms.
                continue;
            }
            int anchor = -1;
            for (int i = 0; i < n; i++) {
                if (bucket.entries.get(i).columns.isIntersect(used)) {
                    if (anchor < 0) {
                        anchor = i;
                    } else {
                        union(parent, anchor, i);
                    }
                }
            }
        }
        // Group atoms by their component root, preserving first-seen (original atom) order.
        Map<Integer, MergeGroup> components = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            components.computeIfAbsent(find(parent, i), k -> new MergeGroup())
                    .add(bucket.entries.get(i));
        }
        return new ArrayList<>(components.values());
    }

    private static int find(int[] parent, int i) {
        while (parent[i] != i) {
            parent[i] = parent[parent[i]];   // path halving
            i = parent[i];
        }
        return i;
    }

    private static void union(int[] parent, int a, int b) {
        parent[find(parent, a)] = find(parent, b);
    }

    /**
     * Split the flattened join-level predicates between the merge groups and the rebuilt
     * local join, and decide which groups actually merge.
     *
     * <p>A predicate is owned by a group when the group covers all its columns (groups have
     * disjoint column sets, so the owner is unique); everything else — spans several groups
     * or references non-JDBC atoms — goes to {@code remainingPredicates}.
     *
     * <p>A group merges only when, all-or-nothing:
     * <ul>
     *   <li><b>every owned predicate is convertible</b> to the external DB's SQL dialect.
     *       Partial pushdown would allow the pathological case where a non-convertible JOIN
     *       predicate becomes a local filter above the merged scan, degrading the remote
     *       comma join into a remote Cartesian product with a potentially huge intermediate result;
     *   <li><b>at least one owned predicate joins two of its tables</b> — otherwise the
     *       pushed SQL would be a pure Cartesian product, losing the external
     *       optimizer/index benefit.
     * </ul>
     *
     * <p>Owned predicates of a merging group are all rendered into the pushed SQL's WHERE clause
     * (the remote comma join makes join and filter predicates positionally equivalent).
     * Owned predicates of a non-merging group fall back to {@code remainingPredicates} so the
     * local join rebuild re-applies them.
     */
    private void routePredicates(List<ScalarOperator> allPredicates, List<MergeGroup> groups,
                                 List<ScalarOperator> remainingPredicates) {
        for (ScalarOperator pred : allPredicates) {
            MergeGroup owner = groups.stream()
                    .filter(g -> g.columns.containsAll(pred.getUsedColumns()))
                    .findFirst().orElse(null);
            if (owner != null) {
                owner.ownedPredicates.add(pred);
            } else {
                remainingPredicates.add(pred);
            }
        }

        for (MergeGroup group : groups) {
            // Classify owned predicates once: cross-table -> INNER JOIN ON, single-table -> post-join
            // WHERE filter. For the inner joins this rule merges, ON-vs-WHERE placement is semantically
            // interchangeable (it is not, and must come from the operator, for outer joins -- which
            // MultiJoinNode never flattens).
            for (ScalarOperator pred : group.ownedPredicates) {
                if (group.isCrossTablePredicate(pred)) {
                    group.onPredicates.add(pred);
                } else {
                    group.filterPredicates.add(pred);
                }
            }
            // Convertibility is checked per group because the dialect may differ across groups.
            // CanPushDownPredicateVisitor mirrors the node coverage ScalarOperatorToJDBCSQLVisitor
            // renders, so predicate gating and SQL rendering stay in sync.
            boolean allPushable = group.ownedPredicates.stream()
                    .allMatch(p -> CanPushDownPredicateVisitor.canPushDown(p, group.dialect()));
            // A merge needs at least one cross-table (join) predicate, else it degenerates into a
            // remote Cartesian product.
            group.shouldMerge = allPushable && !group.onPredicates.isEmpty();
            if (!group.shouldMerge) {
                remainingPredicates.addAll(group.ownedPredicates);
            }
        }
    }

    /**
     * The ColumnRef→expression map the rewritten subtree must expose to upper operators,
     * or null when the original root exposed its raw child outputs unchanged.
     *
     * <p>Starts from the original root's projection (or an identity map over its raw output
     * when MultiJoinNode absorbed inner-join projections without one on the root). Every
     * exposed ref that no atom produces was defined by an expression MultiJoinNode absorbed
     * into {@code expressionMap} during flattening; it is re-expanded recursively down to
     * atom columns so it can be evaluated on top of the merged scans. (Same compensation as
     * {@code ReorderJoinRule.enumerate(...)}.)
     */
    private Map<ColumnRefOperator, ScalarOperator> computeOutputProjection(
            OptExpression input, LinkedHashSet<OptExpression> atoms,
            Map<ColumnRefOperator, ScalarOperator> expressionMap, OptimizerContext context) {
        Projection oldProjection = ((LogicalJoinOperator) input.getOp()).getProjection();
        if (oldProjection == null && expressionMap.isEmpty()) {
            return null;
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        ColumnRefSet originalOutputCols = new ColumnRefSet();
        if (oldProjection == null) {
            originalOutputCols.union(input.getOutputColumns());
            for (int id : originalOutputCols.getColumnIds()) {
                ColumnRefOperator col = context.getColumnRefFactory().getColumnRef(id);
                projectMap.put(col, col);
            }
        } else {
            originalOutputCols.union(oldProjection.getOutputColumns());
            projectMap.putAll(oldProjection.getColumnRefMap());
        }

        ColumnRefSet atomOutputCols = new ColumnRefSet();
        for (OptExpression atom : atoms) {
            atomOutputCols.union(atom.getRowOutputInfo().getOutputColumnRefSet());
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionMap, true);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : expressionMap.entrySet()) {
            ColumnRefOperator col = entry.getKey();
            if (originalOutputCols.contains(col) && !atomOutputCols.contains(col)) {
                projectMap.put(col, rewriter.rewrite(entry.getValue()));
            }
        }
        return projectMap;
    }

    /**
     * Build a single merged LogicalJDBCScanOperator for one merge group.
     *
     * <p>The merged SELECT exposes only {@code neededColumns} (what upper operators and the
     * rebuilt local join actually reference) — join keys and filter columns consumed inside
     * the pushdown SQL, and columns a scan's pruning projection hides, are not fetched back.
     * When nothing is needed (e.g. a bare COUNT(*) over the join), the smallest column is
     * kept so the SELECT list is never empty.
     */
    private OptExpression buildMergedScan(MergeGroup group, ColumnRefSet neededColumns) {
        List<AtomEntry> entries = group.entries;

        // Hand the group's scans to the builder, which owns alias assignment and qualified-name
        // rendering (every scan column participates, since ON/WHERE may reference pruned columns;
        // only the SELECT list below is pruned). allColumns is the union of every scan's columns
        // in a stable order, used to pick the output columns and build the merged schema.
        List<LogicalJDBCScanOperator> scans = new ArrayList<>();
        Map<ColumnRefOperator, Column> allColumns = new LinkedHashMap<>();
        for (AtomEntry entry : entries) {
            scans.add(entry.scanOp);
            allColumns.putAll(entry.scanOp.getColRefToColumnMetaMap());
        }

        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        for (ColumnRefOperator colRef : allColumns.keySet()) {
            if (neededColumns.contains(colRef)) {
                outputColumns.add(colRef);
            }
        }
        if (outputColumns.isEmpty()) {
            outputColumns.add(Utils.findSmallestColumnRef(new ArrayList<>(allColumns.keySet())));
        }

        Map<ColumnRefOperator, Column> mergedColRefToColumnMap = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> mergedColumnToColRefMap = new HashMap<>();
        for (ColumnRefOperator colRef : outputColumns) {
            // Copy Column under an alias name so the Column→ColumnRef reverse map doesn't
            // collide when multiple tables share a column name.
            Column aliasedCol = new Column(allColumns.get(colRef));
            String colAlias = JDBCPushDownSQLBuilder.outputColumnAlias(colRef.getId());
            aliasedCol.setName(colAlias);
            aliasedCol.setColumnId(ColumnId.create(colAlias));
            mergedColRefToColumnMap.put(colRef, aliasedCol);
            mergedColumnToColRefMap.put(aliasedCol, colRef);
        }

        // Render an explicit INNER JOIN chain; the ON conditions vs post-join WHERE filter were
        // pre-classified from ownedPredicates in routePredicates.
        String pushDownSQL = JDBCPushDownSQLBuilder.buildJoinQuery(
                scans, outputColumns, JoinOperator.INNER_JOIN, group.onPredicates, group.filterPredicates);

        // Synthesize a per-query JDBCTable that wraps the merged SELECT as a derived
        // table. Must not mutate the catalog-cached primaryTable.
        JDBCTable primaryTable = entries.get(0).table;
        JDBCTable mergedTable = new JDBCTable(primaryTable);
        mergedTable.setPushDownQuery(pushDownSQL);
        mergedTable.setNewFullSchema(new ArrayList<>(mergedColRefToColumnMap.values()));

        LogicalJDBCScanOperator mergedOp = new LogicalJDBCScanOperator.Builder()
                .setTable(mergedTable)
                .setColRefToColumnMetaMap(mergedColRefToColumnMap)
                .setColumnMetaToColRefMap(mergedColumnToColRefMap)
                .setLimit(-1)        // = Operator.DEFAULT_LIMIT; transform() re-exposes the join's limit on the rewritten root
                .setPredicate(null)  // predicates are in the pushdown SQL
                .setProjection(null) // the output projection is attached on the rewritten root
                .build();

        return new OptExpression(mergedOp);
    }

    /**
     * Append {@code rightAtom} to the join chain rooted at {@code result}, picking up any
     * remaining predicates that become eligible (all referenced columns now in scope, and
     * the predicate spans both left and right). Eligible predicates are removed from
     * {@code remainingPredicates} in place.
     */
    private OptExpression appendJoin(OptExpression result, OptExpression rightAtom,
                                     List<ScalarOperator> remainingPredicates) {
        ColumnRefSet leftCols = result.getRowOutputInfo().getOutputColumnRefSet();
        ColumnRefSet rightCols = rightAtom.getRowOutputInfo().getOutputColumnRefSet();
        ColumnRefSet scopeCols = new ColumnRefSet();
        scopeCols.union(leftCols);
        scopeCols.union(rightCols);

        List<ScalarOperator> onPredicates = new ArrayList<>();
        List<ScalarOperator> newRemaining = new ArrayList<>();
        for (ScalarOperator pred : remainingPredicates) {
            ColumnRefSet predCols = pred.getUsedColumns();
            if (scopeCols.containsAll(predCols)
                    && predCols.isIntersect(leftCols) && predCols.isIntersect(rightCols)) {
                onPredicates.add(pred);
            } else {
                newRemaining.add(pred);
            }
        }
        remainingPredicates.clear();
        remainingPredicates.addAll(newRemaining);

        LogicalJoinOperator joinOp;
        if (onPredicates.isEmpty()) {
            joinOp = new LogicalJoinOperator.Builder()
                    .setJoinType(com.starrocks.sql.ast.JoinOperator.CROSS_JOIN)
                    .setOnPredicate(null)
                    .build();
        } else {
            joinOp = new LogicalJoinOperator.Builder()
                    .setJoinType(com.starrocks.sql.ast.JoinOperator.INNER_JOIN)
                    .setOnPredicate(Utils.compoundAnd(onPredicates))
                    .build();
        }
        OptExpression joinExpr = new OptExpression(joinOp);
        joinExpr.getInputs().add(result);
        joinExpr.getInputs().add(rightAtom);
        return joinExpr;
    }

    /** All JDBC scan atoms of one catalog, plus the join-level predicates routed to them. */
    private static class MergeGroup {
        final List<AtomEntry> entries = new ArrayList<>();
        final ColumnRefSet columns = new ColumnRefSet();
        final List<ScalarOperator> ownedPredicates = new ArrayList<>();
        // ownedPredicates split by routePredicates for a merging group: a cross-table predicate
        // becomes an INNER JOIN ON condition, a single-table predicate a post-join WHERE filter.
        final List<ScalarOperator> onPredicates = new ArrayList<>();
        final List<ScalarOperator> filterPredicates = new ArrayList<>();
        boolean shouldMerge = false;

        void add(AtomEntry entry) {
            entries.add(entry);
            columns.union(entry.columns);
        }

        JDBCTable.ProtocolType dialect() {
            return entries.get(0).table.getProtocolType();
        }

        /** True if {@code pred} touches columns of 2 or more scans in this group. */
        boolean isCrossTablePredicate(ScalarOperator pred) {
            ColumnRefSet usedCols = pred.getUsedColumns();
            int touched = 0;
            for (AtomEntry entry : entries) {
                if (entry.columns.isIntersect(usedCols) && ++touched >= 2) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class AtomEntry {
        final OptExpression atom;
        final LogicalJDBCScanOperator scanOp;
        final JDBCTable table;
        final ColumnRefSet columns;

        AtomEntry(OptExpression atom, LogicalJDBCScanOperator scanOp, JDBCTable table) {
            this.atom = atom;
            this.scanOp = scanOp;
            this.table = table;
            this.columns = new ColumnRefSet(scanOp.getOutputColumns());
        }
    }
}
