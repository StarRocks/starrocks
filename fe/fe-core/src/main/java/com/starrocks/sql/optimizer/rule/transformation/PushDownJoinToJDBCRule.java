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
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.planner.JDBCScanNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
 * <p>Each output column is aliased by its ColumnRefOperator ID (c{id}) so the
 * outer wrapping query built by PlanFragmentBuilder can reference them by name
 * without relying on positional ordering.
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
 *   JDBC_SCAN("SELECT t0.`id` AS c1, t1.`id` AS c2
 *              FROM `a` t0 INNER JOIN `b` t1 ON t0.`id` = t1.`id`")
 * </pre>
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
        // Step 1: Flatten the join tree into atoms and predicates
        MultiJoinNode multiJoin = MultiJoinNode.toMultiJoinNode(input);
        LinkedHashSet<OptExpression> atoms = multiJoin.getAtoms();
        List<ScalarOperator> allPredicates = multiJoin.getPredicates();

        // Need at least 2 atoms to merge
        if (atoms.size() < 2) {
            return Lists.newArrayList();
        }

        // Step 2: Classify atoms into JDBC groups by catalog name. The catalog identifies
        // the JDBC connection (URI + credentials + driver) that BE uses to talk to the
        // external DB
        Map<String, List<AtomEntry>> jdbcGroups = new LinkedHashMap<>();

        for (OptExpression atom : atoms) {
            if (atom.getOp() instanceof LogicalJDBCScanOperator scanOp) {
                JDBCTable jdbcTable = (JDBCTable) scanOp.getTable();
                jdbcGroups.computeIfAbsent(jdbcTable.getCatalogName(), k -> new ArrayList<>())
                        .add(new AtomEntry(atom, scanOp, jdbcTable));
            }
        }

        // Step 3: Identify all groups eligible for merging. A group is eligible when:
        //           - it contains >= 2 JDBC atoms, AND
        //           - no atom has a projection (scans with projections are handled by the
        //             standalone projection-pushdown rule), AND
        //           - no atom is a query-table function (table(jdbc.native_query(...))).
        //             Query-table's inner SQL is user-supplied and may contain arbitrary
        //             structure (ORDER BY / UNION / aggregates), so inlining it as an atom
        //             into a new merged SQL has unsafe semantics; bypass it entirely.
        //         Other groups contribute their atoms individually to the rebuilt join tree.
        List<List<AtomEntry>> mergeableGroups = new ArrayList<>();
        for (List<AtomEntry> group : jdbcGroups.values()) {
            boolean eligible = group.size() >= 2
                    && group.stream().noneMatch(e -> e.scanOp.getProjection() != null)
                    && group.stream().noneMatch(e -> e.table.isQueryTable());
            if (eligible) {
                mergeableGroups.add(group);
            }
        }

        if (mergeableGroups.isEmpty()) {
            return Lists.newArrayList();
        }

        // Step 4: Compute per-group column ref sets — used for predicate ownership and
        //         join/filter classification.
        List<ColumnRefSet> groupColumnRefs = new ArrayList<>();
        for (List<AtomEntry> group : mergeableGroups) {
            ColumnRefSet refs = new ColumnRefSet();
            for (AtomEntry entry : group) {
                refs.union(new ColumnRefSet(entry.scanOp.getOutputColumns()));
            }
            groupColumnRefs.add(refs);
        }

        // Step 5: Partition each predicate. See partitionPredicates() for details.
        List<GroupPredicates> predicatesPerGroup = mergeableGroups.stream()
                .map(g -> new GroupPredicates())
                .collect(java.util.stream.Collectors.toList());
        List<ScalarOperator> remainingPredicates = new ArrayList<>();
        Set<Integer> disqualifiedGroups = new HashSet<>();
        partitionPredicates(allPredicates, mergeableGroups, groupColumnRefs,
                predicatesPerGroup, remainingPredicates, disqualifiedGroups);

        // Step 6: Merge eligible groups. Disqualified groups (computed in partitionPredicates)
        // fall back to local JOIN.
        List<OptExpression> mergedExprs = new ArrayList<>();
        for (int i = 0; i < mergeableGroups.size(); i++) {
            List<AtomEntry> group = mergeableGroups.get(i);
            if (disqualifiedGroups.contains(i)) {
                continue;
            }

            List<ScalarOperator> joinPreds = predicatesPerGroup.get(i).joinPredicates;
            List<ScalarOperator> filterPreds = predicatesPerGroup.get(i).filterPredicates;
            mergedExprs.add(buildMergedScan(group, joinPreds, filterPreds));
        }

        // Bail out if no group ended up actually merging
        if (mergedExprs.isEmpty()) {
            return Lists.newArrayList();
        }

        // Step 7: Rebuild the join tree in the original atom order, substituting merged
        //         scans for their constituent atoms. This preserves the join order that
        //         the earlier RBO join-reorder pass established for non-JDBC atoms and
        //         unmerged JDBC atoms.
        Map<OptExpression, OptExpression> atomSubstitution = new HashMap<>();
        Set<OptExpression> droppedAtoms = new HashSet<>();
        int mergedExprIdx = 0;
        for (int i = 0; i < mergeableGroups.size(); i++) {
            if (disqualifiedGroups.contains(i)) {
                continue;
            }
            List<AtomEntry> group = mergeableGroups.get(i);
            OptExpression mergedScan = mergedExprs.get(mergedExprIdx++);
            atomSubstitution.put(group.get(0).atom, mergedScan);
            for (int j = 1; j < group.size(); j++) {
                droppedAtoms.add(group.get(j).atom);
            }
        }

        List<OptExpression> toJoin = new ArrayList<>();
        for (OptExpression atom : atoms) {
            if (droppedAtoms.contains(atom)) {
                continue;
            }
            OptExpression sub = atomSubstitution.get(atom);
            toJoin.add(sub != null ? sub : atom);
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

        // Step 8: Compensate the original join subtree's output projection on top of the
        //         rewritten result. MultiJoinNode absorbs the root join's projection (and any
        //         safe inner-join projection) into multiJoin.getExpressionMap() during
        //         flattening, so those ColumnRefs would otherwise disappear from the output
        //         and leave upper operators with dangling references.
        attachOutputProjection(result, input, multiJoin.getExpressionMap(), context);

        return Lists.newArrayList(result);
    }

    /**
     * Re-expose every ColumnRef the original join root produced on top of the rewritten result.
     *
     * <p>Cases:
     * <ul>
     *   <li>Original root has a projection: use it as the base projectMap. Any projection key
     *       whose definition was flattened into {@code expressionMap} (and thus no longer
     *       appears in the new output) is rebuilt by recursively expanding through
     *       {@code expressionMap}.</li>
     *   <li>Original root has no projection but {@code expressionMap} is non-empty (an inner
     *       join with a safe projection contributed entries): synthesize an identity projection
     *       over the original input's output columns, then compensate expressionMap keys the
     *       same way.</li>
     *   <li>Both empty: nothing to do — the raw scan outputs already match the original.</li>
     * </ul>
     *
     * <p>Modelled after {@code ReorderJoinRule.enumerate(...)}'s projection compensation.
     */
    private void attachOutputProjection(OptExpression result,
                                        OptExpression originalInput,
                                        Map<ColumnRefOperator, ScalarOperator> expressionMap,
                                        OptimizerContext context) {
        LogicalJoinOperator oldRoot = (LogicalJoinOperator) originalInput.getOp();
        Projection oldProjection = oldRoot.getProjection();

        if (oldProjection == null && expressionMap.isEmpty()) {
            return;
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        ColumnRefSet originalOutputCols = new ColumnRefSet();
        if (oldProjection == null) {
            originalOutputCols.union(originalInput.getOutputColumns());
            for (int id : originalOutputCols.getColumnIds()) {
                ColumnRefOperator col = context.getColumnRefFactory().getColumnRef(id);
                projectMap.put(col, col);
            }
        } else {
            originalOutputCols.union(oldProjection.getOutputColumns());
            projectMap.putAll(oldProjection.getColumnRefMap());
        }

        ColumnRefSet newOutputCols = result.getRowOutputInfo().getOutputColumnRefSet();
        ColumnRefSet expressionKeys = new ColumnRefSet(new ArrayList<>(expressionMap.keySet()));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionMap, true);

        for (int id : originalOutputCols.getColumnIds()) {
            if (!newOutputCols.contains(id) && expressionKeys.contains(id)) {
                ColumnRefOperator col = context.getColumnRefFactory().getColumnRef(id);
                projectMap.put(col, rewriter.rewrite(expressionMap.get(col)));
            }
        }

        result.getOp().setProjection(new Projection(projectMap));
    }

    /**
     * Classify each predicate into one of:
     *   - The join/filter bucket of a single mergeable group it belongs to (all columns
     *     within that group AND fully pushable to the external DB).
     *   - The "remaining" bucket (spans multiple groups, references unmerged atoms,
     *     or belongs to a disqualified group).
     * Within a group, a predicate is a "join" predicate if it touches 2+ tables in the
     * group, otherwise it's a "filter" predicate.
     *
     * <p>A group is disqualified in either of two cases:
     * <ul>
     *   <li><b>Non-pushable predicate:</b> a predicate owned by the group is not fully
     *       convertible to the external DB's SQL dialect. All-or-nothing pushdown avoids
     *       the pathological case where a non-pushable JOIN predicate becomes a local
     *       filter above the merged scan, degrading INNER JOIN into remote CROSS JOIN +
     *       local filter with a potentially huge intermediate result.
     *   <li><b>No cross-table join predicate:</b> the group has no predicate that joins
     *       two of its tables. Pushing would emit a remote CROSS JOIN without any ON
     *       clause, losing the external optimizer/index benefit.
     * </ul>
     *
     * <p>{@code predicatesPerGroup}, {@code remainingPredicates}, and
     * {@code disqualifiedGroups} are populated in place.
     */
    private void partitionPredicates(List<ScalarOperator> allPredicates,
                                     List<List<AtomEntry>> mergeableGroups,
                                     List<ColumnRefSet> groupColumnRefs,
                                     List<GroupPredicates> predicatesPerGroup,
                                     List<ScalarOperator> remainingPredicates,
                                     Set<Integer> disqualifiedGroups) {
        for (ScalarOperator pred : allPredicates) {
            ColumnRefSet usedCols = pred.getUsedColumns();
            int owningGroup = findOwningGroup(usedCols, groupColumnRefs);
            if (owningGroup == -1) {
                remainingPredicates.add(pred);
                continue;
            }

            // If the owning group is already disqualified by a prior non-pushable predicate,
            // skip the pushability check — this pred will go to remainingPredicates regardless.
            if (disqualifiedGroups.contains(owningGroup)) {
                remainingPredicates.add(pred);
                continue;
            }

            // Pushability is checked per-group because dialect (isMySQL) may differ.
            // Reuse the same node coverage that JDBCJoinPushDownSQLBuilder's ToSQLVisitor
            // relies on so predicate gating and SQL rendering stay in sync.
            boolean isMySQL = mergeableGroups.get(owningGroup).get(0).table.isMySQLCompatible();
            if (!JDBCJoinPushDownSQLBuilder.canPushExpression(pred, isMySQL)) {
                disqualifiedGroups.add(owningGroup);
                remainingPredicates.add(pred);
                continue;
            }

            GroupPredicates bucket = predicatesPerGroup.get(owningGroup);
            if (involvesMultipleTables(usedCols, mergeableGroups.get(owningGroup))) {
                bucket.joinPredicates.add(pred);
            } else {
                bucket.filterPredicates.add(pred);
            }
        }

        for (int i = 0; i < predicatesPerGroup.size(); i++) {
            if (!disqualifiedGroups.contains(i)
                    && predicatesPerGroup.get(i).joinPredicates.isEmpty()) {
                disqualifiedGroups.add(i);
            }
        }

        // For disqualified groups, move their already-bucketed predicates back to
        // remainingPredicates so the local JOIN rebuilder can re-apply them.
        for (int groupIdx : disqualifiedGroups) {
            GroupPredicates bucket = predicatesPerGroup.get(groupIdx);
            remainingPredicates.addAll(bucket.joinPredicates);
            remainingPredicates.addAll(bucket.filterPredicates);
            bucket.joinPredicates.clear();
            bucket.filterPredicates.clear();
        }
    }

    /** Return the index of the unique group containing all of {@code usedCols}, or -1. */
    private int findOwningGroup(ColumnRefSet usedCols, List<ColumnRefSet> groupColumnRefs) {
        for (int i = 0; i < groupColumnRefs.size(); i++) {
            if (groupColumnRefs.get(i).containsAll(usedCols)) {
                return i;
            }
        }
        return -1;
    }

    /** True if {@code usedCols} intersects 2 or more atoms in the group. */
    private boolean involvesMultipleTables(ColumnRefSet usedCols, List<AtomEntry> group) {
        int count = 0;
        for (AtomEntry entry : group) {
            if (usedCols.isIntersect(new ColumnRefSet(entry.scanOp.getOutputColumns()))) {
                count++;
                if (count >= 2) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Build a single merged LogicalJDBCScanOperator for one mergeable group.
     *
     * <p>All atoms in the group are guaranteed to have no projection (filtered in Step 3), so
     * the merged scan's outputs are exactly the union of raw scan columns. Pushdown of
     * projection expressions is handled by a separate standalone rule.
     */
    private OptExpression buildMergedScan(List<AtomEntry> group,
                                          List<ScalarOperator> joinPredicates,
                                          List<ScalarOperator> filterPredicates) {
        String jdbcUri = group.get(0).table.getJdbcUri();
        String identifierQuote = JDBCScanNode.getIdentifierSymbol(jdbcUri);

        List<JDBCJoinPushDownSQLBuilder.TableEntry> tableEntries = new ArrayList<>();
        Map<ColumnRefOperator, String> colRefToQualifiedName = new HashMap<>();
        Map<ColumnRefOperator, Column> mergedColRefToColumnMap = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> mergedColumnToColRefMap = new HashMap<>();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();

        for (int i = 0; i < group.size(); i++) {
            AtomEntry entry = group.get(i);
            String alias = "t" + i;
            Map<ColumnRefOperator, Column> colMap = entry.scanOp.getColRefToColumnMetaMap();
            tableEntries.add(new JDBCJoinPushDownSQLBuilder.TableEntry(entry.table, alias, entry.scanOp));

            for (Map.Entry<ColumnRefOperator, Column> colEntry : colMap.entrySet()) {
                ColumnRefOperator colRef = colEntry.getKey();
                Column column = colEntry.getValue();
                colRefToQualifiedName.put(colRef,
                        alias + "." + identifierQuote + column.getName() + identifierQuote);

                // Copy Column under an alias name so the Column→ColumnRef reverse map doesn't
                // collide when multiple tables share a column name.
                Column aliasedCol = new Column(column);
                String colAlias = JDBCJoinPushDownSQLBuilder.outputColumnAlias(colRef.getId());
                aliasedCol.setName(colAlias);
                aliasedCol.setColumnId(ColumnId.create(colAlias));
                mergedColRefToColumnMap.put(colRef, aliasedCol);
                mergedColumnToColRefMap.put(aliasedCol, colRef);
                outputColumns.add(colRef);
            }
        }

        JDBCJoinPushDownSQLBuilder sqlBuilder = new JDBCJoinPushDownSQLBuilder(
                identifierQuote, tableEntries, colRefToQualifiedName);
        String pushDownSQL = sqlBuilder.build(outputColumns, joinPredicates, filterPredicates);

        // Synthesize a per-query JDBCTable that wraps the merged SELECT as a derived
        // table. Must not mutate the catalog-cached primaryTable.
        JDBCTable primaryTable = group.get(0).table;
        JDBCTable mergedTable = new JDBCTable(primaryTable);
        mergedTable.setPushDownQuery(pushDownSQL);

        LogicalJDBCScanOperator mergedOp = new LogicalJDBCScanOperator(
                mergedTable,
                mergedColRefToColumnMap,
                mergedColumnToColRefMap,
                -1,    // no limit on the merged scan
                null,  // predicates are in the pushdown SQL
                null); // no local projection — scans with projections were excluded in Step 3

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

    /** Per-group predicate buckets. Bundled to avoid parallel-list bookkeeping. */
    private static class GroupPredicates {
        final List<ScalarOperator> joinPredicates = new ArrayList<>();
        final List<ScalarOperator> filterPredicates = new ArrayList<>();
    }

    private static class AtomEntry {
        final OptExpression atom;
        final LogicalJDBCScanOperator scanOp;
        final JDBCTable table;

        AtomEntry(OptExpression atom, LogicalJDBCScanOperator scanOp, JDBCTable table) {
            this.atom = atom;
            this.scanOp = scanOp;
            this.table = table;
        }
    }
}
