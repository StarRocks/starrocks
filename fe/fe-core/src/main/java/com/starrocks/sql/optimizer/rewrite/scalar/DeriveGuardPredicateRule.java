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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derive guard predicates from OR expressions where every branch contains
 * a comparison on a common pivot column.  The guard is a single-slot predicate
 * that enters conjunct_ctxs_by_slot in the BE, enabling early evaluation and
 * expression short-circuit on the pivot column before the full OR is evaluated.
 * <p>
 * This is a one-pass transformation (like ScalarRangePredicateExtractor),
 * NOT a ScalarOperatorRewriteRule.  It is called directly from
 * PushDownPredicateScanRule after the scalar rewriter completes.
 * <p>
 * Example:
 * <pre>
 *   (l_shipdate BETWEEN '1998-01-01' AND '1998-01-01' AND l_returnflag = 'R')
 *   OR (l_shipdate BETWEEN '1998-06-15' AND '1998-06-15' AND l_returnflag = 'A')
 *
 *   becomes:
 *
 *   (l_shipdate BETWEEN '1998-01-01' AND '1998-01-01'
 *    OR l_shipdate BETWEEN '1998-06-15' AND '1998-06-15')  -- guard (single-slot)
 *   AND ((l_shipdate BETWEEN '1998-01-01' AND '1998-01-01' AND l_returnflag = 'R')
 *     OR (l_shipdate BETWEEN '1998-06-15' AND '1998-06-15' AND l_returnflag = 'A'))
 * </pre>
 * <p>
 * V1: processes top-level OR conjuncts (children of the root AND). The guard
 * is purely additive (AND with the original), so correctness is preserved.
 */
public class DeriveGuardPredicateRule {

    /**
     * Apply guard derivation to the predicate tree.  Called once per query,
     * outside any fixpoint rewrite loop.
     */
    public static ScalarOperator apply(ScalarOperator predicate) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        boolean changed = false;
        for (int i = 0; i < conjuncts.size(); i++) {
            ScalarOperator conjunct = conjuncts.get(i);
            if (conjunct instanceof CompoundPredicateOperator
                    && ((CompoundPredicateOperator) conjunct).isOr()) {
                List<ScalarOperator> disjuncts = Utils.extractDisjunctive(conjunct);
                // Skip when the OR involves too few columns — with <= 2 columns both
                // are already ACTIVE, so the guard's short-circuit benefit is minimal.
                // Additionally, guard predicates added during MV refresh break MV
                // matching because the stored plan's predicate structure changes.
                if (countDistinctColumns(disjuncts) < MIN_DISTINCT_COLUMNS_FOR_GUARD) {
                    continue;
                }
                ScalarOperator wrapped = tryDeriveGuard((CompoundPredicateOperator) conjunct);
                if (wrapped != conjunct) {
                    conjuncts.set(i, wrapped);
                    changed = true;
                }
            }
        }
        return changed ? Utils.compoundAnd(conjuncts) : predicate;
    }

    /**
     * Try to derive a guard predicate for the given OR expression.
     * Returns the wrapped expression AND(guard, orExpr) if guards can be derived,
     * or the original orExpr unchanged.
     */
    @VisibleForTesting
    static ScalarOperator tryDeriveGuard(CompoundPredicateOperator orExpr) {
        List<ScalarOperator> disjuncts = Utils.extractDisjunctive(orExpr);
        if (disjuncts.size() <= 1) {
            return orExpr;
        }

        // For each disjunct, extract its AND conjuncts
        List<List<ScalarOperator>> disjunctConjuncts = new ArrayList<>();
        for (ScalarOperator disjunct : disjuncts) {
            disjunctConjuncts.add(Utils.extractConjuncts(disjunct));
        }

        // Step 1: For each disjunct, collect range column predicates by column id.
        // Also track the total number of real (non-AND) leaves in each disjunct tree.
        List<Map<Integer, List<ScalarOperator>>> perDisjunctColPreds = new ArrayList<>();
        List<Integer> disjunctLeafCounts = new ArrayList<>();
        for (int di = 0; di < disjuncts.size(); di++) {
            Map<Integer, List<ScalarOperator>> colPreds = new LinkedHashMap<>();
            List<ScalarOperator> conjuncts = disjunctConjuncts.get(di);
            for (ScalarOperator conjunct : conjuncts) {
                if (isGuardCandidatePredicate(conjunct)) {
                    ColumnRefSet usedCols = conjunct.getUsedColumns();
                    if (usedCols.cardinality() == 1) {
                        int colId = usedCols.getFirstId();
                        colPreds.computeIfAbsent(colId, k -> new ArrayList<>()).add(conjunct);
                    }
                }
            }
            perDisjunctColPreds.add(colPreds);
            // Count all leaves in this disjunct's AND tree, not just those from extractConjuncts
            disjunctLeafCounts.add(countFlatConjuncts(disjuncts.get(di)));
        }

        // Step 2: Find columns that appear in ALL disjuncts (intersection)
        Set<Integer> commonColIds = new LinkedHashSet<>(perDisjunctColPreds.get(0).keySet());
        if (commonColIds.isEmpty()) {
            return orExpr;
        }
        for (int i = 1; i < perDisjunctColPreds.size(); i++) {
            commonColIds.retainAll(perDisjunctColPreds.get(i).keySet());
        }

        if (commonColIds.isEmpty()) {
            return orExpr;
        }

        // Step 3: Build guard predicate for each common column.
        List<ScalarOperator> guards = new ArrayList<>();
        for (int colId : commonColIds) {
            if (allDisjunctsAreOnlyColumnPreds(disjunctLeafCounts, perDisjunctColPreds, colId)) {
                continue;
            }

            // Skip if the guard for this column is purely EQ/IN predicates.
            // The ScalarRangePredicateExtractor already handles EQ-only columns
            // via IN lists, so a pure-EQ guard would be redundant and would
            // unnecessarily change the plan text (breaking MV matching, test
            // assertions, etc.).
            if (isOnlyEqualityGuard(perDisjunctColPreds, colId)) {
                continue;
            }

            List<ScalarOperator> perDisjunctGuardParts = new ArrayList<>();
            for (Map<Integer, List<ScalarOperator>> disjunctPreds : perDisjunctColPreds) {
                List<ScalarOperator> colPreds = disjunctPreds.get(colId);
                perDisjunctGuardParts.add(Utils.compoundAnd(colPreds));
            }
            guards.add(Utils.compoundOr(perDisjunctGuardParts));
        }

        if (guards.isEmpty()) {
            return orExpr;
        }

        // Step 4: Wrap original OR with guard: AND(guard1, ..., guardN, original_OR)
        List<ScalarOperator> andArgs = new ArrayList<>(guards);
        andArgs.add(orExpr);
        return Utils.compoundAnd(andArgs);
    }

    /**
     * Check whether every guard predicate for the given column across all disjuncts
     * is an equality (EQ or IN) — no range predicates (GE, LE, GT, LT).
     * <p>
     * An EQ-only guard like {@code col = 1 OR col = 2 OR col = 3} is redundant with
     * what {@code ScalarRangePredicateExtractor} already produces (an IN list).
     * Skipping it avoids unnecessary plan-text changes that can break MV matching
     * and test assertions.
     */
    private static boolean isOnlyEqualityGuard(
            List<Map<Integer, List<ScalarOperator>>> perDisjunctColPreds,
            int colId) {
        for (Map<Integer, List<ScalarOperator>> disjunctPreds : perDisjunctColPreds) {
            List<ScalarOperator> colPreds = disjunctPreds.get(colId);
            if (colPreds == null) {
                continue;
            }
            for (ScalarOperator pred : colPreds) {
                if (isRangePredicate(pred)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Check whether a scalar operator is a range predicate (not EQ or IN).
     */
    private static boolean isRangePredicate(ScalarOperator op) {
        if (op instanceof BinaryPredicateOperator) {
            BinaryType type = ((BinaryPredicateOperator) op).getBinaryType();
            return type == BinaryType.GE || type == BinaryType.LE
                    || type == BinaryType.GT || type == BinaryType.LT;
        }
        // IN predicates are treated as equality (not range)
        return false;
    }

    /**
     * Check if all leaves across all disjuncts for a given column consist only of
     * column predicates for that column. If so, the guard would be redundant.
     */
    private static boolean allDisjunctsAreOnlyColumnPreds(
            List<Integer> disjunctLeafCounts,
            List<Map<Integer, List<ScalarOperator>>> perDisjunctColPreds,
            int colId) {
        int totalLeaves = 0;
        int totalColPreds = 0;
        for (int i = 0; i < disjunctLeafCounts.size(); i++) {
            totalLeaves += disjunctLeafCounts.get(i);
            List<ScalarOperator> colPreds = perDisjunctColPreds.get(i).get(colId);
            totalColPreds += (colPreds != null ? colPreds.size() : 0);
        }
        return totalLeaves == totalColPreds;
    }

    // Minimum number of distinct columns across OR branches required to
    // trigger guard generation.  With <= 2 columns both are already ACTIVE,
    // so the guard's short-circuit benefit is minimal.  Additionally, guards
    // added during MV refresh break MV matching because the stored plan's
    // predicate structure changes (MV matching is not guard-aware yet).
    @VisibleForTesting
    static final int MIN_DISTINCT_COLUMNS_FOR_GUARD = 3;

    /**
     * Count the total number of distinct columns referenced by all disjuncts.
     */
    private static int countDistinctColumns(List<ScalarOperator> disjuncts) {
        ColumnRefSet allCols = new ColumnRefSet();
        for (ScalarOperator disjunct : disjuncts) {
            allCols.union(disjunct.getUsedColumns());
        }
        return allCols.cardinality();
    }

    /**
     * Count the total number of leaf (non-compound) children in the AND tree
     * rooted at the given scalar operator, without being limited to
     * getChild(0)/getChild(1) like extractConjuncts.
     */
    private static int countFlatConjuncts(ScalarOperator node) {
        if (!(node instanceof CompoundPredicateOperator)) {
            return 1;
        }
        CompoundPredicateOperator cpo = (CompoundPredicateOperator) node;
        if (!cpo.isAnd()) {
            return 1;
        }
        int count = 0;
        for (ScalarOperator child : cpo.getChildren()) {
            count += countFlatConjuncts(child);
        }
        return count;
    }

    /**
     * Check if this predicate should contribute to a guard.
     * <p>
     * Guard derivation strategy:
     * <ul>
     *   <li><b>Range predicates (GE, LE, GT, LT)</b>: always collected. These are the primary
     *       target — they form contiguous intervals that are inherently selective.</li>
     *   <li><b>IN predicates</b>: always collected. IN lists on date/numeric columns are
     *       selective and commonly used as pivot columns in OR branches.</li>
     *   <li><b>EQ on DATE / DATETIME / TIMESTAMP / numeric types</b>: collected.
     *       EQ on a high-cardinality date or numeric column (e.g. {@code l_shipdate = '1998-12-01'})
     *       is as selective as a range predicate and should contribute to the guard.
     *       <p>
     *       <b>EQ on VARCHAR / CHAR / BOOLEAN is intentionally excluded.</b>
     *       These columns are typically low-cardinality payload columns (e.g.
     *       {@code l_returnflag = 'R'}). Adding a guard for them would:
     *       <ol>
     *         <li>Produce a non-selective guard (e.g. {@code returnflag = 'R' OR returnflag = 'A'})</li>
     *         <li>Put the column into {@code conjunct_ctxs_by_slot} → ACTIVE →
     *             decoded for all rows instead of LAZY (decoded only for survivor rows)</li>
     *         <li>Negate the benefit of the real guard on the pivot column</li>
     *       </ol>
     *       This is the key difference from Trino's TupleDomain: Trino's TupleDomain is
     *       only used for page-level pruning and does NOT force column decoding order.
     *       Our guard flows through {@code conjunct_ctxs_by_slot} which DOES control the
     *       ACTIVE vs LAZY column split, so we must be more selective.</li>
     * </ul>
     */
    private static boolean isGuardCandidatePredicate(ScalarOperator op) {
        // Nondeterministic predicates (e.g. rand(), uuid()) must not be
        // duplicated — the guard and original expression would evaluate
        // different values, potentially filtering rows incorrectly.
        if (Utils.hasNonDeterministicFunc(op)) {
            return false;
        }
        if (op instanceof BinaryPredicateOperator) {
            return isGuardCandidateBinaryPredicate((BinaryPredicateOperator) op);
        }
        if (op instanceof InPredicateOperator) {
            return op.getUsedColumns().cardinality() == 1;
        }
        return false;
    }

    /**
     * Check if a BinaryPredicateOperator is a guard candidate.
     * Range predicates (GE, LE, GT, LT) are always candidates.
     * EQ is only a candidate on high-cardinality types (date/datetime/numeric).
     */
    private static boolean isGuardCandidateBinaryPredicate(BinaryPredicateOperator bop) {
        ColumnRefSet usedCols = bop.getUsedColumns();
        if (usedCols.cardinality() != 1) {
            return false;
        }
        BinaryType type = bop.getBinaryType();

        // Range predicates: always candidates
        if (type == BinaryType.GE || type == BinaryType.LE
                || type == BinaryType.GT || type == BinaryType.LT) {
            return true;
        }

        // EQ: only for high-cardinality column types
        // Skip VARCHAR/CHAR/BOOLEAN to avoid creating non-selective guards
        // that would make low-cardinality payload columns ACTIVE unnecessarily
        if (type == BinaryType.EQ) {
            // After NormalizePredicateRule, the column reference is child(0)
            Type colType = bop.getChild(0).getType();
            return colType.isNumericType() || colType.isDateType() || colType.isDatetime();
        }

        return false;
    }
}
