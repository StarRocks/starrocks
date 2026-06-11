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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derive guard predicates from OR expressions where every branch contains
 * a comparison on a common pivot column. The guard is a single-slot predicate
 * that enters conjunct_ctxs_by_slot in the BE, making the pivot column ACTIVE
 * while branch-local columns remain LAZY.
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
 * This is V1: only handles top-level OR conjuncts (children of the root AND).
 * The guard is purely additive (AND with the original), so correctness is
 * preserved even if guard logic has gaps.
 */
public class DeriveGuardPredicateRule extends BaseScalarOperatorRewriteRule {

    @Override
    public boolean isOnlyOnce() {
        return true;
    }

    @Override
    public ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context) {
        // Flatten the root AND to find all top-level OR conjuncts.
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(root);
        boolean changed = false;
        for (int i = 0; i < conjuncts.size(); i++) {
            ScalarOperator conjunct = conjuncts.get(i);
            if (conjunct instanceof CompoundPredicateOperator
                    && ((CompoundPredicateOperator) conjunct).isOr()) {
                ScalarOperator wrapped = tryDeriveGuard((CompoundPredicateOperator) conjunct);
                if (wrapped != conjunct) {
                    conjuncts.set(i, wrapped);
                    context.change();
                    changed = true;
                }
            }
        }
        return changed ? Utils.compoundAnd(conjuncts) : root;
    }

    /**
     * Try to derive a guard predicate for the given OR expression.
     * Returns the wrapped expression AND(guard, orExpr) if guards can be derived,
     * or the original orExpr unchanged.
     */
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
            Map<Integer, List<ScalarOperator>> colPreds = new HashMap<>();
            List<ScalarOperator> conjuncts = disjunctConjuncts.get(di);
            for (ScalarOperator conjunct : conjuncts) {
                if (isRangeColumnPredicate(conjunct)) {
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
        Set<Integer> commonColIds = new HashSet<>(perDisjunctColPreds.get(0).keySet());
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
     * Check if this is a single-column predicate that provides a range constraint
     * (GE, LE, GT, LT, IN). Equality-only predicates are excluded to avoid creating
     * non-selective guards for low-cardinality payload columns.
     */
    private static boolean isRangeColumnPredicate(ScalarOperator op) {
        if (op instanceof BinaryPredicateOperator) {
            if (op.getUsedColumns().cardinality() != 1) {
                return false;
            }
            BinaryPredicateOperator bop = (BinaryPredicateOperator) op;
            BinaryType type = bop.getBinaryType();
            return type == BinaryType.GE || type == BinaryType.LE
                    || type == BinaryType.GT || type == BinaryType.LT;
        }
        if (op instanceof InPredicateOperator) {
            return op.getUsedColumns().cardinality() == 1;
        }
        return false;
    }
}
