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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives multi-column predicates for inner/cross joins, using column equality mappings.
 * (Not used for ASOF joins; see {@link JoinPredicatePushdown#equivalenceDerive}.)
 */
public final class JoinMultiColumnPredicateDeriver {

    private static final int MAX_DERIVED_MULTI_COLUMN_PREDICATES = 16;

    private JoinMultiColumnPredicateDeriver() {
    }

    /**
     * Adds derived predicates into {@code allPredicate}.
     */
    public static void derive(OptExpression joinOptExpression,
                              List<ScalarOperator> inputPredicates,
                              Set<ScalarOperator> allPredicate) {
        ColumnRefSet leftColumns = joinOptExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOptExpression.inputAt(1).getOutputColumns();
        Map<ColumnRefOperator, ColumnRefOperator> leftToRight = new HashMap<>();
        Map<ColumnRefOperator, ColumnRefOperator> rightToLeft = new HashMap<>();
        buildJoinColumnMappings(joinOptExpression, leftToRight, rightToLeft);

        if (leftToRight.isEmpty() && rightToLeft.isEmpty()) {
            return;
        }

        int derivedCount = 0;
        for (ScalarOperator input : inputPredicates) {
            if (derivedCount >= MAX_DERIVED_MULTI_COLUMN_PREDICATES) {
                break;
            }
            if (!Utils.canPushDownPredicate(input) || Utils.countColumnRef(input) <= 1) {
                continue;
            }

            ColumnRefSet used = input.getUsedColumns();
            // Correlated subquery / EXISTS rewrites often produce (join-side column) = (non-trivial expr) with
            // columns from both join inputs. Remapping only the join-key column through equality mapping yields
            // wrong semantics (e.g. v4 = CAST(<exists state> AS BIGINT)). Same-side col = expr can still be
            // mapped safely (e.g. v4 = f(v5) when both keys are known on one side).
            boolean spansBothJoinSides =
                    !(leftColumns.containsAll(used) || rightColumns.containsAll(used));
            if (spansBothJoinSides && isEquivalenceBetweenColumnAndNonTrivialExpr(input)) {
                continue;
            }
            if (leftColumns.containsAll(used)) {
                ScalarOperator mapped = rewritePredicateWithMapping(input, leftToRight);
                if (mapped != null && !isTrivialSelfEquivalence(mapped)
                        && rightColumns.containsAll(mapped.getUsedColumns())) {
                    allPredicate.add(mapped);
                    derivedCount++;
                }
            } else if (rightColumns.containsAll(used)) {
                ScalarOperator mapped = rewritePredicateWithMapping(input, rightToLeft);
                if (mapped != null && !isTrivialSelfEquivalence(mapped)
                        && leftColumns.containsAll(mapped.getUsedColumns())) {
                    allPredicate.add(mapped);
                    derivedCount++;
                }
            } else {
                ScalarOperator mappedToRight = rewritePredicateToTargetSide(input, leftToRight, rightColumns);
                if (mappedToRight != null) {
                    allPredicate.add(mappedToRight);
                    derivedCount++;
                    if (derivedCount >= MAX_DERIVED_MULTI_COLUMN_PREDICATES) {
                        break;
                    }
                }
                ScalarOperator mappedToLeft = rewritePredicateToTargetSide(input, rightToLeft, leftColumns);
                if (mappedToLeft != null) {
                    allPredicate.add(mappedToLeft);
                    derivedCount++;
                }
            }
        }
    }

    private static void buildJoinColumnMappings(OptExpression joinOptExpression,
                                                Map<ColumnRefOperator, ColumnRefOperator> leftToRight,
                                                Map<ColumnRefOperator, ColumnRefOperator> rightToLeft) {
        Set<ColumnRefOperator> leftConflict = new HashSet<>();
        Set<ColumnRefOperator> rightConflict = new HashSet<>();
        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> splitOnPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(joinOptExpression);
        List<BinaryPredicateOperator> onEqualPredicates = splitOnPredicates.first;
        for (BinaryPredicateOperator bpo : onEqualPredicates) {
            if (!(bpo.getChild(0) instanceof ColumnRefOperator) || !(bpo.getChild(1) instanceof ColumnRefOperator)) {
                continue;
            }
            ColumnRefOperator leftCol = (ColumnRefOperator) bpo.getChild(0);
            ColumnRefOperator rightCol = (ColumnRefOperator) bpo.getChild(1);
            addMappingWithConflict(leftToRight, leftConflict, leftCol, rightCol);
            addMappingWithConflict(rightToLeft, rightConflict, rightCol, leftCol);
        }
    }

    private static void addMappingWithConflict(Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                               Set<ColumnRefOperator> conflicts,
                                               ColumnRefOperator source,
                                               ColumnRefOperator target) {
        if (conflicts.contains(source)) {
            return;
        }
        ColumnRefOperator old = mapping.computeIfAbsent(source, k -> target);
        if (old.equals(target)) {
            return;
        }
        mapping.remove(source);
        conflicts.add(source);
    }

    private static ScalarOperator rewritePredicateWithMapping(ScalarOperator input,
                                                                Map<ColumnRefOperator, ColumnRefOperator> mapping) {
        if (mapping.isEmpty()) {
            return null;
        }
        Set<ColumnRefOperator> refs = Sets.newHashSet(Utils.extractColumnRef(input));
        if (!mapping.keySet().containsAll(refs)) {
            return null;
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(mapping);
        return rewriter.rewrite(input);
    }

    private static ScalarOperator rewritePredicateToTargetSide(ScalarOperator input,
                                                               Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                               ColumnRefSet targetColumns) {
        if (mapping.isEmpty()) {
            return null;
        }
        Set<ColumnRefOperator> refs = Sets.newHashSet(Utils.extractColumnRef(input));
        Map<ColumnRefOperator, ColumnRefOperator> rewriteMap = new HashMap<>();
        boolean hasRewrite = false;
        for (ColumnRefOperator ref : refs) {
            if (targetColumns.contains(ref.getId())) {
                continue;
            }
            ColumnRefOperator mapped = mapping.get(ref);
            if (mapped == null) {
                return null;
            }
            rewriteMap.put(ref, mapped);
            hasRewrite = true;
        }
        if (!hasRewrite) {
            return null;
        }
        ScalarOperator mapped = new ReplaceColumnRefRewriter(rewriteMap).rewrite(input);
        if (isTrivialSelfEquivalence(mapped)) {
            return null;
        }
        return targetColumns.containsAll(mapped.getUsedColumns()) ? mapped : null;
    }

    /**
     * Drop {@code col = col} and {@code col <=> col}. {@code BinaryType.isEqual()} is only true for {@code =},
     * not {@code <=>}, so callers must use {@code isEquivalence()} for both.
     */
    private static boolean isTrivialSelfEquivalence(ScalarOperator op) {
        if (!(op instanceof BinaryPredicateOperator)) {
            return false;
        }
        BinaryPredicateOperator bpo = (BinaryPredicateOperator) op;
        return bpo.getBinaryType().isEquivalence() && bpo.getChild(0).equals(bpo.getChild(1));
    }

    /**
     * True for {@code col <=> expr} / {@code col = expr} where one operand is a bare column ref and the other is
     * neither a column ref nor a constant (CAST, CASE, subquery artifacts, etc.).
     */
    private static boolean isEquivalenceBetweenColumnAndNonTrivialExpr(ScalarOperator input) {
        if (!(input instanceof BinaryPredicateOperator)) {
            return false;
        }
        BinaryPredicateOperator bpo = (BinaryPredicateOperator) input;
        if (!bpo.getBinaryType().isEquivalence()) {
            return false;
        }
        ScalarOperator c0 = bpo.getChild(0);
        ScalarOperator c1 = bpo.getChild(1);
        return oneSideColumnOtherNonTrivial(c0, c1) || oneSideColumnOtherNonTrivial(c1, c0);
    }

    private static boolean oneSideColumnOtherNonTrivial(ScalarOperator a, ScalarOperator b) {
        if (!(a instanceof ColumnRefOperator)) {
            return false;
        }
        return !(b instanceof ColumnRefOperator) && !(b instanceof ConstantOperator);
    }
}
