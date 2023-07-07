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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;

public class InvertedCaseWhen {
    private final CaseWhenOperator caseWhen;

    private final boolean isWithCaseAndConstWhen;

    private final Map<ConstantOperator, WhenAndOrdinal> thenMap;

    private final ScalarOperator elseBranch;

    private InvertedCaseWhen(boolean isWithCaseAndConstWhen, CaseWhenOperator caseWhen,
                             Map<ConstantOperator, ScalarOperator> thenToWhen,
                             Map<ConstantOperator, Integer> thenToOrdinal) {
        this.isWithCaseAndConstWhen = isWithCaseAndConstWhen;
        this.caseWhen = caseWhen;
        Preconditions.checkArgument(thenToWhen.size() == thenToOrdinal.size());
        this.thenMap = Maps.newLinkedHashMap();
        int maxOrdinal = 0;
        ScalarOperator tmp = null;
        for (Map.Entry<ConstantOperator, Integer> entry : thenToOrdinal.entrySet()) {
            ConstantOperator k = entry.getKey();
            Integer v = entry.getValue();
            this.thenMap.put(k, new WhenAndOrdinal(v, thenToWhen.get(k)));
            if (v > maxOrdinal) {
                maxOrdinal = v;
                tmp = thenToWhen.get(k);
            }
        }
        elseBranch = tmp;
    }

    private Optional<ScalarOperator> getBranchToNull() {
        return Optional.ofNullable(this.thenMap.get(ConstantOperator.NULL)).map(WhenAndOrdinal::getWhen);
    }

    private ScalarOperator getElseBranch() {
        return elseBranch;
    }


    public static ScalarOperator in(boolean isNotIn, ScalarOperator lhs, List<ScalarOperator> values) {
        Preconditions.checkArgument(!values.isEmpty());
        List<ScalarOperator> args = Lists.newArrayList(lhs);
        args.addAll(values);
        return new InPredicateOperator(isNotIn, args);
    }

    public static ScalarOperator in(ScalarOperator lhs, List<ScalarOperator> values) {
        return in(false, lhs, values);
    }

    public static ScalarOperator notIn(ScalarOperator lhs, List<ScalarOperator> values) {
        return in(true, lhs, values);
    }

    private static class WhenAndOrdinal {

        private final int ordinal;
        private final ScalarOperator when;

        public WhenAndOrdinal(int ordinal, ScalarOperator when) {
            this.ordinal = ordinal;
            this.when = when;
        }

        public int getOrdinal() {
            return ordinal;
        }

        public ScalarOperator getWhen() {
            return when;
        }
    }

    private static class InvertCaseWhenVisitor extends ScalarOperatorVisitor<Optional<InvertedCaseWhen>, Void> {
        @Override
        public Optional<InvertedCaseWhen> visit(ScalarOperator scalarOperator, Void context) {
            return Optional.empty();
        }

        private Optional<InvertedCaseWhen> handleCaseWhenWithCaseAndConstantWhens(CaseWhenOperator operator) {

            ScalarOperator lhs = operator.getCaseClause();
            Set<ConstantOperator> uniqueWhens = Sets.newHashSet();
            Map<ConstantOperator, List<ScalarOperator>> thenToWhenValues = Maps.newLinkedHashMap();
            Map<ConstantOperator, Integer> thenToOrdinal = Maps.newLinkedHashMap();
            for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
                ConstantOperator then = operator.getThenClause(i).cast();
                ConstantOperator when = operator.getWhenClause(i).cast();
                // if when value is NULL or duplicate of preceding when value, it never matches case clause,
                // so skip it
                if (when.isConstantNull() || uniqueWhens.contains(when)) {
                    continue;
                }
                if (then.isConstantNull()) {
                    then = ConstantOperator.NULL;
                }
                uniqueWhens.add(when);
                thenToWhenValues.computeIfAbsent(then, (k) -> Lists.newArrayList()).add(when);
                thenToOrdinal.merge(then, i, Math::max);
            }
            Map<ConstantOperator, ScalarOperator> thenToWhen = Maps.newLinkedHashMap();
            thenToWhenValues.forEach((k, v) -> thenToWhen.put(k, in(lhs, v)));

            List<ScalarOperator> allValues = thenToWhenValues.values().stream().flatMap(Collection::stream).collect(
                    Collectors.toList());
            // if allValues is empty, the elsePredicate is true constant, for an example
            // select (case a when NULL then 1 else 2 end) = 2 from t;
            ScalarOperator elsePredicate = allValues.isEmpty() ? ConstantOperator.TRUE :
                    NegateFilterShuttle.getInstance().negateFilter(in(lhs, allValues));

            ConstantOperator alt = (operator.hasElse() && !operator.getElseClause().isConstantNull()) ?
                    operator.getElseClause().cast() :
                    ConstantOperator.NULL;
            thenToWhen.put(alt,
                    CompoundPredicateOperator.or(thenToWhen.getOrDefault(alt, ConstantOperator.FALSE), elsePredicate));
            thenToOrdinal.merge(alt, operator.getWhenClauseSize(), Math::max);
            return Optional.of(new InvertedCaseWhen(true, operator, thenToWhen, thenToOrdinal));
        }

        Optional<InvertedCaseWhen> handleCaseWhen(CaseWhenOperator operator) {
            List<ScalarOperator> whenClauses = operator.getAllConditionClause();
            // If case-when has case clause, the first element of whenClauses is case clause, so
            // remove it, and convert remaining whenClauses into eq predicates.
            if (operator.hasCase()) {
                ScalarOperator lhs = operator.getCaseClause();
                whenClauses = whenClauses.stream().skip(1).map(v -> BinaryPredicateOperator.eq(lhs, v))
                        .collect(Collectors.toList());
            }
            NegateFilterShuttle shuttle = NegateFilterShuttle.getInstance();
            List<ScalarOperator> notWhens =
                    whenClauses.stream().map(shuttle::negateFilter).collect(Collectors.toList());

            for (int i = 1; i < whenClauses.size(); ++i) {
                whenClauses.set(i, CompoundPredicateOperator.and(whenClauses.get(i),
                        CompoundPredicateOperator.and(notWhens.subList(0, i))));
            }
            // else predicates means that any when clauses are not matched, for an example:
            // case c when c1 then v1 when c2 then v2 when c3 then v3 else v4 end
            // else predicate is ¬c1 ∧ ¬c2 ∧ ¬c3
            ScalarOperator elsePredicate = CompoundPredicateOperator.and(notWhens);
            whenClauses.add(elsePredicate);

            // if case-when has no else clause, append const null to thenClauses.
            List<ScalarOperator> thenClauses = operator.getAllValuesClause();
            if (!operator.hasElse()) {
                thenClauses.add(ConstantOperator.NULL);
            }

            Preconditions.checkArgument(whenClauses.size() == thenClauses.size());
            Map<ConstantOperator, List<ScalarOperator>> thenToWhenValues = Maps.newLinkedHashMap();
            Map<ConstantOperator, Integer> thenToOrdinal = Maps.newLinkedHashMap();
            for (int i = 0; i < thenClauses.size(); ++i) {
                ConstantOperator then = thenClauses.get(i).cast();
                // NULL is apt to bug, so here use a constNull
                if (then.isConstantNull()) {
                    then = ConstantOperator.NULL;
                }
                ScalarOperator when = whenClauses.get(i);
                thenToWhenValues.computeIfAbsent(then, (k) -> Lists.newArrayList()).add(when);
                thenToOrdinal.merge(then, i, Math::max);
            }
            Map<ConstantOperator, ScalarOperator> thenToWhen = Maps.newLinkedHashMap();
            thenToWhenValues.forEach(
                    (then, when) -> thenToWhen.put(then.cast(), CompoundPredicateOperator.or(when)));
            return Optional.of(new InvertedCaseWhen(false, operator, thenToWhen, thenToOrdinal));
        }

        @Override
        public Optional<InvertedCaseWhen> visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            if (!operator.getAllValuesClause().stream().allMatch(ScalarOperator::isConstantRef)) {
                return visit(operator, context);
            }
            if (operator.hasCase() &&
                    operator.getAllConditionClause().stream().skip(1).allMatch(ScalarOperator::isConstantRef)) {
                return handleCaseWhenWithCaseAndConstantWhens(operator);
            } else {
                return handleCaseWhen(operator);
            }
        }

        @Override
        public Optional<InvertedCaseWhen> visitCall(CallOperator call, Void context) {
            String fnName = call.getFnName();
            if (fnName.equals(FunctionSet.IF)) {
                ScalarOperator cond = call.getChild(0);
                ScalarOperator then = call.getChild(1);
                ScalarOperator alt = call.getChild(2);
                if (!then.isConstantRef() || !alt.isConstantRef()) {
                    return visit(call, context);
                }
                CaseWhenOperator caseWhen =
                        new CaseWhenOperator(call.getType(), null, alt, Lists.newArrayList(cond, then));
                return caseWhen.accept(this, context);
            } else if (fnName.equals(FunctionSet.NULLIF)) {
                ScalarOperator child0 = call.getChild(0);
                ScalarOperator child1 = call.getChild(1);
                if (!child0.isConstantRef()) {
                    return visit(call, context);
                }

                // transform to case when case child1 when child0 then null else child0
                CaseWhenOperator caseWhen = new CaseWhenOperator(call.getType(), child1, child0,
                        Lists.newArrayList(child0, ConstantOperator.createNull(call.getType())));
                return caseWhen.accept(this, context);
            }
            return visit(call, context);
        }
    }



    private static final InvertCaseWhenVisitor INVERT_CASE_WHEN_VISITOR = new InvertCaseWhenVisitor();

    public static Optional<InvertedCaseWhen> from(ScalarOperator op) {
        return op.accept(INVERT_CASE_WHEN_VISITOR, null);
    }

    private static ScalarOperator buildIfThen(ScalarOperator p, ConstantOperator first, ConstantOperator second) {
        Function ifFunc = Expr.getBuiltinFunction(FunctionSet.IF, new Type[] {Type.BOOLEAN, Type.BOOLEAN, Type.BOOLEAN},
                Function.CompareMode.IS_IDENTICAL);
        return new CallOperator(FunctionSet.IF, Type.BOOLEAN,
                Lists.newArrayList(p, first, second), ifFunc);
    }

    private static class SimplifyVisitor extends ScalarOperatorVisitor<Optional<ScalarOperator>, Void> {

        @Override
        public Optional<ScalarOperator> visit(ScalarOperator scalarOperator, Void context) {
            return Optional.empty();
        }

        @Override
        public Optional<ScalarOperator> visitInPredicate(InPredicateOperator predicate, Void context) {
            Set<ScalarOperator> inSet = predicate.getChildren().stream().skip(1).collect(Collectors.toSet());
            if (!inSet.stream().allMatch(ScalarOperator::isConstantRef)) {
                return Optional.empty();
            }
            Optional<InvertedCaseWhen> maybeInvertedCaseWhen = from(predicate.getChild(0));
            if (!maybeInvertedCaseWhen.isPresent()) {
                return Optional.empty();
            }
            // case when ... in (NULL, NULL) is equivalent to NULL
            // case when ... in (NULL, c1) is equivalent to case when ... in (c1)
            inSet.removeIf(ScalarOperator::isConstantNull);
            if (inSet.isEmpty()) {
                return Optional.of(ConstantOperator.NULL);
            }
            InvertedCaseWhen invertedCaseWhen = maybeInvertedCaseWhen.get();
            boolean isNotIn = predicate.isNotIn();
            Map<ScalarOperator, WhenAndOrdinal> selected = Maps.newLinkedHashMap();
            Map<ScalarOperator, WhenAndOrdinal> unSelected = Maps.newLinkedHashMap();
            invertedCaseWhen.thenMap.entrySet().forEach(e -> {
                if (!e.getKey().isConstantNull()) {
                    if (inSet.contains(e.getKey()) ^ isNotIn) {
                        selected.put(e.getKey(), e.getValue());
                    } else {
                        unSelected.put(e.getKey(), e.getValue());
                    }
                }
            });

            int selectedMaxOrdinal = selected.values().stream()
                    .map(WhenAndOrdinal::getOrdinal).max(Comparator.comparingInt(v -> v)).orElse(0);
            int unSelectedMaxOrdinal = unSelected.values().stream()
                    .map(WhenAndOrdinal::getOrdinal).max(Comparator.comparingInt(v -> v)).orElse(0);
            Optional<ScalarOperator> branchToNull = invertedCaseWhen.getBranchToNull();
            Optional<ScalarOperator> result = Optional.empty();

            // All branches missed.
            // - if then values contains null, just return null if branchToNull is true, otherwise return false.
            // - otherwise return false.
            if (selected.isEmpty()) {
                if (branchToNull.isPresent()) {
                    return Optional.of(buildIfThen(branchToNull.get(), ConstantOperator.NULL, ConstantOperator.FALSE));
                } else {
                    return Optional.of(ConstantOperator.FALSE);
                }
            }

            // All branches hit.
            // - if then values contains null, union all selected branches. If the union result is ture return true
            // otherwise return null.
            // - otherwise return true.
            if (unSelected.isEmpty()) {
                if (!branchToNull.isPresent()) {
                    return Optional.of(ConstantOperator.TRUE);
                } else if (invertedCaseWhen.isWithCaseAndConstWhen || selected.size() <= 2) {
                    return or(selected).map(e -> buildIfThen(e, ConstantOperator.TRUE, ConstantOperator.NULL));
                } else {
                    return result;
                }
            }

            // case-when with case clause and when branch are all consts can be decomposed into several simple
            // non-overlapping branches, in practice it is apt to yields simple and efficient simplified predicates.
            if (invertedCaseWhen.isWithCaseAndConstWhen) {
                if (!branchToNull.isPresent()) {
                    boolean isHitElseBranch = hitElseBranch(selectedMaxOrdinal, unSelectedMaxOrdinal);
                    // take care to process when branch predicate can be null
                    if (invertedCaseWhen.caseWhen.getCaseClause().isNullable()) {
                        result = or(selected).map(
                                e -> {
                                    if (isHitElseBranch) {
                                        return new CompoundPredicateOperator(CompoundType.OR, e,
                                                new IsNullPredicateOperator(invertedCaseWhen.caseWhen.getCaseClause()));
                                    } else {
                                        return buildIfThen(e, ConstantOperator.TRUE, ConstantOperator.FALSE);
                                    }
                                }
                        );
                    } else if (isHitElseBranch) {
                        result = or(unSelected).map(CompoundPredicateOperator::not);
                    } else {
                        result = or(selected);
                    }
                }
                return result;
            }



            // for case when q1 then c1 when q2 then c2 ... else pn end, when it converted into InvertedCaseWhen
            //  thenToWhen records (c1->p1, c2->p2, ..., pn)(lisp style), pi satisfies:
            //  1. p1 = q1;
            //  2. pi = ¬q1 ∧ ¬q2 ∧ ¬q3 ... ∧ ¬qi_1 ∧ qi.  i < i < n
            //  3. p_else = ¬q1 ∧ ¬q2 ∧ ¬q3 ... ∧ ¬qn_1
            // We must ensure the output is totally same as the original one. When there exists a branch to null,
            // it's really hard to rewrite the case when, the or(selected) result may return ture | false | null. When
            // a data yields null for normal select branches but yields true for else branches, it should be return true,
            // but the or(selected) returns null. So we prohibit rewriting it.
            // When there doesn't exist a branch to null but branches is too much, the rewrite result is too inefficient,
            // we prohibit rewriting it.
            if (branchToNull.isPresent() || Math.min(selected.size(), unSelected.size()) > 2) {
                return result;
            }

            if (hitElseBranch(selectedMaxOrdinal, unSelectedMaxOrdinal)) {
                if (selected.size() <= unSelected.size()) {
                    result = or(selected).map(e -> {
                        if (e.isNullable()) {
                            return null;
                        } else {
                            return e;
                        }
                    });
                } else {
                    result = or(unSelected).map(e -> {
                        if (e.isNullable()) {
                            return null;
                        } else {
                            return new CompoundPredicateOperator(CompoundType.NOT, e);
                        }
                    });
                }

            } else {
                result = or(selected).map(e -> {
                    if (e.isNullable()) {
                        return buildIfThen(e, ConstantOperator.TRUE, ConstantOperator.FALSE);
                    } else {
                        return e;
                    }
                });
            }
            return result;
        }

        @Override
        public Optional<ScalarOperator> visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            BinaryPredicateOperator.BinaryType binaryType = predicate.getBinaryType();
            if (!binaryType.isEqual() && !binaryType.isNotEqual()) {
                return Optional.empty();
            }
            return in(binaryType.isNotEqual(),
                    predicate.getChild(0),
                    Lists.newArrayList(predicate.getChild(1))).accept(this, context);
        }

        @Override
        public Optional<ScalarOperator> visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            Optional<InvertedCaseWhen> maybeInvertedCaseWhen = from(predicate.getChild(0));
            if (!maybeInvertedCaseWhen.isPresent()) {
                return Optional.empty();
            }
            InvertedCaseWhen invertedCaseWhen = maybeInvertedCaseWhen.get();
            if (predicate.isNotNull()) {
                return Optional.of(
                        invertedCaseWhen.getBranchToNull().map(NegateFilterShuttle.getInstance()::negateFilter)
                                .orElse(ConstantOperator.TRUE));
            } else {
                return Optional.of(invertedCaseWhen.getBranchToNull().orElse(ConstantOperator.FALSE));
            }
        }

        private Optional<ScalarOperator> or(Map<ScalarOperator, WhenAndOrdinal> argMap) {
            if (argMap.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(CompoundPredicateOperator.or(argMap.values()
                        .stream().map(WhenAndOrdinal::getWhen)
                        .collect(Collectors.toList())));
            }
        }

        private boolean hitElseBranch(int selectedMaxOrdinal, int unSelectedMaxOrdinal) {
            return selectedMaxOrdinal > unSelectedMaxOrdinal;
        }
    }

    private static final SimplifyVisitor SIMPLIFY_VISITOR = new SimplifyVisitor();

    public static ScalarOperator simplify(ScalarOperator op) {
        return op.accept(SIMPLIFY_VISITOR, null).orElse(op);
    }
}
