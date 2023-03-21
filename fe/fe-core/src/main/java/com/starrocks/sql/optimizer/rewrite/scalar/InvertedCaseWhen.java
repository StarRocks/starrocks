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
import com.starrocks.sql.optimizer.Utils;
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

public class InvertedCaseWhen {
    private final CaseWhenOperator caseWhen;

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

    private final Map<ConstantOperator, WhenAndOrdinal> thenMap;

    private InvertedCaseWhen(CaseWhenOperator caseWhen, Map<ConstantOperator, ScalarOperator> thenToWhen,
                             Map<ConstantOperator, Integer> thenToOrdinal) {
        this.caseWhen = caseWhen;
        Preconditions.checkArgument(thenToWhen.size() == thenToOrdinal.size());
        this.thenMap = Maps.newHashMap();
        thenToOrdinal.forEach((k, v) -> {
            this.thenMap.put(k, new WhenAndOrdinal(v, thenToWhen.get(k)));
        });
    }

    private Optional<ScalarOperator> nullToWhen() {
        return Optional.ofNullable(this.thenMap.get(ConstantOperator.NULL)).map(WhenAndOrdinal::getWhen);
    }

    // 1. op = { OR(pi)| pi belongs to selected whens} => op OR if(nullWhen, NULL, FALSE)
    // 2. op = { AND(NOT(pi)| pi belongs to unselected whens} => op AND if(nullWhen, NULL, TRUE)
    // form 2 can not be reduced, so we do not generate from2, so if case-when has null-value
    // branches, we do not use unselected whens.
    private Optional<ScalarOperator> handleNull(ScalarOperator op) {
        Optional<ScalarOperator> nullWhen = nullToWhen();
        if (nullWhen.isPresent()) {
            if (op == ConstantOperator.TRUE) {
                // Theoretical, here should return if(nullWhen, NULL, true), but it is too complex
                // and can not be pushed down, so do not rewrite the case-when.
                return Optional.empty();
            }
            return Optional.of(Utils.compoundOr(op, ifThenNullOrFalse(nullWhen.get())));
        } else {
            // should not generate NULL result
            // 1. op is Constant, it never generates NULL, for an example:
            // case a when 'A' then 1 when 'B' then 2 else 3 end = 4  -- const false
            // case a when 'A' then 1 when 'B' then 2 else 3 end <> 4 -- const true
            // Rhs 4 never hits/misses case-when then clauses and the case-when never yields NULL values,
            // so this predicate is constant false/true. so we just return this op.
            if (op.isConstantRef()) {
                return Optional.of(op);
            }
            // 2. otherwise, build a new predicate: op AND (op IS NOT NULL), for an example:
            // case a when 'A' then 1 when 'B' then 2 else 3 end = 1
            // rhs 1 hit then clause 1, so we get a='A' as simplified result, but a='A' may be yields NULL result,
            // while the original predicate never yields NULL, so we need handle NULL, the correct result is
            // a = 'A' and a is NOT NULL
            ScalarOperator isNotNull = new IsNullPredicateOperator(true,
                    caseWhen.hasCase() ? caseWhen.getCaseClause() : op);
            return Optional.of(Utils.compoundAnd(op, isNotNull));
        }
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

    private static class InvertCaseWhenVisitor extends ScalarOperatorVisitor<Optional<InvertedCaseWhen>, Void> {
        @Override
        public Optional<InvertedCaseWhen> visit(ScalarOperator scalarOperator, Void context) {
            return Optional.empty();
        }

        private Optional<InvertedCaseWhen> handleCaseWhenWithCaseAndConstantWhens(CaseWhenOperator operator) {

            ScalarOperator lhs = operator.getCaseClause();
            Set<ConstantOperator> uniqueWhens = Sets.newHashSet();
            Map<ConstantOperator, List<ScalarOperator>> thenToWhenValues = Maps.newHashMap();
            Map<ConstantOperator, Integer> thenToOrdinal = Maps.newHashMap();
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
            Map<ConstantOperator, ScalarOperator> thenToWhen = Maps.newHashMap();
            thenToWhenValues.forEach((k, v) -> thenToWhen.put(k, in(lhs, v)));

            List<ScalarOperator> allValues = thenToWhenValues.values().stream().flatMap(Collection::stream).collect(
                    Collectors.toList());
            // if allValues is empty, the elsePredicate is true constant, for an example
            // select (case a when NULL then 1 else 2 end) = 2 from t;
            ScalarOperator elsePredicate = allValues.isEmpty() ? ConstantOperator.TRUE : notIn(lhs, allValues);

            elsePredicate = CompoundPredicateOperator.or(elsePredicate, new IsNullPredicateOperator(lhs));

            ConstantOperator alt = (operator.hasElse() && !operator.getElseClause().isConstantNull()) ?
                    operator.getElseClause().cast() :
                    ConstantOperator.NULL;
            thenToWhen.put(alt,
                    CompoundPredicateOperator.or(thenToWhen.getOrDefault(alt, ConstantOperator.FALSE), elsePredicate));
            thenToOrdinal.merge(alt, operator.getWhenClauseSize(), Math::max);
            return Optional.of(new InvertedCaseWhen(operator, thenToWhen, thenToOrdinal));
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
            List<ScalarOperator> notWhens =
                    whenClauses.stream().map(CompoundPredicateOperator::not).collect(Collectors.toList());

            for (int i = 1; i < whenClauses.size(); ++i) {
                whenClauses.set(i, CompoundPredicateOperator.and(whenClauses.get(i),
                        CompoundPredicateOperator.and(notWhens.subList(0, i))));
            }
            // else predicates means that any when clauses are not matched, for an example:
            // case c when c1 then v1 when c2 then v2 when c3 then v3 else c4 end
            // else predicate is (c <> c1 and c <> c2 and c <> c3 and c <> c4) or
            // (c <> c1 and c <> c2 and c <> c3 and c <> c4) is NULL
            ScalarOperator elsePredicate = CompoundPredicateOperator.and(notWhens);
            elsePredicate =
                    CompoundPredicateOperator.or(elsePredicate, new IsNullPredicateOperator(elsePredicate));
            whenClauses.add(elsePredicate);

            // if case-when has no else clause, append const null to thenClauses.
            List<ScalarOperator> thenClauses = operator.getAllValuesClause();
            if (!operator.hasElse()) {
                thenClauses.add(ConstantOperator.NULL);
            }

            Preconditions.checkArgument(whenClauses.size() == thenClauses.size());
            Map<ConstantOperator, List<ScalarOperator>> thenToWhenValues = Maps.newHashMap();
            Map<ConstantOperator, Integer> thenToOrdinal = Maps.newHashMap();
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
            Map<ConstantOperator, ScalarOperator> thenToWhen = Maps.newHashMap();
            thenToWhenValues.forEach(
                    (then, when) -> thenToWhen.put(then.cast(), CompoundPredicateOperator.or(when)));
            return Optional.of(new InvertedCaseWhen(operator, thenToWhen, thenToOrdinal));
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
                CaseWhenOperator caseWhen =
                        new CaseWhenOperator(call.getType(), null, child0,
                                Lists.newArrayList(BinaryPredicateOperator.eq(child1, child0),
                                        ConstantOperator.createNull(call.getType())));
                return caseWhen.accept(this, context);
            }
            return visit(call, context);
        }
    }

    private static Optional<ScalarOperator> or(List<ScalarOperator> args) {
        if (args.isEmpty()) {
            return Optional.empty();
        } else if (args.size() == 1) {
            return Optional.of(args.get(0));
        } else {
            return Optional.of(CompoundPredicateOperator.or(args));
        }
    }

    private static final InvertCaseWhenVisitor INVERT_CASE_WHEN_VISITOR = new InvertCaseWhenVisitor();

    public static Optional<InvertedCaseWhen> from(ScalarOperator op) {
        return op.accept(INVERT_CASE_WHEN_VISITOR, null);
    }

    private static ScalarOperator ifThenNullOrFalse(ScalarOperator p) {
        Function ifFunc = Expr.getBuiltinFunction(FunctionSet.IF, new Type[] {Type.BOOLEAN, Type.BOOLEAN, Type.BOOLEAN},
                Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkArgument(ifFunc != null);
        return new CallOperator(FunctionSet.IF, Type.BOOLEAN,
                Lists.newArrayList(p, ConstantOperator.NULL, ConstantOperator.FALSE), ifFunc);
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
            Map<ScalarOperator, WhenAndOrdinal> selected = Maps.newHashMap();
            Map<ScalarOperator, WhenAndOrdinal> unSelected = Maps.newHashMap();
            invertedCaseWhen.thenMap.entrySet().forEach(e -> {
                if (!e.getKey().isConstantNull()) {
                    if (inSet.contains(e.getKey()) ^ isNotIn) {
                        selected.put(e.getKey(), e.getValue());
                    } else {
                        unSelected.put(e.getKey(), e.getValue());
                    }
                }
            });
            Optional<ScalarOperator> nullWhen = invertedCaseWhen.nullToWhen();
            if (selected.isEmpty()) {
                return invertedCaseWhen.handleNull(ConstantOperator.FALSE);
            } else if (unSelected.isEmpty() && !nullWhen.isPresent()) {
                return Optional.of(ConstantOperator.TRUE);
            }
            // case-when with case clause can be decomposed into several simple non-overlapping branches,
            // in practice it is apt to yields simple and efficient simplified predicates.
            Optional<ScalarOperator> result = Optional.empty();
            if (invertedCaseWhen.caseWhen.hasCase()) {
                if (nullWhen.isPresent() || (selected.size() <= unSelected.size())) {
                    List<ScalarOperator> orArgs = selected.values().stream().map(WhenAndOrdinal::getWhen).collect(
                            Collectors.toList());
                    result = or(orArgs);
                } else {
                    List<ScalarOperator> orArgs = unSelected.values().stream().map(WhenAndOrdinal::getWhen).collect(
                            Collectors.toList());
                    result = or(orArgs).map(CompoundPredicateOperator::not);
                }
            } else {
                // case-when without case clause is apt to yields very complex simplified result, in particular,
                // when values in in-filter hit then-clauses ranked backward. however, when we build the simplified
                // predicate, we have two ways:
                // 1. choose the hit when-clauses: p1,p2,...,pn, then use OR to join them, the result is
                //  p1 OR p2 OR ... OR pn
                // 2. choose the missed when-clauses: p1,p2,...,pn, at first use OR to join them, then negate it,
                //  the result is NOT(p1 OR p2 OR ... OR pn)
                // for case when q1 then c1 when q2 then c2 ... else pn end, when it converted into InvertedCaseWhen
                //  thenToWhen records (c1->p1, c2->p2, ..., pn)(lisp style), pi satisfies:
                //  1. p1 = q1;
                //  2. pn = ((NOT q1) AND (NOT q2) AND ... AND (NOT qn_1)) OR (q1 AND q2 AND ... AND qn_1) is NULL;
                //  3. pi = (NOT q1) AND ... AND (NOT qi_1) AND qi.  i < i < n
                // Because the greater the ordinal of pi is, the more inefficient simplified result is, so when adopt
                // policy: choose between hit when-clauses and missed when-clauses, who owns the minimum maximum
                // ordinal wins, if the winner's maximum ordinal is greater than 1, simplification is forbidden.
                int selectedMaxOrdinal = selected.values().stream()
                        .map(WhenAndOrdinal::getOrdinal).max(Comparator.comparingInt(v -> v)).orElse(0);
                int unSelectedMaxOrdinal = unSelected.values().stream()
                        .map(WhenAndOrdinal::getOrdinal).max(Comparator.comparingInt(v -> v)).orElse(0);
                if (selectedMaxOrdinal > 1 && unSelectedMaxOrdinal > 1) {
                    return Optional.empty();
                }
                if (selectedMaxOrdinal <= unSelectedMaxOrdinal) {
                    List<ScalarOperator> orArgs = selected.values().stream().map(WhenAndOrdinal::getWhen).collect(
                            Collectors.toList());
                    result = or(orArgs);
                } else if (!nullWhen.isPresent()) {
                    List<ScalarOperator> orArgs = unSelected.values().stream().map(WhenAndOrdinal::getWhen).collect(
                            Collectors.toList());
                    result = or(orArgs).map(CompoundPredicateOperator::not);
                } else {
                    return Optional.empty();
                }
            }
            return result.map(invertedCaseWhen::handleNull).orElse(Optional.empty());
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
                        invertedCaseWhen.nullToWhen().map(CompoundPredicateOperator::not)
                                .orElse(ConstantOperator.TRUE));
            } else {
                return Optional.of(invertedCaseWhen.nullToWhen().orElse(ConstantOperator.FALSE));
            }
        }
    }

    private static final SimplifyVisitor SIMPLIFY_VISITOR = new SimplifyVisitor();

    public static ScalarOperator simplify(ScalarOperator op) {
        return op.accept(SIMPLIFY_VISITOR, null).orElse(op);
    }
}
