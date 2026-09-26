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
// limitations under the License

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.type.Type;

import java.util.function.Predicate;

/**
 * FunctionChecker is used to check whether a ScalarOperator only contains a specific type of functions.
 */
public class OperatorFunctionChecker {
    static class FunctionCheckerVisitor extends ScalarOperatorVisitor<Pair<Boolean, String>, Void> {
        private final Predicate<CallOperator> predicate;
        // A cast is checked separately from the call predicate: whether a cast is acceptable depends
        // on what the caller is asking. Every cast is FE-evaluable, but only some keep the order.
        private final Predicate<CastOperator> castPredicate;

        public FunctionCheckerVisitor(Predicate<CallOperator> predicate, Predicate<CastOperator> castPredicate) {
            this.predicate = predicate;
            this.castPredicate = castPredicate;
        }

        @Override
        public Pair<Boolean, String> visit(ScalarOperator scalarOperator, Void context) {
            for (ScalarOperator child : scalarOperator.getChildren()) {
                Pair<Boolean, String> result = child.accept(this, null);
                if (!result.first) {
                    return result;
                }
            }
            return Pair.create(true, "");
        }

        @Override
        public Pair<Boolean, String> visitCastOperator(CastOperator cast, Void context) {
            Pair<Boolean, String> result = cast.getChild(0).accept(this, null);
            if (!result.first) {
                return result;
            }
            if (!castPredicate.test(cast)) {
                return Pair.create(false, cast.toString());
            }
            return Pair.create(true, "");
        }

        public Pair<Boolean, String> visitCall(CallOperator call, Void context) {
            for (ScalarOperator child : call.getChildren()) {
                Pair<Boolean, String> result = child.accept(this, null);
                if (!result.first) {
                    return result;
                }
            }
            if (predicate.test(call)) {
                return Pair.create(true, "");
            } else {
                return Pair.create(false, call.getFnName());
            }
        }
    }

    /**
     * Applies the cast predicate only outside (in)equalities and IN lists, where the order of the
     * compared values is what the caller relies on.
     */
    static class RangeOnlyCastCheckVisitor extends FunctionCheckerVisitor {
        private final FunctionCheckerVisitor anyCastVisitor;

        public RangeOnlyCastCheckVisitor(Predicate<CallOperator> predicate, Predicate<CastOperator> castPredicate) {
            super(predicate, castPredicate);
            this.anyCastVisitor = new FunctionCheckerVisitor(predicate, cast -> true);
        }

        @Override
        public Pair<Boolean, String> visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType().isNotRangeComparison()) {
                return anyCastVisitor.visit(predicate, null);
            }
            return visit(predicate, null);
        }

        @Override
        public Pair<Boolean, String> visitInPredicate(InPredicateOperator predicate, Void context) {
            return anyCastVisitor.visit(predicate, null);
        }
    }

    private static int integerRank(Type type) {
        if (type.isTinyint()) {
            return 1;
        } else if (type.isSmallint()) {
            return 2;
        } else if (type.isInt()) {
            return 3;
        } else if (type.isBigint()) {
            return 4;
        } else if (type.isLargeIntType()) {
            return 5;
        }
        return -1;
    }

    /**
     * Only some type pairs keep the order. Crossing between strings and numbers does not: '99845'
     * sorts after '998425506019' while 99845 is far below 998425506019, so a range predicate mapped
     * through such a cast prunes partitions that hold matching rows. A narrowing numeric cast wraps
     * or saturates and breaks the order the same way.
     * <p>
     * Rendering a date as text is the exception. DATE and DATETIME render as fixed-width, zero-padded
     * fields -- '2020-07-02', '2020-07-02 10:00:00', with a six-digit fraction appended only when it is
     * not zero -- so the text sorts the way the instant does. str2date(dt, '%Y-%m-%d') over a DATE
     * column goes through exactly this cast. The reverse direction is a different matter: an
     * arbitrary string does not parse in text order, so text -> DATE stays refused.
     * <p>
     * This is a question about monotonicity alone. Equality maps soundly through any deterministic
     * function -- a = c implies f(a) = f(c) whatever f does to the order -- so the FE-constant check,
     * which is what the callers use to license the equality rewrite, must not apply it. See
     * ListPartitionPruner.deduceExtraConjuncts, which gates on FE-constant-ness first and only
     * demands monotonicity for the non-equality case.
     */
    private static boolean isOrderPreservingCast(Type from, Type to) {
        if (from.equals(to)) {
            return true;
        }
        int fromRank = integerRank(from);
        int toRank = integerRank(to);
        if (fromRank > 0 && toRank > 0) {
            // widening within the integer family keeps every value and its order
            return toRank >= fromRank;
        }
        if ((from.isDate() || from.isDatetime()) && to.isVarchar()) {
            return true;
        }
        // DATE and DATETIME order the same way; DATETIME -> DATE truncates, which is non-decreasing
        return (from.isDate() && to.isDatetime()) || (from.isDatetime() && to.isDate());
    }

    /**
     * Checks the calls only. Casts are accepted whatever the predicate says, so a caller that cares
     * about the order values come out in - anything driving partition pruning off a range predicate -
     * wants onlyContainMonotonicFunctions instead of passing a monotonicity predicate through here.
     */
    public static Pair<Boolean, String> onlyContainPredicates(ScalarOperator scalarOperator,
                                                              Predicate<CallOperator> predicate) {
        return scalarOperator.accept(new FunctionCheckerVisitor(predicate, cast -> true), null);
    }

    public static Pair<Boolean, String> onlyContainMonotonicFunctions(ScalarOperator scalarOperator) {
        return scalarOperator.accept(
                new FunctionCheckerVisitor(call -> ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call),
                        cast -> isOrderPreservingCast(cast.fromType(), cast.getType())), null);
    }

    /**
     * Like onlyContainMonotonicFunctions(), but a cast only has to keep the order where the order is
     * compared. Under an (in)equality or an IN list any deterministic cast is fine -- a = c implies
     * f(a) = f(c) whatever f does to the order -- so there the casts are accepted as before and only
     * the calls are checked. A retention condition such as str2date(dt, '%Y-%m-%d') = '2020-07-07'
     * must not be refused because of a cast it only ever compares for equality.
     */
    public static Pair<Boolean, String> onlyContainMonotonicFunctionsWhereOrderMatters(ScalarOperator scalarOperator) {
        return scalarOperator.accept(
                new RangeOnlyCastCheckVisitor(call -> ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call),
                        cast -> isOrderPreservingCast(cast.fromType(), cast.getType())), null);
    }

    public static Pair<Boolean, String> onlyContainFEConstantFunctions(ScalarOperator scalarOperator) {
        return onlyContainPredicates(scalarOperator, call -> ScalarOperatorEvaluator.INSTANCE.isFEConstantFunction(call));
    }
}
