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

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.type.Type;

import java.util.function.Predicate;

/**
 * FunctionChecker is used to check whether a ScalarOperator only contains a specific type of functions.
 */
public class OperatorFunctionChecker {
    /**
     * Functions that render an instant as canonical text: their result sorts the way the instant does,
     * so a cast back to a datetime keeps the order even though the same cast reorders an arbitrary
     * varchar column.
     * <p>
     * These two are the whole family that matters: ColumnFilterConverter.ExprRewriter substitutes the
     * constant into the partition expression from a fixed whitelist, and from_unixtime/from_unixtime_ms
     * are its only entries that render an instant as text. date_format() belongs to the family by
     * shape, but the rewriter does not know it, so listing it here would license a cast on an
     * expression that never reaches the rewrite.
     */
    private static final ImmutableSet<String> DATETIME_TEXT_FUNCTIONS = ImmutableSet.of(
            FunctionSet.FROM_UNIXTIME,
            FunctionSet.FROM_UNIXTIME_MS);

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
     * Only some type pairs keep the order. Crossing between strings and numbers or dates does not:
     * '99845' sorts after '998425506019' while 99845 is far below 998425506019, so a range predicate
     * mapped through such a cast prunes partitions that hold matching rows. A narrowing numeric cast
     * wraps or saturates and breaks the order the same way.
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
        // DATE and DATETIME order the same way; DATETIME -> DATE truncates, which is non-decreasing
        return (from.isDate() && to.isDatetime()) || (from.isDatetime() && to.isDate());
    }

    /**
     * Whether a cast keeps the order depends on the values reaching it, not on the type pair alone.
     * A string-to-datetime cast reorders an arbitrary varchar column -- '2021-1-2' sorts before
     * '2021-01-03' as text but after it as an instant -- yet it is order-preserving over the canonical
     * text a from_unixtime()/date_format() produces, which is how an expression partition on a unix
     * timestamp is spelled: RANGE(from_unixtime(ts)) translates to cast(from_unixtime(ts) as datetime).
     * Refusing that cast costs those tables their range pruning.
     * <p>
     * The format is not re-checked here: only a call the monotonicity predicate already accepted can
     * get this far, and for these names that predicate is the format check. A three-argument
     * from_unixtime() escapes it (the check only looks at the two-argument form), so it is not
     * accepted.
     */
    private static boolean producesOrderedDatetimeText(ScalarOperator operator) {
        if (!(operator instanceof CallOperator call)) {
            return false;
        }
        // These render the epoch in the session time zone, so the text they produce is ordered only
        // away from a clock rollback -- across one, an increasing epoch yields a DECREASING local
        // datetime. That is a question about the predicate's constant rather than about the
        // expression, so it is asked where the constant is known:
        // ColumnFilterConverter.constantInsideClockRollback().
        // from_unixtime() takes up to three arguments: the epoch, the format, and the time zone.
        // ScalarOperatorEvaluator.isMonotonicFunction has already vetted the format for all of them.
        return DATETIME_TEXT_FUNCTIONS.contains(call.getFnName().toLowerCase())
                && call.getChildren().size() <= 3;
    }

    private static boolean isOrderPreservingCast(CastOperator cast) {
        Type from = cast.fromType();
        Type to = cast.getType();
        if (isOrderPreservingCast(from, to)) {
            return true;
        }
        if (from.isStringType() && (to.isDate() || to.isDatetime())) {
            return producesOrderedDatetimeText(cast.getChild(0));
        }
        return false;
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
                        OperatorFunctionChecker::isOrderPreservingCast), null);
    }

    public static Pair<Boolean, String> onlyContainFEConstantFunctions(ScalarOperator scalarOperator) {
        return onlyContainPredicates(scalarOperator, call -> ScalarOperatorEvaluator.INSTANCE.isFEConstantFunction(call));
    }
}
