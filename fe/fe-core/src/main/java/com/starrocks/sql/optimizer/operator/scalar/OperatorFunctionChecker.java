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

<<<<<<< HEAD
=======
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

    private static final ImmutableSet<Integer> FIRST_ARGUMENT = ImmutableSet.of(0);
    private static final ImmutableSet<Integer> SECOND_ARGUMENT = ImmutableSet.of(1);
    private static final ImmutableSet<Integer> EITHER_ARGUMENT = ImmutableSet.of(0, 1);

    /**
     * Which argument of a monotonic function may hold a column, for the callers that need the
     * expression to INCREASE with its column rather than merely preserve order.
     * <p>
     * isMonotonicFunction() answers one question -- does this function preserve order -- and callers
     * such as ListPartitionPruner.deduceExtraConjuncts read the answer as the stronger claim that the
     * expression grows with the column, because they keep the comparison operator when they rewrite
     * `col OP c` onto the partition column. An expression running the other way then deduces a bound
     * pointing the wrong way: `c2 AS (100 - c1)` turns `c1 < 20` into `c2 <= 80` while the matching
     * row carries c2 = 90, and the query comes back empty.
     * <p>
     * A name alone cannot answer this, because the answer differs per argument. Subtraction and the
     * differences decrease in their second argument while growing in the first. to_datetime(unixtime,
     * scale) divides the epoch by 10^scale, so a larger scale renders an EARLIER instant -- and only
     * 0, 3 and 6 render at all, the rest being NULL. time_slice(dt, interval, unit) is not even signed
     * in its interval: the bucket floor jumps around as the interval grows (100 with interval 6 gives
     * 96, with 7 gives 98, with 101 gives 0). The format, unit and day-of-week arguments of
     * date_format, last_day, next_day and friends order their results arbitrarily -- 'Friday' sorts
     * before 'Monday' while next_day() sends them the other way round. And date_trunc() takes its unit
     * FIRST, so for that one the ordered argument is the second.
     * <p>
     * Only a COLUMN in an unlisted position is a problem: a constant there is fixed, so `c1 - 7` and
     * `to_datetime(ts, 3)` still grow with their column and stay prunable. An unmapped function is
     * assumed to carry its order in the leading argument, which is the shape of every multi-argument
     * monotonic function registered today; a future one shaped like date_trunc loses pruning until it
     * is listed, which is the safe direction to be wrong in.
     */
    private static final ImmutableMap<String, ImmutableSet<Integer>> COLUMN_SAFE_ARGUMENTS =
            ImmutableMap.<String, ImmutableSet<Integer>>builder()
                    // a + b grows with both, and so does date + n
                    .put(FunctionSet.ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.ADD_MONTHS, EITHER_ARGUMENT)
                    .put(FunctionSet.ADDDATE, EITHER_ARGUMENT)
                    .put(FunctionSet.DATE_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.DAYS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.HOURS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.MILLISECONDS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.MINUTES_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.MONTHS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.QUARTERS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.SECONDS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.WEEKS_ADD, EITHER_ARGUMENT)
                    .put(FunctionSet.YEARS_ADD, EITHER_ARGUMENT)
                    // date_trunc(unit, value): the ordered argument is the second one
                    .put(FunctionSet.DATE_TRUNC, SECOND_ARGUMENT)
                    // everything below carries its order in the leading argument alone
                    .put(FunctionSet.SUBTRACT, FIRST_ARGUMENT)
                    .put(FunctionSet.DATEDIFF, FIRST_ARGUMENT)
                    .put(FunctionSet.TIMEDIFF, FIRST_ARGUMENT)
                    .put(FunctionSet.DATE_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.SUBDATE, FIRST_ARGUMENT)
                    .put(FunctionSet.DAYS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.HOURS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.MILLISECONDS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.MINUTES_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.MONTHS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.QUARTERS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.SECONDS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.WEEKS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.YEARS_SUB, FIRST_ARGUMENT)
                    .put(FunctionSet.TO_DATETIME, FIRST_ARGUMENT)
                    .put(FunctionSet.TIME_SLICE, FIRST_ARGUMENT)
                    .put(FunctionSet.FROM_UNIXTIME, FIRST_ARGUMENT)
                    .put(FunctionSet.STR2DATE, FIRST_ARGUMENT)
                    .put(FunctionSet.STR_TO_DATE, FIRST_ARGUMENT)
                    .put(FunctionSet.DATE_FORMAT, FIRST_ARGUMENT)
                    .put("jodatime_format", FIRST_ARGUMENT)
                    .put(FunctionSet.LAST_DAY, FIRST_ARGUMENT)
                    .put(FunctionSet.NEXT_DAY, FIRST_ARGUMENT)
                    .put(FunctionSet.PREVIOUS_DAY, FIRST_ARGUMENT)
                    .build();

    /**
     * Whether every column this call reads sits in an argument the result increases with. Callers that
     * keep the comparison operator need this; callers that map both endpoints of a range and re-sort
     * them do not, and must keep using onlyContainMonotonicFunctions().
     */
    private static boolean columnOnlyInIncreasingArguments(CallOperator call) {
        if (call.getChildren().size() < 2) {
            return true;
        }
        ImmutableSet<Integer> safe =
                COLUMN_SAFE_ARGUMENTS.getOrDefault(call.getFnName().toLowerCase(), FIRST_ARGUMENT);
        for (int i = 0; i < call.getChildren().size(); i++) {
            if (!safe.contains(i) && !Utils.extractColumnRef(call.getChild(i)).isEmpty()) {
                return false;
            }
        }
        return true;
    }

>>>>>>> f12ad8e ([BugFix] Stop refusing retention conditions that cast a date to text (#79688))
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
     * arbitrary string does not parse in text order (see isOrderPreservingCast(CastOperator)).
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
<<<<<<< HEAD
                        cast -> isOrderPreservingCast(cast.fromType(), cast.getType())), null);
=======
                        OperatorFunctionChecker::isOrderPreservingCast), null);
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
                        OperatorFunctionChecker::isOrderPreservingCast), null);
    }

    /**
     * Stricter than onlyContainMonotonicFunctions(): every function must also INCREASE with the
     * columns it reads, not merely preserve their order. Use this wherever a rewrite carries a
     * comparison operator across the expression -- deducing `partCol OP f(c)` from `col OP c` is only
     * sound while f grows with col. A consumer that maps both endpoints of a range and re-sorts them
     * does not need the direction and should keep using onlyContainMonotonicFunctions().
     */
    public static Pair<Boolean, String> onlyContainIncreasingFunctions(ScalarOperator scalarOperator) {
        return scalarOperator.accept(
                new FunctionCheckerVisitor(
                        call -> ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call)
                                && columnOnlyInIncreasingArguments(call),
                        OperatorFunctionChecker::isOrderPreservingCast), null);
>>>>>>> f12ad8e ([BugFix] Stop refusing retention conditions that cast a date to text (#79688))
    }

    public static Pair<Boolean, String> onlyContainFEConstantFunctions(ScalarOperator scalarOperator) {
        return onlyContainPredicates(scalarOperator, call -> ScalarOperatorEvaluator.INSTANCE.isFEConstantFunction(call));
    }
}
