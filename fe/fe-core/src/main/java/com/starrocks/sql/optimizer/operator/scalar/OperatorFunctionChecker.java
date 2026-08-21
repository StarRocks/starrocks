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

        public FunctionCheckerVisitor(Predicate<CallOperator> predicate) {
            this.predicate = predicate;
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
            // Only some type pairs keep the order. Crossing between strings and numbers or dates
            // does not: '99845' sorts after '998425506019' while 99845 is far below 998425506019,
            // so a range predicate mapped through such a cast prunes partitions that hold matching
            // rows. A narrowing numeric cast wraps or saturates and breaks the order the same way.
            if (!isOrderPreservingCast(cast.fromType(), cast.getType())) {
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

    public static Pair<Boolean, String> onlyContainPredicates(ScalarOperator scalarOperator,
                                                              Predicate<CallOperator> predicate) {
        return scalarOperator.accept(new FunctionCheckerVisitor(predicate), null);
    }

    public static Pair<Boolean, String> onlyContainMonotonicFunctions(ScalarOperator scalarOperator) {
        return onlyContainPredicates(scalarOperator, call -> ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call));
    }

    public static Pair<Boolean, String> onlyContainFEConstantFunctions(ScalarOperator scalarOperator) {
        return onlyContainPredicates(scalarOperator, call -> ScalarOperatorEvaluator.INSTANCE.isFEConstantFunction(call));
    }
}
