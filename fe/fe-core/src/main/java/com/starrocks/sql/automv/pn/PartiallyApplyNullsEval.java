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

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.automv.boolalgebra.Modifier;
import com.starrocks.sql.automv.boolalgebra.PowerBool;
import com.starrocks.sql.automv.boolalgebra.TriBool;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartiallyApplyNullsEval {

    private static final Visitor INSTANCE = new Visitor();

    public static boolean isFalseOrNull(Op op, ColumnRefSet nullIds) {
        return !op.accept(INSTANCE, Objects.requireNonNull(nullIds)).maybeTrue();
    }

    private static final class Visitor extends OpVisitor<PowerBool, ColumnRefSet> {
        @Override
        public PowerBool visitVal(Val val, ColumnRefSet nullIds) {
            if (val.isFalseVal()) {
                return PowerBool.PB_FALSE;
            } else if (val.isNullVal()) {
                return PowerBool.PB_NULL;
            } else if (val.isTrueVal()) {
                return PowerBool.PB_TRUE;
            } else {
                return PowerBool.PB_UNKNOWN;
            }
        }

        @Override
        public PowerBool visitVar(Var var, ColumnRefSet nullIds) {
            if (nullIds.contains(var.getId())) {
                return PowerBool.PB_NULL;
            } else {
                return PowerBool.PB_UNKNOWN;
            }
        }

        @Override
        public PowerBool visitApply(Apply apply, ColumnRefSet nullIds) {
            if (apply.isNullSafeEq() || apply.isEq()) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                PowerBool b = apply.arg(1).accept(this, nullIds);
                if (apply.isNullSafeEq()) {
                    return a.nullSafeEq(b);
                } else {
                    return a.eq(b);
                }
            } else if (apply.isNe()) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                PowerBool b = apply.arg(1).accept(this, nullIds);
                return a.eq(b).not();
            } else if (apply.isGt() || apply.isGe() || apply.isLt() || apply.isLe()
                    || ArithmeticExpr.SUPPORT_FUNCTIONS.containsKey(apply.getKind())) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                PowerBool b = apply.arg(1).accept(this, nullIds);
                if (a == PowerBool.PB_NULL || b == PowerBool.PB_NULL) {
                    return PowerBool.PB_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isNotIn()) {
                boolean hasNull = apply.unmodified().arg(0).getArgs().stream()
                        .map(arg -> arg.accept(this, nullIds))
                        .anyMatch(PowerBool.PB_NULL::equals);
                if (hasNull) {
                    return PowerBool.PB_FALSE_OR_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isIn()) {
                return PowerBool.PB_UNKNOWN;
            } else if (apply.isSetOf()) {
                return PowerBool.PB_UNKNOWN;
            } else if (apply.isModify()) {
                Modifier modifier = apply.getModifier();
                switch (modifier) {
                    case M_ALWAYS_FALSE: {
                        return PowerBool.PB_FALSE;
                    }
                    case M_ALWAYS_NULL: {
                        return PowerBool.PB_NULL;
                    }
                    case M_ALWAYS_TRUE: {
                        return PowerBool.PB_TRUE;
                    }
                    default: {
                        Op unmodified = apply.unmodified();
                        List<TriBool> states = unmodified.accept(this, nullIds).getStates();
                        Set<TriBool> set = states.stream()
                                .map(Enum::ordinal)
                                .map(modifier::transfer)
                                .collect(Collectors.toSet());
                        return PowerBool.from(set);
                    }
                }
            } else if (apply.isInRange() || apply.isRangeOf()) {
                throw new IllegalArgumentException("Apply should not be inRange or rangeOf types");
            } else if (apply.isCase()) {
                int nArgs = apply.getArgs().size();
                Optional<Op> elseBranch = nArgs % 2 == 1 ? Optional.of(apply.arg(-1)) : Optional.empty();
                int numWhens = nArgs / 2;
                List<Op> whens = IntStream.range(0, numWhens)
                        .mapToObj(i -> apply.arg(i * 2))
                        .collect(Collectors.toList());
                List<Op> thens = IntStream.range(0, numWhens)
                        .mapToObj(i -> apply.arg(i * 2 + 1))
                        .collect(Collectors.toList());
                for (int i = 0; i < numWhens; ++i) {
                    PowerBool whenResult = whens.get(i).accept(this, nullIds);
                    if (whenResult == PowerBool.PB_TRUE) {
                        return thens.get(i).accept(this, nullIds);
                    } else if (whenResult.maybeTrue()) {
                        return PowerBool.PB_UNKNOWN;
                    }
                }
                return elseBranch.map(elseBr -> elseBr.accept(this, nullIds)).orElse(PowerBool.PB_NULL);
            } else if (apply.isCast()) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                if (a == PowerBool.PB_NULL) {
                    return PowerBool.PB_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isAnd()) {
                return apply.getArgs().stream()
                        .map(arg -> arg.accept(this, nullIds))
                        .reduce(PowerBool.PB_TRUE, PowerBool::and);
            } else if (apply.isOr()) {
                return apply.getArgs().stream()
                        .map(arg -> arg.accept(this, nullIds))
                        .reduce(PowerBool.PB_FALSE, PowerBool::or);
            } else if (apply.isArray() || apply.isMap()) {
                return PowerBool.PB_UNKNOWN;
            } else if (apply.isArraySlice() ||
                    apply.isCollectionElement() ||
                    apply.isSubfield() ||
                    apply.isLike() ||
                    apply.isRegexp()) {
                boolean hasNull = apply.getArgs().stream().map(arg -> arg.accept(this, nullIds))
                        .anyMatch(PowerBool.PB_NULL::equals);
                if (hasNull) {
                    return PowerBool.PB_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isLambda() || apply.isLocal() || apply.isDistinct()) {
                return PowerBool.PB_UNKNOWN;
            } else if (apply.isFun(FunctionSet.IFNULL)) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                if (a == PowerBool.PB_UNKNOWN) {
                    return PowerBool.PB_UNKNOWN;
                } else if (a == PowerBool.PB_NULL) {
                    return apply.arg(1).accept(this, nullIds);
                } else {
                    return a;
                }
            } else if (apply.isFun(FunctionSet.IF)) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                if (a == PowerBool.PB_TRUE) {
                    return apply.arg(1).accept(this, nullIds);
                } else if (!a.maybeTrue()) {
                    return apply.arg(2).accept(this, nullIds);
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isFun(FunctionSet.NULLIF)) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                PowerBool b = apply.arg(1).accept(this, nullIds);
                boolean equivalent = apply.arg(0).strict().equals(apply.arg(1).strict());
                PowerBool eqResult = a.eq(b);
                if (eqResult.not() == PowerBool.PB_TRUE) {
                    return a;
                } else if (equivalent && !a.maybeNull()) {
                    return PowerBool.PB_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            } else if (apply.isFun(FunctionSet.COALESCE)) {
                int i = 0;
                while (i < apply.getArgs().size()) {
                    PowerBool a = apply.arg(i).accept(this, nullIds);
                    if (!a.maybeNull()) {
                        return a;
                    } else if (a == PowerBool.PB_UNKNOWN) {
                        return PowerBool.PB_UNKNOWN;
                    }
                    ++i;
                }
                return PowerBool.PB_UNKNOWN;
            } else if (apply.isFun(FunctionSet.NULL_OR_EMPTY)) {
                PowerBool a = apply.arg(0).accept(this, nullIds);
                if (a == PowerBool.PB_NULL) {
                    return PowerBool.PB_TRUE;
                } else if (apply.arg(0).isVal()) {
                    Val val = apply.arg(0).cast();
                    Preconditions.checkArgument(val.getType().isStringType());
                    if (val.getValue().getVarchar().isEmpty()) {
                        return PowerBool.PB_TRUE;
                    } else {
                        return PowerBool.PB_FALSE;
                    }
                } else {
                    return PowerBool.PB_NULL;
                }
            } else {
                Function<Type, Boolean> isSimpleType = t -> t.isNumericType() ||
                        t.isStringType() || t.isBinaryType() || t.isNumericType() || t.isDateType();

                boolean allArgsSimpleType = apply.getArgs().stream()
                        .allMatch(arg -> isSimpleType.apply(arg.getType()));
                boolean retSimpleType = isSimpleType.apply(apply.getType());
                boolean hasNull = apply.getArgs().stream()
                        .map(a -> a.accept(this, nullIds)).anyMatch(PowerBool.PB_NULL::equals);
                if (allArgsSimpleType && retSimpleType && hasNull) {
                    return PowerBool.PB_NULL;
                } else {
                    return PowerBool.PB_UNKNOWN;
                }
            }
        }
    }
}