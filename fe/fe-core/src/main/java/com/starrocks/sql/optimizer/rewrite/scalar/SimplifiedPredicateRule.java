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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.EliminateNegationsRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SimplifiedPredicateRule extends BottomUpScalarOperatorRewriteRule {
    private static final ImmutableMap<String, List<String>> TIME_FNS = ImmutableMap.<String, List<String>>builder()
            .put("years_", ImmutableList.of(FunctionSet.YEARS_ADD, FunctionSet.YEARS_SUB))
            .put("quarters_", ImmutableList.of(FunctionSet.QUARTERS_ADD, FunctionSet.QUARTERS_SUB))
            .put("months_", ImmutableList.of(FunctionSet.MONTHS_ADD, FunctionSet.MONTHS_SUB))
            .put("weeks_", ImmutableList.of(FunctionSet.WEEKS_ADD, FunctionSet.WEEKS_SUB))
            .put("days_", ImmutableList.of(FunctionSet.DAYS_ADD, FunctionSet.DAYS_SUB))
            .put("hours_", ImmutableList.of(FunctionSet.HOURS_ADD, FunctionSet.HOURS_SUB))
            .put("minutes_", ImmutableList.of(FunctionSet.MINUTES_ADD, FunctionSet.MINUTES_SUB))
            .put("seconds_", ImmutableList.of(FunctionSet.SECONDS_ADD, FunctionSet.SECONDS_SUB))
            .put("milliseconds_", ImmutableList.of(FunctionSet.MILLISECONDS_ADD, FunctionSet.MILLISECONDS_SUB))
            .put("microseconds_", ImmutableList.of(FunctionSet.MICROSECONDS_ADD, FunctionSet.MICROSECONDS_SUB))
            .put("date", ImmutableList.of(FunctionSet.DATE_ADD, FunctionSet.DATE_SUB))
            .build();
    private static final List<String> TIME_FN_NAMES = ImmutableList.<String>builder()
            .add(FunctionSet.YEARS_ADD).add(FunctionSet.YEARS_SUB)
            .add(FunctionSet.QUARTERS_ADD).add(FunctionSet.QUARTERS_SUB)
            .add(FunctionSet.MONTHS_ADD).add(FunctionSet.MONTHS_SUB)
            .add(FunctionSet.WEEKS_ADD).add(FunctionSet.WEEKS_SUB)
            .add(FunctionSet.DAYS_ADD).add(FunctionSet.DAYS_SUB)
            .add(FunctionSet.HOURS_ADD).add(FunctionSet.HOURS_SUB)
            .add(FunctionSet.MINUTES_ADD).add(FunctionSet.MINUTES_SUB)
            .add(FunctionSet.SECONDS_ADD).add(FunctionSet.SECONDS_SUB)
            .add(FunctionSet.MILLISECONDS_ADD).add(FunctionSet.MILLISECONDS_SUB)
            .add(FunctionSet.MICROSECONDS_ADD).add(FunctionSet.MICROSECONDS_SUB)
            .add(FunctionSet.DATE_ADD).add(FunctionSet.DATE_SUB)
            .build();

    private static final EliminateNegationsRewriter ELIMINATE_NEGATIONS_REWRITER = new EliminateNegationsRewriter();

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, ScalarOperatorRewriteContext context) {
        ScalarOperator result = simplifiedCaseWhenConstClause(operator);
        if (!(result instanceof CaseWhenOperator)) {
            return result;
        }
        return simplifiedCaseWhenToIfFunction((CaseWhenOperator) result);

    }

    // Trans one case when to if function, if function fast than case-when in BE
    // example:
    // case xx when 1 then 2 end => if (xx = 1, 2, NULL)
    ScalarOperator simplifiedCaseWhenToIfFunction(CaseWhenOperator operator) {
        if (operator.getWhenClauseSize() != 1) {
            return operator;
        }

        List<ScalarOperator> args = Lists.newArrayList();
        if (operator.hasCase()) {
            args.add(new BinaryPredicateOperator(BinaryType.EQ, operator.getCaseClause(),
                    operator.getWhenClause(0)));
        } else {
            args.add(operator.getWhenClause(0));
        }

        args.add(operator.getThenClause(0));

        if (operator.hasElse()) {
            args.add(operator.getElseClause());
        } else {
            args.add(ConstantOperator.createNull(operator.getType()));
        }

        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = Expr.getBuiltinFunction(FunctionSet.IF, argTypes, Function.CompareMode.IS_IDENTICAL);

        if (fn == null) {
            return operator;
        }
        if (operator.getChildren().stream().anyMatch(s -> s.getType().isDecimalV3())) {
            Function decimalFn = ScalarFunction.createVectorizedBuiltin(fn.getId(), fn.getFunctionName().getFunction(),
                    Arrays.stream(argTypes).collect(Collectors.toList()), fn.hasVarArgs(), operator.getType());
            decimalFn.setCouldApplyDictOptimize(fn.isCouldApplyDictOptimize());
            return new CallOperator(FunctionSet.IF, operator.getType(), args, decimalFn);
        }

        return new CallOperator(FunctionSet.IF, operator.getType(), args, fn);
    }

    ScalarOperator simplifiedCaseWhenConstClause(CaseWhenOperator operator) {
        // 0. if all result is same, direct return
        if (operator.hasElse()) {
            Set<ScalarOperator> result = Sets.newHashSet();
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                result.add(operator.getThenClause(i));
            }
            result.add(operator.getElseClause());

            if (result.size() == 1) {
                return operator.getElseClause();
            }
        }

        // 1. check caseClause
        ConstantOperator caseOp;
        if (operator.hasCase()) {
            if (!operator.getCaseClause().isConstantRef()) {
                return operator;
            }

            caseOp = (ConstantOperator) operator.getCaseClause();
        } else {
            caseOp = ConstantOperator.createBoolean(true);
        }

        // 2. return if caseClause is NullLiteral
        if (caseOp.isNull()) {
            if (operator.hasElse()) {
                return operator.getElseClause();
            }

            return ConstantOperator.createNull(operator.getType());
        }

        // 3. caseClause is constant, remove not equals when/Then Clause or return directly when equals
        Set<Integer> removeArgumentsSet = Sets.newHashSet();
        int whenStart = operator.getWhenStart();
        boolean allWhenClausConstant = true;
        for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
            if (!operator.getWhenClause(i).isConstantRef()) {
                allWhenClausConstant = false;
                break;
            }
        }

        for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
            if (operator.getWhenClause(i).isConstantRef()) {
                ConstantOperator when = (ConstantOperator) operator.getWhenClause(i);

                if (0 == caseOp.compareTo(when)) {
                    // only when all when clause is constant or first when equals, return directly
                    if (allWhenClausConstant || i == 0) {
                        return operator.getThenClause(i);
                    }
                } else {
                    // record argument index that should be removed.
                    removeArgumentsSet.add(2 * i + whenStart);
                    removeArgumentsSet.add(2 * i + whenStart + 1);
                }
            }
        }
        operator.removeArguments(removeArgumentsSet);

        // 4. if when isn't constant, return direct
        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            if (!operator.getWhenClause(i).isConstantRef()) {
                return operator;
            }
        }

        if (operator.hasElse()) {
            return operator.getElseClause();
        }

        return ConstantOperator.createNull(operator.getType());
    }

    //
    // Simplified Compound Predicate
    //
    // example:
    //        Compound(And)
    //        /      \
    //      true      false
    //
    // After rule:
    //         false
    //
    // example:
    //        Binary(or)
    //        /      \
    // A(Column)      true
    //
    // After rule:
    //          true
    //
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        // collect constant
        List<ConstantOperator> constantChildren = predicate.getChildren().stream().filter(ScalarOperator::isConstantRef)
                .map(d -> (ConstantOperator) d).collect(Collectors.toList());

        switch (predicate.getCompoundType()) {
            case AND: {
                if (constantChildren.stream().anyMatch(d -> (!d.isNull() && !d.getBoolean()))) {
                    // any false
                    return ConstantOperator.createBoolean(false);
                } else if (constantChildren.size() == 1) {
                    if (constantChildren.get(0).isNull()) {
                        // xxx and null
                        return predicate;
                    }

                    // (true and xxx)/(xxx and true)
                    ScalarOperator c0 = predicate.getChild(0);
                    if (c0.isConstantRef() && ((ConstantOperator) c0).getBoolean()) {
                        return predicate.getChild(1);
                    }

                    return predicate.getChild(0);
                } else if (constantChildren.size() == 2) {
                    if (constantChildren.stream().anyMatch(ConstantOperator::isNull)) {
                        // (true and null) or (null and null)
                        return ConstantOperator.createNull(Type.BOOLEAN);
                    }
                    // true and true
                    return ConstantOperator.createBoolean(true);
                }

                return predicate;
            }
            case OR: {
                if (constantChildren.stream().anyMatch(d -> (!d.isNull() && d.getBoolean()))) {
                    // any true
                    return ConstantOperator.createBoolean(true);
                } else if (constantChildren.size() == 1) {
                    if (constantChildren.get(0).isNull()) {
                        // xxx or null
                        return predicate;
                    }

                    // (false or xxx)/(xxx or false)
                    ScalarOperator c0 = predicate.getChild(0);
                    if (c0.isConstantRef() && !((ConstantOperator) c0).getBoolean()) {
                        return predicate.getChild(1);
                    }

                    return predicate.getChild(0);
                } else if (constantChildren.size() == 2) {
                    if (constantChildren.stream().anyMatch(ConstantOperator::isNull)) {
                        // (false or null) or (null or null)
                        return ConstantOperator.createNull(Type.BOOLEAN);
                    }
                    // false or false
                    return ConstantOperator.createBoolean(false);
                }

                return predicate;
            }
            case NOT: {
                ScalarOperator child = predicate.getChild(0);
                ScalarOperator result = child.accept(ELIMINATE_NEGATIONS_REWRITER, null);
                if (null == result) {
                    return predicate;
                }

                return result;
            }
        }

        return predicate;
    }

    //
    // example: a in ('xxxx')
    //
    // after: a = 'xxxx'
    //
    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        if (predicate.isSubquery()) {
            return predicate;
        }
        if (predicate.getChildren().size() != 2) {
            return predicate;
        }

        if (predicate.getChild(1) instanceof SubqueryOperator) {
            return predicate;
        }

        // like a in ("xxxx");
        if (predicate.isNotIn()) {
            return new BinaryPredicateOperator(BinaryType.NE, predicate.getChildren());
        } else {
            return new BinaryPredicateOperator(BinaryType.EQ, predicate.getChildren());
        }
    }

    // Simplify the comparison result of the same column
    // eg a >= a with not nullable transform to true constant;
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).isVariable() && predicate.getChild(0).equals(predicate.getChild(1))) {
            if (predicate.getBinaryType().equals(BinaryType.EQ_FOR_NULL)) {
                return ConstantOperator.createBoolean(true);
            }
        }
        return predicate;
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        if (FunctionSet.IF.equalsIgnoreCase(call.getFnName())) {
            return ifCall(call);
        } else if (FunctionSet.IFNULL.equalsIgnoreCase(call.getFnName())) {
            return ifNull(call);
        } else if (FunctionSet.ARRAY_MAP.equals(call.getFnName())) {
            return arrayMap(call);
        } else if (TIME_FN_NAMES.contains(call.getFnName())) {
            return simplifiedTimeFns(call);
        } else if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(call.getFnName())) {
            return simplifiedDateTrunc(call);
        }
        return call;
    }

    // reduce `date_sub(date_add(x, 1), 2)` -> `date_sub(x, 1)`
    private ScalarOperator simplifiedTimeFns(CallOperator call) {
        String fn = TIME_FNS.keySet().stream().filter(s -> call.getFnName().contains(s))
                .findFirst().orElse("impossible");
        if (!call.getChild(1).isConstantRef() || !Type.INT.equals(call.getChild(1).getType())) {
            return call;
        }
        if (!(call.getChild(0) instanceof CallOperator)) {
            return call;
        }

        CallOperator child = call.getChild(0).cast();
        if (!child.getFnName().contains(fn) || !TIME_FN_NAMES.contains(child.getFnName())) {
            return call;
        }
        if (!child.getChild(1).isConstantRef() || !Type.INT.equals(child.getChild(1).getType())) {
            return call;
        }

        ConstantOperator l1 = call.getChild(1).cast();
        ConstantOperator l2 = call.getChild(0).getChild(1).cast();

        if (l1.isNull() || l2.isNull()) {
            return ConstantOperator.createNull(call.getType());
        }

        int i1 = call.getFnName().contains("add") ? l1.getInt() : l1.getInt() * -1;
        int i2 = child.getFnName().contains("add") ? l2.getInt() : l2.getInt() * -1;

        int result = i1 + i2;
        ConstantOperator interval = ConstantOperator.createInt(Math.abs(result));

        if (result != 0) {
            String fnName = result < 0 ? Objects.requireNonNull(TIME_FNS.get(fn)).get(1) :
                    Objects.requireNonNull(TIME_FNS.get(fn)).get(0);
            Function newFn = Expr.getBuiltinFunction(fnName, call.getFunction().getArgs(),
                    Function.CompareMode.IS_SUPERTYPE_OF);
            return new CallOperator(fnName, call.getType(), Lists.newArrayList(child.getChild(0), interval), newFn);
        } else {
            return child.getChild(0);
        }
    }

    private ScalarOperator simplifiedDateTrunc(CallOperator call) {
        if (!call.getType().isDate() || !call.getChild(0).isConstantRef()) {
            return call;
        }
        ConstantOperator child = call.getChild(0).cast();
        if ("day".equalsIgnoreCase(child.toString())) {
            return call.getChild(1);
        }
        return call;
    }

    // Reduce array_map whose lambda functions is trivial
    // e.g. array_map((x,y)->x, arr1,arr2) is reduced to arr1
    private static ScalarOperator arrayMap(CallOperator call) {
        int index = ((LambdaFunctionOperator) call.getChild(0)).canReduce();
        if (index > 0) {
            return call.getChild(index);
        }
        return call;
    }

    private static ScalarOperator ifNull(CallOperator call) {
        if (!call.getChild(0).isConstantRef()) {
            return call;
        }

        return ((ConstantOperator) call.getChild(0)).isNull() ? call.getChild(1) : call.getChild(0);
    }

    private static ScalarOperator ifCall(CallOperator call) {
        if (!call.getChild(0).isConstantRef()) {
            return call;
        }

        return ((ConstantOperator) call.getChild(0)).getBoolean() ? call.getChild(1) : call.getChild(2);
    }
}
