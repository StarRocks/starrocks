// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class MvNormalizePredicateRule extends NormalizePredicateRule {
    // Normalize Binary Predicate
    // for integer type:
    // a < 3 => a <= 2
    // a > 3 => a >= 4
    // a = 3 => a >= 3 and a <= 3
    // a != 3 => a > 3 and a < 3 (which will be normalized further)
    //
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator tmp = super.visitBinaryPredicate(predicate, context);
        Preconditions.checkState(tmp instanceof BinaryPredicateOperator);
        BinaryPredicateOperator binary = (BinaryPredicateOperator) tmp;
        if (binary.getChild(0).isVariable() && binary.getChild(1).isConstantRef()) {
            ConstantOperator constantOperator = (ConstantOperator) binary.getChild(1);
            if (!constantOperator.getType().isIntegerType()) {
                return tmp;
            }
            ConstantOperator one = createConstantIntegerOne(constantOperator.getType());
            if (one == null) {
                return tmp;
            }
            Type[] argsType = {constantOperator.getType(), constantOperator.getType()};
            switch (binary.getBinaryType()) {
                case LT:
                    Function substractFn = findArithmeticFunction(argsType, FunctionSet.SUBTRACT);
                    CallOperator sub = new CallOperator(FunctionSet.SUBTRACT,
                            substractFn.getReturnType(), Lists.newArrayList(constantOperator, one), substractFn);
                    return new BinaryPredicateOperator(BinaryType.LE, binary.getChild(0), sub);
                case GT:
                    Function addFn = findArithmeticFunction(argsType, FunctionSet.ADD);
                    CallOperator add = new CallOperator(
                            FunctionSet.ADD, addFn.getReturnType(), Lists.newArrayList(constantOperator, one), addFn);
                    return new BinaryPredicateOperator(BinaryType.GE, binary.getChild(0), add);
                case EQ:
                    BinaryPredicateOperator gePart =
                            new BinaryPredicateOperator(BinaryType.GE, binary.getChild(0), constantOperator);
                    BinaryPredicateOperator lePart =
                            new BinaryPredicateOperator(BinaryType.LE, binary.getChild(0), constantOperator);
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, gePart, lePart);
                case NE:
                    BinaryPredicateOperator gtPart =
                            new BinaryPredicateOperator(BinaryType.GT, binary.getChild(0), constantOperator);
                    BinaryPredicateOperator ltPart =
                            new BinaryPredicateOperator(BinaryType.LT, binary.getChild(0), constantOperator);
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, gtPart, ltPart);
                default:
                    break;
            }
        }
        return tmp;
    }

    private Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, Function.CompareMode.IS_IDENTICAL);
    }

    // should maintain sequence for case:
    // a like "%hello%" and (b * c = 100 or b * c = 200)
    // (b * c = 200 or b * c = 100) and a like "%hello%"
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        Set<ScalarOperator> after = Sets.newTreeSet(new Comparator<ScalarOperator>() {
            @Override
            public int compare(ScalarOperator o1, ScalarOperator o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        if (predicate.isAnd()) {
            List<ScalarOperator> before = Utils.extractConjuncts(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
        } else if (predicate.isOr()) {
            List<ScalarOperator> before = Utils.extractDisjunctive(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
        } else {
            // for not
            return predicate;
        }
        return Utils.compoundAnd(Lists.newArrayList(after));
    }

    private ConstantOperator createConstantIntegerOne(Type type) {
        if (Type.SMALLINT.equals(type)) {
            return ConstantOperator.createSmallInt((short) 1);
        } else if (Type.INT.equals(type)) {
            return ConstantOperator.createInt(1);
        } else if (Type.BIGINT.equals(type)) {
            return ConstantOperator.createBigint(1L);
        } else if (Type.LARGEINT.equals(type)) {
            return ConstantOperator.createLargeInt(BigInteger.ONE);
        }
        return null;
    }
}
