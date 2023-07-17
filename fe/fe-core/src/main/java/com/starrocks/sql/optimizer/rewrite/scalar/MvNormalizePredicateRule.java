// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil.findArithmeticFunction;

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

    // should maintain sequence for case:
    // a like "%hello%" and (b * c = 100 or b * c = 200)
    // (b * c = 200 or b * c = 100) and a like "%hello%"
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        Comparator<ScalarOperator> byToString =
                (ScalarOperator o1, ScalarOperator o2) -> o1.toString().compareTo(o2.toString());
        Set<ScalarOperator> after = Sets.newTreeSet(byToString);
        if (predicate.isAnd()) {
            List<ScalarOperator> before = Utils.extractConjuncts(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
            return Utils.compoundAnd(Lists.newArrayList(after));
        } else if (predicate.isOr()) {
            List<ScalarOperator> before = Utils.extractDisjunctive(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
            return Utils.compoundOr(Lists.newArrayList(after));
        } else {
            // for not
            return predicate;
        }
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

    // NOTE: View-Delta Join may produce redundant compensation predicates as below.
    // eg:
    // A(pk: a1)  <-> B (pk: b1)
    // A(pk: a2)  <-> C (pk: c1)
    // Query: a1 = a2
    // view delta deduce: a1 = a2 , a1 = b1, a2 = c1
    // src equal classes  : a1, a2, b1, c1
    // dest equal classes :
    //            target1 : a1, b1
    //            target2 : a2, c1
    // For src equal classes and target1 equal classes, compensation predicates should be:
    //  query.a2 = target.a1 and c1 = b1
    // For src equal classes and target2 equal classes, compensation predicates should be:
    //  query.a1 = target.a2 and b1 = c1
    public static ScalarOperator pruneRedundantPredicates(ScalarOperator predicate) {
        List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
        List<ScalarOperator> prunedPredicates = pruneRedundantPredicates(predicates);
        if (predicates != null) {
            return Utils.compoundAnd(prunedPredicates);
        }
        return predicate;
    }

    public static List<ScalarOperator> pruneRedundantPredicates(List<ScalarOperator> scalarOperators) {
        List<ScalarOperator> prunedPredicates = pruneEqualBinaryPredicates(scalarOperators);
        if (prunedPredicates != null) {
            return prunedPredicates;
        }
        return scalarOperators;
    }

    // a = b & b = a => a = b
    private static List<ScalarOperator> pruneEqualBinaryPredicates(List<ScalarOperator> scalarOperators) {
        Map<ColumnRefOperator, ColumnRefOperator> visited = Maps.newHashMap();
        List<ScalarOperator> prunedPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : scalarOperators) {
            if (!(scalarOperator instanceof BinaryPredicateOperator)) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            BinaryPredicateOperator binaryPred = (BinaryPredicateOperator) scalarOperator;
            if (!binaryPred.getBinaryType().isEqual()) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            if (!binaryPred.getChild(0).isColumnRef() || !binaryPred.getChild(1).isColumnRef()) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            ColumnRefOperator col0 = (ColumnRefOperator) (binaryPred.getChild(0));
            ColumnRefOperator col1 = (ColumnRefOperator) (binaryPred.getChild(1));
            if (visited.containsKey(col0) && visited.get(col0).equals(col1) ||
                    visited.containsKey(col1) && visited.get(col1).equals(col0)) {
                continue;
            }
            prunedPredicates.add(scalarOperator);
            visited.put(col0, col1);
        }
        return prunedPredicates;
    }
}
