// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PredicateSplit {
    // column equality predicates conjuncts
    // a column equality predicate of the form (Ti.Cp =Tj.Cq)
    private final ScalarOperator equalPredicates;
    // range predicates conjuncts
    // a range predicate is any atomic predicate of the form (Ti.Cp op c)
    // where c is a constant and op is one of the operators “<”, “≤”, “=”, “≥”, “>”
    private final ScalarOperator rangePredicates;
    // residual predicates conjuncts
    // residual predicates containing all conjuncts except the two types above
    // eg: Ti.Cp like "%abc%"
    private final ScalarOperator residualPredicates;

    private PredicateSplit(ScalarOperator equalPredicates,
                           ScalarOperator rangePredicates,
                           ScalarOperator residualPredicates) {
        this.equalPredicates = equalPredicates;
        this.rangePredicates = rangePredicates;
        this.residualPredicates = residualPredicates;
    }

    public static PredicateSplit of(ScalarOperator equalPredicates,
                             ScalarOperator rangePredicates,
                             ScalarOperator residualPredicates) {
        return new PredicateSplit(equalPredicates, rangePredicates, residualPredicates);
    }

    public ScalarOperator getEqualPredicates() {
        return equalPredicates;
    }

    public ScalarOperator getRangePredicates() {
        return rangePredicates;
    }

    public ScalarOperator getResidualPredicates() {
        return residualPredicates;
    }

    public ScalarOperator toScalarOperator() {
        return Utils.compoundAnd(equalPredicates, rangePredicates, residualPredicates);
    }

    public List<ScalarOperator> getPredicates() {
        return Lists.newArrayList(equalPredicates, rangePredicates, residualPredicates);
    }

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    public static PredicateSplit splitPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return PredicateSplit.of(null, null, null);
        }
        List<ScalarOperator> predicateConjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
        List<ScalarOperator> rangePredicates = Lists.newArrayList();
        List<ScalarOperator> residualPredicates = Lists.newArrayList();
        // Split predicates into three kinds:
        //  - Equal Predicates:  col1 = col2 (both col1 and col2 are ColumnRefs)
        //  - Range Predicates:  col1 >/>=/=/</<= 2 (col1 is ColumnRef and right is ConstantRef)
        // TODO: support NotEqual/Or Range as range predicates
        //  - Other Predicates:  others(eg: NonEqual BinaryPredicateOperator or others)
        for (ScalarOperator scalarOperator : predicateConjuncts) {
            if (scalarOperator instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binary = (BinaryPredicateOperator) scalarOperator;
                ScalarOperator leftChild = scalarOperator.getChild(0);
                ScalarOperator rightChild = scalarOperator.getChild(1);
                BinaryPredicateOperator.BinaryType binaryType = binary.getBinaryType();
                if (binaryType.isEqual() && leftChild.isColumnRef() && rightChild.isColumnRef()) {
                    columnEqualityPredicates.add(scalarOperator);
                } else if (binaryType.isEqualOrRange() && leftChild.isColumnRef() && rightChild.isConstantRef()) {
                    rangePredicates.add(scalarOperator);
                } else {
                    residualPredicates.add(scalarOperator);
                }
            } else {
                residualPredicates.add(scalarOperator);
            }
        }
        return PredicateSplit.of(Utils.compoundAnd(columnEqualityPredicates), Utils.compoundAnd(rangePredicates),
                Utils.compoundAnd(residualPredicates));
    }
}
