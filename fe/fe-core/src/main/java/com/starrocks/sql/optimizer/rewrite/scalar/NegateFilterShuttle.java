// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

public class NegateFilterShuttle extends BaseScalarOperatorShuttle {

    private static NegateFilterShuttle INSTANCE = new NegateFilterShuttle();

    public static NegateFilterShuttle getInstance() {
        return INSTANCE;
    }

    public ScalarOperator negateFilter(ScalarOperator scalarOperator) {
        return scalarOperator.accept(this, null);
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        ScalarOperator negation;
        if (CompoundType.NOT == predicate.getCompoundType()) {
            negation = predicate.getChild(0);
            if (predicate.getChild(0).isNullable()) {
                return new CompoundPredicateOperator(CompoundType.OR, negation,
                        new IsNullPredicateOperator(predicate.getChild(0)));
            } else {
                return negation;
            }

        } else {
            negation = new CompoundPredicateOperator(CompoundType.NOT, predicate);
            if (predicate.isNullable()) {
                return new CompoundPredicateOperator(CompoundType.OR, negation, new IsNullPredicateOperator(predicate));
            } else {
                return negation;
            }
        }
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        ScalarOperator negation;
        if (BinaryPredicateOperator.BinaryType.EQ_FOR_NULL == predicate.getBinaryType()) {
            negation = new CompoundPredicateOperator(CompoundType.NOT, predicate);
        } else {
            negation = predicate.negative();
        }

        if (predicate.getChild(0).isNullable()) {
            ScalarOperator isNull = new IsNullPredicateOperator(predicate.getChild(0));
            return new CompoundPredicateOperator(CompoundType.OR, negation, isNull);
        } else {
            return negation;
        }
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
        ScalarOperator negation = new InPredicateOperator(!predicate.isNotIn(), predicate.getChildren());
        if (predicate.getChild(0).isNullable()) {
            ScalarOperator isNull = new IsNullPredicateOperator(predicate.getChild(0));
            return new CompoundPredicateOperator(CompoundType.OR, negation, isNull);
        } else {
            return negation;
        }
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        return new IsNullPredicateOperator(!predicate.isNotNull(), predicate.getChild(0));
    }

}
