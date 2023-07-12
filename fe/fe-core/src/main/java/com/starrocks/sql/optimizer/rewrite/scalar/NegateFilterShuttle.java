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

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ_FOR_NULL;

public class NegateFilterShuttle extends BaseScalarOperatorShuttle {

    private static NegateFilterShuttle INSTANCE = new NegateFilterShuttle();

    private NegateFilterShuttle() {}

    public static NegateFilterShuttle getInstance() {
        return INSTANCE;
    }

    public ScalarOperator negateFilter(ScalarOperator scalarOperator) {
        return scalarOperator.accept(this, null);
    }

    @Override
    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
        ScalarOperator negation = new CompoundPredicateOperator(CompoundType.NOT, scalarOperator);
        if (scalarOperator.isNullable()) {
            return new CompoundPredicateOperator(CompoundType.OR, negation, new IsNullPredicateOperator(scalarOperator));
        }
        return negation;
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
        if (EQ_FOR_NULL == predicate.getBinaryType()) {
            return new CompoundPredicateOperator(CompoundType.NOT, predicate);
        }

        negation = predicate.negative();

        if (predicate.getChild(1).isNullable()) {
            ScalarOperator isNull = new IsNullPredicateOperator(predicate);
            return new CompoundPredicateOperator(CompoundType.OR, negation, isNull);
        } else if (predicate.getChild(0).isNullable()) {
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
