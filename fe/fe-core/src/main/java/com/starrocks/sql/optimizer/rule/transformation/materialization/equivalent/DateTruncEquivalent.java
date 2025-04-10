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
package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

import java.util.Set;

public class DateTruncEquivalent extends IPredicateRewriteEquivalent {
    public static final DateTruncEquivalent INSTANCE = new DateTruncEquivalent();

    public DateTruncEquivalent() {}

    /**
     * TODO: we can support this later.
     * Change date_trunc('month', dt) to col = '2023-12-01' will get a wrong result.
     * MV       : select date_trunc('day', dt) as dt from t
     * Query1   : select date_trunc('month, dt) from t dt = '2023-11-01'
     * -- cannot be rewritten, rewrite  result will be wrong
     * Rewritten: select date_trunc('month, dt) from t where date_trunc('month', dt) = '2023-11-01'
     *
     * Query2   : select date_trunc('month, dt) from t where dt between '2023-11-01' and '2023-12-01'
     * -- cannot be rewritten, dt='2023-12-01' doesn't match with date_trunc('month', dt)= '2023-11-01'
     * Rewritten : select date_trunc('month, dt) from t where date_trunc('month', dt) between '2023-11-01' and '2023-12-01'
     */
    private static Set<BinaryType> SUPPORTED_BINARY_TYPES = ImmutableSet.of(
            BinaryType.GE,
            BinaryType.GT,
            BinaryType.LT
    );

    public static boolean isSupportedBinaryType(BinaryType binaryType) {
        return SUPPORTED_BINARY_TYPES.contains(binaryType);
    }

    @Override
    public boolean isEquivalent(ScalarOperator op1, ConstantOperator op2) {
        if (!(op1 instanceof CallOperator)) {
            return false;
        }
        CallOperator func = (CallOperator) op1;
        if (!func.getFnName().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return false;
        }
        if (op2.getType().getPrimitiveType() != PrimitiveType.DATETIME) {
            return false;
        }
        if (!(func.getChild(1) instanceof ColumnRefOperator)) {
            return false;
        }
        ConstantOperator sliced = ScalarOperatorFunctions.dateTrunc(
                ((ConstantOperator) op1.getChild(0)),
                op2);
        return sliced.equals(op2);
    }

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator input) {
        if (input == null || !(input instanceof CallOperator)) {
            return null;
        }
        CallOperator func = (CallOperator) input;
        if (!func.getFnName().equals(FunctionSet.DATE_TRUNC)) {
            return null;
        }
        if (!func.getChild(1).isColumnRef()) {
            return null;
        }
        return new RewriteEquivalentContext(func.getChild(1), input);
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (newInput instanceof BinaryPredicateOperator) {
            ScalarOperator left = newInput.getChild(0);
            ScalarOperator right = newInput.getChild(1);

            if (!right.isConstantRef() || !left.equals(eqContext.getEquivalent())) {
                return null;
            }
            if (!isEquivalent(eqContext.getInput(), (ConstantOperator) right)) {
                return null;
            }
            BinaryPredicateOperator predicate = (BinaryPredicateOperator) newInput.clone();
            if (!isSupportedBinaryType(predicate.getBinaryType())) {
                return null;
            }
            predicate.setChild(0, replace);
            return predicate;
        } else {
            return null;
        }
    }
}
