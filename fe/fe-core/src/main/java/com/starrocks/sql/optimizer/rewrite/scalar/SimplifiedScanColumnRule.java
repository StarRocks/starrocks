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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

public class SimplifiedScanColumnRule extends BottomUpScalarOperatorRewriteRule {
    //Simplify the comparison result of the same column
    //eg a >= a with not nullable transform to true constant;
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).isVariable() && predicate.getChild(0).equals(predicate.getChild(1))) {
            if (predicate.getBinaryType().equals(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL)) {
                return ConstantOperator.createBoolean(true);
            } else if (!predicate.getChild(0).isNullable()) {
                // The nullable is not accurate if child node will produce null. like:
                // select t2.a = t2.a from t1 left outer join t2
                // so we only run the rule in scan node
                switch (predicate.getBinaryType()) {
                    case EQ:
                    case EQ_FOR_NULL:
                    case GE:
                    case LE:
                        return ConstantOperator.createBoolean(true);
                    case NE:
                    case LT:
                    case GT:
                        return ConstantOperator.createBoolean(false);
                }
            }
        }
        return predicate;
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator child = predicate.getChild(0);
        if (child.isColumnRef() && !child.isNullable()) {
            return ConstantOperator.createBoolean(predicate.isNotNull());
        }

        return predicate;
    }
}
