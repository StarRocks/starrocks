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

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class FoldConstantsRule extends BottomUpScalarOperatorRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantsRule.class);

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        if (call.isAggregate() || notAllConstant(call.getChildren())) {
            return call;
        }
        return ScalarOperatorEvaluator.INSTANCE.evaluation(call);
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        ScalarOperator c1 = predicate.getChild(0);
        if (c1.isConstantRef() && ((ConstantOperator) c1).isNull()) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        ConstantOperator child1 = (ConstantOperator) predicate.getChild(0);
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            if (0 == child1.compareTo((ConstantOperator) predicate.getChild(i))) {
                return ConstantOperator.createBoolean(!predicate.isNotIn());
            }
        }

        if (hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        return ConstantOperator.createBoolean(predicate.isNotIn());
    }

    //
    // Add cast function when children's type different with parent required type
    //
    // example:
    //        isNull(+)
    //           |
    //         b(int)
    //
    // After rule:
    //          false
    // example:
    //        isNull(+)
    //           |
    //         b(Null)
    //
    // After rule:
    //          true
    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).getType().equals(Type.NULL)) {
            return ConstantOperator.createBoolean(!predicate.isNotNull());
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        if (predicate.isNotNull()) {
            return ConstantOperator.createBoolean(!hasNull(predicate.getChildren()));
        }

        return ConstantOperator.createBoolean(hasNull(predicate.getChildren()));
    }

    //
    // cast value type
    //
    // example:
    //        Cast(Int)
    //            |
    //          "1"(char)
    //
    // After rule:
    //           1(Int)
    //
    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, ScalarOperatorRewriteContext context) {
        // cast null/null_type to any type
        if (hasNull(operator.getChildren()) || operator.getChild(0).getType().isNull()) {
            return ConstantOperator.createNull(operator.getType());
        }

        if (notAllConstant(operator.getChildren())) {
            return operator;
        }

        ConstantOperator child = (ConstantOperator) operator.getChild(0);

        try {
            return child.castTo(operator.getType());
        } catch (Exception e) {
            LOG.debug("Fold cast constant error: " + operator + ", " + child.toString());
            return operator;
        }
    }

    //
    // Fold Const Binary Predicate
    //
    // example:
    //        Binary(=)
    //        /      \
    //      "a"      "b"
    //
    // After rule:
    //         false
    //
    // example:
    //        Binary(<=)
    //        /      \
    //       1        2
    //
    // After rule:
    //         true
    //
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (!BinaryPredicateOperator.BinaryType.EQ_FOR_NULL.equals(predicate.getBinaryType())
                && hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        ConstantOperator child1 = (ConstantOperator) predicate.getChild(0);
        ConstantOperator child2 = (ConstantOperator) predicate.getChild(1);

        switch (predicate.getBinaryType()) {
            case EQ:
                return ConstantOperator.createBoolean(child1.compareTo(child2) == 0);
            case NE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) != 0);
            case EQ_FOR_NULL: {
                if (child1.isNull() && child2.isNull()) {
                    return ConstantOperator.createBoolean(true);
                } else if (!child1.isNull() && !child2.isNull()) {
                    return ConstantOperator.createBoolean(child1.compareTo(child2) == 0);
                } else {
                    return ConstantOperator.createBoolean(false);
                }
            }
            case GE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) >= 0);
            case GT:
                return ConstantOperator.createBoolean(child1.compareTo(child2) > 0);
            case LE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) <= 0);
            case LT:
                return ConstantOperator.createBoolean(child1.compareTo(child2) < 0);
        }

        return predicate;
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate,
                                                     ScalarOperatorRewriteContext context) {
        if (hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }
        return predicate;
    }

    private boolean notAllConstant(List<ScalarOperator> operators) {
        return !operators.stream().allMatch(ScalarOperator::isConstantRef);
    }

    private boolean hasNull(List<ScalarOperator> operators) {
        return operators.stream().anyMatch(d -> (d.isConstantRef()) && ((ConstantOperator) d).isNull());
    }
}
