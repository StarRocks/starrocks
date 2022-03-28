// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.Optional;

//
// Reduce duplicate cast functions
//
// example:
//             Binary(+)
//             /     \
//   cast(bigint)    cast(bigint)
//          /         \
//   cast(int)        b(bigint)
//       /
// a(String)
//
// After rule:
//             Binary(+)
//             /     \
//   cast(bigint)    b(bigint)
//          /
//   a(String)
//
public class ReduceCastRule extends TopDownScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, ScalarOperatorRewriteContext context) {
        // remove duplicate cast
        if (operator.getChild(0) instanceof CastOperator && checkCastTypeReduceAble(operator.getType(),
                operator.getChild(0).getType(), operator.getChild(0).getChild(0).getType())) {
            ScalarOperator newCastOperator = operator.clone();
            newCastOperator.setChild(0, newCastOperator.getChild(0).getChild(0));
            return newCastOperator;
        }

        // remove same type cast
        if (operator.getType().isDecimalOfAnyVersion()) {
            if (operator.getType().getPrimitiveType().equals(operator.getChild(0).getType().getPrimitiveType())
                    && operator.getType().equals(operator.getChild(0).getType())) {
                return operator.getChild(0);
            }
        } else if (operator.getType().matchesType(operator.getChild(0).getType())) {
            return operator.getChild(0);
        }

        return operator;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator operator,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator child1 = operator.getChild(0);
        ScalarOperator child2 = operator.getChild(1);

        if (!(child1 instanceof CastOperator && child2.isConstantRef())) {
            return operator;
        }

        ScalarOperator castChild = child1.getChild(0);

        // BinaryPredicate involving Decimal
        if (castChild.getType().isDecimalOfAnyVersion()
                || child1.getType().isDecimalOfAnyVersion()
                || child2.getType().isDecimalOfAnyVersion()) {
            Optional<ScalarOperator> resultChild2 =
                    Utils.tryDecimalCastConstant((CastOperator) child1, (ConstantOperator) child2);
            return resultChild2
                    .map(scalarOperator -> new BinaryPredicateOperator(operator.getBinaryType(), castChild,
                            scalarOperator))
                    .orElse(operator);
        }

        if (!(castChild.getType().isNumericType() && child2.getType().isNumericType())) {
            return operator;
        }

        Optional<ScalarOperator> resultChild2 = Utils.tryCastConstant(child2, castChild.getType());
        return resultChild2
                .map(scalarOperator -> new BinaryPredicateOperator(operator.getBinaryType(), castChild, scalarOperator))
                .orElse(operator);
    }

    public boolean checkCastTypeReduceAble(Type parent, Type child, Type grandChild) {
        int parentSlotSize = parent.getTypeSize();
        int childSlotSize = child.getTypeSize();
        int grandChildSlotSize = grandChild.getTypeSize();

        if (parent.isDecimalOfAnyVersion() || child.isDecimalOfAnyVersion() || grandChild.isDecimalOfAnyVersion()) {
            return false;
        }

        if (!(parent.isNumericType() || parent.isStringType()) ||
                !(child.isNumericType() || child.isBoolean()) ||
                !(grandChild.isNumericType() || grandChild.isBoolean())) {
            return false;
        }

        // cascaded cast cannot be reduced if middle type's size is smaller than two sides
        // e.g. cast(cast(smallint as tinyint) as int)
        if (parentSlotSize > childSlotSize && childSlotSize < grandChildSlotSize) {
            return false;
        }

        Type childCompatibleType = Type.getAssignmentCompatibleType(grandChild, child, true);
        Type parentCompatibleType = Type.getAssignmentCompatibleType(child, parent, true);
        return childCompatibleType != Type.INVALID && parentCompatibleType != Type.INVALID;
    }
}
