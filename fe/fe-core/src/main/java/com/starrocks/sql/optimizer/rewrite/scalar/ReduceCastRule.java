// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
        // optimize cast
        boolean isConstantOperator = child2 instanceof ConstantOperator;
        if (child1 instanceof CastOperator && isConstantOperator) {
            boolean isDateType = castChild.getType().isDate();
            boolean isDatetimeType = child2.getType().isDatetime();
            if (isDateType && isDatetimeType) {
                return optimizeCastDateTimeToDate(operator);
            }
        }

        // BinaryPredicate involving Decimal
        if (castChild.getType().isDecimalOfAnyVersion()
                || child1.getType().isDecimalOfAnyVersion()
                || child2.getType().isDecimalOfAnyVersion()) {
            Optional<ScalarOperator> resultChild2 =
                    Utils.tryDecimalCastConstant((CastOperator) child1, (ConstantOperator) child2);
            return resultChild2
                    .map(scalarOperator -> new BinaryPredicateOperator(operator.getBinaryType(), castChild, scalarOperator))
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
        if (parentSlotSize > childSlotSize && grandChildSlotSize > childSlotSize) {
            return false;
        }
        Type childCompatibleType = Type.getAssignmentCompatibleType(grandChild, child, true);
        Type parentCompatibleType = Type.getAssignmentCompatibleType(child, parent, true);
        return childCompatibleType != Type.INVALID && parentCompatibleType != Type.INVALID;
    }

    public ScalarOperator optimizeCastDateTimeToDate(BinaryPredicateOperator operator) {
        ScalarOperator child1 = operator.getChild(0);
        ScalarOperator child2 = operator.getChild(1);
        if (!(child1 instanceof CastOperator && child2.isConstantRef())) {
            return operator;
        }
        ScalarOperator castChild = child1.getChild(0);
        LocalDateTime originalDateTime = ((ConstantOperator) child2).getDatetime();
        LocalDateTime bottomLocalDateTime = ((ConstantOperator) child2).getDatetime().toLocalDate().atTime(0, 0, 0, 0);
        LocalDateTime targetLocalDateTime;
        BinaryPredicateOperator.BinaryType binaryType = operator.getBinaryType();
        int offset;
        if (binaryType.equals(BinaryPredicateOperator.BinaryType.GE)) {
            if (originalDateTime.isEqual(bottomLocalDateTime)) {
                offset = 0;
            } else {
                offset = 1;
            }
            targetLocalDateTime = bottomLocalDateTime.plusDays(offset);
            ConstantOperator newDate = ConstantOperator.createDate(targetLocalDateTime.truncatedTo(ChronoUnit.DAYS));
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDate);
            return binaryPredicateOperator;
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.GT)) {
            offset = 1;
            targetLocalDateTime = bottomLocalDateTime.plusDays(offset);
            ConstantOperator newDate = ConstantOperator.createDate(targetLocalDateTime.truncatedTo(ChronoUnit.DAYS));
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDate);
            return binaryPredicateOperator;
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.LE)) {
            offset = 0;
            targetLocalDateTime = bottomLocalDateTime.plusDays(offset);
            ConstantOperator newDate = ConstantOperator.createDate(targetLocalDateTime.truncatedTo(ChronoUnit.DAYS));
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castChild, newDate);
            return binaryPredicateOperator;
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.LT)) {
            offset = -1;
            targetLocalDateTime = bottomLocalDateTime.plusDays(offset);
            ConstantOperator newDate = ConstantOperator.createDate(targetLocalDateTime.truncatedTo(ChronoUnit.DAYS));
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castChild, newDate);
            return binaryPredicateOperator;
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.EQ)) {
            if (!originalDateTime.isEqual(bottomLocalDateTime)) {
                return operator;
            } else {
                ConstantOperator newDate = ConstantOperator.createDate(bottomLocalDateTime.truncatedTo(ChronoUnit.DAYS));
                BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, castChild, newDate);
                return binaryPredicateOperator;
            }
        } else {
            // current not support !=
            return operator;
        }
    }
}
