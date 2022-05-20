// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
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
        // abandon cast function when cast datetime to date
        boolean isOriginalDateType = castChild.getType().isDate();
        boolean isDatetimeType = child2.getType().isDatetime();
        if (isOriginalDateType && isDatetimeType) {
            return reduceDateToDatetimeCast(operator);
        }

        boolean isOriginalDateTimeType = castChild.getType().isDatetime();
        boolean isDateType = child2.getType().isDate();
        if (isOriginalDateTimeType && isDateType) {
            return reduceDatetimeToDateCast(operator);
        }

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

    public ScalarOperator reduceDateToDatetimeCast(BinaryPredicateOperator operator) {
        ScalarOperator child1 = operator.getChild(0);
        ScalarOperator child2 = operator.getChild(1);
        ScalarOperator castChild = child1.getChild(0);

        if (((ConstantOperator) child2).isNull()) {
            return operator;
        }

        LocalDateTime originalDateTime = ((ConstantOperator) child2).getDatetime();
        LocalDateTime bottomDateTime = ((ConstantOperator) child2).getDatetime().toLocalDate().atTime(0, 0, 0, 0);
        LocalDateTime targetDateTime;
        BinaryPredicateOperator.BinaryType binaryType = operator.getBinaryType();
        int offset;
        BinaryPredicateOperator resultBinaryPredicateOperator;
        ConstantOperator newDate;
        switch (binaryType) {
            case GE:
                // when the BinaryType is >= ,cast dateTime to minimum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-29)
                if (originalDateTime.isEqual(bottomDateTime)) {
                    offset = 0;
                } else {
                    offset = 1;
                }
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime.truncatedTo(ChronoUnit.DAYS));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDate);
                break;
            case GT:
                // when the BinaryType is > ,cast dateTime to minimum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-29)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-29)
                offset = 1;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime.truncatedTo(ChronoUnit.DAYS));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDate);
                break;
            case LE:
                // when the BinaryType is <= ,cast dateTime to maximum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-27)
                offset = 0;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime.truncatedTo(ChronoUnit.DAYS));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castChild, newDate);
                break;
            case LT:
                // when the BinaryType is < ,cast dateTime to maximum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-27)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-28)
                if (originalDateTime.isEqual(bottomDateTime)) {
                    offset = -1;
                } else {
                    offset = 0;
                }
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime.truncatedTo(ChronoUnit.DAYS));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castChild, newDate);
                break;
            case EQ:
                // when the BinaryType is = ,cast dateTime to equivalent date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                if (!originalDateTime.isEqual(bottomDateTime)) {
                    resultBinaryPredicateOperator = operator;
                } else {
                    newDate = ConstantOperator.createDate(bottomDateTime.truncatedTo(ChronoUnit.DAYS));
                    resultBinaryPredicateOperator =
                            new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, castChild, newDate);
                }
                break;
            default:
                // current not support !=
                resultBinaryPredicateOperator = operator;
                break;
        }
        return resultBinaryPredicateOperator;
    }

    public ScalarOperator reduceDatetimeToDateCast(BinaryPredicateOperator operator) {
        ScalarOperator child1 = operator.getChild(0);
        ScalarOperator child2 = operator.getChild(1);
        ScalarOperator castChild = child1.getChild(0);
        LocalDateTime originalDate = ((ConstantOperator) child2).getDate();
        LocalDateTime targetDate;
        BinaryPredicateOperator.BinaryType binaryType = operator.getBinaryType();
        int offset;
        ScalarOperator resultBinaryPredicateOperator;
        ConstantOperator newDatetime;
        switch (binaryType) {
            case GE:
                // when the BinaryType is >= , cast date to equivalent datetime type
                // E.g. cast(id_datetime as date) >= 2021-12-28
                // optimized to id_datetime >= 2021-12-28 00:00:00.0
                offset = 0;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate.toLocalDate().atTime(0, 0, 0, 0));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDatetime);
                break;
            case GT:
                // when the BinaryType is > , cast date to equivalent datetime type of next day
                // E.g. cast(id_datetime as date) > 2021-12-28
                // optimized to id_datetime >= 2021-12-29 00:00:00.0
                offset = 1;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate.toLocalDate().atTime(0, 0, 0, 0));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild, newDatetime);
                break;
            case LE:
                // when the BinaryType is <= , cast date to equivalent datetime type of next day
                // E.g. cast(id_datetime as date) <= 2021-12-28
                // optimized to id_datetime < 2021-12-29 00:00:00.0
                offset = 1;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate.toLocalDate().atTime(0, 0, 0, 0));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castChild, newDatetime);
                break;
            case LT:
                // when the BinaryType is < , cast date to equivalent datetime type
                // E.g. cast(id_datetime as date) < 2021-12-28
                // optimized to id_datetime < 2021-12-28 00:00:00.0
                offset = 0;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate.toLocalDate().atTime(0, 0, 0, 0));
                resultBinaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castChild, newDatetime);
                break;
            case EQ:
                // when the BinaryType is = , replace it with compound operator
                // E.g. cast(id_datetime as date) = 2021-12-28
                // optimized to id_datetime >= 2021-12-28 and id_datetime < 2021-12-29
                ConstantOperator beginDatetime =
                        ConstantOperator.createDatetime(originalDate.plusDays(0).toLocalDate().atTime(0, 0, 0, 0));
                ConstantOperator endDatetime =
                        ConstantOperator.createDatetime(originalDate.plusDays(1).toLocalDate().atTime(0, 0, 0, 0));
                resultBinaryPredicateOperator =
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castChild,
                                        beginDatetime),
                                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castChild,
                                        endDatetime));
                break;
            default:
                resultBinaryPredicateOperator = operator;
                break;
        }
        return resultBinaryPredicateOperator;
    }
}
