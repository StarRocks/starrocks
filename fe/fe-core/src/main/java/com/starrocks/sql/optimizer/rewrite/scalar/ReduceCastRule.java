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

import com.starrocks.analysis.BinaryType;
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

        // remove varchar cast inside date/datetime cast
        if (operator.getType().isDate() || operator.getType().isDatetime()) {
            ScalarOperator castChild = operator.getChild(0);
            if (castChild instanceof CastOperator
                    && castChild.getType().isVarchar()
                    && (castChild.getChild(0).getType().isDate() || castChild.getChild(0).getType().isDatetime())) {
                CastOperator newCastOperator = (CastOperator) operator.clone();
                newCastOperator.setChild(0, castChild.getChild(0));
                operator = newCastOperator;
            }
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

        ScalarOperator cast1Child = child1.getChild(0);
        // abandon cast function when cast datetime to date
        if (cast1Child.getType().isDate() && child2.getType().isDatetime()) {
            return reduceDateToDatetimeCast(operator);
        }

        if (cast1Child.getType().isDatetime() && child2.getType().isDate()) {
            return reduceDatetimeToDateCast(operator);
        }

        // BinaryPredicate involving Decimal
        if (cast1Child.getType().isDecimalOfAnyVersion()
                || child1.getType().isDecimalOfAnyVersion()
                || child2.getType().isDecimalOfAnyVersion()) {
            Optional<ScalarOperator> resultChild2 =
                    Utils.tryDecimalCastConstant((CastOperator) child1, (ConstantOperator) child2);
            return resultChild2
                    .map(scalarOperator -> new BinaryPredicateOperator(operator.getBinaryType(), cast1Child,
                            scalarOperator))
                    .orElse(operator);
        }

        if (!(cast1Child.getType().isNumericType() && child2.getType().isNumericType())) {
            return operator;
        }

        Type child1Type = child1.getType();
        Type cast1ChildType = cast1Child.getType();

        // if cast(cast1ChildType cast1Child) as child1Type will loss precision, we can't optimize it
        // e.g. cast(96.1) as int = 96, we can't change it into 96.1 = cast(96) as double
        if (!Type.isImplicitlyCastable(cast1ChildType, child1Type, true)) {
            return operator;
        }

        Optional<ScalarOperator> resultChild2 = Utils.tryCastConstant(child2, cast1Child.getType());
        return resultChild2
                .map(scalarOperator -> new BinaryPredicateOperator(operator.getBinaryType(), cast1Child,
                        scalarOperator))
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

    private ScalarOperator reduceDateToDatetimeCast(BinaryPredicateOperator operator) {
        ScalarOperator castChild = operator.getChild(0).getChild(0);
        ConstantOperator child2 = (ConstantOperator) operator.getChild(1);

        if (child2.isNull()) {
            return operator;
        }

        LocalDateTime originalDateTime = child2.getDatetime();
        LocalDateTime bottomDateTime = child2.getDatetime().truncatedTo(ChronoUnit.DAYS);

        LocalDateTime targetDateTime;
        BinaryType binaryType = operator.getBinaryType();
        int offset;
        BinaryPredicateOperator resultBinaryPredicateOperator;
        ConstantOperator newDate;
        switch (binaryType) {
            case GE:
                // when the BinaryType is >= ,cast dateTime to minimum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-29)
                offset = originalDateTime.isEqual(bottomDateTime) ? 0 : 1;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime);
                resultBinaryPredicateOperator = BinaryPredicateOperator.ge(castChild, newDate);
                break;
            case GT:
                // when the BinaryType is > ,cast dateTime to minimum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-29)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-29)
                offset = 1;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime);
                resultBinaryPredicateOperator = BinaryPredicateOperator.ge(castChild, newDate);
                break;
            case LE:
                // when the BinaryType is <= ,cast dateTime to maximum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-27)
                offset = 0;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime);
                resultBinaryPredicateOperator = BinaryPredicateOperator.le(castChild, newDate);
                break;
            case LT:
                // when the BinaryType is < ,cast dateTime to maximum date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-27)
                // Eg:cast dateTime(2021-12-28 00:00:00.1) to date(2021-12-28)
                offset = originalDateTime.isEqual(bottomDateTime) ? -1 : 0;
                targetDateTime = bottomDateTime.plusDays(offset);
                newDate = ConstantOperator.createDate(targetDateTime);
                resultBinaryPredicateOperator = BinaryPredicateOperator.le(castChild, newDate);
                break;
            case EQ:
                // when the BinaryType is = ,cast dateTime to equivalent date type；
                // Eg:cast dateTime(2021-12-28 00:00:00.0) to date(2021-12-28)
                if (!originalDateTime.isEqual(bottomDateTime)) {
                    resultBinaryPredicateOperator = operator;
                } else {
                    newDate = ConstantOperator.createDate(bottomDateTime);
                    resultBinaryPredicateOperator = BinaryPredicateOperator.eq(castChild, newDate);
                }
                break;
            default:
                // current not support !=
                resultBinaryPredicateOperator = operator;
                break;
        }
        return resultBinaryPredicateOperator;
    }

    private ScalarOperator reduceDatetimeToDateCast(BinaryPredicateOperator operator) {
        ScalarOperator castChild = operator.getChild(0).getChild(0);
        ConstantOperator child2 = (ConstantOperator) operator.getChild(1);
        if (child2.isNull()) {
            return operator;
        }

        LocalDateTime originalDate = child2.getDate().truncatedTo(ChronoUnit.DAYS);
        LocalDateTime targetDate;
        BinaryType binaryType = operator.getBinaryType();
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
                newDatetime = ConstantOperator.createDatetime(targetDate);
                resultBinaryPredicateOperator = BinaryPredicateOperator.ge(castChild, newDatetime);
                break;
            case GT:
                // when the BinaryType is > , cast date to equivalent datetime type of next day
                // E.g. cast(id_datetime as date) > 2021-12-28
                // optimized to id_datetime >= 2021-12-29 00:00:00.0
                offset = 1;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate);
                resultBinaryPredicateOperator = BinaryPredicateOperator.ge(castChild, newDatetime);
                break;
            case LE:
                // when the BinaryType is <= , cast date to equivalent datetime type of next day
                // E.g. cast(id_datetime as date) <= 2021-12-28
                // optimized to id_datetime < 2021-12-29 00:00:00.0
                offset = 1;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate);
                resultBinaryPredicateOperator = BinaryPredicateOperator.lt(castChild, newDatetime);
                break;
            case LT:
                // when the BinaryType is < , cast date to equivalent datetime type
                // E.g. cast(id_datetime as date) < 2021-12-28
                // optimized to id_datetime < 2021-12-28 00:00:00.0
                offset = 0;
                targetDate = originalDate.plusDays(offset);
                newDatetime = ConstantOperator.createDatetime(targetDate);
                resultBinaryPredicateOperator = BinaryPredicateOperator.lt(castChild, newDatetime);
                break;
            case EQ:
                // when the BinaryType is = , replace it with compound operator
                // E.g. cast(id_datetime as date) = 2021-12-28
                // optimized to id_datetime >= 2021-12-28 and id_datetime < 2021-12-29
                ConstantOperator beginDatetime = ConstantOperator.createDatetime(originalDate.plusDays(0));
                ConstantOperator endDatetime = ConstantOperator.createDatetime(originalDate.plusDays(1));
                resultBinaryPredicateOperator = CompoundPredicateOperator
                        .and(BinaryPredicateOperator.ge(castChild, beginDatetime),
                                BinaryPredicateOperator.lt(castChild, endDatetime));
                break;
            default:
                resultBinaryPredicateOperator = operator;
                break;
        }
        return resultBinaryPredicateOperator;
    }
}
