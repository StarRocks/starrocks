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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.DateTextShape;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

import java.time.DateTimeException;
import java.util.regex.Pattern;

import static com.starrocks.sql.ast.expression.BinaryType.GE;
import static com.starrocks.sql.ast.expression.BinaryType.GT;
import static com.starrocks.sql.ast.expression.BinaryType.LE;
import static com.starrocks.sql.ast.expression.BinaryType.LT;

/**
 * if t is date
 * date_format(t, '%Y%m%d') >= '20230327' -> `t` >= '20230327'
 * date_format(t, '%Y-%m-%d') >= '2023-03-27' -> `t` >= '2023-03-27'
 * substr(cast(t as varchar), 1, 10) >= '2023-03-27' -> `t` >= '2023-03-27'
 * substring(cast(t as varchar), 1, 10) >= '2023-03-27' -> `t` >= '2023-03-27'
 * replace(substring(cast(t as varchar), 1, 10), "-", "") >= '20230327' -> `t` >= '20230327'
 *
 * if it is datetime
 * date_format(t, '%Y-%m-%d') >=/< '2023-03-27' -> t >=/< '2023-03-27'
 * date_format(t, '%Y-%m-%d') >/<= '2023-03-27' -> t >=/< days_add('2023-03-27', 1)
 *
 * The shapes are recognized by {@link DateTextShape}, which {@link com.starrocks.sql.optimizer.rewrite.FunctionPreimage}
 * shares to derive pruning-only supersets for the cases not rewritten here (e.g. {@code =} on a DATETIME).
 */
public class SimplifiedDateColumnPredicateRule extends BottomUpScalarOperatorRewriteRule {
    private static final String DATE_PATTERN1 = DateTextShape.DATE_PATTERN_COMPACT;
    private static final String DATE_PATTERN2 = DateTextShape.DATE_PATTERN_DASHED;
    private static final Pattern DATE_PATTERN_REG = Pattern.compile("\\d{8}");
    private static final Pattern DATE_PATTERN_REG2 = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator op = predicate.getChild(0);
        if (!(op instanceof CallOperator)) {
            return predicate;
        }
        ScalarOperator right = predicate.getChild(1);
        if (!(right instanceof ConstantOperator)) {
            return predicate;
        }
        DateTextShape shape = DateTextShape.match((CallOperator) op);
        if (shape == null || !isDatePattern(shape.pattern, (ConstantOperator) right)) {
            return predicate;
        }
        ScalarOperator columnRef = shape.operand;
        BinaryType binaryType = predicate.getBinaryType();
        if (columnRef.getType().getPrimitiveType() == PrimitiveType.DATE) {
            return new BinaryPredicateOperator(binaryType, columnRef, right);
        }
        // if t is datetime,
        if (columnRef.getType().getPrimitiveType() == PrimitiveType.DATETIME) {
            // date_format(t, '%Y-%m-%d') >/>=/< '2023-03-27' -> t >/>=/< '2023-03-27'
            if (binaryType == BinaryType.GE || binaryType == BinaryType.LT) {
                return new BinaryPredicateOperator(binaryType, columnRef, right);
            } else if (binaryType == GT || binaryType == LE) {
                // date_format(t, '%Y-%m-%d') >/<= '2023-03-27' -> t >=/< days_add('2023-03-27', 1)
                Function daysAddFn = ExprUtils.getBuiltinFunction(FunctionSet.DAYS_ADD,
                        new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_IDENTICAL);
                binaryType = binaryType == GT ? GE : LT;
                return new BinaryPredicateOperator(binaryType, columnRef, new CallOperator(
                        FunctionSet.DAYS_ADD, DateType.DATETIME,
                        ImmutableList.of(right, ConstantOperator.createInt(1)), daysAddFn));
            }
        }
        return predicate;
    }

    private static boolean isDatePattern(String datePattern, ConstantOperator date) {
        if (DATE_PATTERN1.equalsIgnoreCase(datePattern) && DATE_PATTERN_REG.matcher(date.getVarchar()).matches()) {
            return verifyDate(datePattern, date);
        }
        if (DATE_PATTERN2.equalsIgnoreCase(datePattern) && DATE_PATTERN_REG2.matcher(date.getVarchar()).matches()) {
            return verifyDate(datePattern, date);
        }
        return false;
    }

    private static boolean verifyDate(String datePattern, ConstantOperator date) {
        try {
            ScalarOperatorFunctions.str2Date(date, ConstantOperator.createVarchar(datePattern));
        } catch (DateTimeException ignore) {
            return false;
        }
        return true;
    }
}
