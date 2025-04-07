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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.time.DateTimeException;
import java.util.regex.Pattern;

/**
 * if t is date
 * date_format(t, '%Y%m%d') >= '20230327' -> `t` >= '20230327'
 * date_format(t, '%Y-%m-%d') >= '2023-03-27' -> `t` >= '2023-03-27'
 * substr(cast(t as varchar), 1, 10) >= '2023-03-27' -> `t` >= '2023-03-27'
 * substring(cast(t as varchar), 1, 10) >= '2023-03-27' -> `t` >= '2023-03-27'
 * replace(substring(cast(t as varchar), 1, 10), "-", "") >= '20230327' -> `t` >= '20230327'
 *
 * if it is datetime
 * date_format(t, '%Y-%m-%d') >/>=/< '2023-03-27' -> t >/>=/< '2023-03-27'
 * date_format(t, '%Y-%m-%d') <= '2023-03-27' -> t < days_add('2023-03-27', 1)
 */
public class SimplifiedDateColumnPredicateRule extends BottomUpScalarOperatorRewriteRule {
    private static final String DATE_PATTERN1 = "%Y%m%d";
    private static final String DATE_PATTERN2 = "%Y-%m-%d";
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
        ScalarOperator columnRef = null;
        Extractor extractor = newExtractor((CallOperator) op, (ConstantOperator) right);
        if (extractor != null && extractor.check()) {
            columnRef = extractor.extractColumn();
        }
        if (columnRef == null) {
            return predicate;
        }
        BinaryType binaryType = predicate.getBinaryType();
        if (columnRef.getType().getPrimitiveType() == PrimitiveType.DATE) {
            return new BinaryPredicateOperator(binaryType, columnRef, right);
        }
        // if t is datetime,
        if (columnRef.getType().getPrimitiveType() == PrimitiveType.DATETIME) {
            // date_format(t, '%Y-%m-%d') >/>=/< '2023-03-27' -> t >/>=/< '2023-03-27'
            if (binaryType == BinaryType.GT || binaryType == BinaryType.GE || binaryType == BinaryType.LT) {
                return new BinaryPredicateOperator(binaryType, columnRef, right);
            }
            // date_format(t, '%Y-%m-%d') <= '2023-03-27' -> t < days_add('2023-03-27', 1)
            if (binaryType == BinaryType.LE) {
                Function daysAddFn = Expr.getBuiltinFunction(FunctionSet.DAYS_ADD,
                        new Type[] {Type.DATETIME, Type.INT}, Function.CompareMode.IS_IDENTICAL);
                return new BinaryPredicateOperator(BinaryType.LT, columnRef, new CallOperator(
                        FunctionSet.DAYS_ADD, Type.DATETIME,
                        ImmutableList.of(right, ConstantOperator.createInt(1)), daysAddFn));
            }
        }
        return predicate;
    }

    private static boolean isSubstrFn(CallOperator call) {
        return FunctionSet.SUBSTR.equalsIgnoreCase(call.getFnName())
                || FunctionSet.SUBSTRING.equalsIgnoreCase(call.getFnName());
    }

    private Extractor newExtractor(CallOperator call, ConstantOperator value) {
        if (FunctionSet.DATE_FORMAT.equalsIgnoreCase(call.getFnName())) {
            return new DateFormatExtractor(call, value);
        } else if (isSubstrFn(call)) {
            return new SubstrExtractor(call, value, DATE_PATTERN2);
        } else if (FunctionSet.REPLACE.equalsIgnoreCase(call.getFnName())) {
            return new ReplaceAndSubstrExtractor(call, value);
        }
        return null;
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

    private interface Extractor {
        ScalarOperator extractColumn();

        boolean check();
    }

    /**
     * date_format(t, '%Y%m%d') -> t
     */
    private static class DateFormatExtractor implements Extractor {
        private final CallOperator call;
        private final ConstantOperator value;

        public DateFormatExtractor(CallOperator call, ConstantOperator value) {
            this.call = call;
            this.value = value;
        }

        @Override
        public boolean check() {
            if (!call.getChild(1).isConstantRef()) {
                return false;
            }
            ScalarOperator dateColumn = call.getChild(0);
            return dateColumn.getType().getPrimitiveType() == PrimitiveType.DATE
                    || dateColumn.getType().getPrimitiveType() == PrimitiveType.DATETIME;
        }

        @Override
        public ScalarOperator extractColumn() {
            ScalarOperator dateColumn = call.getChild(0);
            String pattern = ((ConstantOperator) call.getChild(1)).getVarchar();
            if (isDatePattern(pattern, value)) {
                return dateColumn;
            }
            return null;
        }
    }

    /**
     * substr(cast(t as varchar), 1, 10) -> t
     */
    private static class SubstrExtractor implements Extractor {
        private final CallOperator call;
        private final ConstantOperator value;
        private final String datePattern;

        public SubstrExtractor(CallOperator call, ConstantOperator value, String datePattern) {
            this.call = call;
            this.value = value;
            this.datePattern = datePattern;
        }

        @Override
        public boolean check() {
            if (!(call.getChild(1).isConstantRef() && ((ConstantOperator) call.getChild(1)).getInt() == 1)
                    || !(call.getChild(2).isConstantRef() && ((ConstantOperator) call.getChild(2)).getInt() == 10)) {
                return false;
            }
            if (!(call.getChild(0) instanceof CastOperator)) {
                return false;
            }
            CastOperator op = call.getChild(0).cast();
            return op.getChild(0).getType().getPrimitiveType() == PrimitiveType.DATE
                    || op.getChild(0).getType().getPrimitiveType() == PrimitiveType.DATETIME;
        }

        @Override
        public ScalarOperator extractColumn() {
            CastOperator castOperator = call.getChild(0).cast();
            ScalarOperator dateColumn = castOperator.getChild(0);
            if (isDatePattern(datePattern, value)) {
                return dateColumn;
            }
            return null;
        }
    }

    /**
     * replace(substring(cast(t as varchar), 1, 10), "-", "") -> t
     */
    private static class ReplaceAndSubstrExtractor implements Extractor {
        private final CallOperator call;
        private final ConstantOperator value;

        public ReplaceAndSubstrExtractor(CallOperator call, ConstantOperator value) {
            this.call = call;
            this.value = value;
        }

        @Override
        public boolean check() {
            if (!(call.getChild(0) instanceof CallOperator && isSubstrFn((CallOperator) call.getChild(0)))) {
                return false;
            }
            if (!call.getChild(1).isConstantRef() || !((ConstantOperator) call.getChild(1)).getVarchar().equals("-")) {
                return false;
            }
            if (!call.getChild(2).isConstantRef() || !((ConstantOperator) call.getChild(2)).getVarchar().isEmpty()) {
                return false;
            }
            // yyyyMMdd
            if (value.getVarchar().length() != 8) {
                return false;
            }
            return new SubstrExtractor((CallOperator) call.getChild(0), value, DATE_PATTERN1).check();
        }

        @Override
        public ScalarOperator extractColumn() {
            return new SubstrExtractor((CallOperator) call.getChild(0), value, DATE_PATTERN1).extractColumn();
        }
    }
}
