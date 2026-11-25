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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.PrimitiveType;

/**
 * Recognizes a DATE or DATETIME operand formatted as text, the shapes a predicate like
 * {@code <shape> op 'constant'} is written in:
 * <pre>
 *   date_format(t, '&lt;pattern&gt;')                          -> (t, pattern)
 *   substr(cast(t as varchar), 1, 10)                       -> (t, '%Y-%m-%d')
 *   replace(substr(cast(t as varchar), 1, 10), '-', '')     -> (t, '%Y%m%d')
 * </pre>
 * ({@code substring} works like {@code substr}.) Shared by the in-place rewrite
 * {@link com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedDateColumnPredicateRule}, which maps the exact
 * equivalences onto the column for every table, and {@link FunctionPreimage}, which derives pruning-only supersets
 * for external scans. Each decides on its own whether the constant fits the pattern.
 */
public final class DateTextShape {
    public static final String DATE_PATTERN_COMPACT = "%Y%m%d";
    public static final String DATE_PATTERN_DASHED = "%Y-%m-%d";

    // the DATE or DATETIME operand
    public final ScalarOperator operand;
    // a date_format pattern, as written
    public final String pattern;

    private DateTextShape(ScalarOperator operand, String pattern) {
        this.operand = operand;
        this.pattern = pattern;
    }

    /**
     * @return null when {@code call} is none of the shapes
     */
    public static DateTextShape match(CallOperator call) {
        String fn = call.getFnName();
        if (FunctionSet.DATE_FORMAT.equalsIgnoreCase(fn)) {
            if (call.getChildren().size() != 2 || !call.getChild(1).isConstantRef()
                    || !isTemporal(call.getChild(0))) {
                return null;
            }
            return new DateTextShape(call.getChild(0), ((ConstantOperator) call.getChild(1)).getVarchar());
        }
        if (isSubstr(call)) {
            ScalarOperator operand = substrDateOperand(call);
            return operand == null ? null : new DateTextShape(operand, DATE_PATTERN_DASHED);
        }
        if (FunctionSet.REPLACE.equalsIgnoreCase(fn)) {
            if (call.getChildren().size() != 3 || !(call.getChild(0) instanceof CallOperator inner)
                    || !isSubstr(inner)
                    || !call.getChild(1).isConstantRef()
                    || !((ConstantOperator) call.getChild(1)).getVarchar().equals("-")
                    || !call.getChild(2).isConstantRef()
                    || !((ConstantOperator) call.getChild(2)).getVarchar().isEmpty()) {
                return null;
            }
            ScalarOperator operand = substrDateOperand(inner);
            return operand == null ? null : new DateTextShape(operand, DATE_PATTERN_COMPACT);
        }
        return null;
    }

    private static boolean isSubstr(CallOperator call) {
        return FunctionSet.SUBSTR.equalsIgnoreCase(call.getFnName())
                || FunctionSet.SUBSTRING.equalsIgnoreCase(call.getFnName());
    }

    // substr(cast(t as varchar), 1, 10) -> t
    private static ScalarOperator substrDateOperand(CallOperator call) {
        if (call.getChildren().size() != 3
                || !(call.getChild(1).isConstantRef() && ((ConstantOperator) call.getChild(1)).getInt() == 1)
                || !(call.getChild(2).isConstantRef() && ((ConstantOperator) call.getChild(2)).getInt() == 10)
                || !(call.getChild(0) instanceof CastOperator cast)) {
            return null;
        }
        return isTemporal(cast.getChild(0)) ? cast.getChild(0) : null;
    }

    private static boolean isTemporal(ScalarOperator operand) {
        PrimitiveType type = operand.getType().getPrimitiveType();
        return type == PrimitiveType.DATE || type == PrimitiveType.DATETIME;
    }
}
