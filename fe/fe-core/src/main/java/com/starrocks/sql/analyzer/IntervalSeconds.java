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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;

import java.math.BigDecimal;

/**
 * The amount of an INTERVAL in SECOND. seconds_add and seconds_sub take it as an INT, so a fraction of a second would be
 * lost; such an amount is added as whole seconds and microseconds instead.
 */
public final class IntervalSeconds {
    private IntervalSeconds() {
    }

    /**
     * Splits an amount of seconds into whole seconds and microseconds.
     *
     * @return {whole seconds, microseconds}, or null if the amount doesn't fit in an INT, which leaves it to the INT cast
     * @throws SemanticException if the amount has more than 6 decimals: a datetime has no digits below microseconds
     */
    public static int[] split(BigDecimal seconds, NodePosition pos) {
        // An INT holds at most 10 digits before the point, so a larger amount is left to the INT cast. This also keeps
        // the arithmetic below away from the huge numbers a short exponent can describe, as in '1e100000000'.
        if ((long) seconds.precision() - seconds.scale() > 10) {
            return null;
        }
        BigDecimal exact = seconds.stripTrailingZeros();
        if (exact.scale() > 6) {
            // BigDecimal.toString() writes a long fraction such as '1e-100000000' with an exponent, so the message stays
            // short
            throw new SemanticException("The amount of an INTERVAL in SECOND can have at most 6 decimals "
                    + "because a datetime has no digits below microseconds: " + seconds, pos);
        }
        BigDecimal[] wholeAndFraction = exact.divideAndRemainder(BigDecimal.ONE);
        try {
            return new int[] {wholeAndFraction[0].intValueExact(), wholeAndFraction[1].movePointRight(6).intValueExact()};
        } catch (ArithmeticException e) {
            // doesn't fit in an INT, which is what the function takes
            return null;
        }
    }

    /**
     * The value of a decimal, floating point or string literal, or null for any other expression (also for a string that
     * is not a number, which is left to the INT cast)
     */
    public static BigDecimal literalValue(Expr amount) {
        try {
            if (amount instanceof DecimalLiteral) {
                return ((DecimalLiteral) amount).getValue();
            } else if (amount instanceof FloatLiteral) {
                return BigDecimal.valueOf(((FloatLiteral) amount).getValue());
            } else if (amount instanceof StringLiteral) {
                return new BigDecimal(((StringLiteral) amount).getValue().trim());
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return null;
    }
}
