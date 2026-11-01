// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprCastFunction;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateLiteralTest {

    @Test
    public void TwoDigitYear() {
        boolean hasException = false;
        try {
            DateLiteral literal = createDateLiteral("1997-10-07", DateType.DATE);
            Assertions.assertEquals(1997, literal.getYear());

            DateLiteral literal2 = createDateLiteral("97-10-07", DateType.DATE);
            Assertions.assertEquals(1997, literal2.getYear());

            DateLiteral literal3 = createDateLiteral("0097-10-07", DateType.DATE);
            Assertions.assertEquals(97, literal3.getYear());

            DateLiteral literal4 = createDateLiteral("99-10-07", DateType.DATE);
            Assertions.assertEquals(1999, literal4.getYear());

            DateLiteral literal5 = createDateLiteral("70-10-07", DateType.DATE);
            Assertions.assertEquals(1970, literal5.getYear());

            DateLiteral literal6 = createDateLiteral("69-10-07", DateType.DATE);
            Assertions.assertEquals(2069, literal6.getYear());

            DateLiteral literal7 = createDateLiteral("00-10-07", DateType.DATE);
            Assertions.assertEquals(2000, literal7.getYear());

        } catch (ParsingException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assertions.assertFalse(hasException);
    }

    @Test
    public void uncheckedCastTo() {
        boolean hasException = false;
        try {
            DateLiteral literal = createDateLiteral("1997-10-07", DateType.DATE);
            Expr castToExpr = ExprCastFunction.uncheckedCastTo(literal, DateType.DATETIME);
            Assertions.assertTrue(castToExpr instanceof DateLiteral);
            Assertions.assertEquals(castToExpr.getType(), DateType.DATETIME);

            DateLiteral literal2 = createDateLiteral("1997-10-07 12:23:23", DateType.DATETIME);
            Expr castToExpr2 = ExprCastFunction.uncheckedCastTo(literal2, DateType.DATETIME);
            Assertions.assertTrue(castToExpr2 instanceof DateLiteral);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assertions.assertFalse(hasException);
    }

    @Test
    public void testCompareLiteralMicro() throws ParsingException {
        DateLiteral thisEqual = createDateLiteral("2026-10-08 08:30:18.282585", DateType.DATETIME);
        DateLiteral otherEqual = createDateLiteral("2026-10-08 08:30:18.282585", DateType.DATETIME);
        Assertions.assertEquals(0, thisEqual.compareLiteral(otherEqual));

        DateLiteral thisLess = createDateLiteral("2026-10-08 08:30:18.282585", DateType.DATETIME);
        DateLiteral otherLess = createDateLiteral("2026-10-08 08:30:18.282586", DateType.DATETIME);
        Assertions.assertEquals(-1, thisLess.compareLiteral(otherLess));

        DateLiteral thisGreater = createDateLiteral("2026-10-08 08:30:18.282586", DateType.DATETIME);
        DateLiteral otherGreater = createDateLiteral("2026-10-08 08:30:18.282585", DateType.DATETIME);
        Assertions.assertEquals(1, thisGreater.compareLiteral(otherGreater));

        DateLiteral thisLessSec = createDateLiteral("2026-10-08 08:30:17.282585", DateType.DATETIME);
        DateLiteral otherLessSec = createDateLiteral("2026-10-08 08:30:18.282585", DateType.DATETIME);
        Assertions.assertEquals(-1, thisLessSec.compareLiteral(otherLessSec));

        DateLiteral thisGreaterSec = createDateLiteral("2026-10-08 08:30:18.282586", DateType.DATETIME);
        DateLiteral otherGreaterSec = createDateLiteral("2026-10-08 08:30:17.282585", DateType.DATETIME);
        Assertions.assertEquals(1, thisGreaterSec.compareLiteral(otherGreaterSec));
    }

    @Test
    public void testCompareLiteralMili() throws ParsingException {
        DateLiteral thisEqual = createDateLiteral("2026-10-08 08:30:18.000282", DateType.DATETIME);
        DateLiteral otherEqual = createDateLiteral("2026-10-08 08:30:18.000282", DateType.DATETIME);
        Assertions.assertEquals(0, thisEqual.compareLiteral(otherEqual));

        DateLiteral thisLess = createDateLiteral("2026-10-08 08:30:18.000282", DateType.DATETIME);
        DateLiteral otherLess = createDateLiteral("2026-10-08 08:30:18.000283", DateType.DATETIME);
        Assertions.assertEquals(-1, thisLess.compareLiteral(otherLess));

        DateLiteral thisGreater = createDateLiteral("2026-10-08 08:30:18.000283", DateType.DATETIME);
        DateLiteral otherGreater = createDateLiteral("2026-10-08 08:30:18.000282", DateType.DATETIME);
        Assertions.assertEquals(1, thisGreater.compareLiteral(otherGreater));
    }

    @Test
    public void testCompareLiteralLongOnly() throws ParsingException {
        DateLiteral thisEqual = createDateLiteral("2026-10-08 08:30:18", DateType.DATETIME);
        DateLiteral otherEqual = createDateLiteral("2026-10-08 08:30:18", DateType.DATETIME);
        Assertions.assertEquals(0, thisEqual.compareLiteral(otherEqual));

        DateLiteral thisLess = createDateLiteral("2026-10-08 08:30:18", DateType.DATETIME);
        DateLiteral otherLess = createDateLiteral("2026-10-08 08:30:19", DateType.DATETIME);
        Assertions.assertEquals(-1, thisLess.compareLiteral(otherLess));

        DateLiteral thisGreater = createDateLiteral("2026-10-08 08:30:19", DateType.DATETIME);
        DateLiteral otherGreater = createDateLiteral("2026-10-08 08:30:18", DateType.DATETIME);
        Assertions.assertEquals(1, thisGreater.compareLiteral(otherGreater));
    }

    @Test
    public void testCompareLiteralNonDate() {
        DateLiteral thisEqual = new DateLiteral(0, 0, 0, 0, 0, 0, 0);
        LiteralExpr otherEqual = new IntLiteral(0);
        Assertions.assertEquals(0, thisEqual.compareLiteral(otherEqual));

        DateLiteral thisLess = new DateLiteral(0, 0, 0, 0, 0, 3, 0);
        LiteralExpr otherLess = new IntLiteral(5);
        Assertions.assertEquals(-1, thisLess.compareLiteral(otherLess));

        DateLiteral thisGreater = new DateLiteral(0, 0, 0, 0, 0, 10, 0);
        LiteralExpr otherGreater = new IntLiteral(5);
        Assertions.assertEquals(1, thisGreater.compareLiteral(otherGreater));

        LiteralExpr otherFloat = new FloatLiteral(0.5, FloatType.DOUBLE);
        Assertions.assertEquals(thisEqual.getLongValue(), otherFloat.getLongValue());
        Assertions.assertNotEquals(thisEqual.getDoubleValue(), otherFloat.getDoubleValue(), 0.0);
        Assertions.assertEquals(-1, thisEqual.compareLiteral(otherFloat));
    }

    private DateLiteral createDateLiteral(String value, DateType type) throws ParsingException {
        try {
            return new DateLiteral(DateUtils.parseStrictDateTime(value), type);
        } catch (RuntimeException e) {
            throw new ParsingException("Invalid date literal: " + value, e);
        }
    }
}
