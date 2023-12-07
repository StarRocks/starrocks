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

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

public class DateLiteralTest {

    @Test
    public void TwoDigitYear() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Assert.assertEquals(1997, literal.getYear());

            DateLiteral literal2 = new DateLiteral("97-10-07", Type.DATE);
            Assert.assertEquals(1997, literal2.getYear());

            DateLiteral literal3 = new DateLiteral("0097-10-07", Type.DATE);
            Assert.assertEquals(97, literal3.getYear());

            DateLiteral literal4 = new DateLiteral("99-10-07", Type.DATE);
            Assert.assertEquals(1999, literal4.getYear());

            DateLiteral literal5 = new DateLiteral("70-10-07", Type.DATE);
            Assert.assertEquals(1970, literal5.getYear());

            DateLiteral literal6 = new DateLiteral("69-10-07", Type.DATE);
            Assert.assertEquals(2069, literal6.getYear());

            DateLiteral literal7 = new DateLiteral("00-10-07", Type.DATE);
            Assert.assertEquals(2000, literal7.getYear());

        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void uncheckedCastTo() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Expr castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);

            DateLiteral literal2 = new DateLiteral("1997-10-07 12:23:23", Type.DATETIME);
            Expr castToExpr2 = literal2.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr2 instanceof DateLiteral);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void invalidDate() {
        String[] testDateCases = {
                // Invalid year.
                "20190-05-31",
                "1-05-31",
                // Invalid month.
                "2019-16-31",
                // Invalid day.
                "2019-02-29",
                "2019-04-31",
                "2019-05-32",

                // Other invalid formats.
                "2019-05-31-1",
                "not-date",
        };
        for (String c : testDateCases) {
            Assert.assertThrows(AnalysisException.class, () -> new DateLiteral(c, Type.DATE));
        }

        String[] testDatetimeCases = {
                // Invalid year.
                "20190-05-31 10:11:12",
                "20190-05-31 10:11:12.123",
                "1-05-31 10:11:12",
                "1-05-31 10:11:12.123",
                // Invalid month.
                "2019-16-31 10:11:12",
                "2019-16-31 10:11:12.123",
                // Invalid day.
                "2019-02-29 10:11:12",
                "2019-02-29 10:11:12.123",
                "2019-04-31 10:11:12",
                "2019-04-31 10:11:12.123",
                "2019-05-32 10:11:12",
                "2019-05-32 10:11:12.123",
                // Invalid hour, minute, or second.
                "2019-05-31 25:11:12",
                "2019-05-31 25:11:12.123",
                "2019-05-31 10:61:12",
                "2019-05-31 10:61:12.123",
                "2019-05-31 10:11:61",
                "2019-05-31 10:11:61.123",

                // Other invalid formats.
                "not-date",
        };
        for (String c : testDatetimeCases) {
            Assert.assertThrows(c, AnalysisException.class, () -> new DateLiteral(c, Type.DATETIME));
        }
    }
}
