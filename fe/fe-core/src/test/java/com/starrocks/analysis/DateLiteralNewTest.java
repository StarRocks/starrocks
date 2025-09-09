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

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.DateLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateLiteralNewTest {

    @Test
    public void testFromString() throws AnalysisException {
        DateLiteral dateLiteral = new DateLiteral("2022-12-12 16:20:17.123456", Type.DATETIME);
        Assertions.assertEquals("2022-12-12 16:20:17.123456", dateLiteral.getStringValue());
        Assertions.assertEquals(123456, dateLiteral.getMicrosecond());
        Assertions.assertEquals(20221212162017.123456, dateLiteral.getDoubleValue(), 0.000001);

        dateLiteral = new DateLiteral("2023-03-29 01:01:01.12", Type.DATETIME);
        Assertions.assertEquals("2023-03-29 01:01:01.120000", dateLiteral.getStringValue());
        Assertions.assertEquals(120000, dateLiteral.getMicrosecond());

        dateLiteral = new DateLiteral("2023-03-29 01:01:01.1234", Type.DATETIME);
        Assertions.assertEquals("2023-03-29 01:01:01.123400", dateLiteral.getStringValue());
        Assertions.assertEquals(123400, dateLiteral.getMicrosecond());
    }

    @Test
    public void testTimeWithMs() {
        DateLiteral dateLiteral = new DateLiteral(2022, 12, 12, 16, 20, 17, 123456);
        Assertions.assertEquals("2022-12-12 16:20:17.123456", dateLiteral.getStringValue());
        Assertions.assertEquals(123456, dateLiteral.getMicrosecond());
        Assertions.assertEquals(20221212162017.123456, dateLiteral.getDoubleValue(), 0.000001);
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
            Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral(c, Type.DATE));
        }

        String[] testDatetimeCases = {
                // Invalid year.
                "20190-05-31 10:11:12",
                "20190-05-31 10:11:12.123",
                "1-05-31 10:11:12",
                "1-05-31 10:11:12.123",
                // Invalid month.
                "2019-15-31 10:11:12.123",
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
                "2019-05-31 10:11:12.1234567",

                // Other invalid formats.
                "2019-05-31-1 10:11:12",
                "2019-05-31-1 10:11:12.123",
                "not-date",
        };
        for (String c : testDatetimeCases) {
            Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral(c, Type.DATETIME));
        }
    }

}
