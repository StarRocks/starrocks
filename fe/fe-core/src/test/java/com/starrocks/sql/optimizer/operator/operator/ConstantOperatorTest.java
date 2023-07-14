<<<<<<< HEAD
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
=======
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
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))

package com.starrocks.sql.optimizer.operator.operator;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

import java.time.format.DateTimeParseException;

public class ConstantOperatorTest {
    @Test
    public void testCastToDateValid() throws Exception {
        String[][] testCases = {
                // YYYY-MM-dd or yy-MM-dd
                {"1997-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"0097-10-07", "0097-10-07T00:00", "0097-10-07T00:00"},
                {"2020-02-29", "2020-02-29T00:00", "2020-02-29T00:00"}, // Leap year.
                {"97-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"99-10-07", "1999-10-07T00:00", "1999-10-07T00:00"},
                {"70-10-07", "1970-10-07T00:00", "1970-10-07T00:00"},
                {"69-10-07", "2069-10-07T00:00", "2069-10-07T00:00"},
                {"00-10-07", "2000-10-07T00:00", "2000-10-07T00:00"},
                // YYYY-MM-dd HH:mm:ss or yy-MM-dd HH:mm:ss
                {"1997-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"0097-10-07 10:11:12", "0097-10-07T00:00", "0097-10-07T10:11:12"},
                {"2020-02-29 10:11:12", "2020-02-29T00:00", "2020-02-29T10:11:12"}, // Leap year.
                {"97-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"99-10-07 10:11:12", "1999-10-07T00:00", "1999-10-07T10:11:12"},
                {"70-10-07 10:11:12", "1970-10-07T00:00", "1970-10-07T10:11:12"},
                {"69-10-07 10:11:12", "2069-10-07T00:00", "2069-10-07T10:11:12"},
                {"00-10-07 10:11:12", "2000-10-07T00:00", "2000-10-07T10:11:12"},
                // YYYY-MM-dd HH:mm:ss.SSS
                {"1997-10-07 10:11:12.123", "1997-10-07T10:11:12.000123", "1997-10-07T10:11:12.000123"},
                {"0097-10-07 10:11:12.123", "0097-10-07T10:11:12.000123", "0097-10-07T10:11:12.000123"},
                {"2020-02-29 10:11:12.123", "2020-02-29T10:11:12.000123", "2020-02-29T10:11:12.000123"}, // Leap year.
        };

        for (String[] c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c[0]);
            Assert.assertEquals(c[1], in.castTo(Type.DATE).getDate().toString());
            Assert.assertEquals(c[2], in.castTo(Type.DATETIME).getDate().toString());
        }
    }

    @Test
    public void testCaseToDateInvalid() {
        String[] testCases = {
                // Invalid year.
                "20190-05-31",
                "20190-05-31 10:11:12",
                "20190-05-31 10:11:12.123",
                "1-05-31",
                "1-05-31 10:11:12",
                "1-05-31 10:11:12.123",
                // Invalid month.
                "2019-5-31",
                "2019-5-31 10:11:12",
                "2019-5-31 10:11:12.123",
                "2019-16-31",
                "2019-16-31 10:11:12",
                "2019-16-31 10:11:12.123",
                // Invalid day.
                "2019-02-29",
                "2019-02-29 10:11:12",
                "2019-02-29 10:11:12.123",
                "2019-04-31",
                "2019-04-31 10:11:12",
                "2019-04-31 10:11:12.123",
                "2019-05-32",
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
                "2019-05-31-1",
                "2019-05-31-1 10:11:12",
                "2019-05-31-1 10:11:12.123",
                "not-date",
        };
        for (String c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c);
<<<<<<< HEAD
            Assert.assertThrows(AnalysisException.class, () -> in.castTo(Type.DATE));
            Assert.assertThrows(AnalysisException.class, () -> in.castTo(Type.DATETIME));
=======
            Assert.assertThrows(in.getVarchar(), DateTimeParseException.class, () -> in.castTo(Type.DATE));
            Assert.assertThrows(in.getVarchar(), DateTimeParseException.class, () -> in.castTo(Type.DATETIME));
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
        }
    }
}
