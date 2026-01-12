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

import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.UnitIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntervalLiteralTest {

    @Test
    public void testToSecondsWithValidUnits() {
        IntervalLiteral seconds = new IntervalLiteral(new IntLiteral(1), new UnitIdentifier("SECOND"));
        Assertions.assertEquals(1, seconds.toSeconds());

        IntervalLiteral minutes = new IntervalLiteral(new IntLiteral(2), new UnitIdentifier("MINUTE"));
        Assertions.assertEquals(120, minutes.toSeconds());

        IntervalLiteral hours = new IntervalLiteral(new IntLiteral(3), new UnitIdentifier("HOUR"));
        Assertions.assertEquals(10800, hours.toSeconds());

        IntervalLiteral days = new IntervalLiteral(new IntLiteral(4), new UnitIdentifier("DAY"));
        Assertions.assertEquals(345600, days.toSeconds());

        IntervalLiteral weeks = new IntervalLiteral(new IntLiteral(5), new UnitIdentifier("WEEK"));
        Assertions.assertEquals(3024000, weeks.toSeconds());
    }

    @Test
    public void testToSecondsRejectsInvalidValues() {
        IntervalLiteral zero = new IntervalLiteral(new IntLiteral(0), new UnitIdentifier("SECOND"));
        Assertions.assertThrows(IllegalArgumentException.class, zero::toSeconds);

        IntervalLiteral negative = new IntervalLiteral(new IntLiteral(-1), new UnitIdentifier("SECOND"));
        Assertions.assertThrows(IllegalArgumentException.class, negative::toSeconds);
    }

    @Test
    public void testToSecondsRejectsInvalidLiteralAndUnit() {
        IntervalLiteral nonInt = new IntervalLiteral(new StringLiteral("x"), new UnitIdentifier("SECOND"));
        Assertions.assertThrows(IllegalArgumentException.class, nonInt::toSeconds);

        IntervalLiteral unsupportedUnit = new IntervalLiteral(new IntLiteral(1), new UnitIdentifier("MONTH"));
        Assertions.assertThrows(IllegalArgumentException.class, unsupportedUnit::toSeconds);
    }
}
