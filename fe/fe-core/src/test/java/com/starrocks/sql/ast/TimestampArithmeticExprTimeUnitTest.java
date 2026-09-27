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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.TimestampArithmeticExpr.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimestampArithmeticExprTimeUnitTest {

    @Test
    public void testQuarterIsRecognized() {
        // QUARTER is a valid interval unit in the grammar; the enum must expose it so that
        // TimeUnit.fromName() can resolve it (used by partition analyzers and, going forward,
        // by enum-based checks in the expression analyzer) instead of returning null.
        Assertions.assertEquals(TimeUnit.QUARTER, TimeUnit.fromName("QUARTER"));
        Assertions.assertEquals(TimeUnit.QUARTER, TimeUnit.fromName("quarter"));
        Assertions.assertEquals(TimeUnit.QUARTER, TimeUnit.fromName("QuArTeR"));
        Assertions.assertEquals("QUARTER", TimeUnit.QUARTER.toString());
    }

    @Test
    public void testOtherDateGranularityUnitsStillResolve() {
        // Adding QUARTER in the middle of the enum must not disturb the sibling date-granularity units.
        Assertions.assertEquals(TimeUnit.YEAR, TimeUnit.fromName("year"));
        Assertions.assertEquals(TimeUnit.MONTH, TimeUnit.fromName("month"));
        Assertions.assertEquals(TimeUnit.WEEK, TimeUnit.fromName("week"));
        Assertions.assertEquals(TimeUnit.DAY, TimeUnit.fromName("day"));
        Assertions.assertEquals(TimeUnit.HOUR, TimeUnit.fromName("hour"));
    }

    @Test
    public void testUnknownUnitReturnsNull() {
        // MILLISECOND exists in the grammar but not in this enum; fromName must stay null-safe
        // so enum-based callers fall through rather than throwing.
        Assertions.assertNull(TimeUnit.fromName("millisecond"));
        Assertions.assertNull(TimeUnit.fromName("not_a_unit"));
    }
}