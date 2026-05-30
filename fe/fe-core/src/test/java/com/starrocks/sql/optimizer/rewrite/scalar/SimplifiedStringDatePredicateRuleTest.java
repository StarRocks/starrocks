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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

public class SimplifiedStringDatePredicateRuleTest {
    private final SimplifiedStringDatePredicateRule rule = new SimplifiedStringDatePredicateRule();

    private static ColumnRefOperator stringColumn() {
        return new ColumnRefOperator(1, VarcharType.VARCHAR, "datestr", true);
    }

    // CAST(strcol AS DATETIME) = datetime '2026-05-30 00:00:00' -> strcol = '2026-05-30'
    @Test
    public void testEqualityCastColumnDatetimeMidnight() {
        ScalarOperator castColumn = new CastOperator(DateType.DATETIME, stringColumn());
        ConstantOperator midnight =
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 5, 30, 0, 0, 0));
        ScalarOperator result = rule.apply(new BinaryPredicateOperator(BinaryType.EQ, castColumn, midnight), null);
        Assertions.assertEquals("1: datestr = 2026-05-30", result.toString());
    }

    // bare DATE constant: CAST(strcol AS DATE) = date '2026-05-30' -> strcol = '2026-05-30'
    @Test
    public void testEqualityCastColumnDate() {
        ScalarOperator castColumn = new CastOperator(DateType.DATE, stringColumn());
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2026, 5, 30, 0, 0, 0));
        ScalarOperator result = rule.apply(new BinaryPredicateOperator(BinaryType.EQ, castColumn, date), null);
        Assertions.assertEquals("1: datestr = 2026-05-30", result.toString());
    }

    // range comparison preserved (lexicographic == chronological for canonical yyyy-MM-dd)
    @Test
    public void testRangeCastColumn() {
        ScalarOperator castColumn = new CastOperator(DateType.DATETIME, stringColumn());
        ConstantOperator midnight =
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 5, 29, 0, 0, 0));
        ScalarOperator result = rule.apply(new BinaryPredicateOperator(BinaryType.GE, castColumn, midnight), null);
        Assertions.assertEquals("1: datestr >= 2026-05-29", result.toString());
    }

    // datetime constant with a real time component must NOT be rewritten (would lose precision).
    @Test
    public void testNonMidnightDatetimeNotRewritten() {
        ScalarOperator castColumn = new CastOperator(DateType.DATETIME, stringColumn());
        ConstantOperator noon = ConstantOperator.createDatetime(LocalDateTime.of(2026, 5, 30, 12, 0, 0));
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, castColumn, noon);
        ScalarOperator result = rule.apply(predicate, null);
        Assertions.assertSame(predicate, result);
    }

    // string column directly compared to a varchar datetime-midnight literal -> date-only.
    @Test
    public void testBareStringColumnMidnightStringLiteral() {
        ConstantOperator midnightString = ConstantOperator.createVarchar("2026-05-30 00:00:00");
        ScalarOperator result =
                rule.apply(new BinaryPredicateOperator(BinaryType.EQ, stringColumn(), midnightString), null);
        Assertions.assertEquals("1: datestr = 2026-05-30", result.toString());
    }

    // IN with mixed-format render: ('2026-05-30', '2026-05-18 00:00:00') -> both date-only.
    @Test
    public void testInPredicateMixedFormat() {
        InPredicateOperator in = new InPredicateOperator(false, ImmutableList.of(
                stringColumn(),
                ConstantOperator.createVarchar("2026-05-30"),
                ConstantOperator.createVarchar("2026-05-18 00:00:00")));
        ScalarOperator result = rule.apply(in, null);
        Assertions.assertEquals("1: datestr IN (2026-05-30, 2026-05-18)", result.toString());
    }

    // IN with a temporal constant element (DATETIME midnight) -> date-only string.
    @Test
    public void testInPredicateDatetimeConstant() {
        InPredicateOperator in = new InPredicateOperator(false, ImmutableList.of(
                stringColumn(),
                ConstantOperator.createDate(LocalDateTime.of(2026, 5, 30, 0, 0, 0)),
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 5, 18, 0, 0, 0))));
        ScalarOperator result = rule.apply(in, null);
        Assertions.assertEquals("1: datestr IN (2026-05-30, 2026-05-18)", result.toString());
    }

    // non-string column is untouched.
    @Test
    public void testNonStringColumnNotRewritten() {
        ColumnRefOperator dateColumn = new ColumnRefOperator(1, DateType.DATE, "dt", true);
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2026, 5, 30, 0, 0, 0));
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, dateColumn, date);
        ScalarOperator result = rule.apply(predicate, null);
        Assertions.assertSame(predicate, result);
    }
}
