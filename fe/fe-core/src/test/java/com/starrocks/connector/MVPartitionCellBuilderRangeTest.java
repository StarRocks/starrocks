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

package com.starrocks.connector;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

public class MVPartitionCellBuilderRangeTest {

    private static PartitionKey key(LocalDateTime value, PrimitiveType type) throws Exception {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new DateLiteral(value, type == PrimitiveType.DATE ? DateType.DATE : DateType.DATETIME),
                type);
        return key;
    }

    private static LocalDateTime upper(LocalDateTime lower, PartitionUtil.DateTimeInterval interval,
                                       PrimitiveType type) throws Exception {
        PartitionKey next = MVPartitionCellBuilder.nextPartitionKey(key(lower, type), interval, type);
        return ((DateLiteral) next.getKeys().get(0)).toLocalDateTime();
    }

    private static Range<PartitionKey> range(LocalDateTime lower) throws Exception {
        PartitionKey lowerKey = key(lower, PrimitiveType.DATETIME);
        return Range.closedOpen(lowerKey, MVPartitionCellBuilder.nextPartitionKey(
                lowerKey, PartitionUtil.DateTimeInterval.NONE, PrimitiveType.DATETIME));
    }

    @Test
    public void testIdentityDatetimeEndsOneSecondLater() throws Exception {
        LocalDateTime value = LocalDateTime.of(2026, 1, 1, 0, 3, 11);
        Assertions.assertEquals(value.plusSeconds(1),
                upper(value, PartitionUtil.DateTimeInterval.NONE, PrimitiveType.DATETIME));
    }

    @Test
    public void testIdentityDatetimeValuesOnOneDayDoNotOverlap() throws Exception {
        // Stretched to a day, these three overlapped and the view's refresh failed.
        Range<PartitionKey> a = range(LocalDateTime.of(2026, 1, 1, 0, 3, 11));
        Range<PartitionKey> b = range(LocalDateTime.of(2026, 1, 1, 8, 0, 0));
        Range<PartitionKey> c = range(LocalDateTime.of(2026, 1, 1, 8, 0, 1));
        Assertions.assertFalse(a.isConnected(b) && !a.intersection(b).isEmpty());
        Assertions.assertFalse(b.isConnected(c) && !b.intersection(c).isEmpty());
        Assertions.assertFalse(a.isConnected(c) && !a.intersection(c).isEmpty());
    }

    @Test
    public void testOtherCasesKeepTheirInterval() throws Exception {
        LocalDateTime day = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
        // A DATE identity value still covers its whole day.
        Assertions.assertEquals(day.plusDays(1),
                upper(day, PartitionUtil.DateTimeInterval.NONE, PrimitiveType.DATE));
        // A DATETIME partition with a granularity keeps that granularity.
        Assertions.assertEquals(day.plusDays(1),
                upper(day, PartitionUtil.DateTimeInterval.DAY, PrimitiveType.DATETIME));
        Assertions.assertEquals(day.plusHours(1),
                upper(day, PartitionUtil.DateTimeInterval.HOUR, PrimitiveType.DATETIME));
        Assertions.assertEquals(day.plusMonths(1),
                upper(day, PartitionUtil.DateTimeInterval.MONTH, PrimitiveType.DATETIME));
    }
}
