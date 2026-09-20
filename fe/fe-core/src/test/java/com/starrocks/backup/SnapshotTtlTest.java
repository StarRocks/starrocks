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

package com.starrocks.backup;

import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class SnapshotTtlTest {

    private static long millisOf(int year, int month, int day, int hour) {
        ZoneId zone = TimeUtils.getSystemTimeZone().toZoneId();
        return LocalDateTime.of(year, month, day, hour, 0).atZone(zone).toInstant().toEpochMilli();
    }

    @Test
    public void testParseDuration() {
        Assertions.assertNotNull(SnapshotTtl.parse("7 DAY"));
        Assertions.assertNotNull(SnapshotTtl.parse("12 HOUR"));
        Assertions.assertNotNull(SnapshotTtl.parse("1 MONTH"));
        Assertions.assertNotNull(SnapshotTtl.parse("  30 second  "));
    }

    @Test
    public void testParseEmptyKeepsForever() {
        Assertions.assertNull(SnapshotTtl.parse(null));
        Assertions.assertNull(SnapshotTtl.parse(""));
        Assertions.assertNull(SnapshotTtl.parse("   "));
    }

    @Test
    public void testParseRejectsBadValue() {
        Assertions.assertThrows(SemanticException.class, () -> SnapshotTtl.parse("7d"));
        Assertions.assertThrows(SemanticException.class, () -> SnapshotTtl.parse("forever"));
        Assertions.assertThrows(SemanticException.class, () -> SnapshotTtl.parse("7 PARSEC"));
        Assertions.assertThrows(SemanticException.class, () -> SnapshotTtl.parse("0 DAY"));
        Assertions.assertThrows(SemanticException.class, () -> SnapshotTtl.parse("-1 DAY"));
    }

    @Test
    public void testComputeExpireTime() {
        long finishTime = millisOf(2026, 1, 31, 10);

        // A word-form duration parses to a non-zero period and a zero Duration, so converting it to
        // milliseconds would give 0. The expire time has to come out of calendar arithmetic.
        Assertions.assertEquals(millisOf(2026, 2, 7, 10), SnapshotTtl.computeExpireTime(finishTime, "7 DAY"));
        Assertions.assertEquals(millisOf(2026, 1, 31, 22), SnapshotTtl.computeExpireTime(finishTime, "12 HOUR"));
        // One month from January 31 is the end of February, not 30 days later.
        Assertions.assertEquals(millisOf(2026, 2, 28, 10), SnapshotTtl.computeExpireTime(finishTime, "1 MONTH"));
    }

    @Test
    public void testComputeExpireTimeOfEmptyTtl() {
        long finishTime = millisOf(2026, 1, 31, 10);
        Assertions.assertNull(SnapshotTtl.computeExpireTime(finishTime, null));
        Assertions.assertNull(SnapshotTtl.computeExpireTime(finishTime, ""));
    }
}
