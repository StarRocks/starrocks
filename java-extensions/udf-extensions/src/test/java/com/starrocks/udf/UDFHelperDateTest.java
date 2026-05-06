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

package com.starrocks.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class UDFHelperDateTest {

    // 1970-01-01 in StarRocks Julian-day encoding. Anchor for the round-trip checks.
    private static final int JULIAN_UNIX_EPOCH = 2440588;

    private static ByteBuffer littleEndian(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer nullFlags(byte... flags) {
        ByteBuffer buf = ByteBuffer.allocate(flags.length);
        buf.put(flags);
        buf.flip();
        return buf;
    }

    @Test
    public void testCreateBoxedLocalDateArrayNonNullable() {
        // 2024-01-15 -> 19737 days since unix epoch -> julian = 19737 + 2440588.
        long epoch20240115 = LocalDate.of(2024, 1, 15).toEpochDay();
        ByteBuffer data = littleEndian(3 * 4);
        data.putInt(JULIAN_UNIX_EPOCH);                          // 1970-01-01
        data.putInt((int) (epoch20240115 + JULIAN_UNIX_EPOCH));  // 2024-01-15
        data.putInt(JULIAN_UNIX_EPOCH - 1);                      // 1969-12-31
        data.flip();

        Object[] result = UDFHelper.createBoxedLocalDateArray(3, null, data);
        Assertions.assertEquals(3, result.length);
        Assertions.assertEquals(LocalDate.of(1970, 1, 1), result[0]);
        Assertions.assertEquals(LocalDate.of(2024, 1, 15), result[1]);
        Assertions.assertEquals(LocalDate.of(1969, 12, 31), result[2]);
    }

    @Test
    public void testCreateBoxedLocalDateArrayNullable() {
        ByteBuffer data = littleEndian(2 * 4);
        data.putInt(JULIAN_UNIX_EPOCH);
        data.putInt(0); // ignored, masked by null flag
        data.flip();
        ByteBuffer nulls = nullFlags((byte) 0, (byte) 1);

        Object[] result = UDFHelper.createBoxedLocalDateArray(2, nulls, data);
        Assertions.assertEquals(LocalDate.of(1970, 1, 1), result[0]);
        Assertions.assertNull(result[1]);
    }

    @Test
    public void testCreateBoxedLocalDateTimeArrayNonNullable() {
        ByteBuffer data = littleEndian(2 * 8);
        data.putLong(packed(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
        data.putLong(packed(LocalDateTime.of(2024, 1, 15, 12, 34, 56, 789_000_000)));
        data.flip();

        Object[] result = UDFHelper.createBoxedLocalDateTimeArray(2, null, data);
        Assertions.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 0), result[0]);
        Assertions.assertEquals(LocalDateTime.of(2024, 1, 15, 12, 34, 56, 789_000_000), result[1]);
    }

    @Test
    public void testCreateBoxedLocalDateTimeArrayNullable() {
        ByteBuffer data = littleEndian(2 * 8);
        data.putLong(packed(LocalDateTime.of(2025, 6, 30, 23, 59, 59, 999_000)));
        data.putLong(0L);
        data.flip();
        ByteBuffer nulls = nullFlags((byte) 0, (byte) 1);

        Object[] result = UDFHelper.createBoxedLocalDateTimeArray(2, nulls, data);
        Assertions.assertEquals(LocalDateTime.of(2025, 6, 30, 23, 59, 59, 999_000), result[0]);
        Assertions.assertNull(result[1]);
    }

    // packedTimestampFromLocalDateTime / localDateTimeFromPackedTimestamp must round-trip.
    @Test
    public void testLocalDateTimePackedRoundTrip() {
        LocalDateTime[] cases = {
                LocalDateTime.of(1, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1_000), // 1 microsecond
                LocalDateTime.of(2024, 1, 15, 12, 34, 56, 789_000),
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000),
        };
        for (LocalDateTime input : cases) {
            long packed = UDFHelper.packedTimestampFromLocalDateTime(input);
            LocalDateTime decoded = UDFHelper.localDateTimeFromPackedTimestamp(packed);
            Assertions.assertEquals(input, decoded, "round-trip failed for " + input);
        }
    }

    // Layout of the StarRocks TimestampValue:
    //   high 24 bits = Julian day, low 40 bits = microseconds-of-day.
    private static long packed(LocalDateTime ldt) {
        long julian = ldt.toLocalDate().toEpochDay() + JULIAN_UNIX_EPOCH;
        long microsOfDay = ldt.toLocalTime().toNanoOfDay() / 1000L;
        return (julian << 40) | microsOfDay;
    }
}
