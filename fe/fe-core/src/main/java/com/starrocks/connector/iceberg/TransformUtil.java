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
// limitations under the License.package com.starrocks.connector.iceberg;

package com.starrocks.connector.iceberg;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

public class TransformUtil {

    private TransformUtil() {
    }

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final int EPOCH_YEAR = EPOCH.getYear();

    public static String humanYear(int yearOrdinal) {
        return String.format("%04d", EPOCH_YEAR + yearOrdinal);
    }

    public static String humanMonth(int monthOrdinal) {
        return String.format("%04d-%02d",
                EPOCH_YEAR + Math.floorDiv(monthOrdinal, 12), 1 + Math.floorMod(monthOrdinal, 12));
    }

    public static String humanDay(int dayOrdinal) {
        OffsetDateTime day = EPOCH.plusDays(dayOrdinal);
        return String.format("%04d-%02d-%02d",
                day.getYear(), day.getMonth().getValue(), day.getDayOfMonth());
    }

    static String humanTime(Long microsFromMidnight) {
        return LocalTime.ofNanoOfDay(microsFromMidnight * 1000).toString();
    }

    static String humanTimestampWithZone(Long timestampMicros) {
        return ChronoUnit.MICROS.addTo(EPOCH, timestampMicros).toString();
    }

    static String humanTimestampWithoutZone(Long timestampMicros) {
        return ChronoUnit.MICROS.addTo(EPOCH, timestampMicros).toLocalDateTime().toString();
    }

    static String humanHour(int hourOrdinal) {
        OffsetDateTime time = EPOCH.plusHours(hourOrdinal);
        return String.format("%04d-%02d-%02d-%02d",
                time.getYear(), time.getMonth().getValue(), time.getDayOfMonth(), time.getHour());
    }

    static String base64encode(ByteBuffer buffer) {
        // use direct encoding because all of the encoded bytes are in ASCII
        return StandardCharsets.ISO_8859_1.decode(Base64.getEncoder().encode(buffer)).toString();
    }
}