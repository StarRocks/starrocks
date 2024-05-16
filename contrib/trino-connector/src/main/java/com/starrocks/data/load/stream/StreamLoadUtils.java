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

package com.starrocks.data.load.stream;

import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Base64;

import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;

public class StreamLoadUtils
{
    private StreamLoadUtils()
    {
    }

    public static String getTableUniqueKey(String database, String table)
    {
        return database + "-" + table;
    }

    public static String getSendUrl(String host, String database, String table)
    {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + "/api/" + database + "/" + table + "/_stream_load";
    }

    public static String getBasicAuthHeader(String username, String password)
    {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);
    }

    public static LocalDateTime toLocalDateTime(TimestampType type, Block block, int position)
    {
        int precision = type.getPrecision();

        long epochMicros;
        int picosOfMicro = 0;
        if (precision <= MAX_SHORT_PRECISION) {
            epochMicros = type.getLong(block, position);
        }
        else {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            epochMicros = timestamp.getEpochMicros();
            picosOfMicro = timestamp.getPicosOfMicro();
        }

        long epochSecond = scaleEpochMicrosToSeconds(epochMicros);
        long nanoFraction = getMicrosOfSecond(epochMicros) * NANOSECONDS_PER_MICROSECOND + (roundToNearest(picosOfMicro, PICOSECONDS_PER_NANOSECOND) / PICOSECONDS_PER_NANOSECOND);

        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC);
    }

    public static long scaleEpochMicrosToSeconds(long epochMicros)
    {
        return Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static long getMicrosOfSecond(long epochMicros)
    {
        return floorMod(epochMicros, MICROSECONDS_PER_SECOND);
    }

    public static long roundToNearest(long value, long bound)
    {
        return roundDiv(value, bound) * bound;
    }
}
