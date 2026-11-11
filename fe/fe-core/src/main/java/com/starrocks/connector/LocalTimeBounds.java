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

import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.ast.expression.BinaryType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;
import java.util.List;

/**
 * A DATETIME constant compared with a column that holds instants (an Iceberg timestamptz, a Delta TIMESTAMP) is a local
 * time in the session time zone. Most local times name exactly one instant, but at a DST overlap a local time names two
 * and in a DST gap none, so a single instant is neither a superset nor a subset of the matching values.
 * <p>
 * For pruning, a superset is kept by comparing lower bounds with the earlier instant and upper bounds with the later
 * one; {@code =} becomes a range. A local time in a gap names no instant: every local time after it starts at the
 * transition instant, which serves as both. There is no subset to give, so under NOT, for {@code !=}, NOT IN or in a
 * strict (exact) conversion an ambiguous constant must not be pushed down.
 */
public final class LocalTimeBounds {
    private LocalTimeBounds() {
    }

    /**
     * {@code column op micros} on the column's epoch microseconds.
     */
    public record Bound(BinaryType op, long micros) {
    }

    /**
     * Whether {@code local} names other than exactly one instant in the session time zone.
     */
    public static boolean isAmbiguous(LocalDateTime local) {
        return zone().getRules().getValidOffsets(local).size() != 1;
    }

    /**
     * A superset of {@code column op local}: one bound, or a lower and an upper bound for an ambiguous {@code =}.
     *
     * @return null for an operator without a superset bound (e.g. {@code !=}).
     */
    public static List<Bound> superset(BinaryType op, LocalDateTime local) {
        long earlier = toMicros(local, true);
        long later = toMicros(local, false);
        switch (op) {
            case EQ:
                return earlier == later ? List.of(new Bound(BinaryType.EQ, earlier))
                        : List.of(new Bound(BinaryType.GE, earlier), new Bound(BinaryType.LE, later));
            case GE:
                return List.of(new Bound(BinaryType.GE, earlier));
            case GT:
                // c > a gap time holds from the transition instant itself
                return List.of(new Bound(isInGap(local) ? BinaryType.GE : BinaryType.GT, earlier));
            case LT:
                return List.of(new Bound(BinaryType.LT, later));
            case LE:
                return List.of(new Bound(BinaryType.LE, later));
            default:
                return null;
        }
    }

    /**
     * The instants a column may hold where it equals {@code local}: both at a DST overlap, and the gap's transition
     * instant for a time in a gap (which no value equals, so any instant is a superset).
     */
    public static List<Long> equalInstants(LocalDateTime local) {
        long earlier = toMicros(local, true);
        long later = toMicros(local, false);
        return earlier == later ? List.of(earlier) : List.of(earlier, later);
    }

    private static boolean isInGap(LocalDateTime local) {
        return zone().getRules().getValidOffsets(local).isEmpty();
    }

    private static long toMicros(LocalDateTime local, boolean earlier) {
        ZoneId zone = zone();
        ZoneOffsetTransition gap = zone.getRules().getTransition(local);
        if (gap != null && gap.isGap()) {
            // atZone() would move the time forward by the gap length, past local times that follow the gap
            return toMicros(gap.getInstant());
        }
        ZonedDateTime zoned = local.atZone(zone);
        zoned = earlier ? zoned.withEarlierOffsetAtOverlap() : zoned.withLaterOffsetAtOverlap();
        return toMicros(zoned.toInstant());
    }

    /**
     * Epoch microseconds; ChronoUnit.MICROS.between counts in nanoseconds and overflows before 1677 and after 2262.
     */
    public static long toMicros(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
    }

    private static ZoneId zone() {
        return TimeUtils.getTimeZone().toZoneId();
    }
}
