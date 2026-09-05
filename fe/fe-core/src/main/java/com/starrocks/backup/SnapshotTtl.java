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

import com.google.common.base.Strings;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.threeten.extra.PeriodDuration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * The BACKUP {@code ttl} property: how long the snapshot it creates is kept.
 *
 * <p>The value is written into the snapshot's job info file in the repository, together with the
 * absolute time it resolves to, and is never rewritten afterwards.
 */
public class SnapshotTtl {

    public static final String PROP_TTL = "ttl";

    private SnapshotTtl() {
    }

    /**
     * @return the parsed ttl, or null for an empty value, meaning the snapshot is kept forever.
     * @throws SemanticException if the value is neither empty nor a positive duration.
     */
    public static PeriodDuration parse(String value) {
        if (value == null || Strings.isNullOrEmpty(value.trim())) {
            return null;
        }

        PeriodDuration parsed;
        try {
            parsed = TimeUtils.parseHumanReadablePeriodOrDuration(value.trim());
        } catch (Exception e) {
            throw new SemanticException("Invalid ttl: '" + value
                    + "'. Expected a duration such as '7 DAY' or '12 HOUR', or an empty value to keep"
                    + " the snapshot forever");
        }

        boolean isZero = parsed.getPeriod().isZero() && parsed.getDuration().isZero();
        if (isZero || parsed.getPeriod().isNegative() || parsed.getDuration().isNegative()) {
            throw new SemanticException("ttl must be a positive duration, but got: " + value);
        }
        return parsed;
    }

    /**
     * When a snapshot whose backup wrapped up at {@code backupFinishTime} expires, or null if it
     * never does.
     *
     * <p>Added as a calendar period, so "1 MONTH" lands on the same day of the next month, as it
     * does for {@code storage_cooldown_ttl}.
     */
    public static Long computeExpireTime(long backupFinishTime, String ttl) {
        PeriodDuration duration = parse(ttl);
        if (duration == null) {
            return null;
        }

        ZoneId zone = TimeUtils.getSystemTimeZone().toZoneId();
        LocalDateTime expireAt =
                Instant.ofEpochMilli(backupFinishTime).atZone(zone).toLocalDateTime().plus(duration);
        return expireAt.atZone(zone).toInstant().toEpochMilli();
    }
}
