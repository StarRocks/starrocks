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

package com.starrocks.context;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Resolves an {@code as_of_time} or explicit {@code snapshot_version} to the canonical
 * {@code snapshot_version} fence that the batch-read paths use. The architecture doc §8.4 specifies
 * that {@code as_of_time} is interpreted against {@link ContextInternalTables#COMMITS}, not BE
 * tablet state, so cross-collection reads always see a consistent frontier.
 *
 * <p>The three read modes ({@link ReadMode}) each take one SELECT against the commits table:
 * {@code CURRENT} picks the max snapshot, {@code AS_OF_TIME} picks the max snapshot with
 * {@code commit_time &lt;= as_of}, and {@code EXACT} just echoes the caller-supplied value after a
 * visibility check. Unresolvable requests return {@code -1} so callers can short-circuit with a
 * clear error.
 */
public class SnapshotResolver {

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public enum ReadMode {
        CURRENT,
        AS_OF_TIME,
        EXACT
    }

    public static final class Request {
        public ReadMode mode = ReadMode.CURRENT;
        public Long contextBaseId;
        public String asOfTime;
        public Long exactSnapshot;
    }

    public long resolve(Request request) {
        switch (request.mode) {
            case CURRENT:
                return resolveCurrent(request.contextBaseId);
            case AS_OF_TIME:
                if (Strings.isNullOrEmpty(request.asOfTime)) {
                    throw new IllegalArgumentException("AS_OF_TIME mode requires as_of_time");
                }
                return resolveAsOf(request.contextBaseId, request.asOfTime);
            case EXACT:
                if (request.exactSnapshot == null) {
                    throw new IllegalArgumentException("EXACT mode requires exact_snapshot");
                }
                return resolveExact(request.exactSnapshot);
            default:
                throw new IllegalArgumentException("unknown mode: " + request.mode);
        }
    }

    /**
     * Quality-of-life helper: accept an ISO-like timestamp, a raw snapshot number, or a blank string
     * (meaning "current"). Strings that do not parse as either fall back to CURRENT with a hint in
     * the caller-facing exception.
     *
     * <p>Both numeric and timestamp inputs use "at-or-before" semantics: the returned fence is the
     * largest visible {@code snapshot_version} that is &lt;= the requested point. This matches the
     * contract that every read/search endpoint applies via {@code snapshot_version &lt;= fence} on
     * the version table. A future input (above the latest commit) clamps to the latest visible
     * snapshot; a past input (below the earliest commit) returns {@code -1} so callers can short-
     * circuit with a clear error.
     */
    public long resolveFromSelector(Long contextBaseId, String asOfTimeOrSnapshot) {
        if (Strings.isNullOrEmpty(asOfTimeOrSnapshot)) {
            return resolveCurrent(contextBaseId);
        }
        try {
            long snapshot = Long.parseLong(asOfTimeOrSnapshot.trim());
            return resolveAsOfSnapshot(contextBaseId, snapshot);
        } catch (NumberFormatException ignored) {
            // fall through to timestamp path
        }
        // Accept "YYYY-MM-DD" by padding with midnight so the caller doesn't have to.
        String normalized = asOfTimeOrSnapshot.trim();
        if (normalized.length() == 10) {
            normalized = normalized + " 00:00:00";
        }
        try {
            LocalDateTime.parse(normalized, TS_FMT);
        } catch (Exception e) {
            throw new IllegalArgumentException("as_of selector must be a snapshot_version or "
                    + "yyyy-MM-dd[ HH:mm:ss] timestamp; got: " + asOfTimeOrSnapshot);
        }
        return resolveAsOf(contextBaseId, normalized);
    }

    private long resolveCurrent(Long contextBaseId) {
        StringBuilder sql = new StringBuilder(
                "SELECT MAX(snapshot_version) FROM __internal_context.context_commits "
                        + "WHERE visibility_state = 'VISIBLE'");
        if (contextBaseId != null) {
            sql.append(" AND contextbase_id = ").append(contextBaseId);
        }
        return readScalarLong(sql.toString());
    }

    private long resolveAsOf(Long contextBaseId, String asOfTime) {
        StringBuilder sql = new StringBuilder(
                "SELECT MAX(snapshot_version) FROM __internal_context.context_commits "
                        + "WHERE visibility_state = 'VISIBLE' AND commit_time <= '");
        sql.append(asOfTime.replace("'", "''")).append('\'');
        if (contextBaseId != null) {
            sql.append(" AND contextbase_id = ").append(contextBaseId);
        }
        return readScalarLong(sql.toString());
    }

    private long resolveExact(long snapshot) {
        String sql = String.format(
                "SELECT snapshot_version FROM __internal_context.context_commits "
                        + "WHERE snapshot_version = %d AND visibility_state = 'VISIBLE'",
                snapshot);
        return readScalarLong(sql);
    }

    /**
     * "At-or-before" snapshot resolution: returns the largest visible snapshot_version that is
     * &lt;= the requested point. Used by {@link #resolveFromSelector(Long, String)} so a numeric
     * fence input has the same semantics as a timestamp string (which uses {@link #resolveAsOf}).
     * Returns {@code -1} when nothing is visible at or before the request.
     */
    private long resolveAsOfSnapshot(Long contextBaseId, long snapshot) {
        StringBuilder sql = new StringBuilder(
                "SELECT MAX(snapshot_version) FROM __internal_context.context_commits "
                        + "WHERE visibility_state = 'VISIBLE' AND snapshot_version <= ");
        sql.append(snapshot);
        if (contextBaseId != null) {
            sql.append(" AND contextbase_id = ").append(contextBaseId);
        }
        return readScalarLong(sql.toString());
    }

    private long readScalarLong(String sql) {
        JsonArray rows;
        try {
            rows = ContextSqlSupport.executeDql(sql);
        } catch (Exception e) {
            // Internal tables not yet populated: treat as "no snapshot available".
            return -1L;
        }
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() > 0 && !data.get(0).isJsonNull()) {
                return data.get(0).getAsLong();
            }
        }
        return -1L;
    }
}
