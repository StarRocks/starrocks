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

package com.starrocks.load.loadv2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.Pair;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.thrift.TLoadInfo;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * {@code _statistics_.load_jobs}: one row per load job or task of every entry point, kept
 * current while the load runs and after it finishes.
 *
 * <p>The table is a primary key table keyed by {@code (create_day, entry, id)}. Every write
 * is a full-row {@code INSERT ... VALUES} of the job's current {@code information_schema.loads}
 * row; on a primary key table that replaces the row with the same key, so the job's row
 * always reflects the last state the FE saw. The columns are those of the {@code loads} view
 * plus {@code create_day}, {@code entry} (which entry point created the load) and
 * {@code level} (JOB or TASK).
 *
 * <p>The key must never change for a load, or a later write would add a second row and leave
 * the first one in an outdated state. {@code create_day} is therefore the UTC day of the
 * moment the load was first created: it does not follow the {@code time_zone} variable, which
 * the datetime columns are rendered in and which can be changed while a load runs, and it
 * does not follow a retry, which gives a load job a new creation time (see
 * {@link LoadJob#toLoadJobsRow()}).
 *
 * <p>Unlike {@code loads_history}, which only receives finished loads, this table also holds
 * rows for running loads and one JOB row per routine load job, so that the job's state
 * (RUNNING, PAUSED, NEED_SCHEDULE, STOPPED, CANCELLED) and pause reason are queryable next to
 * its TASK rows.
 *
 * <p>The table is partitioned by {@code create_day}, but it has no partition TTL:
 * a load may run for longer than the retention (a routine load job usually does), and a TTL
 * would drop its row with the partition of the day it was created. {@link LoadJobsSyncer}
 * expires the table instead, see {@link #planExpiry}.
 */
public class LoadJobsTable {
    public static final String DB_NAME = LoadsHistorySyncer.LOADS_HISTORY_DB_NAME;
    public static final String TABLE_NAME = "load_jobs";
    public static final String FULL_NAME = CatalogUtils.normalizeTableName(DB_NAME, TABLE_NAME);

    public static final String ENTRY = "entry";
    public static final String LEVEL = "level";

    public static final String LEVEL_JOB = "JOB";
    public static final String LEVEL_TASK = "TASK";

    public static final String ENTRY_BROKER = "BROKER";
    public static final String ENTRY_SPARK = "SPARK";
    public static final String ENTRY_INSERT = "INSERT";
    public static final String ENTRY_STREAM_LOAD = "STREAM_LOAD";
    public static final String ENTRY_ROUTINE_LOAD = "ROUTINE_LOAD";
    public static final String ENTRY_MERGE_COMMIT = "MERGE_COMMIT";

    /** States after which a row no longer changes; everything else is a running load. */
    public static final Set<String> FINAL_STATES = ImmutableSet.of("FINISHED", "CANCELLED", "UNKNOWN", "STOPPED");

    // The key columns lead the definition because a primary key table requires it.
    public static final String CREATE_DAY = "create_day";

    static final List<String> KEY_COLUMNS = ImmutableList.of(CREATE_DAY, ENTRY, "id");

    // (column name, type) for every column, key columns first, then the loads view columns
    // that are not part of the key. TableKeeper adds whichever of these an existing table
    // lacks; the key columns are never missing because they are in the initial definition.
    static final List<Pair<String, String>> COLUMNS;

    static {
        ImmutableList.Builder<Pair<String, String>> columns = ImmutableList.builder();
        columns.add(Pair.create(CREATE_DAY, "date"));
        columns.add(Pair.create(ENTRY, "varchar(32)"));
        columns.add(Pair.create("id", "bigint"));
        columns.add(Pair.create(LEVEL, "varchar(16)"));
        for (Pair<String, String> column : LoadsHistorySyncer.HISTORY_COLUMNS) {
            if (!KEY_COLUMNS.contains(column.first)) {
                columns.add(column);
            }
        }
        COLUMNS = columns.build();
    }

    private static String columnDefinitions() {
        return COLUMNS.stream()
                .map(column -> "`" + column.first + "` " + column.second
                        + (KEY_COLUMNS.contains(column.first) || LEVEL.equals(column.first) ? " not null" : ""))
                .collect(Collectors.joining(", "));
    }

    /** Every column as a back-quoted, comma separated list in table order. */
    static String columnList() {
        return COLUMNS.stream()
                .map(column -> "`" + column.first + "`")
                .collect(Collectors.joining(", "));
    }

    static final String CREATE_TABLE = String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s) " +
            "PRIMARY KEY (`create_day`, `entry`, `id`) " +
            "PARTITION BY date_trunc('DAY', create_day) " +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 3 " +
            "PROPERTIES( " +
            "'replication_num' = '1'" +
            ")",
            FULL_NAME, columnDefinitions());

    // No TTL supplier: the syncer expires the table, see the class comment.
    private static final TableKeeper KEEPER = new TableKeeper(DB_NAME, TABLE_NAME, CREATE_TABLE, null, COLUMNS);

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    // ---- entry and level ----------------------------------------------------------------

    private static final Map<String, String> ENTRY_BY_TYPE = ImmutableMap.<String, String>builder()
            .put("BROKER", ENTRY_BROKER)
            .put("SPARK", ENTRY_SPARK)
            .put("INSERT", ENTRY_INSERT)
            .put("STREAM_LOAD", ENTRY_STREAM_LOAD)
            .put("PARALLEL_STREAM_LOAD", ENTRY_STREAM_LOAD)
            .put("MULTI_STATEMENT_STREAM_LOAD", ENTRY_STREAM_LOAD)
            .put("ROUTINE_LOAD", ENTRY_ROUTINE_LOAD)
            .put("MERGE_COMMIT", ENTRY_MERGE_COMMIT)
            .build();

    /** The entry point behind the {@code type} of a loads row; unknown types pass through. */
    public static String entryOf(TLoadInfo info) {
        String type = info.getType();
        if (type == null || type.isEmpty()) {
            return "UNKNOWN";
        }
        return ENTRY_BY_TYPE.getOrDefault(type.toUpperCase(), type.toUpperCase());
    }

    /**
     * JOB for self-contained loads, TASK for the units a job spawns: routine load tasks
     * (which carry their job in {@code parent_id}) and merge commit tasks.
     */
    public static String levelOf(TLoadInfo info) {
        String entry = entryOf(info);
        if (ENTRY_MERGE_COMMIT.equals(entry)) {
            return LEVEL_TASK;
        }
        if (ENTRY_ROUTINE_LOAD.equals(entry) && info.isSetParent_id()) {
            return LEVEL_TASK;
        }
        return LEVEL_JOB;
    }

    // ---- rendering ----------------------------------------------------------------------

    // The history renderers, with the counters rendered as NULL when the row does not carry
    // them (routine load JOB rows have no counters) and the two columns of this table added.
    private static final Map<String, Function<TLoadInfo, String>> RENDERERS;

    static {
        Map<String, Function<TLoadInfo, String>> renderers = new HashMap<>(LoadsHistoryWriter.RENDERERS);
        renderers.put(CREATE_DAY, info -> LoadsHistoryWriter.quoteRaw(keyDay(info.getCreate_time_ms())));
        renderers.put(ENTRY, info -> LoadsHistoryWriter.quote(entryOf(info)));
        renderers.put(LEVEL, info -> LoadsHistoryWriter.quote(levelOf(info)));
        renderers.put("scan_rows", info -> LoadsHistoryWriter.nullableLong(info.isSetNum_scan_rows(),
                info.getNum_scan_rows()));
        renderers.put("scan_bytes", info -> LoadsHistoryWriter.nullableLong(info.isSetNum_scan_bytes(),
                info.getNum_scan_bytes()));
        renderers.put("filtered_rows", info -> LoadsHistoryWriter.nullableLong(info.isSetNum_filtered_rows(),
                info.getNum_filtered_rows()));
        renderers.put("unselected_rows", info -> LoadsHistoryWriter.nullableLong(info.isSetNum_unselected_rows(),
                info.getNum_unselected_rows()));
        renderers.put("sink_rows", info -> LoadsHistoryWriter.nullableLong(info.isSetNum_sink_rows(),
                info.getNum_sink_rows()));
        RENDERERS = ImmutableMap.copyOf(renderers);
    }

    // Renderers in table column order; fails at class load if a column has no rendering.
    private static final List<Function<TLoadInfo, String>> COLUMN_RENDERERS = COLUMNS.stream()
            .map(column -> {
                Function<TLoadInfo, String> renderer = RENDERERS.get(column.first);
                if (renderer == null) {
                    throw new IllegalStateException("no load_jobs rendering for column " + column.first);
                }
                return renderer;
            })
            .collect(ImmutableList.toImmutableList());

    // Every INSERT names every column, so that each write replaces the whole row of its key
    // rather than updating part of it.
    static final String INSERT_PREFIX = "INSERT INTO " + FULL_NAME + " (" + columnList() + ") VALUES ";

    @VisibleForTesting
    static String renderRow(TLoadInfo info) {
        return LoadsHistoryWriter.renderRow(COLUMN_RENDERERS, info);
    }

    /** The primary key of the row, as one string: the rendered values of {@link #KEY_COLUMNS}. */
    static String keyOf(TLoadInfo info) {
        return keyDay(info.getCreate_time_ms()) + '|' + entryOf(info) + '|' + info.getJob_id();
    }

    private static final DateTimeFormatter KEY_DAY_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter UTC_DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /** The {@code create_day} of a load created at {@code epochMs}: the day in UTC, whatever {@code time_zone} is. */
    static String keyDay(long epochMs) {
        return KEY_DAY_FORMAT.format(Instant.ofEpochMilli(epochMs));
    }

    /** {@code epochMs} as {@code yyyy-MM-dd HH:mm:ss} in UTC, to compare with the bounds of the day partitions. */
    static String utcDatetime(long epochMs) {
        return UTC_DATETIME_FORMAT.format(Instant.ofEpochMilli(epochMs));
    }

    /** Rows of any state, as long as they carry the key. */
    static final LoadsHistoryWriter.Target TARGET = new LoadsHistoryWriter.Target() {
        @Override
        public String tableName() {
            return FULL_NAME;
        }

        @Override
        public boolean accepts(TLoadInfo info) {
            return info.isSetCreate_time_ms() && info.getCreate_time_ms() > 0;
        }

        @Override
        public String renderRow(TLoadInfo info) {
            return LoadJobsTable.renderRow(info);
        }

        @Override
        public String insertPrefix() {
            return INSERT_PREFIX;
        }

        @Override
        public String keyOf(TLoadInfo info) {
            return LoadJobsTable.keyOf(info);
        }

        @Override
        public LongCounterMetric droppedCounter() {
            return MetricRepo.hasInit ? MetricRepo.COUNTER_LOAD_JOBS_DROPPED : null;
        }

        @Override
        public GaugeMetricImpl<Long> flushLatencyGauge() {
            return MetricRepo.hasInit ? MetricRepo.GAUGE_LOAD_JOBS_FLUSH_LATENCY_MS : null;
        }
    };

    // ---- producers ----------------------------------------------------------------------

    /**
     * The writer that accepts rows on this node. Set while the leader daemon that drains it
     * runs, null on followers, so state changes on a node that is not the leader are ignored.
     */
    private static volatile LoadsHistoryWriter active;

    static void activate(LoadsHistoryWriter writer) {
        active = writer;
    }

    static void deactivate(LoadsHistoryWriter writer) {
        if (active == writer) {
            active = null;
        }
    }

    /**
     * Enqueue the current row of a load whose state just changed. The supplier is only
     * invoked when this node accepts rows, so followers do not pay for the conversion.
     */
    public static void offer(Supplier<TLoadInfo> row) {
        LoadsHistoryWriter.offerTo(active, row);
    }

    /** Same as {@link #offer(Supplier)} for jobs whose conversion yields a list of rows. */
    public static void offerAll(Supplier<List<TLoadInfo>> rows) {
        LoadsHistoryWriter.offerAllTo(active, rows);
    }

    /** Rows waiting on this node, for the queue gauge. */
    public static long queuedRows() {
        return LoadsHistoryWriter.queuedRows(active);
    }

    // ---- reconciliation -----------------------------------------------------------------

    /** Largest {@code IN} list one reconciliation statement carries. */
    static final int RECONCILE_IDS_PER_STATEMENT = 10000;

    /** Most non-final rows one reconciliation pass reads back. */
    static final int RECONCILE_MAX_ROWS = 100000;

    private static final String FINAL_STATE_LIST = FINAL_STATES.stream().sorted()
            .map(LoadsHistoryWriter::quoteRaw).collect(Collectors.joining(", "));

    /** A row the table holds in a non-final state. */
    static final class NonFinalRow {
        final long id;
        final String state;
        // Whether the load is past its timeout, so that it cannot be running any more.
        final boolean pastTimeout;

        NonFinalRow(long id, String state, boolean pastTimeout) {
            this.id = id;
            this.state = state;
            this.pastTimeout = pastTimeout;
        }
    }

    /**
     * The query that reads the rows whose state is not final: {@code id}, {@code state} and
     * whether the load is past its timeout at {@code now}.
     *
     * <p>The timeout is the {@code timeout} in {@code properties} when the row has a numeric
     * one, else a default per entry point ({@code streamLoadTimeoutSecond} for stream load and
     * merge commit rows, {@code routineLoadTaskTimeoutSecond} for routine load task rows,
     * {@code loadTimeoutSecond} for broker, spark and insert jobs). A routine load JOB row has
     * no timeout and always counts as past it: its job runs until it is stopped.
     *
     * <p>{@code createdSince} is a {@link #keyDay} that bounds {@code create_day} from below so
     * that only the partitions from that day on are read; null reads every partition.
     * {@code now} is a literal in the zone the datetime columns are written in
     * ({@link LoadsHistoryWriter#formatDatetime}), not NOW(), which would follow the session.
     */
    static String buildNonFinalRowsSql(String createdSince, String now, long loadTimeoutSecond,
                                       long streamLoadTimeoutSecond, long routineLoadTaskTimeoutSecond) {
        return "SELECT `id`, `state`, "
                + "IF((`entry` = '" + ENTRY_ROUTINE_LOAD + "' AND `level` = '" + LEVEL_JOB + "') "
                + "OR seconds_add(`create_time`, COALESCE(get_json_int(`properties`, '$.timeout'), "
                + "CASE `entry` WHEN '" + ENTRY_STREAM_LOAD + "' THEN " + streamLoadTimeoutSecond
                + " WHEN '" + ENTRY_MERGE_COMMIT + "' THEN " + streamLoadTimeoutSecond
                + " WHEN '" + ENTRY_ROUTINE_LOAD + "' THEN " + routineLoadTaskTimeoutSecond
                + " ELSE " + loadTimeoutSecond + " END)) < " + LoadsHistoryWriter.quoteRaw(now) + ", 1, 0) "
                + "FROM " + FULL_NAME + " WHERE `state` NOT IN (" + FINAL_STATE_LIST + ")"
                + (createdSince == null ? "" : " AND `create_day` >= " + LoadsHistoryWriter.quoteRaw(createdSince))
                + " LIMIT " + RECONCILE_MAX_ROWS;
    }

    /** The rows of {@link #buildNonFinalRowsSql}, from the cells of each result row. */
    static List<NonFinalRow> toNonFinalRows(List<List<String>> rows) {
        List<NonFinalRow> result = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            if (row.size() < 3 || row.get(0) == null || row.get(1) == null) {
                continue;
            }
            try {
                result.add(new NonFinalRow(Long.parseLong(row.get(0)), row.get(1), "1".equals(row.get(2))));
            } catch (NumberFormatException e) {
                // Not a row of this query; skip it.
            }
        }
        return result;
    }

    /**
     * Statements that mark the rows of {@code ids} as UNKNOWN, finished at {@code now}, unless
     * they reached a final state in the meantime. No statement carries more than
     * {@code idsPerStatement} ids, so a failure of one leaves the rest to the next pass.
     * {@code createdSince} is the bound the ids were read with, see {@link #buildNonFinalRowsSql}.
     */
    static List<String> buildMarkUnknownSql(Collection<Long> ids, int idsPerStatement, String createdSince, String now) {
        List<Long> sorted = ids.stream().distinct().sorted().collect(Collectors.toList());
        List<String> statements = new ArrayList<>();
        int chunk = Math.max(1, idsPerStatement);
        for (int from = 0; from < sorted.size(); from += chunk) {
            List<Long> part = sorted.subList(from, Math.min(sorted.size(), from + chunk));
            statements.add("UPDATE " + FULL_NAME + " SET `state` = 'UNKNOWN', `load_finish_time` = "
                    + LoadsHistoryWriter.quoteRaw(now)
                    + " WHERE `state` NOT IN (" + FINAL_STATE_LIST + ")"
                    + (createdSince == null ? "" : " AND `create_day` >= " + LoadsHistoryWriter.quoteRaw(createdSince))
                    + " AND `id` IN (" + join(part) + ")");
        }
        return statements;
    }

    /**
     * Queries that return which of {@code ids} have a row, in any state. No query carries more
     * than {@code idsPerStatement} ids.
     */
    static List<String> buildExistingIdsSql(Collection<Long> ids, int idsPerStatement) {
        List<Long> sorted = ids.stream().distinct().sorted().collect(Collectors.toList());
        List<String> statements = new ArrayList<>();
        int chunk = Math.max(1, idsPerStatement);
        for (int from = 0; from < sorted.size(); from += chunk) {
            List<Long> part = sorted.subList(from, Math.min(sorted.size(), from + chunk));
            statements.add("SELECT `id` FROM " + FULL_NAME + " WHERE `id` IN (" + join(part) + ")");
        }
        return statements;
    }

    private static String join(List<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }

    // ---- expiry -------------------------------------------------------------------------

    /** A day partition of the table: its name and the {@code create_day} range it holds. */
    static final class PartitionRange {
        final String name;
        // yyyy-MM-dd HH:mm:ss, lower inclusive and upper exclusive.
        final String lower;
        final String upper;

        PartitionRange(String name, String lower, String upper) {
            this.name = name;
            this.lower = lower;
            this.upper = upper;
        }
    }

    // A row is kept while its load is alive and, once it is final, until the retention has
    // passed since the load finished. A final row without a finish time counts from its creation.
    private static String keptCondition(String finishCutoff) {
        return "(`state` NOT IN (" + FINAL_STATE_LIST + ") OR COALESCE(`load_finish_time`, `create_time`) >= "
                + LoadsHistoryWriter.quoteRaw(finishCutoff) + ")";
    }

    /**
     * The query that lists, as the start of their day, the days before {@code partitionCutoff}
     * that hold a row to keep: a load that is not final yet, or one that finished at or after
     * {@code finishCutoff}.
     */
    static String buildLiveDaysSql(String partitionCutoff, String finishCutoff) {
        return "SELECT DISTINCT date_format(`create_day`, '%Y-%m-%d %H:%i:%s') FROM " + FULL_NAME
                + " WHERE `create_day` < " + LoadsHistoryWriter.quoteRaw(partitionCutoff)
                + " AND " + keptCondition(finishCutoff);
    }

    /**
     * Statements that expire the finished loads whose retention has passed.
     *
     * <p>The retention counts from the time a load finished, not from its creation: the final
     * row of a load that ran for longer than the retention (a routine load job that is
     * cancelled after months, with the reason in its row) stays for the full retention after
     * it ended. So a partition that ends at or before {@code partitionCutoff} (a UTC time, as
     * the partition bounds are) is dropped only if it holds no row to keep ({@code liveDays},
     * from {@link #buildLiveDaysSql}); otherwise it is kept and only its rows that finished
     * before {@code finishCutoff} are deleted. A partition that ends after the cutoff cannot
     * hold an expired row, since a load does not finish before it is created.
     */
    static List<String> planExpiry(List<PartitionRange> partitions, Collection<String> liveDays,
                                   String partitionCutoff, String finishCutoff) {
        List<String> statements = new ArrayList<>();
        for (PartitionRange partition : partitions) {
            if (partition.upper.compareTo(partitionCutoff) > 0) {
                continue;
            }
            boolean live = liveDays.stream()
                    .anyMatch(day -> day.compareTo(partition.lower) >= 0 && day.compareTo(partition.upper) < 0);
            if (live) {
                statements.add("DELETE FROM " + FULL_NAME
                        + " WHERE `create_day` >= " + LoadsHistoryWriter.quoteRaw(partition.lower)
                        + " AND `create_day` < " + LoadsHistoryWriter.quoteRaw(partition.upper)
                        + " AND NOT " + keptCondition(finishCutoff));
            } else {
                statements.add("ALTER TABLE " + FULL_NAME + " DROP PARTITION IF EXISTS `" + partition.name + "` FORCE");
            }
        }
        return statements;
    }
}
