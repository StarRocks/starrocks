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

package com.starrocks.load.rejected;

import com.starrocks.common.Config;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.statistic.StatsConstants;

/**
 * System table that stores rejected records produced during data loading.
 *
 * <p>Records filtered by {@code max_filter_ratio}, {@code strict_mode}, type conversion
 * failures, NOT NULL violations, partition-range misses, and similar row-level rejection
 * paths are persisted into this table so operators can query, inspect, and replay them
 * via standard SQL instead of scraping BE local files.
 *
 * <p>The table lives in the {@code _statistics_} database alongside other system-owned
 * tables (loads_history, task_run_history, tablet_write_log_history, ...). It is a
 * Primary Key table so that the BE-assigned UUID ({@code id}) makes background
 * write-back retries idempotent (at-least-once + PK dedup = exactly-once).
 *
 * <p>See the design document referenced from the feature branch for the end-to-end
 * data flow (BE writer -> local JSON Lines -> BE sync daemon -> merge-commit Stream
 * Load -> this table).
 */
public final class RejectedRecordsTable {

    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "rejected_records";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;

    /**
     * Lower bound on the daily-partition retention count. Callers go through
     * {@link #resolvedRetentionDays(int)} which clamps operator-supplied values
     * to this floor so that {@code partition_live_number} can never be set to 0
     * (which would disable partition GC entirely).
     */
    static final int MIN_RETAINED_DAYS = 1;

    /**
     * Clamp an operator-supplied retention value before it is written to either
     * the initial DDL or the live table property. Zero / negative values are
     * pinned to {@link #MIN_RETAINED_DAYS}.
     */
    static int resolvedRetentionDays(int configValue) {
        return Math.max(MIN_RETAINED_DAYS, configValue);
    }

    /**
     * DDL for the rejected records system table.
     *
     * Column layout (14 columns) mirrors the cross-industry survey in the design doc
     * (Redshift STL_LOAD_ERRORS / SYS_LOAD_ERROR_DETAIL, ClickHouse dead_letter_queue,
     * Vertica rejected-data-table). The raw rejected row is stored as JSON so that
     * CSV, JSON, Parquet, ORC, Avro, and Sink-stage rejections can share one schema
     * and so that replay queries can extract columns by name via {@code raw_record->'col'}.
     *
     * <p>The DDL is built once at class load and interpolates the live
     * {@link Config#rejected_records_retained_days} value so the first-ever
     * partition honors the operator's configured retention instead of waiting for
     * the next TableKeeper tick to reconcile it.
     */
    public static final String CREATE_TABLE = buildCreateTableSql();

    private static String buildCreateTableSql() {
        int retainedDays = resolvedRetentionDays(Config.rejected_records_retained_days);
        return "CREATE TABLE IF NOT EXISTS " + TABLE_FULL_NAME + " (" +
                // Identity (PK).
                "id varchar(64) NOT NULL " +
                "COMMENT 'UUID assigned at rejection time on BE', " +
                "created_at datetime NOT NULL " +
                "COMMENT 'Wall-clock time the record was produced; also the partition key', " +

                // Target (permission-filtering basis).
                "target_database varchar(256) NOT NULL " +
                "COMMENT 'Database name of the load target table', " +
                "target_table varchar(256) NOT NULL " +
                "COMMENT 'Name of the load target table', " +

                // Load context.
                // load_label uses varchar(2048) to match `_statistics_.loads_history.label`
                // and the actual upper bound accepted by StarRocks load labels.
                "load_label varchar(2048) NOT NULL " +
                "COMMENT 'Load label', " +
                "load_type varchar(32) NOT NULL " +
                "COMMENT 'STREAM_LOAD / ROUTINE_LOAD / BROKER_LOAD / INSERT', " +
                "txn_id bigint " +
                "COMMENT 'Transaction id, joinable with information_schema.loads', " +
                "user_name varchar(128) " +
                "COMMENT 'User that submitted the load', " +

                // Error context.
                "error_code varchar(64) " +
                "COMMENT 'Error code enum e.g. TYPE_MISMATCH / NULL_VIOLATION / PARSE_ERROR', " +
                "error_message varchar(1024) " +
                "COMMENT 'Detailed error description', " +
                "error_column varchar(256) " +
                "COMMENT 'Offending column name when determinable', " +

                // Payload.
                "raw_record json " +
                "COMMENT 'Rejected row as JSON; falls back to {\"_raw\": text} on parse failure', " +
                "source_info json " +
                "COMMENT 'Source metadata (file+line / Kafka topic+partition+offset / INSERT fragment)', " +

                // Diagnostics.
                "backend_id bigint " +
                "COMMENT 'BE node that produced the record'" +
                ") " +
                "PRIMARY KEY (id, created_at) " +
                "PARTITION BY date_trunc('DAY', created_at) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 8 " +
                "PROPERTIES(" +
                "'replication_num' = '1', " +
                "'partition_live_number' = '" + retainedDays + "'" +
                ")";
    }

    private static final TableKeeper KEEPER = new TableKeeper(
            DATABASE_NAME,
            TABLE_NAME,
            CREATE_TABLE,
            () -> resolvedRetentionDays(Config.rejected_records_retained_days));

    private RejectedRecordsTable() {
    }

    public static TableKeeper createKeeper() {
        return KEEPER;
    }
}
