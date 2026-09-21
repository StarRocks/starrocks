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

package com.starrocks.connector.jdbc;

import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Dialect-neutral table statistics handed back by a {@link JDBCSchemaResolver}.
 *
 * <p>"No row count" is a first-class state here, distinct from "zero rows" and from any default.
 * That distinction is the whole point of the type: a resolver that could not reach the source, or
 * found a table the source has never analyzed, returns {@link #unknown()}, and the caller must not
 * stamp such a result as trustworthy table metadata.
 *
 * <p>Column lookups are case-insensitive so a dialect that folds identifiers (PostgreSQL stores
 * {@code attname} exactly as created, Oracle upper-cases) still matches the column names
 * StarRocks captured from the JDBC driver's metadata.
 */
public class JdbcTableStats {

    private static final JdbcTableStats UNKNOWN = new JdbcTableStats(OptionalLong.empty(), Map.of(), false);

    private final OptionalLong rowCount;
    private final TreeMap<String, JdbcColumnStats> columnStats;
    private final boolean columnsWereRead;

    /**
     * For a dialect that reads per-column statistics. An empty map then means "this source was
     * asked and reported none for this table", which is a different answer from a dialect that
     * cannot be asked at all -- see {@link #describesColumns()}.
     */
    public JdbcTableStats(OptionalLong rowCount, Map<String, JdbcColumnStats> columnStats) {
        this(rowCount, columnStats, true);
    }

    private JdbcTableStats(OptionalLong rowCount, Map<String, JdbcColumnStats> columnStats, boolean columnsWereRead) {
        this.rowCount = rowCount == null ? OptionalLong.empty() : rowCount;
        this.columnStats = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnStats != null) {
            this.columnStats.putAll(columnStats);
        }
        this.columnsWereRead = columnsWereRead;
    }

    /** Nothing could be established: neither a row count nor any column statistic. */
    public static JdbcTableStats unknown() {
        return UNKNOWN;
    }

    /** A dialect that can report a row count but no column statistics (e.g. MySQL today). */
    public static JdbcTableStats ofRowCount(long rowCount) {
        return new JdbcTableStats(OptionalLong.of(rowCount), Map.of(), false);
    }

    /**
     * Whether the source was asked about its columns at all.
     *
     * <p>This separates two situations an empty {@link #getColumnStats()} cannot: a dialect that
     * reports no column statistics in the first place (MySQL, ClickHouse), and a dialect that does
     * but had nothing to say about <em>this</em> table. The two deserve opposite answers. For the
     * first, the caller's long-standing type-ratio estimate is the best available and nothing has
     * changed. For the second, the estimate is actively harmful: a PostgreSQL partition parent has
     * an honest row count from its children but no {@code pg_stats} rows of its own, because
     * PostgreSQL never analyzes the parent on its own, and feeding that real row count to a
     * heuristic produces a confident, badly wrong NDV. Measured on a 20,000-row parent whose
     * {@code region} column holds 5 values: the heuristic answered 10,000, and an equality on
     * {@code region} was then costed at 2 rows against a true 12,000.
     */
    public boolean describesColumns() {
        return columnsWereRead;
    }

    public OptionalLong getRowCount() {
        return rowCount;
    }

    public boolean hasRowCount() {
        return rowCount.isPresent();
    }

    public Map<String, JdbcColumnStats> getColumnStats() {
        return columnStats;
    }

    @Nullable
    public JdbcColumnStats getColumnStats(String columnName) {
        return columnName == null ? null : columnStats.get(columnName);
    }

    @Override
    public String toString() {
        return "JdbcTableStats{rowCount=" + rowCount + ", columns=" + columnStats.size() + "}";
    }
}
