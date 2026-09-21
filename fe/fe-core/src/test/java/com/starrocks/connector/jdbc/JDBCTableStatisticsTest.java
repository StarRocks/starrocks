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

import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What a JDBC catalog hands the optimizer, given what the dialect was able to read.
 *
 * <p>These tests stop at the connector boundary: a stub resolver stands in for the source database
 * so that each case states exactly what the source knew, and the assertions are about how that is
 * translated — in particular, about what happens when the source knew nothing.
 */
public class JDBCTableStatisticsTest {

    @Mocked
    HikariDataSource dataSource;
    @Mocked
    Connection connection;
    @Mocked
    PreparedStatement preparedStatement;

    private Map<String, String> properties;
    private MockResultSet columnResult;

    /** Stands in for the source database: returns exactly what it was told to, and counts reads. */
    private static class StubResolver extends PostgresSchemaResolver {
        private final Optional<JdbcTableStats> stats;
        private final AtomicInteger loads = new AtomicInteger();

        StubResolver(Optional<JdbcTableStats> stats) {
            this.stats = stats;
        }

        @Override
        public Optional<JdbcTableStats> getTableStatistics(Connection connection, String dbName, String tableName) {
            loads.incrementAndGet();
            return stats;
        }
    }

    @BeforeEach
    public void setUp() throws SQLException {
        columnResult = new MockResultSet("columns");
        // The last two are the decimal shapes a PostgreSQL numeric maps to: a declared
        // numeric(10,2) becomes DECIMAL64 (8 bytes), an undeclared numeric is narrowed to
        // DECIMAL128(38,18) (16 bytes).
        columnResult.addColumn("DATA_TYPE", Arrays.asList(Types.INTEGER, Types.VARCHAR, Types.DOUBLE, Types.DATE,
                Types.NUMERIC, Types.NUMERIC));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("INTEGER", "VARCHAR", "FLOAT8", "DATE",
                "numeric", "numeric"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(10, 64, 17, 13, 10, 0));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 17, 0, 2, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("company_id", "name", "val", "created_at",
                "amount", "unbounded_amount"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("NO", "YES", "YES", "YES", "YES", "YES"));
        columnResult.addColumn("REMARKS", Arrays.asList(null, null, null, null, null, null));

        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "org.postgresql.Driver");
        properties.put(JDBCResource.URI, "jdbc:postgresql://127.0.0.1:5432/t1");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");

        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "t1";
                minTimes = 0;

                connection.getMetaData().getColumns("t1", "test", "tbl1", "%");
                result = columnResult;
                minTimes = 0;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    private static JdbcColumnStats column(double nullsFraction, double distinct, int width) {
        return new JdbcColumnStats(OptionalDouble.of(nullsFraction), OptionalDouble.of(distinct),
                OptionalInt.of(width));
    }

    /**
     * Build a catalog whose source reports {@code stats}, and drive it until the asynchronous load
     * has landed. Returns the statistics the optimizer would be handed for {@code requestedColumns}.
     */
    private Statistics planningStatisticsFor(Optional<JdbcTableStats> stats, List<String> requestedColumns)
            throws Exception {
        JDBCMetadata metadata = new JDBCMetadata(properties, "catalog", dataSource);
        JDBCTable table = (JDBCTable) metadata.getTable(new ConnectContext(), "test", "tbl1");
        StubResolver resolver = new StubResolver(stats);
        metadata.schemaResolver = resolver;
        return awaitStatistics(metadata, table, requestedColumns, resolver);
    }

    private Statistics awaitStatistics(JDBCMetadata metadata, JDBCTable table, List<String> requestedColumns,
                                       StubResolver resolver) throws Exception {
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        int id = 0;
        for (String name : requestedColumns) {
            Column column = table.getColumn(name);
            Assertions.assertNotNull(column, "no such column in the fixture: " + name);
            columns.put(new ColumnRefOperator(id++, column.getType(), name, column.isAllowNull()), column);
        }
        Statistics statistics = null;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            statistics = metadata.getTableStatistics(null, table, columns,
                    Collections.<PartitionKey>emptyList(), null, -1, null);
            if (resolver.loads.get() > 0 && statistics.getStatsSource() != Statistics.StatsSource.NONE) {
                return statistics;
            }
            if (resolver.loads.get() > 0 && !resolver.stats.map(JdbcTableStats::hasRowCount).orElse(false)) {
                // Nothing to wait for: the source had no row count to report.
                return statistics;
            }
            Thread.sleep(20);
        }
        return statistics;
    }

    // -------------------------------------------------------------------------
    // "The source told us nothing" must not look like "the source told us one row"
    // -------------------------------------------------------------------------

    @Test
    public void testSourceWithoutARowCountIsNotStampedAsTableMetadata() throws Exception {
        // The dialect could not establish a row count — unreachable source, never-analyzed table,
        // a dialect with no statistics support at all. The old code folded all of those into
        // Config.default_statistics_output_row_count and marked the result TABLE_METADATA, so a
        // million-row table was reported as one row and declared trustworthy.
        Statistics statistics = planningStatisticsFor(Optional.empty(), List.of("company_id"));
        Assertions.assertEquals(Statistics.StatsSource.NONE, statistics.getStatsSource(),
                "A row count we never obtained must not be presented as read from the source");
        Assertions.assertEquals(Config.default_statistics_output_row_count, (long) statistics.getOutputRowCount());
        Assertions.assertTrue(statistics.getColumnStatistics().values().iterator().next().isUnknown());
    }

    @Test
    public void testResolverReportingNoRowCountButColumnsIsStillUnknown() throws Exception {
        // hasRowCount() is the gate: column statistics are ratios, and without a row count there is
        // nothing to scale them against.
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.empty(),
                Map.of("company_id", column(0, 100, 4)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id"));
        Assertions.assertEquals(Statistics.StatsSource.NONE, statistics.getStatsSource());
        Assertions.assertTrue(statistics.getColumnStatistics().values().iterator().next().isUnknown());
    }

    @Test
    public void testRealRowCountIsStampedAsTableMetadata() throws Exception {
        Statistics statistics = planningStatisticsFor(
                Optional.of(JdbcTableStats.ofRowCount(1_000_000L)), List.of("company_id"));
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, statistics.getStatsSource());
        Assertions.assertEquals(1_000_000L, (long) statistics.getOutputRowCount());
        // Row count without column statistics is the shape MySQL and ClickHouse are in: the row
        // count is now real, and the columns keep the type-ratio estimate they have always had.
        // See testDialectWithoutColumnStatisticsKeepsTheTypeRatioEstimate.
        Assertions.assertFalse(statistics.getColumnStatistics().values().iterator().next().isUnknown());
    }

    @Test
    public void testEveryRequestedColumnGetsAnEntry() throws Exception {
        // Statistics.getColumnStatistic throws on a missing column, so a partial answer from the
        // source must still produce a full map.
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L),
                Map.of("company_id", column(0, 100, 4)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats),
                List.of("company_id", "name", "val", "created_at"));
        Assertions.assertEquals(4, statistics.getColumnStatistics().size());
        statistics.getColumnStatistics().forEach((ref, stat) -> statistics.getColumnStatistic(ref));
    }

    // -------------------------------------------------------------------------
    // A pushed-down scan carries someone else's name
    // -------------------------------------------------------------------------

    @Test
    public void testPushedDownScanReportsNoStatisticsAndNeverConsultsTheCache() throws Exception {
        JDBCMetadata metadata = new JDBCMetadata(properties, "catalog", dataSource);
        JDBCTable table = (JDBCTable) metadata.getTable(new ConnectContext(), "test", "tbl1");
        StubResolver resolver = new StubResolver(Optional.of(JdbcTableStats.ofRowCount(1_000_000L)));
        metadata.schemaResolver = resolver;

        // What PushDownJoinToJDBCRule produces: a copy of one atom carrying the merged query. Its
        // name is still "tbl1", but it no longer denotes tbl1 — it denotes a join of two tables.
        JDBCTable merged = new JDBCTable(table);
        merged.setPushDownQuery("SELECT * FROM tbl1 JOIN tbl2 ON tbl1.company_id = tbl2.company_id");
        Assertions.assertTrue(merged.isInlineTable());

        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        Column column = merged.getColumn("company_id");
        columns.put(new ColumnRefOperator(0, column.getType(), "company_id", false), column);

        for (int i = 0; i < 5; i++) {
            Statistics statistics = metadata.getTableStatistics(null, merged, columns,
                    Collections.<PartitionKey>emptyList(), null, -1, null);
            Assertions.assertEquals(Statistics.StatsSource.NONE, statistics.getStatsSource(),
                    "A merged scan must not inherit whichever atom's name it happens to carry");
            Assertions.assertEquals(Config.default_statistics_output_row_count,
                    (long) statistics.getOutputRowCount());
            Thread.sleep(20);
        }
        Assertions.assertEquals(0, resolver.loads.get(),
                "The merged scan's name identifies no table in the source, so nothing should be loaded for it");
    }

    // -------------------------------------------------------------------------
    // Column statistics
    // -------------------------------------------------------------------------

    @Test
    public void testColumnStatisticsReachTheOptimizer() throws Exception {
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of(
                "company_id", column(0, 100, 4),
                "name", column(0.5, 4996, 9)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id", "name", "val"));

        ColumnStatistic companyId = statisticFor(statistics, "company_id");
        Assertions.assertFalse(companyId.isUnknown());
        Assertions.assertEquals(100d, companyId.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0d, companyId.getNullsFraction(), 1e-9);
        // avg_width is already a per-row average; multiplying it by the row count (as a total-bytes
        // reading of the source field would) would make it 4,000,000 bytes per row.
        Assertions.assertEquals(4d, companyId.getAverageRowSize(), 1e-9);

        ColumnStatistic name = statisticFor(statistics, "name");
        Assertions.assertEquals(4996d, name.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0.5d, name.getNullsFraction(), 1e-9);

        // A column the source said nothing about stays unknown rather than borrowing a neighbour's
        // numbers — this is what bounds the blast radius of the change.
        Assertions.assertTrue(statisticFor(statistics, "val").isUnknown());
    }

    @Test
    public void testColumnWithoutDistinctCountStaysUnknown() throws Exception {
        JdbcColumnStats partial = new JdbcColumnStats(OptionalDouble.of(0.1), OptionalDouble.empty(),
                OptionalInt.of(4));
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of("company_id", partial));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id"));
        Assertions.assertTrue(statisticFor(statistics, "company_id").isUnknown(),
                "A half-filled statistic would read as trustworthy; it must not be emitted");
    }

    // -------------------------------------------------------------------------
    // A decimal costs what StarRocks stores, not what the source stores
    // -------------------------------------------------------------------------

    /**
     * PostgreSQL's numeric is a variable-length value and pg_stats measures it as such — around
     * four bytes. StarRocks materializes the mapped type at a fixed width, so believing the source
     * under-counts the per-row cost by two to four times, which is enough to choose a broadcast
     * join where a shuffle was wanted.
     */
    @Test
    public void testDecimalTakesItsStarRocksWidthRatherThanTheSourceWidth() throws Exception {
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of(
                "amount", column(0, 5000, 4),
                "unbounded_amount", column(0, 5000, 4)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats),
                List.of("amount", "unbounded_amount"));

        Assertions.assertEquals(8d, statisticFor(statistics, "amount").getAverageRowSize(), 1e-9,
                "numeric(10,2) is a DECIMAL64 here: eight bytes, not the four pg_stats measured");
        Assertions.assertEquals(16d, statisticFor(statistics, "unbounded_amount").getAverageRowSize(), 1e-9,
                "an undeclared numeric is narrowed to DECIMAL128(38,18): sixteen bytes");
    }

    /**
     * The override is decimal-only on purpose. {@code Type#getTypeSize} is the tuple slot size in
     * this engine — 16 for DATE and DATETIME — so handing every fixed-width type its type size
     * would inflate a date from the measured 4 bytes to 16. The measured width stays.
     */
    @Test
    public void testNonDecimalColumnsKeepTheMeasuredWidth() throws Exception {
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of(
                "created_at", column(0, 365, 4),
                "company_id", column(0, 100, 4),
                "name", column(0, 4996, 9)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats),
                List.of("created_at", "company_id", "name"));

        Assertions.assertEquals(4d, statisticFor(statistics, "created_at").getAverageRowSize(), 1e-9,
                "a date measured at 4 bytes must not become the 16-byte slot size");
        Assertions.assertEquals(4d, statisticFor(statistics, "company_id").getAverageRowSize(), 1e-9);
        Assertions.assertEquals(9d, statisticFor(statistics, "name").getAverageRowSize(), 1e-9);
    }

    // -------------------------------------------------------------------------
    // No distinct count: an all-NULL column is not the same as a type without equality
    // -------------------------------------------------------------------------

    @Test
    public void testEntirelyNullColumnKeepsItsNullFraction() throws Exception {
        // PostgreSQL reports n_distinct = 0 for a column that is entirely NULL — there are no
        // values to count — but null_frac = 1 alongside it, which is strong information. Dropping
        // the whole row to protect against the missing NDV throws that away.
        JdbcColumnStats allNull = new JdbcColumnStats(OptionalDouble.of(1.0), OptionalDouble.empty(),
                OptionalInt.of(4));
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of("company_id", allNull));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id"));

        ColumnStatistic companyId = statisticFor(statistics, "company_id");
        Assertions.assertFalse(companyId.isUnknown(),
                "null_frac = 1 is a measurement, not an absence of one");
        Assertions.assertEquals(1d, companyId.getNullsFraction(), 1e-9);
        Assertions.assertEquals(1d, companyId.getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testTypeWithoutEqualityStaysUnknownRatherThanBeingCalledAllNull() throws Exception {
        // The other source of n_distinct = 0: a type with no equality operator (json, xml, point).
        // The column may be fully populated, so reading the zero as "all NULL" would assert
        // something plainly false about it. Only null_frac tells the two cases apart.
        JdbcColumnStats noEquality = new JdbcColumnStats(OptionalDouble.of(0.0), OptionalDouble.empty(),
                OptionalInt.of(25));
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of("name", noEquality));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("name"));
        Assertions.assertTrue(statisticFor(statistics, "name").isUnknown(),
                "a populated json column must not be declared entirely NULL");
    }

    // -------------------------------------------------------------------------
    // The escape hatch
    // -------------------------------------------------------------------------

    @Test
    public void testSessionVariableTurnsColumnStatisticsOffButKeepsTheRowCount() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.getSessionVariable().setEnableJdbcColumnStatistics(false);

        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of(
                "company_id", column(0, 100, 4)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id"));

        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, statistics.getStatsSource());
        Assertions.assertEquals(1_000_000L, (long) statistics.getOutputRowCount(),
                "The row count is a correctness fix, not a new estimate, so the switch leaves it alone");
        ColumnStatistic companyId = statisticFor(statistics, "company_id");
        Assertions.assertFalse(companyId.isUnknown(),
                "Turning the switch off puts the columns back where they were, which was a type-ratio "
                        + "estimate -- not a blank");
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, companyId.getType());
        Assertions.assertNotEquals(100.0, companyId.getDistinctValuesCount(),
                "The source's own distinct count must not survive the switch being off");
    }

    // -------------------------------------------------------------------------
    // Dialects this feature reads nothing for
    // -------------------------------------------------------------------------

    /**
     * MySQL and ClickHouse report a row count and no column statistics. They were given a
     * type-ratio estimate before this feature and must still be given one: reading pg_stats for
     * PostgreSQL is not a reason to blank their columns.
     */
    @Test
    public void testDialectWithoutColumnStatisticsKeepsTheTypeRatioEstimate() throws Exception {
        JdbcTableStats rowCountOnly = JdbcTableStats.ofRowCount(1_000_000L);
        Statistics statistics = planningStatisticsFor(Optional.of(rowCountOnly), List.of("company_id"));

        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, statistics.getStatsSource());
        ColumnStatistic companyId = statisticFor(statistics, "company_id");
        Assertions.assertFalse(companyId.isUnknown(),
                "A dialect that reports no column statistics must keep the estimate it always had");
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, companyId.getType());
    }

    /**
     * The other half of the split above, and the one that is easy to get wrong because an empty
     * column map looks identical from the outside: a source that <em>does</em> read column
     * statistics but had none for this table. A PostgreSQL partition parent is the live case — its
     * row count is honest, summed from the children, while pg_stats holds nothing for it because
     * PostgreSQL never analyzes a parent on its own.
     *
     * <p>Estimating there is worse than saying nothing, because the estimate scales NDV by the row
     * count and so looks measured. On a 20,000-row parent whose region column holds 5 values it
     * answered 10,000, and an equality on region was costed at 2 rows against a true 12,000.
     */
    @Test
    public void testSourceThatReadColumnsAndFoundNoneReportsUnknownRatherThanEstimating() throws Exception {
        JdbcTableStats readButEmpty = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of());
        Assertions.assertTrue(readButEmpty.describesColumns(),
                "The two-argument constructor is the one dialects that read columns use");

        Statistics statistics = planningStatisticsFor(Optional.of(readButEmpty), List.of("company_id"));

        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, statistics.getStatsSource(),
                "The row count is still honest and must keep its provenance");
        Assertions.assertEquals(1_000_000.0, statistics.getOutputRowCount());
        Assertions.assertTrue(statisticFor(statistics, "company_id").isUnknown(),
                "A source that was asked about its columns and answered nothing must not be "
                        + "topped up with a row-count-scaled guess");
    }

    /**
     * A column the source described and a column it did not, in the same table: the described one
     * wins, the other is honestly unknown rather than quietly estimated.
     */
    @Test
    public void testColumnTheSourceDidNotDescribeStaysUnknown() throws Exception {
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L), Map.of(
                "company_id", column(0, 100, 4)));
        Statistics statistics = planningStatisticsFor(Optional.of(stats), List.of("company_id", "name"));

        Assertions.assertEquals(100.0, statisticFor(statistics, "company_id").getDistinctValuesCount());
        Assertions.assertTrue(statisticFor(statistics, "name").isUnknown(),
                "Once the source describes this table, a column it left out is unknown, not invented");
    }

    // -------------------------------------------------------------------------
    // Caching
    // -------------------------------------------------------------------------

    @Test
    public void testOneLoadCoversRowCountAndColumnsTogether() throws Exception {
        JDBCMetadata metadata = new JDBCMetadata(properties, "catalog", dataSource);
        JDBCTable table = (JDBCTable) metadata.getTable(new ConnectContext(), "test", "tbl1");
        JdbcTableStats stats = new JdbcTableStats(OptionalLong.of(1_000_000L),
                Map.of("company_id", column(0, 100, 4)));
        StubResolver resolver = new StubResolver(Optional.of(stats));
        metadata.schemaResolver = resolver;

        Statistics statistics = awaitStatistics(metadata, table, List.of("company_id"), resolver);
        Assertions.assertEquals(1_000_000L, (long) statistics.getOutputRowCount());
        Assertions.assertFalse(statisticFor(statistics, "company_id").isUnknown());

        int loadsAfterWarmup = resolver.loads.get();
        for (int i = 0; i < 10; i++) {
            awaitStatistics(metadata, table, List.of("company_id"), resolver);
        }
        Assertions.assertEquals(loadsAfterWarmup, resolver.loads.get(),
                "Row count and column statistics live in one cache entry: acquiring the connection "
                        + "costs far more than the catalog queries, so they must be read together");
    }

    private static ColumnStatistic statisticFor(Statistics statistics, String columnName) {
        List<ColumnRefOperator> matches = new ArrayList<>();
        statistics.getColumnStatistics().keySet().forEach(ref -> {
            if (ref.getName().equals(columnName)) {
                matches.add(ref);
            }
        });
        Assertions.assertEquals(1, matches.size(), "expected exactly one ref named " + columnName);
        return statistics.getColumnStatistic(matches.get(0));
    }
}
