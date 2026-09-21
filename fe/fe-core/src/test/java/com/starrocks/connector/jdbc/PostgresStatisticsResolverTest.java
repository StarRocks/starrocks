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

import com.mockrunner.jdbc.PreparedStatementResultSetHandler;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The PostgreSQL side of the statistics path: the row-count fallback chain and the pg_stats read.
 *
 * <p>Each catalog query is routed by a distinctive fragment of its SQL, so a test states exactly
 * which of the six statements the source is expected to answer — and, just as importantly, which
 * it is expected never to be asked.
 */
public class PostgresStatisticsResolverTest {

    // Fragments unique to each statement in the chain.
    private static final String Q_RELTUPLES = "c.reltuples::bigint";
    private static final String Q_IS_PARTITIONED = "c.relkind = 'p'";
    // Deliberately keyed on the per-child CASE rather than on "SUM(child.reltuples)". The mock
    // matches a statement by substring, so anything that collapses the decision back to one taken
    // over the whole sum stops matching and the tests below fail rather than quietly passing.
    private static final String Q_CHILD_ROWS = "ELSE COALESCE(stat.n_live_tup, 0) END";
    private static final String Q_LIVE_TUPLES = "FROM pg_stat_all_tables WHERE";
    private static final String Q_PG_STATS = "FROM pg_stats WHERE";

    private PostgresSchemaResolver resolver;
    private MockConnection connection;
    private PreparedStatementResultSetHandler handler;

    @BeforeEach
    public void setUp() {
        resolver = new PostgresSchemaResolver();
        connection = new MockConnection();
        handler = connection.getPreparedStatementResultSetHandler();
    }

    private void answer(String sqlFragment, String columnName, List<Object> values) {
        MockResultSet rs = handler.createResultSet();
        rs.addColumn(columnName, values);
        handler.prepareResultSet(sqlFragment, rs);
    }

    private MockResultSet pgStatsResultSet() {
        MockResultSet rs = handler.createResultSet();
        rs.addColumn("attname");
        rs.addColumn("null_frac");
        rs.addColumn("n_distinct");
        rs.addColumn("avg_width");
        return rs;
    }

    // -------------------------------------------------------------------------
    // Row-count fallback chain
    // -------------------------------------------------------------------------

    @Test
    public void testRelTuplesIsUsedDirectly() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(1_000_000L));
        // Nothing else is registered: reaching for any other statement would return no rows and
        // the assertions below would not hold.
        Optional<JdbcTableStats> stats = resolver.getTableStatistics(connection, "pgstats_bench", "big");
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(1_000_000L, stats.get().getRowCount().getAsLong());
    }

    @Test
    public void testMissingTableYieldsNoRowCount() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of());
        Assertions.assertTrue(resolver.getTableStatistics(connection, "pgstats_bench", "nope").isEmpty(),
                "A table pg_class does not know about must produce no row count at all");
    }

    @Test
    public void testNeverAnalyzedTableYieldsNoRowCount() throws SQLException {
        // PostgreSQL 14+ reports -1, not 0, for a table ANALYZE has never touched.
        answer(Q_RELTUPLES, "reltuples", List.of(-1L));
        answer(Q_IS_PARTITIONED, "p", List.of());
        answer(Q_LIVE_TUPLES, "n_live_tup", List.of());
        Assertions.assertTrue(resolver.getTableStatistics(connection, "pgstats_bench", "never_analyzed").isEmpty(),
                "reltuples = -1 with no live-tuple count either: unknown must not be dressed up "
                        + "as a row count");
    }

    @Test
    public void testNeverAnalyzedTableFallsBackToLiveTuples() throws SQLException {
        // reltuples = -1 says ANALYZE has never run, but the statistics collector counts rows on
        // every insert regardless, so it is populated for exactly the tables reltuples is not.
        // Giving up here would also throw away the pg_stats column statistics autovacuum may
        // already have written -- the two are maintained by different machinery.
        answer(Q_RELTUPLES, "reltuples", List.of(-1L));
        answer(Q_IS_PARTITIONED, "p", List.of());
        answer(Q_LIVE_TUPLES, "n_live_tup", List.of(50_000L));

        JdbcTableStats stats = resolver.getTableStatistics(connection, "pgstats_bench", "never_analyzed").get();
        Assertions.assertEquals(50_000L, stats.getRowCount().getAsLong(),
                "A never-analyzed table with a live-tuple count is not a table without a row count");
    }

    @Test
    public void testPartitionParentSumsOverItsChildren() throws SQLException {
        // autovacuum analyzes the partitions but never the parent, so the parent sits at -1
        // forever while the children carry perfectly good counts.
        answer(Q_RELTUPLES, "reltuples", List.of(-1L));
        answer(Q_IS_PARTITIONED, "p", List.of(Boolean.TRUE));
        answer(Q_CHILD_ROWS, "sum", List.of(200_000L));
        Optional<JdbcTableStats> stats = resolver.getTableStatistics(connection, "pgstats_bench", "part_parent");
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(200_000L, stats.get().getRowCount().getAsLong());
    }

    /**
     * The decision between reltuples and the live-tuple counter belongs to each child, not to the
     * sum. A partitioned table is analyzed one partition at a time, so a parent normally has both
     * kinds of child at once and no single choice serves them: summing reltuples straight adds -1
     * for each never-analyzed partition, which both subtracts a row that does not exist and
     * contributes nothing for a partition that may hold millions.
     *
     * <p>Measured against PostgreSQL 16 on a four-partition table, two analyzed at 5,000 rows and
     * two never analyzed also holding 5,000: {@code SUM(child.reltuples)} gives 9,998,
     * {@code SUM(GREATEST(child.reltuples, 0))} gives 10,000, and choosing per child gives 20,000.
     *
     * <p>This asserts the shape of the statement rather than the arithmetic, because the choice is
     * made by PostgreSQL inside the SQL — the mock can only report what a real server would have
     * returned for it. {@link #Q_CHILD_ROWS} is keyed on the CASE for the same reason: a statement
     * that decides over the whole sum no longer matches, and this test fails.
     */
    @Test
    public void testPartitionParentDecidesPerChildRatherThanOverTheSum() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(-1L));
        answer(Q_IS_PARTITIONED, "p", List.of(Boolean.TRUE));
        answer(Q_CHILD_ROWS, "sum", List.of(20_000L));

        Optional<JdbcTableStats> stats = resolver.getTableStatistics(connection, "pgstats_bench", "part_parent");

        Assertions.assertTrue(stats.isPresent(),
                "A statement keyed on the per-child CASE must be the one the resolver sends");
        Assertions.assertEquals(20_000L, stats.get().getRowCount().getAsLong());
    }

    /**
     * Every child unanalyzed and unknown to the statistics collector too. The CASE turns each into
     * 0 rather than -1, so the sum cannot come back negative from that direction any more; what is
     * left is a parent with no children at all, where PostgreSQL returns no row.
     */
    @Test
    public void testPartitionParentWithNothingToSumYieldsNoRowCount() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(-1L));
        answer(Q_IS_PARTITIONED, "p", List.of(Boolean.TRUE));
        answer(Q_CHILD_ROWS, "sum", List.of());
        Assertions.assertTrue(resolver.getTableStatistics(connection, "pgstats_bench", "part_parent").isEmpty());
    }

    @Test
    public void testZeroRelTuplesCrossChecksLiveTuples() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(0L));
        answer(Q_IS_PARTITIONED, "p", List.of());
        answer(Q_LIVE_TUPLES, "n_live_tup", List.of(500L));
        Optional<JdbcTableStats> stats = resolver.getTableStatistics(connection, "pgstats_bench", "fresh");
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(500L, stats.get().getRowCount().getAsLong());
    }

    @Test
    public void testEmptyTableReportsZeroAndSkipsColumnStats() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(0L));
        answer(Q_IS_PARTITIONED, "p", List.of());
        answer(Q_LIVE_TUPLES, "n_live_tup", List.of(0L));
        // Registered but must never be consulted: there is nothing to scale a ratio by.
        MockResultSet pgStats = pgStatsResultSet();
        pgStats.addRow(new Object[] {"k", 0.0d, 5.0d, 4});
        handler.prepareResultSet(Q_PG_STATS, pgStats);

        Optional<JdbcTableStats> stats = resolver.getTableStatistics(connection, "pgstats_bench", "empty_tbl");
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(0L, stats.get().getRowCount().getAsLong());
        Assertions.assertTrue(stats.get().getColumnStats().isEmpty());
    }

    // -------------------------------------------------------------------------
    // pg_stats
    // -------------------------------------------------------------------------

    @Test
    public void testColumnStatisticsMapping() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(1_000_000L));
        MockResultSet pgStats = pgStatsResultSet();
        // Straight from pg_stats on a 1,000,000-row table.
        pgStats.addRow(new Object[] {"id", 0.0d, -1.0d, 4});
        pgStats.addRow(new Object[] {"company_id", 0.0d, 100.0d, 4});
        pgStats.addRow(new Object[] {"maybe_null", 0.5008d, -0.4992d, 7});
        handler.prepareResultSet(Q_PG_STATS, pgStats);

        Map<String, JdbcColumnStats> columns =
                resolver.getTableStatistics(connection, "pgstats_bench", "big").get().getColumnStats();
        Assertions.assertEquals(3, columns.size());

        // n_distinct = -1 is a ratio, not a count: every row distinct.
        Assertions.assertEquals(1_000_000d, columns.get("id").getDistinctValues().getAsDouble(), 0.001);
        // n_distinct >= 0 is already a count.
        Assertions.assertEquals(100d, columns.get("company_id").getDistinctValues().getAsDouble(), 0.001);
        // avg_width is bytes per row and is carried through unscaled.
        Assertions.assertEquals(4, columns.get("company_id").getAverageWidth().getAsInt());
        Assertions.assertEquals(0d, columns.get("company_id").getNullsFraction().getAsDouble(), 1e-9);

        Assertions.assertEquals(499_200d, columns.get("maybe_null").getDistinctValues().getAsDouble(), 0.5);
        Assertions.assertEquals(0.5008d, columns.get("maybe_null").getNullsFraction().getAsDouble(), 1e-9);
        Assertions.assertEquals(7, columns.get("maybe_null").getAverageWidth().getAsInt());
    }

    @Test
    public void testColumnLookupIsCaseInsensitive() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(1_000L));
        MockResultSet pgStats = pgStatsResultSet();
        pgStats.addRow(new Object[] {"company_id", 0.0d, 10.0d, 4});
        handler.prepareResultSet(Q_PG_STATS, pgStats);

        JdbcTableStats stats = resolver.getTableStatistics(connection, "pgstats_bench", "big").get();
        Assertions.assertNotNull(stats.getColumnStats("COMPANY_ID"));
    }

    /**
     * pg_stats reports attname raw; the schema StarRocks holds ran the same name through
     * normalizeColumnName, which for this dialect quotes anything not already lower case. The two
     * sides have to produce the same string or every mixed-case column is silently unknown — and
     * the difference is the quote characters, not the case, so the case-insensitive map does not
     * paper over it. This test builds both sides from the same raw names and makes them meet.
     */
    @Test
    public void testMixedCaseColumnKeysMatchTheNameTheSchemaCarries() throws SQLException {
        answer(Q_RELTUPLES, "reltuples", List.of(1_000L));
        MockResultSet pgStats = pgStatsResultSet();
        // Exactly what pg_stats returns for CREATE TABLE t (lower_col int, "MixedCol" int,
        // stringBig int): PostgreSQL folded the unquoted stringBig to lower case at creation, and
        // kept MixedCol as written because it was created quoted.
        pgStats.addRow(new Object[] {"lower_col", 0.0d, 10.0d, 4});
        pgStats.addRow(new Object[] {"MixedCol", 0.0d, 20.0d, 4});
        pgStats.addRow(new Object[] {"stringbig", 0.0d, 30.0d, 4});
        handler.prepareResultSet(Q_PG_STATS, pgStats);

        JdbcTableStats stats = resolver.getTableStatistics(connection, "pgstats_bench", "mixed").get();

        // The other side: the column names StarRocks actually asks with.
        List<Column> schema = resolver.convertToSRTable(columnsResultSet("lower_col", "MixedCol", "stringbig"));
        Assertions.assertEquals(3, schema.size());
        Assertions.assertEquals("\"MixedCol\"", schema.get(1).getName(),
                "guards the premise of this test: the dialect quotes a non-lower-case name");

        Assertions.assertNotNull(stats.getColumnStats(schema.get(0).getName()));
        Assertions.assertEquals(20d, stats.getColumnStats(schema.get(1).getName()).getDistinctValues().getAsDouble(),
                0.001, "a mixed-case column must find its own row, not fall through to unknown");
        Assertions.assertEquals(30d, stats.getColumnStats(schema.get(2).getName()).getDistinctValues().getAsDouble(),
                0.001);
    }

    /** A getColumns() result of plain 4-byte integer columns with the given names. */
    private MockResultSet columnsResultSet(String... columnNames) {
        MockResultSet rs = handler.createResultSet();
        List<Object> names = new ArrayList<>(List.of(columnNames));
        rs.addColumn("DATA_TYPE", Collections.nCopies(names.size(), Types.INTEGER));
        rs.addColumn("TYPE_NAME", Collections.nCopies(names.size(), "int4"));
        rs.addColumn("COLUMN_SIZE", Collections.nCopies(names.size(), 10));
        rs.addColumn("DECIMAL_DIGITS", Collections.nCopies(names.size(), 0));
        rs.addColumn("COLUMN_NAME", names);
        rs.addColumn("IS_NULLABLE", Collections.nCopies(names.size(), "YES"));
        rs.addColumn("REMARKS", Collections.nCopies(names.size(), null));
        return rs;
    }

    @Test
    public void testRowSecurityHidingPgStatsStillLeavesTheRowCount() throws SQLException {
        // pg_stats is defined with a row_security_active() filter, so a table with RLS enabled
        // returns no column rows at all — while reltuples reads exactly as before.
        answer(Q_RELTUPLES, "reltuples", List.of(1_000_000L));
        handler.prepareResultSet(Q_PG_STATS, pgStatsResultSet());

        JdbcTableStats stats = resolver.getTableStatistics(connection, "pgstats_bench", "rls_tbl").get();
        Assertions.assertEquals(1_000_000L, stats.getRowCount().getAsLong());
        Assertions.assertTrue(stats.getColumnStats().isEmpty());
    }

    // -------------------------------------------------------------------------
    // Other dialects
    // -------------------------------------------------------------------------

    @Test
    public void testDialectsWithoutStatisticsReportNothing() throws SQLException {
        // Neither dialect implements either statistics hook, so both keep their behaviour exactly.
        for (JDBCSchemaResolver other : List.of(new OracleSchemaResolver(), new SqlServerSchemaResolver())) {
            Assertions.assertTrue(other.getTableStatistics(connection, "db", "t").isEmpty(),
                    other.getClass().getSimpleName() + " must be unchanged by the statistics path");
        }
    }

    @Test
    public void testRowCountOnlyDialectsBridgeThroughTheBaseClass() throws SQLException {
        // MySQL and ClickHouse implement the older row-count hook and no column statistics; the
        // base class turns that into a JdbcTableStats carrying a row count and no columns.
        MockResultSet mysqlRows = handler.createResultSet();
        mysqlRows.addColumn("table_rows", List.of(3_000_000L));
        handler.prepareResultSet("information_schema.tables", mysqlRows);
        MockResultSet clickhouseRows = handler.createResultSet();
        clickhouseRows.addColumn("total_rows", List.of(7_000_000L));
        handler.prepareResultSet("FROM system.tables", clickhouseRows);

        Optional<JdbcTableStats> mysql = new MysqlSchemaResolver().getTableStatistics(connection, "db", "t");
        Assertions.assertTrue(mysql.isPresent());
        Assertions.assertEquals(3_000_000L, mysql.get().getRowCount().getAsLong());
        Assertions.assertTrue(mysql.get().getColumnStats().isEmpty());

        Optional<JdbcTableStats> clickhouse =
                new ClickhouseSchemaResolver(Map.of()).getTableStatistics(connection, "db", "t");
        Assertions.assertTrue(clickhouse.isPresent());
        Assertions.assertEquals(7_000_000L, clickhouse.get().getRowCount().getAsLong());
        Assertions.assertTrue(clickhouse.get().getColumnStats().isEmpty());
    }

    @Test
    public void testRowCountOnlyDialectReportsNothingWhenTheSourceHasNoAnswer() throws SQLException {
        // An empty result from information_schema means "no row count", not "one row".
        handler.prepareResultSet("information_schema.tables", handler.createResultSet());
        Assertions.assertTrue(new MysqlSchemaResolver().getTableStatistics(connection, "db", "t").isEmpty());
    }
}
