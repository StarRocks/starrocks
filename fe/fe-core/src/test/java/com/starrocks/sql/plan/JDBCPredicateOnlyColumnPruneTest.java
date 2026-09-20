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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A JDBC scan's predicate is answered by the remote database, so a column the predicate names and
 * nothing else has already done its work before the first row travels back. Fetching it anyway is
 * pure waste -- for an array or JSON column, tens of values per row that no operator ever reads.
 *
 * <p>These tests pin the line between the two roles the scan's column map plays: every column stays
 * in {@code colRefToColumnMetaMap} (predicate rendering, identifier quoting and PostgreSQL
 * collation all read it, and none of them may lose a column), while only the columns the plan above
 * consumes stay materialized and therefore appear in the remote SELECT list.
 *
 * <p>The complement matters just as much: where the predicate is <em>not</em> answered remotely --
 * a function the dialect can't express, so the filter stays above the scan -- the column has to
 * keep coming back or there is nothing to evaluate against.
 */
public class JDBCPredicateOnlyColumnPruneTest extends ConnectorPlanTestBase {
    // A table of this class's own, so the schema it installs can't race another test class's.
    private static final String TABLE = "jdbc_postgres.partitioned_db0.prune_probe";
    private static final String REMOTE_TABLE = "prune_probe";
    // The same shape behind the MySQL-dialect mock catalog, to pin that this is about what the
    // remote answers, not about one dialect's syntax.
    private static final String MYSQL_TABLE = "jdbc0.partitioned_db0.prune_probe_mysql";

    private boolean previousJoinPushdown;
    private boolean previousAggregatePushdown;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                "jdbc_postgres", "partitioned_db0", "prune_probe");
        table.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("items", new ArrayType(VarcharType.VARCHAR)),
                new Column("nums", new ArrayType(IntegerType.INT)),
                new Column("name", VarcharType.VARCHAR),
                new Column("tag", VarcharType.VARCHAR),
                new Column("note", VarcharType.VARCHAR)));
        // PostgreSQL only collates what it reports as a string type, and a column without that
        // metadata fails closed -- so the collation cases need the remote type names. "note" is
        // deliberately left without one: nothing about its ordering may be pushed down, which is
        // what the locally-sorted cases need.
        table.setOriginalJdbcColumnTypeNames(Map.of("name", "text", "tag", "text"));

        JDBCTable mysqlTable = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                "jdbc0", "partitioned_db0", "prune_probe_mysql");
        mysqlTable.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("name", VarcharType.VARCHAR),
                new Column("tag", VarcharType.VARCHAR)));
    }

    @BeforeEach
    public void enablePushdown() {
        previousJoinPushdown = connectContext.getSessionVariable().isEnableJdbcJoinPushDown();
        previousAggregatePushdown = connectContext.getSessionVariable().isEnableJdbcAggPushDown();
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
    }

    @AfterEach
    public void restorePushdown() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(previousJoinPushdown);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(previousAggregatePushdown);
    }

    private String remoteQueries(String plan) {
        return plan.lines().filter(line -> line.contains("QUERY:")).collect(Collectors.joining("\n"));
    }

    /**
     * The columns the scan of {@code remoteTable} actually asks the remote database for. This is the
     * materialized-slot list rather than a substring of the remote SQL because it is the same list
     * three consumers have to agree on: {@code JDBCScanNode.createJDBCTableColumns()} builds the
     * SELECT list from it, {@code DescriptorTable} serializes exactly these slots, and the BE fills
     * its chunk by position over them.
     */
    private List<String> fetchedColumns(String sql, String remoteTable) throws Exception {
        List<ScanNode> scans = getExecPlan(sql).getScanNodes().stream()
                .filter(scan -> scan.getDesc().getTable() != null
                        && remoteTable.equals(scan.getDesc().getTable().getName()))
                .collect(Collectors.toList());
        Assertions.assertEquals(1, scans.size(), "expected exactly one scan of " + remoteTable);
        return scans.get(0).getDesc().getSlots().stream()
                .filter(SlotDescriptor::isMaterialized)
                .map(slot -> slot.getColumn().getName())
                .collect(Collectors.toList());
    }

    /**
     * The case the whole change exists for: the array column is named only by a predicate the
     * database answers, so not one of its elements has to travel back.
     */
    @Test
    public void testPredicateOnlyArrayColumnIsNotFetched() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE items[1] = 'alpha'";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, REMOTE_TABLE));

        String remote = remoteQueries(getFragmentPlan(sql));
        // The SELECT list loses the column; the WHERE clause keeps it, quoted -- the scan's column
        // map is untouched, so the renderer still knows the column's remote name.
        Assertions.assertTrue(remote.contains("SELECT \"id\" FROM"), remote);
        Assertions.assertTrue(remote.contains("\"items\""), remote);
    }

    /** Same for an ordinary scalar column: the saving is smaller, the reasoning identical. */
    @Test
    public void testPredicateOnlyScalarColumnIsNotFetched() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name = 'x'";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, REMOTE_TABLE));

        String remote = remoteQueries(getFragmentPlan(sql));
        Assertions.assertTrue(remote.contains("SELECT \"id\" FROM"), remote);
        Assertions.assertTrue(remote.contains("(\"name\" = 'x')"), remote);
    }

    /** Not a dialect quirk: the MySQL-dialect scan prunes the same column for the same reason. */
    @Test
    public void testPredicateOnlyColumnIsNotFetchedOnMysqlDialect() throws Exception {
        String sql = "SELECT id FROM " + MYSQL_TABLE + " WHERE name = 'x'";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, "prune_probe_mysql"));
        Assertions.assertTrue(remoteQueries(getFragmentPlan(sql)).contains("SELECT `id` FROM"), sql);
    }

    /**
     * The other half of the rule: a column the user actually selected comes back, predicate or not.
     * Without this, "prune what the predicate names" would silently drop data the query asked for.
     */
    @Test
    public void testSelectedPredicateColumnIsStillFetched() throws Exception {
        String sql = "SELECT id, items FROM " + TABLE + " WHERE items[1] = 'alpha'";
        Assertions.assertEquals(List.of("id", "items"), fetchedColumns(sql, REMOTE_TABLE));
        Assertions.assertTrue(remoteQueries(getFragmentPlan(sql)).contains("SELECT \"id\", \"items\" FROM"), sql);
    }

    /** A predicate column consumed by an expression above the scan is consumed all the same. */
    @Test
    public void testPredicateColumnUsedByProjectionIsStillFetched() throws Exception {
        String sql = "SELECT id, upper(name) FROM " + TABLE + " WHERE name = 'x'";
        Assertions.assertEquals(List.of("id", "name"), fetchedColumns(sql, REMOTE_TABLE));
    }

    /**
     * When the predicate is <em>not</em> answered remotely the column must keep coming back: the
     * filter is evaluated here, over the rows the scan produced. {@code array_length} is not
     * expressible for the JDBC dialects, so it stays above the scan and the scan keeps {@code nums}.
     */
    @Test
    public void testLocallyEvaluatedPredicateColumnIsStillFetched() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE array_length(nums) > 3";
        List<String> fetched = fetchedColumns(sql, REMOTE_TABLE);
        Assertions.assertTrue(fetched.contains("nums"), "local predicate lost its input: " + fetched);
        Assertions.assertTrue(fetched.contains("id"), fetched.toString());
        // And nothing was pushed: the remote SQL carries no WHERE clause for it.
        Assertions.assertFalse(remoteQueries(getFragmentPlan(sql)).contains("array_length(\"nums\")"), sql);
    }

    /**
     * A predicate that only partly pushes down leaves a filter between scan and projection, which
     * is the conservative case: the scan can no longer see what the plan above consumes, so it
     * fetches everything it holds. Wasteful but never wrong -- and the remotely answered half is
     * still answered remotely.
     */
    @Test
    public void testPartiallyPushedPredicateFetchesConservatively() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name = 'x' AND array_length(nums) > 3";
        List<String> fetched = fetchedColumns(sql, REMOTE_TABLE);
        Assertions.assertTrue(fetched.contains("nums"), fetched.toString());
        Assertions.assertTrue(fetched.contains("name"), fetched.toString());
        Assertions.assertTrue(remoteQueries(getFragmentPlan(sql)).contains("(\"name\" = 'x')"), sql);
    }

    /**
     * The two roles side by side in one query: the sort key is consumed by the local TopN and comes
     * back, the predicate column is consumed by the database and does not -- even though neither is
     * in the SELECT list the user wrote. ("note" carries no remote type name, so nothing about its
     * ordering may be pushed down and the sort really does happen here.)
     */
    @Test
    public void testLocalSortKeyIsFetchedAndPredicateColumnIsNot() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name = 'x' ORDER BY note LIMIT 5";
        Assertions.assertEquals(List.of("id", "note"), fetchedColumns(sql, REMOTE_TABLE));
        Assertions.assertTrue(remoteQueries(getFragmentPlan(sql)).contains("(\"name\" = 'x')"), sql);
    }

    /**
     * The converse, and the reason the rule is stated as "what the plan above consumes" rather than
     * "what the predicate names": when the ordering is pushed down too, nothing above the scan
     * reads the sort key either, and it stops coming back -- while the inner query still names it,
     * because that is where the ordering is applied.
     */
    @Test
    public void testRemotelySortedKeyIsNotFetched() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name > 'x' ORDER BY name LIMIT 5";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, REMOTE_TABLE));
        String remote = remoteQueries(getFragmentPlan(sql));
        Assertions.assertTrue(remote.contains("ORDER BY \"name\" COLLATE \"C\""), remote);
    }

    /**
     * {@code COLLATE "C"} is decided from the scan's column map, which this change does not touch.
     * A pruned column has to keep its collation, or the remote comparison silently runs under the
     * database's own collation and answers different rows -- the one failure mode that would be
     * invisible.
     */
    @Test
    public void testPrunedColumnKeepsCollate() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name > 'Z'";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, REMOTE_TABLE));
        String remote = remoteQueries(getFragmentPlan(sql));
        Assertions.assertTrue(remote.contains("\"name\" COLLATE \"C\" > 'Z'"), remote);
    }

    /** Two pruned collatable columns keep their collations independently. */
    @Test
    public void testTwoPrunedColumnsKeepCollate() throws Exception {
        String sql = "SELECT id FROM " + TABLE + " WHERE name > 'Z' AND tag < 'b'";
        Assertions.assertEquals(List.of("id"), fetchedColumns(sql, REMOTE_TABLE));
        String remote = remoteQueries(getFragmentPlan(sql));
        Assertions.assertTrue(remote.contains("\"name\" COLLATE \"C\" > 'Z'"), remote);
        Assertions.assertTrue(remote.contains("\"tag\" COLLATE \"C\" < 'b'"), remote);
    }

    /**
     * {@code count(*)} reads no column at all. Leaving the tuple empty would make the remote SELECT
     * list fall back to {@code *} and drag back every column of every row -- the opposite of the
     * point -- so exactly one column stays materialized.
     */
    @Test
    public void testCountStarKeepsOneColumn() throws Exception {
        String sql = "SELECT count(*) FROM " + TABLE + " WHERE name = 'x'";
        List<String> fetched = fetchedColumns(sql, REMOTE_TABLE);
        Assertions.assertEquals(1, fetched.size(), "count(*) should read exactly one column: " + fetched);
        String remote = remoteQueries(getFragmentPlan(sql));
        Assertions.assertFalse(remote.contains("SELECT * FROM"), remote);
        Assertions.assertTrue(remote.contains("(\"name\" = 'x')"), remote);
    }

    /**
     * The same with the pushdowns that normally rewrite {@code count(*)} into a synthetic constant
     * column switched off, which is the configuration that reaches the empty case for real: without
     * the guard this query asks PostgreSQL for {@code SELECT *} and every column of every row comes
     * back to be counted.
     */
    @Test
    public void testCountStarKeepsOneColumnWithoutProjectPushdown() throws Exception {
        boolean previousProjectPushdown = connectContext.getSessionVariable().isEnableJdbcProjectPushDown();
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(false);
        connectContext.getSessionVariable().setEnableJdbcProjectPushDown(false);
        try {
            String sql = "SELECT count(*) FROM " + TABLE + " WHERE name = 'x'";
            List<String> fetched = fetchedColumns(sql, REMOTE_TABLE);
            Assertions.assertEquals(1, fetched.size(), "count(*) should read exactly one column: " + fetched);
            String remote = remoteQueries(getFragmentPlan(sql));
            Assertions.assertFalse(remote.contains("SELECT * FROM"), remote);
            Assertions.assertTrue(remote.contains("(\"name\" = 'x')"), remote);
        } finally {
            connectContext.getSessionVariable().setEnableJdbcProjectPushDown(previousProjectPushdown);
        }
    }

    /**
     * A predicate on a Hive scan is evaluated by the backend over the rows it read, so the rule
     * that governs JDBC must not reach it: {@code c_name} is fetched even though only the predicate
     * names it.
     */
    @Test
    public void testHiveScanStillFetchesPredicateColumn() throws Exception {
        String sql = "SELECT c_custkey FROM hive0.tpch.customer WHERE c_name = 'x'";
        List<String> fetched = fetchedColumns(sql, "customer");
        Assertions.assertTrue(fetched.contains("c_name"), "hive predicate lost its input: " + fetched);
        Assertions.assertTrue(fetched.contains("c_custkey"), fetched.toString());
    }

    /** Same for an Iceberg scan, which reaches the backend down a different scan node still. */
    @Test
    public void testIcebergScanStillFetchesPredicateColumn() throws Exception {
        String sql = "SELECT id FROM iceberg0.partitioned_db.t1 WHERE data = 'x'";
        List<String> fetched = fetchedColumns(sql, "t1");
        Assertions.assertTrue(fetched.contains("data"), "iceberg predicate lost its input: " + fetched);
    }
}
