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
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JDBCPostgresArrayTest extends ConnectorPlanTestBase {
    private static final String TABLE = "jdbc_postgres.partitioned_db0.pg_array_probe";
    // Same shape behind the MySQL-dialect mock catalog, to pin that the subscript rewrite is
    // PostgreSQL-only: array_lower and the [] subscript are not MySQL syntax.
    private static final String MYSQL_TABLE = "jdbc0.partitioned_db0.mysql_array_probe";
    private boolean previousJoinPushdown;
    private boolean previousAggregatePushdown;
    private boolean previousLowerBoundCorrection;
    private boolean previousSubscriptPushDown;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                "jdbc_postgres", "partitioned_db0", "pg_array_probe");
        table.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("items", new ArrayType(VarcharType.VARCHAR)),
                new Column("nums", new ArrayType(IntegerType.INT)),
                // One column per remaining PostgreSQL array element type the reader maps, so the
                // gate's "element type is scalar" condition is stated against each of them rather
                // than inferred from the two the suite happened to start with.
                new Column("flags", new ArrayType(BooleanType.BOOLEAN)),
                new Column("shorts", new ArrayType(IntegerType.SMALLINT)),
                new Column("longs", new ArrayType(IntegerType.BIGINT)),
                new Column("floats", new ArrayType(FloatType.FLOAT)),
                new Column("doubles", new ArrayType(FloatType.DOUBLE)),
                new Column("days", new ArrayType(DateType.DATE)),
                new Column("stamps", new ArrayType(DateType.DATETIME)),
                new Column("matrix", new ArrayType(new ArrayType(VarcharType.VARCHAR))),
                new Column("name", VarcharType.VARCHAR),
                new Column("payload", JsonType.JSON)));
        JDBCTable mysqlTable = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                "jdbc0", "partitioned_db0", "mysql_array_probe");
        mysqlTable.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("items", new ArrayType(VarcharType.VARCHAR)),
                new Column("name", VarcharType.VARCHAR),
                new Column("payload", JsonType.JSON)));
    }

    @BeforeEach
    public void enablePushdown() {
        previousJoinPushdown = connectContext.getSessionVariable().isEnableJdbcJoinPushDown();
        previousAggregatePushdown = connectContext.getSessionVariable().isEnableJdbcAggPushDown();
        previousLowerBoundCorrection = connectContext.getSessionVariable().isEnableJdbcArrayLowerBoundCorrection();
        previousSubscriptPushDown = connectContext.getSessionVariable().isEnableJdbcArraySubscriptPushDown();
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
        // enable_jdbc_array_subscript_push_down is set to its shipped value rather than left as the
        // previous test found it, so that a test turning it off cannot silently disarm the next one.
        connectContext.getSessionVariable().setEnableJdbcArraySubscriptPushDown(true);
        // Deliberately not set here: every rendering test states the mode it means, and
        // testLowerBoundCorrectionIsOffByDefault reads the shipped default off a fresh
        // SessionVariable rather than off this shared one.
    }

    @AfterEach
    public void restorePushdown() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(previousJoinPushdown);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(previousAggregatePushdown);
        connectContext.getSessionVariable().setEnableJdbcArrayLowerBoundCorrection(previousLowerBoundCorrection);
        connectContext.getSessionVariable().setEnableJdbcArraySubscriptPushDown(previousSubscriptPushDown);
    }

    private void setLowerBoundCorrection(boolean enabled) {
        connectContext.getSessionVariable().setEnableJdbcArrayLowerBoundCorrection(enabled);
    }

    private void setSubscriptPushDown(boolean enabled) {
        connectContext.getSessionVariable().setEnableJdbcArraySubscriptPushDown(enabled);
    }

    private String remoteQueries(String plan) {
        return plan.lines().filter(line -> line.contains("QUERY:")).collect(Collectors.joining("\n"));
    }

    /**
     * The SQL a constant subscript is expected to render as with
     * {@code enable_jdbc_array_lower_bound_correction} on: PostgreSQL's element at its own lower
     * bound plus the render-time offset, taken only where StarRocks' index k addresses an element.
     */
    private static String correctedSubscript(String column, int index, String shift) {
        String quoted = "\"" + column + "\"";
        return "(CASE WHEN " + index + " BETWEEN 1 AND array_length(" + quoted + ", 1)"
                + " THEN " + quoted + "[array_lower(" + quoted + ", 1) " + shift + "]"
                + " ELSE NULL END)";
    }

    /**
     * The SQL the same subscript renders as with the correction off, which is the default: the
     * plain subscript, no CASE and no array_lower, assuming a lower bound of 1.
     */
    private static String uncorrectedSubscript(String column, int index) {
        return "\"" + column + "\"[" + index + "]";
    }

    private static String subscript(boolean corrected, String column, int index, String shift) {
        return corrected ? correctedSubscript(column, index, shift) : uncorrectedSubscript(column, index);
    }

    /**
     * Whether the subscript reached PostgreSQL, worded so it reads the same in both modes: both
     * renderings index the quoted column, and only the bracket after the column name is common to
     * them. Lets a push-down verdict be compared across the two modes without the assertion
     * silently becoming "did it render the way this mode renders".
     */
    private boolean subscriptWentRemote(String plan, String column) {
        return remoteQueries(plan).contains("\"" + column + "\"[");
    }

    /** Runs {@code query} in both modes and hands each plan to the caller, keyed by the mode. */
    private Map<Boolean, String> plansInBothModes(String query) throws Exception {
        setLowerBoundCorrection(false);
        String uncorrected = getFragmentPlan(query);
        setLowerBoundCorrection(true);
        String corrected = getFragmentPlan(query);
        return Map.of(false, uncorrected, true, corrected);
    }

    /**
     * The shipped default. A constant subscript is answered by PostgreSQL as the plain {@code a[k]},
     * with no correction for the value's own lower bound -- the trade this default accepts is that a
     * row whose lower bound is not 1 then answers differently than the same subscript evaluated in
     * StarRocks (see the class-level note on
     * {@link #testCorrectionOnlyChangesRenderingNotWhatIsPushedDown}).
     */
    @Test
    public void testConstantSubscriptPredicatePushesUncorrectedByDefault() throws Exception {
        String plan = getFragmentPlan("SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'");
        String remote = remoteQueries(plan);
        Assertions.assertTrue(remote.contains("(" + uncorrectedSubscript("items", 1) + " = 'abc')"), plan);
        Assertions.assertFalse(remote.contains("array_lower"), plan);
        Assertions.assertFalse(remote.contains("array_length"), plan);
    }

    @Test
    public void testConstantSubscriptProjectionPushesUncorrectedByDefault() throws Exception {
        String remote = remoteQueries(getFragmentPlan("SELECT items[2] FROM " + TABLE));
        Assertions.assertTrue(remote.contains(uncorrectedSubscript("items", 2)), remote);
        Assertions.assertFalse(remote.contains("array_lower"), remote);
    }

    /**
     * With the correction on, StarRocks' element k is PostgreSQL's element
     * {@code array_lower(a, 1) + k - 1}, because reading rebased the value's lower bound to 1; the
     * offset is folded at render time, so k = 1 emits {@code + 0}.
     */
    @Test
    public void testConstantSubscriptPredicatePushesWithLowerBoundCorrection() throws Exception {
        setLowerBoundCorrection(true);
        String plan = getFragmentPlan("SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'");
        String remote = remoteQueries(plan);
        Assertions.assertTrue(remote.contains("(" + correctedSubscript("items", 1, "+ 0") + " = 'abc')"), plan);
    }

    @Test
    public void testConstantSubscriptProjectionPushesWithLowerBoundCorrection() throws Exception {
        setLowerBoundCorrection(true);
        String plan = getFragmentPlan("SELECT items[2] FROM " + TABLE);
        Assertions.assertTrue(remoteQueries(plan).contains(correctedSubscript("items", 2, "+ 1")), plan);
    }

    /**
     * k below 1: uncorrected it stays the literal PostgreSQL answers NULL for; corrected, the offset
     * arithmetic stays honest and renders as a subtraction, not "+ -1".
     */
    @Test
    public void testSubscriptBelowOneRendersInBothModes() throws Exception {
        Map<Boolean, String> plans = plansInBothModes("SELECT items[0] FROM " + TABLE);
        Assertions.assertTrue(remoteQueries(plans.get(false)).contains(uncorrectedSubscript("items", 0)),
                plans.get(false));
        Assertions.assertTrue(remoteQueries(plans.get(true)).contains(correctedSubscript("items", 0, "- 1")),
                plans.get(true));
    }

    /**
     * A subscript that is a valid int4 on its own can still overflow once a row's own lower bound is
     * added to it -- {@code items[2147483647]} over a row whose lower bound is 5 asks PostgreSQL for
     * element 2147483651. PostgreSQL raises "integer out of range" there while StarRocks answers
     * NULL, so the corrected subscript is only evaluated where the element exists.
     *
     * <p>The uncorrected rendering needs no such guard and deliberately carries none: it does no
     * arithmetic on the index, and PostgreSQL answers NULL for a subscript past the array's end --
     * measured on a live PostgreSQL, {@code a[2147483647]} is NULL and raises nothing, on lower
     * bounds of 1, 0 and 5 alike.
     */
    @Test
    public void testLargeSubscriptIsGuardedOnlyWhereTheCorrectionCanOverflow() throws Exception {
        Map<Boolean, String> plans = plansInBothModes("SELECT items[2147483647] FROM " + TABLE);

        String uncorrected = remoteQueries(plans.get(false));
        Assertions.assertTrue(uncorrected.contains(uncorrectedSubscript("items", 2147483647)), uncorrected);
        Assertions.assertFalse(uncorrected.contains("array_length"), uncorrected);

        String corrected = remoteQueries(plans.get(true));
        Assertions.assertTrue(corrected.contains(correctedSubscript("items", 2147483647, "+ 2147483646")), corrected);
        Assertions.assertTrue(corrected.contains("BETWEEN 1 AND array_length(\"items\", 1)"), corrected);
    }

    /** The element type does not have to be a string; the gate looks at the shape, not at VARCHAR. */
    @Test
    public void testIntegerArraySubscriptPushesInBothModes() throws Exception {
        Map<Boolean, String> plans = plansInBothModes("SELECT nums[3] FROM " + TABLE);
        Assertions.assertTrue(remoteQueries(plans.get(false)).contains(uncorrectedSubscript("nums", 3)),
                plans.get(false));
        Assertions.assertTrue(remoteQueries(plans.get(true)).contains(correctedSubscript("nums", 3, "+ 2")),
                plans.get(true));
    }

    /**
     * The push-down gate asks only whether the element type is scalar, and the rendered
     * {@code a[k]}, {@code array_lower} and {@code array_length} are all type-independent. Opening
     * the reader to more element types therefore needs no change in the gate or the renderer --
     * this states that as an assertion instead of an expectation, one element type at a time.
     */
    @Test
    public void testEveryMappedElementTypeSubscriptPushes() throws Exception {
        for (String column : List.of("items", "nums", "flags", "shorts", "longs", "floats", "doubles",
                "days", "stamps")) {
            Map<Boolean, String> plans = plansInBothModes("SELECT " + column + "[2] FROM " + TABLE);
            Assertions.assertTrue(remoteQueries(plans.get(false)).contains(uncorrectedSubscript(column, 2)),
                    column + " " + plans.get(false));
            Assertions.assertTrue(remoteQueries(plans.get(true)).contains(correctedSubscript(column, 2, "+ 1")),
                    column + " " + plans.get(true));
            // The filter path asks the same gate, and is the one that decides whether the array
            // column has to be fetched at all. plansInBothModes leaves the correction on, so the
            // mode this asserts has to be stated rather than inherited.
            setLowerBoundCorrection(false);
            String filtered = getFragmentPlan("SELECT id FROM " + TABLE + " WHERE " + column + "[2] IS NOT NULL");
            Assertions.assertTrue(remoteQueries(filtered).contains(uncorrectedSubscript(column, 2)), filtered);
            Assertions.assertFalse(remoteQueries(filtered).contains("\"" + column + "\" FROM"),
                    "The array column itself must not be fetched: " + filtered);
        }
    }

    /**
     * The whole point of exempting identity passthroughs from the dialect gate: an array column that
     * is selected alongside a subscript no longer drags the entire projection back to StarRocks. The
     * bare column is emitted as a plain column name -- what ordinary column pruning would emit --
     * while the subscript is evaluated remotely.
     */
    @Test
    public void testSubscriptAndBareArrayColumnPushTogether() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT items[1], items FROM " + TABLE);
            String remote = remoteQueries(plan);
            Assertions.assertTrue(remote.contains(subscript(corrected, "items", 1, "+ 0")), plan);
            Assertions.assertTrue(remote.contains("\"items\""), plan);
        }
    }

    @Test
    public void testSubscriptAndScalarExpressionPushTogether() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT items[1], id + 1, items FROM " + TABLE);
            String remote = remoteQueries(plan);
            Assertions.assertTrue(remote.contains(subscript(corrected, "items", 1, "+ 0")), plan);
            Assertions.assertTrue(remote.contains("(\"id\" + 1)"), plan);
        }
    }

    /**
     * A variable subscript is refused outright rather than left to "the renderer cannot spell it":
     * pushing it would need the index materialised in an inner derived table first, and the constant
     * subscripts this exists for do not need that.
     */
    @Test
    public void testVariableSubscriptStaysLocal() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT items[id] FROM " + TABLE);
            Assertions.assertFalse(subscriptWentRemote(plan, "items"), plan);
        }
    }

    /** A subscript into a nested array yields an array, which is the whole-array case again. */
    @Test
    public void testMultiDimensionalSubscriptStaysLocal() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT matrix[1] FROM " + TABLE);
            Assertions.assertFalse(subscriptWentRemote(plan, "matrix"), plan);
        }
    }

    /** The rewrite is PostgreSQL-only; the MySQL-dialect gate never sees a subscript as pushable. */
    @Test
    public void testSubscriptStaysLocalOnMySqlDialect() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT items[1] FROM " + MYSQL_TABLE + " WHERE items[1] = 'abc'");
            String remote = remoteQueries(plan);
            Assertions.assertFalse(remote.contains("array_lower"), plan);
            Assertions.assertFalse(remote.contains("`items`["), plan);
            Assertions.assertFalse(remote.toUpperCase().contains("WHERE"), plan);
        }
    }

    /** element_at is the same optimizer operator as [], so it is pushed identically, in both modes. */
    @Test
    public void testElementAtPushesLikeSubscript() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String bracket = getFragmentPlan("SELECT items[1] FROM " + TABLE);
            String elementAt = getFragmentPlan("SELECT element_at(items, 1) FROM " + TABLE);
            Assertions.assertTrue(remoteQueries(elementAt).contains(subscript(corrected, "items", 1, "+ 0")),
                    elementAt);
            Assertions.assertEquals(remoteQueries(bracket), remoteQueries(elementAt), elementAt);
        }
    }

    /** Ordering by a subscript is a string order, which PostgreSQL would answer under its collation. */
    @Test
    public void testOrderByArrayStaysLocal() throws Exception {
        String plan = getFragmentPlan("SELECT id FROM " + TABLE + " ORDER BY items LIMIT 5");
        Assertions.assertFalse(remoteQueries(plan).toUpperCase().contains("ORDER BY"), plan);
    }

    @Test
    public void testOrderBySubscriptStaysLocal() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT id FROM " + TABLE + " ORDER BY items[1] LIMIT 5");
            Assertions.assertFalse(remoteQueries(plan).toUpperCase().contains("ORDER BY"), plan);
        }
    }

    /**
     * Grouping by a subscript composes with aggregate push-down: the projection rule turns the
     * subscript into a derived scan column and the aggregate rule then groups on that bare column,
     * the same way {@code GROUP BY id + 1} already works. The group key is a string compared under
     * PostgreSQL's deterministic collation, i.e. by bytes, which is what grouping by a plain text
     * column already relies on. Grouping by the ARRAY column itself stays local
     * (see {@link #testArrayGroupingAndDistinctStayLocal}).
     */
    @Test
    public void testGroupBySubscriptComposesWithAggregatePushDown() throws Exception {
        for (boolean corrected : List.of(false, true)) {
            setLowerBoundCorrection(corrected);
            String plan = getFragmentPlan("SELECT items[1], COUNT(*) FROM " + TABLE + " GROUP BY items[1]");
            String remote = remoteQueries(plan);
            Assertions.assertTrue(remote.contains(subscript(corrected, "items", 1, "+ 0")), plan);
            Assertions.assertTrue(remote.toUpperCase().contains("GROUP BY"), plan);
        }
    }

    @Test
    public void testScalarFilterStillPushesWithArrayOutput() throws Exception {
        String plan = getFragmentPlan("SELECT items FROM " + TABLE + " WHERE id = 1");
        Assertions.assertTrue(remoteQueries(plan).contains("WHERE"), plan);
        Assertions.assertTrue(remoteQueries(plan).contains("items"), plan);
    }

    @Test
    public void testArrayGroupingAndDistinctStayLocal() throws Exception {
        for (String query : List.of("SELECT items, COUNT(*) FROM %s GROUP BY items",
                "SELECT DISTINCT items FROM %s", "SELECT COUNT(DISTINCT items) FROM %s")) {
            String plan = getFragmentPlan(String.format(query, TABLE));
            String remote = remoteQueries(plan).toUpperCase();
            Assertions.assertFalse(remote.contains("GROUP BY"), plan);
            Assertions.assertFalse(remote.contains("COUNT("), plan);
            Assertions.assertFalse(remote.contains("DISTINCT"), plan);
        }
    }

    @Test
    public void testArrayJoinEqualityStaysLocal() throws Exception {
        String plan = getFragmentPlan("SELECT a.id FROM " + TABLE + " a JOIN " + TABLE + " b ON a.items = b.items");
        Assertions.assertFalse(remoteQueries(plan).toUpperCase().contains(" JOIN "), plan);
    }

    @Test
    public void testScalarAggregationStillPushes() throws Exception {
        String plan = getFragmentPlan("SELECT id, COUNT(*) FROM " + TABLE + " GROUP BY id");
        Assertions.assertTrue(remoteQueries(plan).contains("GROUP BY"), plan);
    }

    /**
     * {@code enable_jdbc_array_lower_bound_correction} is off in a freshly built SessionVariable,
     * which is what a new session starts from. Read off a fresh instance rather than off the shared
     * test session, so that a test leaving the variable set cannot make this pass.
     */
    @Test
    public void testLowerBoundCorrectionIsOffByDefault() {
        Assertions.assertFalse(new SessionVariable().isEnableJdbcArrayLowerBoundCorrection());
    }

    /**
     * The variable chooses between two renderings of a subscript and nothing else: the set of shapes
     * that go to PostgreSQL is identical in both modes. The gate
     * ({@code CanPushDownPredicateVisitor.PostgresPushDownGate}) never reads it, and this pins that
     * -- including for the shapes that stay local, so relaxing the gate under one mode would be
     * caught here rather than in whichever rendering test happens to run.
     *
     * <p>The verdict is worded as "the quoted column is indexed in the remote SQL", which is common
     * to both renderings, so it cannot degrade into "did it render the way this mode renders".
     */
    @Test
    public void testCorrectionOnlyChangesRenderingNotWhatIsPushedDown() throws Exception {
        Map<String, Boolean> expected = new LinkedHashMap<>();
        expected.put("SELECT items[1] FROM " + TABLE, true);
        expected.put("SELECT items[0] FROM " + TABLE, true);
        expected.put("SELECT items[2147483647] FROM " + TABLE, true);
        expected.put("SELECT element_at(items, 1) FROM " + TABLE, true);
        expected.put("SELECT nums[3] FROM " + TABLE, true);
        expected.put("SELECT items[1], items FROM " + TABLE, true);
        expected.put("SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'", true);
        expected.put("SELECT items[1], COUNT(*) FROM " + TABLE + " GROUP BY items[1]", true);
        expected.put("SELECT items[id] FROM " + TABLE, false);
        expected.put("SELECT matrix[1] FROM " + TABLE, false);
        expected.put("SELECT items[1] FROM " + MYSQL_TABLE, false);

        for (Map.Entry<String, Boolean> entry : expected.entrySet()) {
            String column = entry.getKey().contains("nums") ? "nums"
                    : entry.getKey().contains("matrix") ? "matrix" : "items";
            Map<Boolean, String> plans = plansInBothModes(entry.getKey());
            boolean uncorrected = subscriptWentRemote(plans.get(false), column);
            boolean corrected = subscriptWentRemote(plans.get(true), column);
            Assertions.assertEquals(uncorrected, corrected,
                    "push-down verdict differs between the two modes for: " + entry.getKey());
            Assertions.assertEquals(entry.getValue(), uncorrected, entry.getKey() + "\n" + plans.get(false));
        }
    }

    /**
     * Guard for the blast radius of exempting identity passthroughs from the gate: a non-array
     * passthrough next to a derived expression pushed before the change and still pushes. The gate
     * never rejected one -- only a PostgreSQL ARRAY column ref -- so nothing else moves, on either
     * dialect. A JSON column is the interesting case because it is the other type whose remote
     * reading goes through the type checker rather than through the SQL renderer.
     */
    @Test
    public void testNonArrayPassthroughStillPushesAlongsideDerivedExpression() throws Exception {
        for (String table : List.of(TABLE, MYSQL_TABLE)) {
            for (String column : List.of("payload", "name")) {
                String plan = getFragmentPlan("SELECT " + column + ", id + 1 FROM " + table);
                String remote = remoteQueries(plan);
                Assertions.assertTrue(remote.contains("jdbc_proj_"), plan);
                Assertions.assertTrue(remote.contains(column), plan);
            }
        }
    }

    /**
     * {@code enable_jdbc_array_subscript_push_down} is on in a freshly built SessionVariable, which
     * is what a new session starts from: this is a rollback switch, not a rollout gate, so the
     * feature is live without anyone setting anything. Read off a fresh instance rather than off the
     * shared test session, so that the per-test reset in {@link #enablePushdown()} cannot make it
     * pass.
     */
    @Test
    public void testSubscriptPushDownIsOnByDefault() {
        Assertions.assertTrue(new SessionVariable().isEnableJdbcArraySubscriptPushDown());
    }

    /**
     * The predicate path, turned off. This is the one that matters: before the variable existed it
     * was the path with no switch at all -- {@code enable_jdbc_project_push_down} closes the
     * projection path only, and the scan filter is rendered independently of it, so
     * {@code WHERE items[1] = 'abc'} went to PostgreSQL regardless. With the subscript judged
     * unpushable the whole predicate stays in StarRocks, and the remote SQL carries no WHERE at all.
     */
    @Test
    public void testSubscriptPredicateStaysLocalWhenPushDownDisabled() throws Exception {
        String query = "SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'";

        setSubscriptPushDown(true);
        Assertions.assertTrue(subscriptWentRemote(getFragmentPlan(query), "items"));

        setSubscriptPushDown(false);
        String plan = getFragmentPlan(query);
        Assertions.assertFalse(subscriptWentRemote(plan, "items"), plan);
        Assertions.assertFalse(remoteQueries(plan).toUpperCase().contains("WHERE"), plan);
        // The column the predicate reads is fetched instead, so StarRocks can evaluate it.
        Assertions.assertTrue(remoteQueries(plan).contains("\"items\""), plan);
    }

    /** The projection path, turned off: the array column comes back whole and is indexed locally. */
    @Test
    public void testSubscriptProjectionStaysLocalWhenPushDownDisabled() throws Exception {
        String query = "SELECT items[2] FROM " + TABLE;

        setSubscriptPushDown(true);
        Assertions.assertTrue(subscriptWentRemote(getFragmentPlan(query), "items"));

        setSubscriptPushDown(false);
        String plan = getFragmentPlan(query);
        Assertions.assertFalse(subscriptWentRemote(plan, "items"), plan);
        Assertions.assertFalse(remoteQueries(plan).contains("array_lower"), plan);
        Assertions.assertTrue(remoteQueries(plan).contains("\"items\""), plan);
    }

    /**
     * Every shape the push-down covers goes local together when the variable is off -- both paths,
     * both spellings of a subscript, and the shapes that compose with aggregate push-down -- so that
     * the rollback is one switch and not a per-shape lottery.
     */
    @Test
    public void testDisablingPushDownClosesEveryPushedShape() throws Exception {
        setSubscriptPushDown(false);
        for (String query : List.of(
                "SELECT items[1] FROM " + TABLE,
                "SELECT element_at(items, 1) FROM " + TABLE,
                "SELECT nums[3] FROM " + TABLE,
                "SELECT items[1], items FROM " + TABLE,
                "SELECT items[1], id + 1, items FROM " + TABLE,
                "SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'",
                "SELECT id FROM " + TABLE + " WHERE items[1] = 'abc' AND id = 1",
                "SELECT items[1], COUNT(*) FROM " + TABLE + " GROUP BY items[1]")) {
            String plan = getFragmentPlan(query);
            String column = query.contains("nums") ? "nums" : "items";
            Assertions.assertFalse(subscriptWentRemote(plan, column), query + "\n" + plan);
            Assertions.assertFalse(remoteQueries(plan).contains("array_lower"), query + "\n" + plan);
        }
    }

    /**
     * With the push-down off, {@code enable_jdbc_array_lower_bound_correction} has nothing left to
     * govern: it chooses between two renderings of a subscript, and no subscript reaches the
     * renderer. Both of its values must therefore produce the same remote SQL, on both paths -- the
     * only way a user gets "exactly what local evaluation answers" without paying for the
     * correction, and the reason the two variables are documented together.
     */
    @Test
    public void testCorrectionIsMootWhenPushDownDisabled() throws Exception {
        setSubscriptPushDown(false);
        for (String query : List.of("SELECT items[1] FROM " + TABLE,
                "SELECT id FROM " + TABLE + " WHERE items[1] = 'abc'")) {
            setLowerBoundCorrection(false);
            String off = remoteQueries(getFragmentPlan(query));
            setLowerBoundCorrection(true);
            String on = remoteQueries(getFragmentPlan(query));
            Assertions.assertEquals(off, on, "correction still changes the remote SQL for: " + query);
            Assertions.assertFalse(off.contains("\"items\"["), query + "\n" + off);
        }
    }

    /**
     * The blast radius of turning it off, pinned: it closes the array subscript and nothing else. A
     * scalar predicate still reaches PostgreSQL, scalar aggregation still folds, and the shapes that
     * were already local -- a whole array column, a variable subscript -- are unmoved.
     */
    @Test
    public void testDisablingPushDownLeavesEverythingElseAlone() throws Exception {
        setSubscriptPushDown(false);

        String scalarFilter = getFragmentPlan("SELECT items FROM " + TABLE + " WHERE id = 1");
        Assertions.assertTrue(remoteQueries(scalarFilter).contains("(\"id\" = 1)"), scalarFilter);

        String scalarAgg = getFragmentPlan("SELECT id, COUNT(*) FROM " + TABLE + " GROUP BY id");
        Assertions.assertTrue(remoteQueries(scalarAgg).contains("GROUP BY"), scalarAgg);

        String derived = getFragmentPlan("SELECT name, id + 1 FROM " + TABLE);
        Assertions.assertTrue(remoteQueries(derived).contains("(\"id\" + 1)"), derived);

        String wholeArray = getFragmentPlan("SELECT items, COUNT(*) FROM " + TABLE + " GROUP BY items");
        Assertions.assertFalse(remoteQueries(wholeArray).toUpperCase().contains("GROUP BY"), wholeArray);

        String variableSubscript = getFragmentPlan("SELECT items[id] FROM " + TABLE);
        Assertions.assertFalse(subscriptWentRemote(variableSubscript, "items"), variableSubscript);
    }

    /**
     * The variable is PostgreSQL's alone in effect, because no other dialect's gate ever called a
     * subscript pushable. Pinned in both of its positions so that a future dialect opening the shape
     * cannot quietly escape the switch.
     */
    @Test
    public void testMySqlDialectIsUnaffectedByTheVariable() throws Exception {
        for (boolean enabled : List.of(true, false)) {
            setSubscriptPushDown(enabled);
            String plan = getFragmentPlan("SELECT items[1] FROM " + MYSQL_TABLE + " WHERE items[1] = 'abc'");
            String remote = remoteQueries(plan);
            Assertions.assertFalse(remote.contains("`items`["), plan);
            Assertions.assertFalse(remote.toUpperCase().contains("WHERE"), plan);
        }
    }
}
