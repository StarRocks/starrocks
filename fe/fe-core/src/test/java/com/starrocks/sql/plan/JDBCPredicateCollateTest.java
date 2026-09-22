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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.CharType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * StarRocks compares strings byte by byte; PostgreSQL compares them under the column's collation,
 * so a database created with e.g. en_US.UTF-8 orders 'B' before 'a'. Any comparison the pushdown
 * hands to PostgreSQL therefore has to name COLLATE "C" -- PostgreSQL's byte order -- or the remote
 * answer differs from the one StarRocks would have produced locally.
 *
 * <p>Only the types PostgreSQL actually collates may carry it: asking for a collation on anything
 * else is an error rather than a differently ordered result, so a column whose remote type is not
 * text/varchar keeps its comparison local -- unless its remote order already agrees with
 * StarRocks' bytes without a collation, which so far is {@code uuid} alone. Those push down with
 * no COLLATE at all, and the tests below pin both halves of that: the predicate reaches the remote
 * SQL, and it reaches it bare.
 *
 * <p>Ordering safety is not the only thing a push-down needs, which is why one uuid shape still
 * stays local: PostgreSQL has no {@code min}/{@code max} aggregate for the type, however well its
 * values order. See {@link #testUuidMinMaxStaysLocal}.
 */
public class JDBCPredicateCollateTest extends ConnectorPlanTestBase {
    private static final String TABLE = "jdbc_postgres.partitioned_db0.tbl0";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    private static JDBCTable table() {
        return (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext, "jdbc_postgres", "partitioned_db0", "tbl0");
    }

    /**
     * Run {@code body} with one extra column appended to tbl0, declared as {@code type} in
     * StarRocks and reported as {@code sourceTypeName} by the remote, restoring both afterwards.
     * A null {@code sourceTypeName} leaves the column without source-type metadata.
     */
    private static void withColumn(String name, com.starrocks.type.Type type, String sourceTypeName,
                                   Consumer<String> body) {
        JDBCTable table = table();
        List<Column> originalSchema = table.getFullSchema();
        Map<String, String> originalTypeNames = table.getOriginalJdbcColumnTypeNames();
        List<Column> schema = new ArrayList<>(originalSchema);
        schema.add(new Column(name, type, true));
        try {
            table.setNewFullSchema(schema);
            table.setOriginalJdbcColumnTypeNames(
                    sourceTypeName == null ? Map.of() : Map.of(name, sourceTypeName));
            body.accept(name);
        } finally {
            table.setNewFullSchema(originalSchema);
            table.setOriginalJdbcColumnTypeNames(originalTypeNames);
        }
    }

    private String planOf(String sql) throws Exception {
        return getFragmentPlan(sql);
    }

    @Test
    public void testStringRangePredicateCarriesCollate() {
        withColumn("label", VarcharType.VARCHAR, "text", column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " > 'Z'");
                // Without the collation PostgreSQL answers under the column's own, which orders
                // 'Z' after 'b' in a linguistic locale and returns different rows.
                assertContains(plan, "\"label\" COLLATE \"C\" > 'Z'");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBetweenCarriesCollate() {
        withColumn("label", VarcharType.VARCHAR, "text", column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " between 'Z' and 'b'");
                // The optimizer rewrites BETWEEN into two comparisons before the scan predicate is
                // built, so both of them -- not a BETWEEN -- have to carry the collation. (The
                // renderer still handles a surviving BETWEEN by collating its value rather than a
                // bound, since a bound's collation would only govern that one comparison.)
                assertContains(plan, "\"label\" COLLATE \"C\" >= 'Z'");
                assertContains(plan, "\"label\" COLLATE \"C\" <= 'b'");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testEqualityKeepsNoCollate() {
        withColumn("label", VarcharType.VARCHAR, "text", column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " = 'apple'");
                // A deterministic collation compares equal exactly when the bytes are equal, so
                // equality needs no collation and keeps whatever index the column already has.
                assertContains(plan, "\"label\" = 'apple'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUnknownSourceTypeStaysLocal() {
        withColumn("label", VarcharType.VARCHAR, null, column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " > 'Z'");
                // Without the source type name the column may not be a collatable one at all, and
                // a collation on e.g. numeric is an error, so the comparison stays local.
                Assertions.assertFalse(plan.contains("\"label\" >"), plan);
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUnconstrainedNumericStaysLocal() {
        withColumn("amount", VarcharType.VARCHAR, "numeric", column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " > '5'");
                // An unconstrained numeric maps to VARCHAR here, but PostgreSQL would compare it
                // numerically and rejects a collation on it outright.
                Assertions.assertFalse(plan.contains("\"amount\" >"), plan);
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testCharStaysLocal() {
        withColumn("code", CharType.CHAR, "bpchar", column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " > 'Z'");
                // PostgreSQL's bpchar ignores trailing spaces when comparing and StarRocks' CHAR
                // does not, which a collation cannot reconcile.
                Assertions.assertFalse(plan.contains("\"code\" >"), plan);
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testMinMaxCarriesCollate() {
        withColumn("label", VarcharType.VARCHAR, "text", column -> {
            try {
                String plan = planOf("select min(" + column + "), max(" + column + ") from " + TABLE);
                // min/max pick the extreme under the comparison order, so they need the same
                // collation the comparisons use.
                assertContains(plan, "min(\"label\" COLLATE \"C\")");
                assertContains(plan, "max(\"label\" COLLATE \"C\")");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testHavingAggregateRangeCarriesCollate() {
        withColumn("label", VarcharType.VARCHAR, "text", column -> {
            try {
                String plan = planOf("select c, max(" + column + ") m from " + TABLE
                        + " group by c having max(" + column + ") > 'Z'");
                // PostgreSQL derives an aggregate's result collation from its argument, so
                // collating the argument is enough for the HAVING comparison to inherit it.
                // Rejecting the aggregate instead would abandon the whole pushdown and pull every
                // qualifying row back for a local GROUP BY.
                assertContains(plan, "max(\"label\" COLLATE \"C\")");
                assertContains(plan, "HAVING (max(\"label\" COLLATE \"C\") > 'Z')");
                Assertions.assertFalse(plan.contains("AGGREGATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * A uuid column, as PostgreSQL stores it and as StarRocks now maps it: VARCHAR(36) holding the
     * canonical text. Its remote order and StarRocks' byte order agree without a collation, and
     * PostgreSQL rejects one outright ({@code collations are not supported by type uuid}), so every
     * one of these has to push down with no COLLATE in the rendered SQL.
     */
    private static void withUuidColumn(Consumer<String> body) {
        withColumn("uid", TypeFactory.createVarcharType(36), "uuid", body);
    }

    private static final String UUID_LITERAL = "4c1f8b8e-1a2b-4c3d-8e9f-0a1b2c3d4e5f";

    @Test
    public void testUuidEqualityPushesWithoutCollate() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " = '" + UUID_LITERAL + "'");
                assertContains(plan, "\"uid\" = '" + UUID_LITERAL + "'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUuidInequalityPushesWithoutCollate() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " != '" + UUID_LITERAL + "'");
                assertContains(plan, "\"uid\" != '" + UUID_LITERAL + "'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUuidNullSafeEqualityPushesWithoutCollate() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " <=> '" + UUID_LITERAL + "'");
                assertContains(plan, "\"uid\" IS NOT DISTINCT FROM '" + UUID_LITERAL + "'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUuidRangePushesWithoutCollate() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column + " < '" + UUID_LITERAL + "'");
                // The pair of assertions is the point. "no COLLATE" alone would also hold if the
                // predicate had silently stayed local, which is what an ordinary VARCHAR whose
                // remote type is unknown does.
                assertContains(plan, "\"uid\" < '" + UUID_LITERAL + "'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUuidBetweenPushesWithoutCollate() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c from " + TABLE + " where " + column
                        + " between '00000000-0000-0000-0000-000000000000' and '" + UUID_LITERAL + "'");
                assertContains(plan, "\"uid\" >= '00000000-0000-0000-0000-000000000000'");
                assertContains(plan, "\"uid\" <= '" + UUID_LITERAL + "'");
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * The one uuid shape that stays local, and not for an ordering reason. PostgreSQL catalogues
     * {@code min}/{@code max} per type instead of deriving them from the type's btree ordering,
     * and it never got them for {@code uuid}: measured on PostgreSQL 16.15, {@code SELECT
     * min(uuid_col)} fails with {@code function min(uuid) does not exist} while {@code ORDER BY
     * uuid_col} and {@code uuid_col < '...'} both work and both use the column's index. Pushing
     * the aggregate would render a statement the database rejects, so the whole aggregate
     * push-down is abandoned and the GROUP BY stays local.
     */
    @Test
    public void testUuidMinMaxStaysLocal() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select min(" + column + "), max(" + column + ") from " + TABLE);
                Assertions.assertFalse(plan.contains("min(\"uid\")"), plan);
                Assertions.assertFalse(plan.contains("max(\"uid\")"), plan);
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
                assertContains(plan, "AGGREGATE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUuidHavingAggregateStaysLocal() {
        withUuidColumn(column -> {
            try {
                String plan = planOf("select c, max(" + column + ") m from " + TABLE
                        + " group by c having max(" + column + ") > '" + UUID_LITERAL + "'");
                // The HAVING gate would admit this -- a uuid comparison is order-safe -- but the
                // aggregate it is written over is refused first, which abandons the push-down.
                Assertions.assertFalse(plan.contains("HAVING"), plan);
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
                assertContains(plan, "AGGREGATE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testHavingAggregateOverNonCollatableStaysLocal() {
        withColumn("code", CharType.CHAR, "bpchar", column -> {
            try {
                String plan = planOf("select c, max(" + column + ") m from " + TABLE
                        + " group by c having max(" + column + ") > 'Z'");
                // The aggregate is only admitted because its argument is collatable; bpchar is not,
                // so this must still fall back rather than push down an uncollated max().
                Assertions.assertFalse(plan.contains("COLLATE"), plan);
                Assertions.assertFalse(plan.contains("HAVING"), plan);
                assertContains(plan, "AGGREGATE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
