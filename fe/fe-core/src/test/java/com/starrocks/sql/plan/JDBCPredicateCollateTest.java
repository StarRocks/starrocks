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
 * text/varchar keeps its comparison local.
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
