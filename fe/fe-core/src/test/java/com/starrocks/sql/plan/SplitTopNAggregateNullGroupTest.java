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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SplitTopNAggregateRule rewrites
 *
 * <pre>
 *   select a, count(distinct b), count(*) from t group by a order by 1, 2 limit 10
 * </pre>
 *
 * into a join of the table against a cheap top-N aggregate over the same table, joined back on the
 * grouping key. The join has to match every group the top-N produced -- including the group whose
 * key is NULL -- and a plain equality does not: NULL = NULL is NULL, the row finds no partner in the
 * inner join, and the whole NULL group vanishes from the answer.
 *
 * <p>Under the default configuration that silently dropped rows: a table whose smallest grouping key
 * is NULL answered `limit 1` with no rows at all, and `limit 100` with 99.
 *
 * <p>Asserted on the plan rather than on rows because the defect is a property of the join this rule
 * builds. Both directions are here: a nullable key must get the null-safe comparison, and a
 * non-nullable one must keep the plain equality, so the join strategies that only accept `=` stay
 * reachable wherever they were reachable before.
 */
public class SplitTopNAggregateNullGroupTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `stn_nullable` (\n"
                + "  `c0` bigint NULL,\n"
                + "  `c1` bigint NULL,\n"
                + "  `c2` bigint NULL\n"
                + ") ENGINE=OLAP DUPLICATE KEY(`c0`)\n"
                + "DISTRIBUTED BY HASH(`c0`) BUCKETS 48\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `stn_not_null` (\n"
                + "  `c0` bigint NOT NULL,\n"
                + "  `c1` bigint NULL,\n"
                + "  `c2` bigint NULL\n"
                + ") ENGINE=OLAP DUPLICATE KEY(`c0`)\n"
                + "DISTRIBUTED BY HASH(`c0`) BUCKETS 48\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    private String splitPlan(String table) throws Exception {
        connectContext.getSessionVariable().setEnableSplitTopNAgg(true);
        try {
            return getFragmentPlan("select c0, count(distinct c1), count(*) from " + table
                    + " group by c0 order by 1, 2 limit 10");
        } finally {
            connectContext.getSessionVariable().setEnableSplitTopNAgg(true);
        }
    }

    @Test
    public void testNullableGroupingKeyJoinsNullSafely() throws Exception {
        String plan = splitPlan("stn_nullable");
        assertTrue(plan.contains("<=>"),
                "a nullable grouping key must be joined null-safely or its NULL group is dropped:\n" + plan);
    }

    @Test
    public void testNotNullGroupingKeyKeepsPlainEquality() throws Exception {
        String plan = splitPlan("stn_not_null");
        // Not a formality: widening every key to <=> would cost the plain-equality join strategies
        // on the overwhelmingly common non-nullable case, for a NULL that cannot occur.
        assertFalse(plan.contains("<=>"),
                "a NOT NULL grouping key has no NULL to match and should keep plain equality:\n" + plan);
        assertTrue(plan.contains("equal join conjunct"),
                "expected the rule to still split this query:\n" + plan);
    }

    @Test
    public void testTheRuleStillFiresForBothTables() throws Exception {
        // If the rule stopped applying, both assertions above would pass vacuously -- the plan would
        // simply have no join to look at.
        assertTrue(splitPlan("stn_nullable").contains("equal join conjunct"),
                "the split must still happen for the nullable table");
        assertTrue(splitPlan("stn_not_null").contains("equal join conjunct"),
                "the split must still happen for the non-nullable table");
    }
}
