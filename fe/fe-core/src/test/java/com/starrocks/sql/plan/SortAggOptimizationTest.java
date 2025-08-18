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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SortAggOptimizationTest extends PlanTestBase {

    @BeforeEach
    public void beforeEAch() {
        connectContext.getSessionVariable().setEnableSortAggregate(true);
    }

    @AfterEach
    public void afterEach() {
        connectContext.getSessionVariable().setEnableSortAggregate(false);
    }

    @Test
    public void testSortAggWithOrderBySyntax() throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE `sort_key_table` (\n" +
                "  `user_id` bigint NULL COMMENT \"\",\n" +
                "  `event_time` datetime NULL COMMENT \"\",\n" +
                "  `event_type` varchar(32) NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY HASH(`user_id`) BUCKETS 3\n" +
                "ORDER BY (`user_id`, `event_time`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: GROUP BY prefix of sort key
        String sql = "select user_id, count(*) from sort_key_table group by user_id";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: GROUP BY exact match with sort key
        sql = "select user_id, event_time, count(*) from sort_key_table group by user_id, event_time";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 3: GROUP BY not prefix of sort key (should not use sort agg)
        sql = "select event_time, count(*) from sort_key_table group by event_time";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

        // Test 4: GROUP BY with wrong order (should not use sort agg)
        sql = "select event_time, user_id, count(*) from sort_key_table group by event_time, user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");
    }

    @Test
    public void testSortAggWithEqualityPredicates() throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE `events_table` (\n" +
                "  `user_id` bigint NULL COMMENT \"\",\n" +
                "  `event_time` datetime NULL COMMENT \"\",\n" +
                "  `event_type` varchar(32) NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`user_id`, `event_time`)\n" +
                "DISTRIBUTED BY HASH(`user_id`) BUCKETS 3\n" +
                "ORDER BY (`user_id`, `event_time`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: Equality predicate on non-GROUP BY column (should work)
        // WHERE event_type = 'click' GROUP BY user_id
        // The new logic checks each GROUP BY key individually
        String sql = "select user_id, count(*) from events_table where event_type = 'click' group by user_id";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: Equality predicate on GROUP BY column (should work)
        // WHERE user_id = 123 GROUP BY user_id
        // The equality predicate on user_id is detected and skipped in the check
        sql = "select user_id, count(*) from events_table where user_id = 123 group by user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 3: Multiple equality predicates (should work)
        // WHERE event_type = 'click' and value > 0 GROUP BY user_id
        // The range predicate on value doesn't interfere with sort aggregation
        sql = "select user_id, count(*) from events_table where event_type = 'click' and value > 0 group by user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 4: Equality predicate on both GROUP BY and non-GROUP BY columns (should work)
        // WHERE user_id = 123 and event_type = 'click' GROUP BY user_id
        sql = "select user_id, count(*) from events_table where user_id = 123 and event_type = 'click' group by " +
                "user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 5: Equality predicate on sort key prefix (should work)
        // WHERE user_id = 123 GROUP BY user_id, event_time
        // user_id has equality predicate, so it's skipped; event_time is checked against sort key
        sql = "select user_id, event_time, count(*) from events_table where user_id = 123 group by user_id, event_time";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 6: Complex case with multiple equality predicates
        // WHERE user_id = 123 AND event_time = '2023-01-01' GROUP BY user_id, event_time
        sql =
                "select user_id, event_time, count(*) from events_table where user_id = 123 and event_time = " +
                        "'2023-01-01' group by user_id, event_time";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 7: Non-Equality Predicate
        sql = "select user_id, count(*) from events_table where user_id < 123 and event_type < 'click' group by " +
                "user_id";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

    }

    @Test
    public void testSortAggWithSortKeyTraversal() throws Exception {
        // Test the new logic that traverses sort keys and checks each against GROUP BY and equality predicates
        starRocksAssert.withTable("CREATE TABLE `sort_key_traversal_table` (\n" +
                "  `a` bigint NULL COMMENT \"\",\n" +
                "  `b` bigint NULL COMMENT \"\",\n" +
                "  `c` bigint NULL COMMENT \"\",\n" +
                "  `d` bigint NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`a`, `b`, `c`, `d`)\n" +
                "DISTRIBUTED BY HASH(`a`) BUCKETS 3\n" +
                "ORDER BY (`a`, `b`, `c`, `d`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: All sort keys covered by GROUP BY
        // GROUP BY a, b, c, d - all sort keys are covered
        String sql = "select a, b, c, d, count(*) from sort_key_traversal_table group by a, b, c, d";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: Sort key prefix covered by GROUP BY
        // GROUP BY a, b - first two sort keys are covered
        sql = "select a, b, count(*) from sort_key_traversal_table group by a, b";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 3: Equality predicate on first sort key
        // WHERE a = 1 GROUP BY b, c, d
        // a is covered by equality predicate; b, c, d are covered by GROUP BY
        sql = "select b, c, d, count(*) from sort_key_traversal_table where a = 1 group by b, c, d";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 4: Equality predicate on second sort key
        // WHERE b = 2 GROUP BY a, c, d
        // a is covered by GROUP BY; b is covered by equality predicate; c, d are covered by GROUP BY
        sql = "select a, c, d, count(*) from sort_key_traversal_table where b = 2 group by a, c, d";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 5: Multiple equality predicates
        // WHERE a = 1 AND c = 3 GROUP BY b, d
        // a and c are covered by equality predicates; b and d are covered by GROUP BY
        sql = "select b, d, count(*) from sort_key_traversal_table where a = 1 and c = 3 group by b, d";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 6: encode_sort_key covering sort keys
        // GROUP BY encode_sort_key(a, b, c)
        // a, b, c are covered by encode_sort_key; d is not covered (but that's OK for prefix)
        sql =
                "select encode_sort_key(a, b, c), count(*) from sort_key_traversal_table group by encode_sort_key(a, " +
                        "b, c)";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 7: Mixed case with equality predicate and encode_sort_key
        // WHERE a = 1 GROUP BY encode_sort_key(b, c), d
        // a is covered by equality predicate; b, c are covered by encode_sort_key; d is covered by GROUP BY
        sql =
                "select encode_sort_key(b, c), d, count(*) from sort_key_traversal_table where a = 1 group by " +
                        "encode_sort_key(b, c), d";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 8: Case where sort aggregation should NOT be used
        // WHERE a = 1 GROUP BY c, d
        // a is covered by equality predicate; b is NOT covered (missing from GROUP BY and no equality predicate)
        sql = "select c, d, count(*) from sort_key_traversal_table where a = 1 group by c, d";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

    }

    @Test
    public void testSortAggFallbackToKeyColumns() throws Exception {
        // Create a table without ORDER BY syntax (uses key columns as sort key)
        starRocksAssert.withTable("CREATE TABLE `key_sort_table` (\n" +
                "  `user_id` bigint NULL COMMENT \"\",\n" +
                "  `event_time` datetime NULL COMMENT \"\",\n" +
                "  `event_type` varchar(32) NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`user_id`, `event_time`)\n" +
                "DISTRIBUTED BY HASH(`user_id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: GROUP BY prefix of key columns
        String sql = "select user_id, count(*) from key_sort_table group by user_id";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: GROUP BY exact match with key columns
        sql = "select user_id, event_time, count(*) from key_sort_table group by user_id, event_time";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 3: GROUP BY not prefix of key columns (should not use sort agg)
        sql = "select event_time, count(*) from key_sort_table group by event_time";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

        // Test 4: GROUP BY with wrong order (should not use sort agg)
        sql = "select event_time, user_id, count(*) from key_sort_table group by event_time, user_id";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

    }

    @Test
    public void testSortAggWithComplexPredicates() throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE `complex_pred_table` (\n" +
                "  `user_id` bigint NULL COMMENT \"\",\n" +
                "  `event_time` datetime NULL COMMENT \"\",\n" +
                "  `event_type` varchar(32) NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`user_id`, `event_time`)\n" +
                "DISTRIBUTED BY HASH(`user_id`) BUCKETS 3\n" +
                "ORDER BY (`user_id`, `event_time`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: AND predicates with equality
        String sql = "select user_id, count(*) from complex_pred_table " +
                "where event_type = 'click' and value > 0 and user_id = 123 " +
                "group by user_id";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: Range predicates (should still work if they don't interfere with sort key)
        sql = "select user_id, count(*) from complex_pred_table " +
                "where event_time >= '2023-01-01' and event_time < '2023-02-01' " +
                "group by user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 3: Complex AND predicates
        sql = "select user_id, count(*) from complex_pred_table " +
                "where event_type = 'click' and value > 0 and event_time >= '2023-01-01' " +
                "group by user_id";
        plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

    }

    @Test
    public void testSortAggWithMixedExpressions() throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE `mixed_expr_table` (\n" +
                "  `a` bigint NULL COMMENT \"\",\n" +
                "  `b` bigint NULL COMMENT \"\",\n" +
                "  `c` bigint NULL COMMENT \"\",\n" +
                "  `value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`a`, `b`, `c`)\n" +
                "DISTRIBUTED BY HASH(`a`) BUCKETS 3\n" +
                "ORDER BY (`a`, `b`, `c`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // Test 1: Mix of column references and encode_sort_key
        String sql =
                "select a, encode_sort_key(b, c), count(*) from mixed_expr_table group by a, encode_sort_key(b, c)";
        String plan = getCostExplain(sql);
        assertContains(plan, "sorted streaming: true");

        // Test 2: encode_sort_key followed by column reference (should not work)
        sql = "select encode_sort_key(a, b), c, count(*) from mixed_expr_table group by encode_sort_key(a, b), c";
        plan = getCostExplain(sql);
        assertNotContains(plan, "sorted streaming: true");

    }

}
