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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SortAggOptimizationTest extends PlanTestBase {

    @BeforeEach
    public void beforeEAch() {
        connectContext.getSessionVariable().setEnableSortAggregate(true);
    }

    @AfterEach
    public void afterEach() {
        connectContext.getSessionVariable().setEnableSortAggregate(false);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        // Test 1: GROUP BY prefix of sort key
            "select user_id, count(*) from sort_key_table group by user_id | true",
        // Test 2: GROUP BY exact match with sort key
            "select user_id, event_time, count(*) from sort_key_table group by user_id, event_time | true",
        // Test 3: GROUP BY not prefix of sort key (should not use sort agg)
            "select event_time, count(*) from sort_key_table group by event_time | false",
        // Test 4: GROUP BY with wrong order (should not use sort agg)
            "select event_time, user_id, count(*) from sort_key_table group by event_time, user_id | true"
    })
    public void testSortAggWithOrderBySyntax(String sql, boolean shouldUseSortAgg) throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `sort_key_table` (\n" +
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

        String plan = getCostExplain(sql);
        if (shouldUseSortAgg) {
            assertContains(plan, "sorted streaming: true");
        } else {
            assertNotContains(plan, "sorted streaming: true");
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        // Test 1: Equality predicate on non-GROUP BY column (should work)
            "select user_id, count(*) from events_table where event_type = 'click' group by user_id | true",
        // Test 2: Equality predicate on GROUP BY column (should work)
            "select user_id, count(*) from events_table where user_id = 123 group by user_id | true",
        // Test 3: Multiple equality predicates (should work)
            "select user_id, count(*) from events_table where event_type = 'click' and value > 0 group by user_id | " +
                    "true",
        // Test 4: Equality predicate on both GROUP BY and non-GROUP BY columns (should work)
            "select user_id, count(*) from events_table where user_id = 123 and event_type = 'click' group by user_id" +
                    " | true",
        // Test 5: Equality predicate on sort key prefix (should work)
            "select user_id, event_time, count(*) from events_table where user_id = 123 group by user_id, event_time " +
                    "| true",
        // Test 6: Complex case with multiple equality predicates
            "select user_id, event_time, count(*) from events_table where user_id = 123 and event_time = '2023-01-01'" +
                    " group by user_id, event_time | true",
        // Test 7: Non-Equality Predicate
            "select event_time, count(*) from events_table where user_id < 123 group by event_time | false"
    })
    public void testSortAggWithEqualityPredicates(String sql, boolean shouldUseSortAgg) throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `events_table` (\n" +
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

        String plan = getCostExplain(sql);
        if (shouldUseSortAgg) {
            assertContains(plan, "sorted streaming: true");
        } else {
            assertNotContains(plan, "sorted streaming: true");
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        // Test 1: GROUP BY prefix of key columns
            "select user_id, count(*) from key_sort_table group by user_id | true",
        // Test 2: GROUP BY exact match with key columns
            "select user_id, event_time, count(*) from key_sort_table group by user_id, event_time | true",
        // Test 3: GROUP BY not prefix of key columns (should not use sort agg)
            "select event_time, count(*) from key_sort_table group by event_time | false",
            // Test 4: GROUP BY with different order
            "select event_time, user_id, count(*) from key_sort_table group by event_time, user_id | true"
    })
    public void testSortAggFallbackToKeyColumns(String sql, boolean shouldUseSortAgg) throws Exception {
        // Create a table without ORDER BY syntax (uses key columns as sort key)
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `key_sort_table` (\n" +
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

        String plan = getCostExplain(sql);
        if (shouldUseSortAgg) {
            assertContains(plan, "sorted streaming: true");
        } else {
            assertNotContains(plan, "sorted streaming: true");
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        // Test 1: AND predicates with equality
            "select user_id, count(*) from complex_pred_table where event_type = 'click' and value > 0 and user_id = " +
                    "123 group by user_id | true",
        // Test 2: Range predicates (should still work if they don't interfere with sort key)
            "select user_id, count(*) from complex_pred_table where event_time >= '2023-01-01' and event_time < " +
                    "'2023-02-01' group by user_id | true",
        // Test 3: Complex AND predicates
            "select user_id, count(*) from complex_pred_table where event_type = 'click' and value > 0 and event_time" +
                    " >= '2023-01-01' group by user_id | true"
    })
    public void testSortAggWithComplexPredicates(String sql, boolean shouldUseSortAgg) throws Exception {
        // Create a table with ORDER BY syntax for sort key
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `complex_pred_table` (\n" +
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

        String plan = getCostExplain(sql);
        if (shouldUseSortAgg) {
            assertContains(plan, "sorted streaming: true");
        } else {
            assertNotContains(plan, "sorted streaming: true");
        }
    }


    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select get_json_int(data, 'k1') k1, count(*) from json_sorted_table group by k1 | true",
            "select get_json_int(data, 'k1') k1, get_json_string(data, 'k2') k2, count(*) from json_sorted_table " +
                    "group by k1, k2| true",

            // Unsupported cases
            "select get_json_string(data, 'k1') k1, count(*) from json_sorted_table group by k1 | false",
            "select get_json_double(data, 'k1') k1, count(*) from json_sorted_table group by k1 | false",
            "select get_json_int(data, 'k3') k3, count(*) from json_sorted_table group by k3 | false",
            "select get_json_int(data, 'k2') k2, count(*) from json_sorted_table group by k2 | false",
    })
    public void testJsonSortKey(String sql, boolean shouldUseSortAgg) throws Exception {
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `json_sorted_table` (\n" +
                "  `user_id` bigint NULL,\n" +
                "  `data` JSON NULL, \n" +
                "  `value` double NULL, \n" +
                "   sort_key VARBINARY AS encode_sort_key(get_json_int(data, 'k1'), get_json_string(data, 'k2') )\n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY HASH(sort_key) BUCKETS 3\n" +
                "ORDER BY (sort_key)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        String plan = getCostExplain(sql);
        if (shouldUseSortAgg) {
            assertContains(plan, "sorted streaming: true");
        } else {
            assertNotContains(plan, "sorted streaming: true");
        }
    }

}
