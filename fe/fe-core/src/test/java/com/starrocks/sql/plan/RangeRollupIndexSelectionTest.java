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
 * Proves that the optimizer's prefix-index scorer ({@code matchBestPrefixIndex}) routes
 * a query to the rollup index whose sort key matches the query predicate — not to the base.
 *
 * <p>Table: base sort key (k1, k2), rollup schema (k2, k1, v1) = sort-key-ordered with k2 first.
 * <ul>
 *   <li>{@code WHERE k2 = X} → rollup selected ({@code rollup_name:t8_sel_rollup})</li>
 *   <li>{@code WHERE k1 = X} → base selected ({@code rollup_name:t8_sel})</li>
 * </ul>
 */
public class RangeRollupIndexSelectionTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        // DUP table with base sort key (k1, k2).
        starRocksAssert.withTable(
                "CREATE TABLE t8_sel (k1 INT NOT NULL, k2 INT NOT NULL, v1 INT)\n"
                        + "DUPLICATE KEY(k1, k2)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES('replication_num' = '1');");

        // Rollup schema (k2, k1, v1): k2 is the first (leading) key column,
        // so matchBestPrefixIndex scores it higher for WHERE k2 = X.
        starRocksAssert.withRollup(
                "ALTER TABLE t8_sel ADD ROLLUP t8_sel_rollup (k2, k1, v1)");
    }

    /**
     * {@code WHERE k2 = X} should select the rollup whose leading sort column is k2.
     */
    @Test
    public void testRollupSelectedWhenQueryOnRollupLeadingKey() throws Exception {
        String plan = getThriftPlan("SELECT k2, v1 FROM t8_sel WHERE k2 = 5");
        assertTrue(plan.contains("rollup_name:t8_sel_rollup"),
                "Expected rollup t8_sel_rollup to be selected for WHERE k2=5, got:\n" + plan);
    }

    /**
     * {@code WHERE k1 = X} should select the base index whose leading sort column is k1.
     */
    @Test
    public void testBaseSelectedWhenQueryOnBaseLeadingKey() throws Exception {
        String plan = getThriftPlan("SELECT k1, v1 FROM t8_sel WHERE k1 = 5");
        assertFalse(plan.contains("rollup_name:t8_sel_rollup"),
                "Expected base index for WHERE k1=5 (rollup leads with k2), got:\n" + plan);
        assertTrue(plan.contains("rollup_name:t8_sel"),
                "Expected base rollup_name:t8_sel for WHERE k1=5, got:\n" + plan);
    }
}
