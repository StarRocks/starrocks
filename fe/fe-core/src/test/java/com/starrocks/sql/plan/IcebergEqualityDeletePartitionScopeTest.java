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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for the Iceberg equality-delete partition-scoping rewrite
 * ({@code IcebergEqualityDeleteRewriteRule}).
 *
 * <p>The rule rewrites a scan of a V2 Iceberg table that has equality-delete files into a
 * broadcast LEFT-ANTI-JOIN chain + UNION ALL. Per equality-id set, the delete files are split by
 * scope:
 * <ul>
 *   <li>GLOBAL leg: deletes written under an unpartitioned spec; the anti-join keys on the
 *       equality columns + {@code $data_sequence_number} only (no {@code $partition_id}).</li>
 *   <li>PARTITIONED leg: deletes written under a partitioned spec; the anti-join also keys on the
 *       synthetic {@code $partition_id} column, scoping the delete to its own (specId, partition).</li>
 * </ul>
 *
 * <p>The mock tables are registered in {@code MockIcebergMetadata#mockEqualityDeleteTables()}.
 */
public class IcebergEqualityDeletePartitionScopeTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    /**
     * GLOBAL-only table: equality deletes live under an unpartitioned spec, so the rewrite emits a
     * single GLOBAL anti-join leg keyed on the equality column + $data_sequence_number, and never
     * materializes the $partition_id column.
     */
    @Test
    public void testGlobalOnlyScopeHasNoPartitionId() throws Exception {
        String plan = getFragmentPlan("select * from iceberg0.eq_delete_db.eq_delete_global");
        // The rewrite fires -> anti join chain present.
        assertContains(plan, "ANTI JOIN");
        // A purely global delete must not introduce the partition-scoping key.
        assertNotContains(plan, "$partition_id");
    }

    /**
     * PARTITIONED-only table: equality deletes live under a partitioned identity("p") spec, so the
     * rewrite emits a PARTITIONED anti-join leg that additionally keys on the synthetic
     * $partition_id column.
     */
    @Test
    public void testPartitionedScopeMaterializesPartitionId() throws Exception {
        String plan = getFragmentPlan("select * from iceberg0.eq_delete_db.eq_delete_partitioned");
        assertContains(plan, "ANTI JOIN");
        // The partitioned leg scopes the delete via $partition_id.
        assertContains(plan, "$partition_id");
    }

    /**
     * MIXED table (spec evolution): the same equality-id set has delete files under both an
     * unpartitioned spec (GLOBAL) and, after evolving the spec to identity("p"), a partitioned spec
     * (PARTITIONED). The rewrite must emit one anti-join leg per scope, so the plan contains
     * $partition_id (from the partitioned leg) and at least two anti-join legs.
     */
    @Test
    public void testMixedScopeHasBothLegs() throws Exception {
        String plan = getFragmentPlan("select * from iceberg0.eq_delete_db.eq_delete_mixed");
        assertContains(plan, "ANTI JOIN");
        // The partitioned leg is present.
        assertContains(plan, "$partition_id");
        // One anti-join per scope (GLOBAL + PARTITIONED) -> at least two anti-join legs.
        int antiJoinCount = countOccurrences(plan, "ANTI JOIN");
        org.junit.jupiter.api.Assertions.assertTrue(antiJoinCount >= 2,
                "expected >= 2 ANTI JOIN legs (one per scope), but found " + antiJoinCount
                        + " in plan:\n" + plan);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
