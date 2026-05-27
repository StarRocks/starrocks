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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ConvertEqualityDeletesPlanner;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for the equality-delete -> position-delete conversion planner
 * ({@code ConvertEqualityDeletesPlanner}). The convert plan is built programmatically (not from a
 * SELECT, which would trigger the anti-join read rewrite that hides deleted rows). It is the De
 * Morgan dual of the read path: a broadcast LEFT SEMI JOIN keeps the rows an equality delete would
 * hide, projects their ($file_path, $pos), and writes them through an iceberg delete sink. One leg
 * per (equality-ids, scope); legs are unioned (distinct).
 *
 * <p>Reuses the equality-delete mock tables registered in MockIcebergMetadata: eq_delete_global
 * (unpartitioned spec -> GLOBAL), eq_delete_partitioned (identity("p") -> PARTITIONED), and
 * eq_delete_mixed (both, after spec evolution).
 */
public class ConvertEqualityDeletesPlannerTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    private String convertPlan(String tableName) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "iceberg0", "eq_delete_db", tableName);
        IcebergTable icebergTable = (IcebergTable) table;
        long snapshotId = icebergTable.getNativeTable().currentSnapshot().snapshotId();
        ExecPlan plan = new ConvertEqualityDeletesPlanner().plan(icebergTable, snapshotId, connectContext);
        Assertions.assertNotNull(plan, "convert planner must produce a plan for a table with equality deletes");
        return plan.getExplainString(TExplainLevel.NORMAL);
    }

    /**
     * GLOBAL-only table: a single semi-join leg keyed on the equality column + $data_sequence_number,
     * feeding an iceberg delete sink, and never materializing the partition-scoping key.
     */
    @Test
    public void testGlobalScopeConvertPlanShape() {
        String plan = convertPlan("eq_delete_global");
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "ICEBERG DELETE SINK");
        assertNotContains(plan, "$partition_id");
    }

    /**
     * PARTITIONED-only table: the semi-join leg additionally keys on $partition_id, scoping the
     * delete to its own (specId, partition).
     */
    @Test
    public void testPartitionedScopeConvertPlanHasPartitionId() {
        String plan = convertPlan("eq_delete_partitioned");
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "ICEBERG DELETE SINK");
        assertContains(plan, "$partition_id");
    }

    /**
     * MIXED table (spec evolution): one semi-join leg per scope (GLOBAL + PARTITIONED), unioned into a
     * single delete sink; the partitioned leg contributes $partition_id.
     */
    @Test
    public void testMixedScopeConvertUnionsBothLegs() {
        String plan = convertPlan("eq_delete_mixed");
        assertContains(plan, "ICEBERG DELETE SINK");
        assertContains(plan, "$partition_id");
        assertContains(plan, "UNION");
        int semiJoins = countOccurrences(plan, "LEFT SEMI JOIN");
        Assertions.assertTrue(semiJoins >= 2,
                "expected >= 2 LEFT SEMI JOIN legs (one per scope), found " + semiJoins + " in plan:\n" + plan);
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
