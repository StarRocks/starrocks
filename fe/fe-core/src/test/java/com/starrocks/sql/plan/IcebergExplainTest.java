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

public class IcebergExplainTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @Test
    public void testVerboseExplainSeparatesPushedAndNonPushedPredicates() throws Exception {
        // id > 10: pushable to Iceberg (integer comparison)
        // data LIKE '%test': NOT pushable (only 'prefix%' is supported)
        String sql = "SELECT * FROM iceberg0.unpartitioned_db.t0 WHERE id > 10 AND data LIKE '%test'";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "PREDICATES: 1: id > 10");
        assertContains(plan, "NON-PUSHED PREDICATES: 2: data LIKE '%test'");
    }

    @Test
    public void testNormalExplainDoesNotSeparatePredicates() throws Exception {
        String sql = "SELECT * FROM iceberg0.unpartitioned_db.t0 WHERE id > 10 AND data LIKE '%test'";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "NON-PUSHED PREDICATES:");
        assertContains(plan, "PREDICATES: 1: id > 10, 2: data LIKE '%test'");
    }

    @Test
    public void testVerboseExplainAllPushable() throws Exception {
        // All predicates are pushable: integer comparison only
        String sql = "SELECT * FROM iceberg0.unpartitioned_db.t0 WHERE id > 10 AND id < 100";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "PREDICATES: 1: id > 10, 1: id < 100");
        assertNotContains(plan, "NON-PUSHED PREDICATES:");
    }

    @Test
    public void testVerboseExplainNoPredicates() throws Exception {
        String sql = "SELECT * FROM iceberg0.unpartitioned_db.t0";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "PREDICATES:");
        assertNotContains(plan, "NON-PUSHED PREDICATES:");
    }
}
