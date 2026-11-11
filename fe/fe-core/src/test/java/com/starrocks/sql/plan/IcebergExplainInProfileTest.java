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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The partition line of an Iceberg scan lists every partition of the table. The plan embedded into a finished
 * query's profile (enable_explain_in_profile) skips it, since nothing bounds that metadata read after the query.
 */
public class IcebergExplainInProfileTest extends ConnectorPlanTestBase {

    @Test
    public void testExplainForProfileSkipsPartitionLine() throws Exception {
        String sql = "select * from iceberg0.unpartitioned_db.t0";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("partitions="), plan);

        connectContext.setExplainForProfile(true);
        try {
            plan = getVerboseExplain(sql);
        } finally {
            connectContext.setExplainForProfile(false);
        }
        Assertions.assertTrue(plan.contains("IcebergScanNode"), plan);
        Assertions.assertFalse(plan.contains("partitions="), plan);
    }
}
