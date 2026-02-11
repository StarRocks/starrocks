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

import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PaimonPartitionPruneLimitTest extends ConnectorPlanTestBase {
    @BeforeEach
    public void setUp() {
        super.setUp();
        try {
            connectContext.changeCatalogDb("paimon0.pmn_db1");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPaimonPartitionPruneLimit() throws Exception {
        // 1.partition table
        String sql1 = "select * from partitioned_table;";

        // variable default value: 0
        String plan1 = getFragmentPlan(sql1);
        assertContains(plan1, "partitions=10/10");

        // variable value > scan partition num
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(20);
        String plan2 = getFragmentPlan(sql1);
        assertContains(plan2, "partitions=10/10");

        // variable value < scan partition num
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(3);
        String msg = "Exceeded the limit of number of paimon table partitions to be scanned. " +
                "Number of partitions allowed: 3, number of partitions to be scanned: 10. " +
                "Please adjust the SQL or change the limit by set variable scan_lake_partition_num_limit.";
        ExceptionChecker.expectThrowsWithMsg(StarRocksPlannerException.class, msg,
                () -> getFragmentPlan(sql1));

        // 2.unpartition table
        String sql2 = "select * from unpartitioned_table;";

        connectContext.getSessionVariable().setScanLakePartitionNumLimit(3);
        String plan3 = getFragmentPlan(sql2);
        assertContains(plan3, "partitions=1/1");

        // 3.system table
        String sql3 = "select * from partitioned_table$snapshots;";

        connectContext.getSessionVariable().setScanLakePartitionNumLimit(3);
        String plan4 = getFragmentPlan(sql3);
        assertContains(plan4, "partitions=0/1");
    }
}