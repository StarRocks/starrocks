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

public class ExprRangePartitionPruneTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        starRocksAssert.withTable("CREATE TABLE `test32` (\n" +
                "  `busness_date` varchar(8) NULL COMMENT \"\",\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`busness_date`)\n" +
                "PARTITION BY RANGE(str2date(busness_date, '%Y%m%d'))\n" +
                "(PARTITION p2025 VALUES [(\"0000-01-01\"), (\"2026-01-01\")),\n" +
                "PARTITION p2026 VALUES [(\"2026-01-01\"), (\"2027-01-01\")))\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")");

    }

    @Test
    public void testExprRangePartitionUpperBoundReserved1() throws Exception {
        String sql = "select * from test32 "
                + "where str2date(busness_date, '%Y%m%d')>= '2026-01-01' "
                + "and str2date(busness_date, '%Y%m%d')<= '2026-03-01'";
        String plan = getFragmentPlan(sql);

        assertCContains(plan, "PREDICATES: str2date(1: busness_date, '%Y%m%d') >= '2026-01-01', " +
                "str2date(1: busness_date, '%Y%m%d') <= '2026-03-01'");

        assertCContains(plan, "partitions=2/2");
    }

    @Test
    public void testExprRangePartitionUpperBoundReserved2() throws Exception {
        String sql = "select * from test32 "
                + "where busness_date >= '20260101' "
                + "and busness_date <= '20260301'";
        String plan = getFragmentPlan(sql);

        assertCContains(plan, "1: busness_date <= '20260301");
        assertCContains(plan, "partitions=1/2");
    }
}

