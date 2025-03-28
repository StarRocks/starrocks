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

package com.starrocks.planner;

import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class HoistHeavyCostExprsUponTopnTest extends PlanTestNoneDBBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("test_db0").useDatabase("test_db0");
        String createTableSql = "CREATE TABLE t0 (\n" +
                "    EventDate DATE NOT NULL,\n" +
                "    UserID STRING NOT NULL,\n" +
                "    M0 DECIMAL(20,2),\n" +
                "    M1 DECIMAL(20,2),\n" +
                "    M2 LARGEINT,\n" +
                "    M3 LARGEINT\n" +
                ")  \n" +
                "DUPLICATE KEY (EventDate)\n" +
                "DISTRIBUTED BY HASH(UserID) BUCKETS 1\n" +
                "PROPERTIES ( \"replication_num\"=\"1\");";
        starRocksAssert.withTable(createTableSql);
    }

    @Test
    public void testDecimalDivExprHoisted() throws Exception {
        String sql = "select M1, M0, M0/M1\n" +
                "from t0\n" +
                "order by EventDate\n" +
                "limit 10 offset 20";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertCContains(plan, "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: M0, DECIMAL128(20,2), true]\n" +
                "  |  4 <-> [4: M1, DECIMAL128(20,2), true]\n" +
                "  |  7 <-> [3: M0, DECIMAL128(20,2), true] / [4: M1, DECIMAL128(20,2), true]\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE");
    }

    @Test
    public void testDecimalDivExprNotHoisted() throws Exception {
        String sql = "select M1, M0/M1\n" +
                "from t0\n" +
                "order by EventDate\n" +
                "limit 10 offset 20";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertCContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: EventDate, DATE, false]\n" +
                "  |  4 <-> [4: M1, DECIMAL128(20,2), true]\n" +
                "  |  7 <-> [3: M0, DECIMAL128(20,2), true] / [4: M1, DECIMAL128(20,2), true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testLargeInt128DivHoisted() throws Exception {
        String sql = "select M2, M3, M2/M3\n" +
                "from t0\n" +
                "order by EventDate\n" +
                "limit 10 offset 20";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertCContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: EventDate, DATE, false]\n" +
                "  |  5 <-> [5: M2, LARGEINT, true]\n" +
                "  |  6 <-> [6: M3, LARGEINT, true]\n" +
                "  |  7 <-> cast([5: M2, LARGEINT, true] as DOUBLE) / cast([6: M3, LARGEINT, true] as DOUBLE)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }
}