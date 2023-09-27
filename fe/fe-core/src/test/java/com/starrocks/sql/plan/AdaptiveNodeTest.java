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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdaptiveNodeTest extends DistributedEnvPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.showLocalShuffleColumnsInExplain = false;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        connectContext.getSessionVariable().setEnableAdaptiveExecuteNodeNum(true);
        connectContext.getSessionVariable().setAdaptiveExecuteNodeMinFactor(0);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableAdaptiveExecuteNodeNum(false);
        FeConstants.showLocalShuffleColumnsInExplain = true;
    }

    @Test
    public void testShuffle() throws Exception {
        String sql = "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, " +
                "            c_acctbal, n_name, c_address, c_phone, c_comment\n" +
                "from lineitem join[shuffle] orders on l_orderkey = o_orderkey" +
                "              join[shuffle] customer on c_custkey = o_custkey" +
                "              join[shuffle] nation on c_nationkey = n_nationkey\n" +
                "where o_orderdate >= date '1994-05-01'\n" +
                "  and o_orderdate < date '1994-08-01'\n" +
                "  and l_returnflag = 'R'\n" +
                "group by\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    c_acctbal,\n" +
                "    c_phone,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_comment\n" +
                "order by\n" +
                "    revenue desc limit 20;";

        String plan = getCostExplain(sql);
        assertContains(plan, "Host Factor: 0.99");
    }

    @Test
    public void testShuffle2() throws Exception {
        String sql = "select * " +
                "from lineitem join[shuffle] (select distinct o_orderkey, n_nationkey " +
                "                             from orders join nation on n_nationkey = o_custkey" +
                "                             ) x on l_orderkey = o_orderkey " +
                "where l_returnflag = 'R' and L_SUPPKEY = 2";

        String plan = getCostExplain(sql);
        assertContains(plan, "Host Factor: 0.00");
    }
}
