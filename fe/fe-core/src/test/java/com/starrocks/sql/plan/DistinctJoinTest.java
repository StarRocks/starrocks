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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistinctJoinTest extends TPCDS1TTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDS1TTestBase.beforeClass();
        connectContext.getSessionVariable().disableJoinReorder();
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().enableJoinReorder();
        PlanTestBase.afterClass();
    }

    @Test
    public void testInnerToSemi() throws Exception {
        String sql = "select distinct(t2.v9) from t0 join t1 on t0.v1=t1.v4 join t2 on t0.v2 = t2.v8;";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan, "RIGHT SEMI JOIN (join-predicate [2: v2 = 8: v8] post-join-predicate [null])\n" +
                "                EXCHANGE SHUFFLE[2]\n" +
                "                    LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])\n" +
                "                        SCAN (columns[1: v1, 2: v2] predicate[2: v2 IS NOT NULL])\n" +
                "                        EXCHANGE BROADCAST\n" +
                "                            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])\n" +
                "                EXCHANGE SHUFFLE[8]\n" +
                "                    SCAN (columns[8: v8, 9: v9] predicate[8: v8 IS NOT NULL])");

        sql = "select distinct(t0.v1) from t0 join t1 on t0.v1 = t1.v4 join t2 on t1.v4 = t2.v7;";
        plan = getLogicalFragmentPlan(sql);
        assertContains(plan, " LEFT SEMI JOIN (join-predicate [1: v1 = 7: v7] post-join-predicate [null])\n" +
                "        LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])\n" +
                "            EXCHANGE SHUFFLE[1]\n" +
                "                SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])\n" +
                "            EXCHANGE SHUFFLE[4]\n" +
                "                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])\n" +
                "        EXCHANGE SHUFFLE[7]\n" +
                "            SCAN (columns[7: v7] predicate[7: v7 IS NOT NULL])");

    }

    @Test
    public void testAddDistinct() throws Exception {
        String sql =
                "select count(*) from (select catalog_sales.cs_bill_cdemo_sk from catalog_sales" +
                        " left semi join store_sales on catalog_sales.cs_bill_cdemo_sk = store_sales.ss_addr_sk) a;";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(
                "AGGREGATE ([GLOBAL] aggregate [{58: count=count(58: count)}] group by [[]] having [null]\n" +
                        "    EXCHANGE GATHER\n" +
                        "        AGGREGATE ([LOCAL] aggregate [{58: count=count()}] group by [[]] having [null]\n" +
                        "            LEFT SEMI JOIN " +
                        "(join-predicate [7: cs_bill_cdemo_sk = 42: ss_addr_sk] post-join-predicate [null])\n" +
                        "                SCAN (columns[7: cs_bill_cdemo_sk] predicate[null])\n" +
                        "                EXCHANGE BROADCAST\n" +
                        "                    AGGREGATE ([GLOBAL] aggregate [{}] group by [[42: ss_addr_sk]] having [null]\n" +
                        "                        EXCHANGE SHUFFLE[42]\n" +
                        "                            AGGREGATE ([LOCAL] aggregate [{}] group by [[42: ss_addr_sk]] having " +
                        "[null]\n" +
                        "                                SCAN (columns[42: ss_addr_sk] predicate[42: ss_addr_sk IS NOT NULL])");
    }

}
