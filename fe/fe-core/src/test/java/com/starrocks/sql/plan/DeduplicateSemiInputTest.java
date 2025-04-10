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

public class DeduplicateSemiInputTest extends TPCDS1TTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDS1TTestBase.beforeClass();
        connectContext.getSessionVariable().disableJoinReorder();
        FeConstants.enableJoinReorderInLogicalPhase = true;
    }

    @AfterClass
    public static void afterClass() {
        FeConstants.enableJoinReorderInLogicalPhase = false;
        connectContext.getSessionVariable().enableJoinReorder();
        TPCDS1TTestBase.afterClass();
    }

    @Test
    public void testAddDistinct() throws Exception {
        String sql =
                "select count(*) from (select catalog_sales.cs_bill_cdemo_sk from catalog_sales" +
                        " left semi join store_sales on catalog_sales.cs_bill_cdemo_sk = store_sales.ss_addr_sk) a;";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan,
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
