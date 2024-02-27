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

public class TPCDS1TRuntimeFilterTest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
    }

    @Test
    public void testBroadcastRuntimeFilter() throws Exception {
        String sql = Q87;
        String plan = getVerboseExplain(sql);
        System.out.println(Q87);

        System.out.println(plan);
    }

    @Test
    public void testStr() throws Exception {
        String sql = "select count(*) \n" +
                "from (" +
                "      select distinct c_last_name, c_first_name, d_date\n" +
                "       from catalog_sales, date_dim, customer\n" +
                "       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
                "         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
                "         and d_month_seq between 1200 and 1200+11\n" +
                ") cool_cust\n" +
                ";";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
    }

    @Test
    public void testStr1() throws Exception {
        String sql = "select count(*) \n" +
                "from (" +
                "      select c_last_name, c_first_name\n" +
                "       from catalog_sales join[broadcast] customer\n" +
                "         on catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
                ") cool_cust\n" +
                ";";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
    }
}
