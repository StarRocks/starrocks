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

import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.sql.plan.TPCDSTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MaterializedViewTPCDSTest extends MaterializedViewTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        TPCDSTestUtil.prepareTables(starRocksAssert);
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
    }

    @Test
    public void testQuery87() {
        String mv = "SELECT\n" +
                "  _ta0000.d_month_seq\n" +
                "  ,_ta0000.c_last_name\n" +
                "  ,_ta0000.c_first_name\n" +
                "  ,_ta0000.d_date\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      `customer`.c_first_name\n" +
                "      ,`customer`.c_last_name\n" +
                "      ,`date_dim`.d_date\n" +
                "      ,`date_dim`.d_month_seq\n" +
                "    FROM\n" +
                "      `web_sales`\n" +
                "      INNER JOIN\n" +
                "      `date_dim`\n" +
                "      ON (`web_sales`.ws_sold_date_sk = `date_dim`.d_date_sk)\n" +
                "      INNER JOIN\n" +
                "      `customer`\n" +
                "      ON (`web_sales`.ws_bill_customer_sk = `customer`.c_customer_sk)\n" +
                "  ) _ta0000\n" +
                "GROUP BY\n" +
                "  _ta0000.d_month_seq\n" +
                "  , _ta0000.c_last_name\n" +
                "  , _ta0000.c_first_name\n" +
                "  , _ta0000.d_date;";
        String query = TPCDSPlanTestBase.Q87;
        testRewriteOK(mv, query);
    }
}
