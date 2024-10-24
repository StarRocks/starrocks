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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        MvRewriteTestBase.prepareDefaultDatas();
    }

    @Test
    public void test() throws Exception {
        connectContext.executeSql("drop table if exists t11");
        starRocksAssert.withTable("create table t11(\n" +
                "shop_id int,\n" +
                "region int,\n" +
                "shop_type string,\n" +
                "shop_flag string,\n" +
                "store_id String,\n" +
                "store_qty Double\n" +
                ") DUPLICATE key(shop_id) distributed by hash(shop_id) buckets 1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        cluster.runSql("test", "insert into\n" +
                "t11\n" +
                "values\n" +
                "(1, 1, 's', 'o', '1', null),\n" +
                "(1, 1, 'm', 'o', '2', 2),\n" +
                "(1, 1, 'b', 'c', '3', 1);");
        connectContext.executeSql("drop materialized view if exists mv11");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW mv11 (region, ct) " +
                "DISTRIBUTED BY HASH(`region`) buckets 1 REFRESH MANUAL as\n" +
                "select region,\n" +
                "count(\n" +
                "distinct (\n" +
                "case\n" +
                "when store_qty > 0 then store_id\n" +
                "else null\n" +
                "end\n" +
                ")\n" +
                ")\n" +
                "from t11\n" +
                "group by region;");
        cluster.runSql("test", "refresh materialized view mv11 with sync mode");
        {
            String query = "select region,\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") as ct\n" +
                    "from t11\n" +
                    "group by region\n" +
                    "having\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") > 0\n";
            String plan = getFragmentPlan(query);
            Assert.assertTrue(plan.contains("mv11"));
            Assert.assertTrue(plan.contains("PREDICATES: 10: ct > 0"));
        }
    }
}
