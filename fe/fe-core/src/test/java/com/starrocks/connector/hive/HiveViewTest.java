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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

public class HiveViewTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert.withTable("create table test(c0 INT, " +
                "c1 struct<a int, b array<struct<a int, b int>>>," +
                "c2 struct<a int, b int>," +
                "c3 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
    }

    @Test
    public void testHiveView1() throws Exception {
        String sql = "select * from hive0.tpch.customer_view";
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "0:HdfsScanNode\n" +
                "     TABLE: customer");
    }

    @Test
    public void testHive2() throws Exception {
        String sql = "select * from hive0.tpch.customer_nation_view where c_custkey = 1";
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "1:HdfsScanNode\n" +
                        "     TABLE: customer",
                "0:HdfsScanNode\n" +
                "     TABLE: nation");
    }

    @Test
    public void testHive3() throws Exception {
        String sql = "select o_orderkey, o_custkey, c_name, c_address, n_name from orders join hive0.tpch.customer_nation_view " +
                "on orders.o_custkey = customer_nation_view.c_custkey";
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: c_custkey = 2: O_CUSTKEY",
                " 7:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 19: n_nationkey = 14: c_nationkey");
    }

    @Test
    public void testHiveViewParseFail() throws Exception {
        HiveView hiveView = new HiveView(1, "hive0", "test", null, "select\n" +
                 "    t1b,t1a\n" +
                 "from\n" +
                 "    test_all_type\n" +
                 "    lateral view explode(split(t1a,',')) t1a");
        expectedException.expect(StarRocksPlannerException.class);
        expectedException.expectMessage("Failed to parse view-definition statement of view");
        hiveView.getQueryStatementWithSRParser();
    }

    @Test
    public void testQueryHiveViewWithTrinoSQLDialect() throws Exception {
        String sql = "select * from hive0.tpch.customer_alias_view where c_custkey = 1";
        connectContext.getSessionVariable().setSqlDialect("trino");
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "0:HdfsScanNode\n" +
                "     TABLE: customer");
    }

    @Test
    public void testRefreshHiveView(@Mocked CachingHiveMetastore hiveMetastore) throws Exception {
        CacheUpdateProcessor cacheUpdateProcessor = new CacheUpdateProcessor("hive0", hiveMetastore,
                null, null, true, false);
        HiveMetadata hiveMetadata = new HiveMetadata("hive0", null, null, null, null,
                Optional.of(cacheUpdateProcessor));

        Table hiveView = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "tpch", "customer_view");
        new Expectations() {
            {
                hiveMetastore.refreshView(anyString, anyString);
                result = true;
            }
        };
        try {
            hiveMetadata.refreshTable("tpch", hiveView, null, false);
            Assert.assertTrue(hiveView.isHiveView());
            HiveView view = (HiveView) hiveView;
            Assert.assertEquals("hive0", view.getCatalogName());
        } catch (Exception e) {
            Assert.fail();
        }
        HiveMetastore hiveMetastore1 = new HiveMetastore(null, "hive0");
        Assert.assertTrue(hiveMetastore1.refreshView("tpch", "customer_view"));

        Table table  = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "tpch", "customer");

        new Expectations() {
            {
                hiveMetastore.getPartition(anyString, anyString, Lists.newArrayList());
                result = new Partition(Maps.newHashMap(), RemoteFileInputFormat.PARQUET,
                        new TextFileFormatDesc("", "", "", ""),
                        "hdfs://emr_host/test_path", false);

                hiveMetastore.refreshTable(anyString, anyString, anyBoolean);
                result = true;
            }
        };

        try {
            hiveMetadata.refreshTable("tpch", table, null, true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    //    @Test
    public void test() throws Exception {
        connectContext.getSessionVariable().setCboPruneShuffleColumnRate(1.0);
        //        String sql = "select  unnest, generate_series from (select 1) as a, " +
        //                "unnest(['direct', 'Brand.DTI', 'SEO.U.google.com', 'SEO.B.google.com', " +
        //                "'SEM.US.UB.GOOGLE.DT-m-EN.HOTEL']), generate_series(0, 1000)";

        String sql = "select test.c0, test.c2.a, t3.c2 from default_catalog.test.test join[skew|test.test.c1.a(1,2)] " +
                    "hive0.partitioned_db.t3 on c1.a = t3.c1 ";

        //                String sql = "with a as (select 1), \n" +
        //                        "salt_table as (\n" +
        //                        "  select unnest, generate_series as generate_series from a, unnest([1, 2]), generate_series(0, 3000000)\n" +
        //                        "),\n" +
        //                        "left_table as (\n" +
        //                        "\tselect *,\n" +
        //                        "\tcase when c1.a in ('1', '2') then round(\n" +
        //                        "      rand() * 3000000\n" +
        //                        "    ) else 1 end as rand_col \n" +
        //                        "    from default_catalog.test.test\n" +
        //                        "),\n" +
        //                        "right_table as (\n" +
        //                        "\tselect *, \n" +
        //                        "\t  case when generate_series is not null then generate_series else 1 end as rand_col \n" +
        //                        "\t  from hive0.partitioned_db.t3\n" +
        //                        "\t  left join salt_table\n" +
        //                        "\t  on t3.c1 = salt_table.unnest\n" +
        //                        ")\n" +
        //                        "select x.c0, x.c2.a, y.c2 from left_table x left join[shuffle] right_table y on x.c1.a=y.c1 and x.rand_col = y.rand_col";


        //        String sql = "select *, unnest, generate_series from (select 1) as a, " +
        //                "unnest(['direct', 'Brand.DTI', 'SEO.U.google.com', 'SEO.B.google.com', 'SEM.US.UB.GOOGLE.DT-m-EN.HOTEL']), " +
        //                "generate_series(0, 1000);";
        //        String sql = "select case when c0 is not null then c0 else 1 end as rand_col from test";
        String sqlPlan = getFragmentPlan(sql);
        System.out.println(sqlPlan);
    }
}