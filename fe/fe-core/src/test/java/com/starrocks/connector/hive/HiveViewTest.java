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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.MetastoreType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HiveViewTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
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
    public void testHiveViewParseFail() {
        HiveView hiveView = new HiveView(1, "hive0", "testDb", "test", null,
                "select\n" +
                        "    t1b,t1a\n" +
                        "from\n" +
                        "    test_all_type\n" +
                        "    lateral view explode(split(t1a,',')) t1a", HiveView.Type.Hive);
        Throwable exception = assertThrows(StarRocksPlannerException.class, () -> hiveView.getQueryStatement());
        assertThat(exception.getMessage(), containsString("Failed to parse view-definition statement of view"));
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
    public void testQueryTrinoViewWithoutDb() throws Exception {
        // test query trino view without db
        String sql = "select * from hive0.tpch.customer_view_without_db where c_custkey = 1";
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "0:HdfsScanNode\n" +
                "     TABLE: customer");
    }

    @Test
    public void testQueryHiveViewCaseInsensitive() throws Exception {
        String sql = "select * from hive0.tpch.customer_case_insensitive_view where c_custkey = 1";
        String sqlPlan = getFragmentPlan(sql);
        assertContains(sqlPlan, "TABLE: customer");

        Throwable exception = assertThrows(SemanticException.class, () -> {
            getFragmentPlan("select * from hive0.tpch.customer_case_insensitive_view v1 join test.t0 T0 on v1.c_custkey = t0.v1");
        });
        assertThat(exception.getMessage(), containsString("Column '`t0`.`v1`' cannot be resolved"));
    }

    @Test
    public void testRefreshHiveView(@Mocked CachingHiveMetastore hiveMetastore) throws Exception {
        HiveCacheUpdateProcessor hiveCacheUpdateProcessor = new HiveCacheUpdateProcessor("hive0", hiveMetastore,
                null, null, true, false);
        HiveMetadata hiveMetadata = new HiveMetadata("hive0", null, null, null, null,
                Optional.of(hiveCacheUpdateProcessor), null, null,
                new ConnectorProperties(ConnectorType.HIVE));

        Table hiveView = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(new ConnectContext(), "hive0", "tpch", "customer_view");
        new Expectations() {
            {
                hiveMetastore.refreshView(anyString, anyString);
                result = true;
            }
        };
        try {
            hiveMetadata.refreshTable("tpch", hiveView, null, false);
            Assertions.assertTrue(hiveView.isHiveView());
            HiveView view = (HiveView) hiveView;
            Assertions.assertEquals("hive0", view.getCatalogName());
        } catch (Exception e) {
            Assertions.fail();
        }
        HiveMetastore hiveMetastore1 = new HiveMetastore(null, "hive0", MetastoreType.HMS);
        Assertions.assertTrue(hiveMetastore1.refreshView("tpch", "customer_view"));

        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(new ConnectContext(), "hive0", "tpch", "customer");

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
            Assertions.fail();
        }
    }

    @Test
    public void testShowHiveView() {
        HiveView hiveView = new HiveView(1, "hive0", "testDb", "test",
                Lists.newArrayList(new Column("t1a", IntegerType.INT), new Column("t1b", IntegerType.INT)),
                "select\n" +
                        "    t1b,t1a\n" +
                        "from\n" +
                        "    test_all_type", HiveView.Type.Hive);
        String viewDdl = AstToStringBuilder.getExternalCatalogViewDdlStmt(hiveView);
        Assertions.assertEquals("CREATE VIEW `test` (`t1a`, `t1b`) AS select\n" +
                "    t1b,t1a\n" +
                "from\n" +
                "    test_all_type;", viewDdl);
    }
}