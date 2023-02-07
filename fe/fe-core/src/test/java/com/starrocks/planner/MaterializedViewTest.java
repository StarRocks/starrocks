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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MaterializedViewTest extends PlanTestBase {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewTest.class);
    private static final String MATERIALIZED_DB_NAME = "test_mv";

    private static final List<String> outerJoinTypes = ImmutableList.of("left", "right");

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext= UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(false);
        connectContext.getSessionVariable().setEnableQueryDebugTrace(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);

        String deptsTable = "" +
                "CREATE TABLE depts(\n" +
                "   deptno INT NOT NULL,\n" +
                "   deptname VARCHAR(20)\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`deptno`)\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String locationsTable = "" +
                "CREATE TABLE locations(\n" +
                "    locationid INT NOT NULL,\n" +
                "    state CHAR(2)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`locationid`)\n" +
                "DISTRIBUTED BY HASH(`locationid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String empsTable = "" +
                "CREATE TABLE emps\n" +
                "(\n" +
                "    empid      INT         NOT NULL,\n" +
                "    deptno     INT         NOT NULL,\n" +
                "    locationid INT         NOT NULL,\n" +
                "    empname    VARCHAR(20) NOT NULL,\n" +
                "    salary     DECIMAL(18, 2)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";

        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME)
                .withTable(empsTable)
                .withTable(locationsTable)
                .withTable(deptsTable);
    }

    @AfterClass
    public static void afterClass() {
        try {
            starRocksAssert.dropDatabase(MATERIALIZED_DB_NAME) ;
        } catch (Exception e) {
            LOG.warn("drop database failed:", e);
        }
    }

    private boolean testRewrite(String mv, String query) {
        // NOTE: distribution by keys should not
        String mvSQL = "CREATE MATERIALIZED VIEW mv0 \n" +
                "   DISTRIBUTED BY HASH(`col1`) BUCKETS 12\n" +
                " AS " +
                mv;
        try {
            starRocksAssert.withMaterializedView(mvSQL);
            String rewritePlan = getFragmentPlan(query);
            System.out.println(rewritePlan);
            starRocksAssert.dropMaterializedView("mv0");
            return rewritePlan.contains("TABLE: mv0");
        } catch (Exception e) {
            LOG.warn("test rewwrite failed:", e);
        }
        return false;
    }
    private void testRewriteOK(String mv, String query) {
        Assert.assertTrue(testRewrite(mv, query));
    }

    private void testRewriteFail(String mv, String query) {
        Assert.assertTrue(!testRewrite(mv, query));
    }

    @Test
    public void testFilter0() {
        String mv = "select empid + 1 as col1 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select max(empid + 1) from emps where deptno = 10");
        testRewriteFail(mv, "select max(empid) from emps where deptno = 10");
        testRewriteFail(mv, "select max(empid) from emps where deptno = 11");
        testRewriteFail(mv, "select max(empid) from emps");
        testRewriteFail(mv, "select empid from emps where deptno = 10");
    }

    @Test
    public void testFilterProject0() {
        String mv = "select empid as col1, locationid * 2 as col2 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select empid from emps where deptno = 10 and (locationid * 2) < 10");
    }

    @Test
    public void testSwapInnerJoin() {
        String mv = "select count(*) as col1 from emps join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*) from  locations join emps on emps.locationid = locations.locationid");
        testRewriteOK(mv, "select count(*) + 1 from  locations join emps on emps.locationid = locations.locationid");
    }

    @Test
    public void testsInnerJoinComplete() {
        String mv = "select deptno as col1, empid as col2, emps.locationid as col3 from emps " +
                "join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*)  from emps " +
                "join locations on emps.locationid = locations.locationid ");
        testRewriteOK(mv, "select count(*)  from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and emps.locationid > 10");
        testRewriteOK(mv, "select empid as col2, emps.locationid from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and emps.locationid > 10");
        testRewriteOK(mv, "select empid as col2, locations.locationid from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and locations.locationid > 10");
    }

    @Test
    public void testMultiInnerJoinQueryDelta() {
        String mv = "select deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*)  from emps join locations " +
                "join depts on emps.locationid = locations.locationid and depts.deptno = emps.deptno");
    }

    @Test
    public void testMultiInnerJoinViewDelta() {
        String mv = "select emps.deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations join depts " +
                "on emps.locationid = locations.locationid and depts.deptno = emps.deptno";
        testRewriteFail(mv, "select deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations on emps.locationid = locations.locationid");
    }

    @Test
    public void testSwapOuterJoin() {
        for (String joinType: outerJoinTypes) {
            String mv = "select count(*) as col1 from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*)  + 1 from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid");

            // outer join cannot swap join orders
            testRewriteFail(mv, "select count(*) from " +
                    "locations " + joinType + " join emps on emps.locationid = locations.locationid");
            testRewriteFail(mv, "select count(*) + 1 from " +
                    "locations " + joinType + " join emps on emps.locationid = locations.locationid");

            // outer join cannot change join type
            if (joinType.equalsIgnoreCase("right")) {
                testRewriteFail(mv, "select count(*) from " +
                        "emps left join locations on emps.locationid = locations.locationid");
            } else {
                testRewriteFail(mv, "select count(*) from " +
                        "emps right join locations on emps.locationid = locations.locationid");
            }
        }
    }

    @Test
    public void testMultiOuterJoinQueryComplete() {
        for (String joinType: outerJoinTypes) {
            String mv = "select deptno as col1, empid as col2, emps.locationid as col3 from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*) from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid");
            testRewriteOK(mv, "select empid as col2, emps.locationid from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and emps.locationid > 10");
            testRewriteOK(mv, "select count(*) from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and emps.locationid > 10");
            testRewriteOK(mv, "select empid as col2, locations.locationid from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and locations.locationid > 10");
            testRewriteFail(mv, "select empid as col2, locations.locationid from " +
                    "emps inner join locations on emps.locationid = locations.locationid " +
                    "and locations.locationid > 10");
        }
    }
    @Test
    public void testMultiOuterJoinQueryDelta() {
        for (String joinType: outerJoinTypes) {
            String mv = "select deptno as col1, empid as col2, locations.locationid as col3 from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*)  from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid " +
                    "" + joinType + " join depts on depts.deptno = emps.deptno");
        }
    }

    @Test
    public void testAggregate0() {
        String mv = "select deptno as col1, locationid + 1 as b, count(*) as c, sum(empid) as s " +
                "from emps group by locationid + 1, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select count(*) as c, locationid + 1 from emps group by locationid + 1");
    }

    @Test
    public void testAggregateRollup() {
        String mv = "select deptno as col1, count(*) as c, sum(empid) as s from emps group by locationid, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select sum(empid), count(*) as c, deptno from emps group by deptno");
        testRewriteFail(mv, "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testsInnerJoinAggregate() {
        String mv = "select count(*) as col1, emps.deptno from " +
                "emps join locations on emps.locationid = locations.locationid + 1 " +
                "group by emps.deptno";
        testRewriteOK(mv, "select count(*) from " +
                "locations join emps on emps.locationid = locations.locationid + 1 " +
                "group by emps.deptno");
        testRewriteFail(mv, "select count(*) , emps.deptno from " +
                "locations join emps on emps.locationid = locations.locationid + 1 " +
                "where emps.deptno > 10 " +
                "group by emps.deptno");
    }

    @Test
    public void testUnionAll0() {
        String mv = "select deptno as col1, count(*) as c, sum(empid) as s from emps group by locationid + 1, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno " +
                "union all " +
                "select count(*) as c, deptno from emps where deptno > 10 group by deptno ");
    }

    @Test
    public void testUserCase1() throws Exception {
        String testTable1 = "CREATE TABLE `test_table1` (\n" +
                "  `ad_account_id` bigint(20) NULL COMMENT \"\",\n" +
                "  `campaign_type` varchar(65533) NULL COMMENT \"\",\n" +
                "  `p_dt` date NULL COMMENT \"\",\n" +
                "  `adunit_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `dsp_placement` varchar(65533) NULL COMMENT \"\",\n" +
                "  `campaign_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `ad_group_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `representative_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `goal` varchar(65533) NULL COMMENT \"\",\n" +
                "  `spending_method` varchar(65533) NULL COMMENT \"\",\n" +
                "  `creative_format` varchar(65533) NULL COMMENT \"\",\n" +
                "  `d_inventory` json NULL COMMENT \"\",\n" +
                "  `d_ad` json NULL COMMENT \"\",\n" +
                "  `d_user` json NULL COMMENT \"\",\n" +
                "  `d_others` json NULL COMMENT \"\",\n" +
                "  `win_amount` decimal128(20, 3) NULL COMMENT \"\",\n" +
                "  `amount` decimal128(20, 3) NULL COMMENT \"\",\n" +
                "  `dsp_cash_amount` decimal128(20, 7) NULL COMMENT \"\",\n" +
                "  `dsp_free_cash_amount` decimal128(20, 7) NULL COMMENT \"\",\n" +
                "  `served_impression_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `rendered_impression_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `viewable_impression_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `click_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `video_play_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `action_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `conversion_count` bigint(20) NULL COMMENT \"\",\n" +
                "  `m_detail_count` json NULL COMMENT \"\",\n" +
                "  `m_other_dec` json NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`ad_account_id`, `campaign_type`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`p_dt`)\n" +
                "(PARTITION p20220930 VALUES [(\"2022-09-30\"), (\"2022-10-01\")),\n" +
                "PARTITION p20221001 VALUES [(\"2022-10-01\"), (\"2022-10-02\")))\n" +
                "DISTRIBUTED BY HASH(`ad_account_id`) BUCKETS 6 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa\"\n" +
                ");";
        starRocksAssert.withTable(testTable1);
        String mv = "   select\n" +
                "    p_dt as col1\n" +
                "    , campaign_type\n" +
                "    , sum(dsp_cash_amount) paid_sales\n" +
                "    , sum(dsp_free_cash_amount) free_sales\n" +
                "    , sum(case when d_inventory->'imp_condition'  = 'WIN' then served_impression_count else " +
                "rendered_impression_count end) impression\n" +
                "    , sum(viewable_impression_count) viewable\n" +
                "    , sum(click_count) click\n" +
                "    , 0 message_send\n" +
                "    , 0 message_open\n" +
                "    , 0 message_click\n" +
                "    from test_table1 \n" +
                "    where campaign_type is not null\n" +
                "    group by p_dt , campaign_type";
        String query = "select\n" +
                "    p_dt\n" +
                "    , campaign_type\n" +
                "    , sum(dsp_cash_amount) paid_sales\n" +
                "    , sum(dsp_free_cash_amount) free_sales\n" +
                "    , sum(case when d_inventory->'imp_condition'  = 'WIN' then served_impression_count else " +
                "rendered_impression_count end) impression\n" +
                "    , sum(viewable_impression_count) viewable\n" +
                "    , sum(click_count) click\n" +
                "    , 0 message_send\n" +
                "    , 0 message_open\n" +
                "    , 0 message_click\n" +
                "    from test_table1 \n" +
                "    where p_dt between '2022-10-01' and '2022-11-30'\n" +
                "    and campaign_type is not null\n" +
                "    group by p_dt , campaign_type";
        testRewriteOK(mv, query);
        starRocksAssert.dropTable("test_table1");
    }
}
