// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PlanFragmentWithCostTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.default_scheduler_interval_millisecond = 1;

        Catalog catalog = connectContext.getCatalog();
        OlapTable table2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(table2, 10000);

        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 10000);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE test_mv\n" +
                "    (\n" +
                "        event_day int,\n" +
                "        siteid INT,\n" +
                "        citycode SMALLINT,\n" +
                "        username VARCHAR(32),\n" +
                "        pv BIGINT SUM DEFAULT '0'\n" +
                "    )\n" +
                "    AGGREGATE KEY(event_day, siteid, citycode, username)\n" +
                "    DISTRIBUTED BY HASH(siteid) BUCKETS 10\n" +
                "    rollup (\n" +
                "    r1(event_day,siteid),\n" +
                "    r2(event_day,citycode),\n" +
                "    r3(event_day),\n" +
                "    r4(event_day,pv),\n" +
                "    r5(event_day,siteid,pv)\n" +
                "    )\n" +
                "    PROPERTIES(\"replication_num\" = \"1\");");

        starRocksAssert.withTable(" CREATE TABLE `duplicate_table_with_null` ( `k1`  date, `k2`  datetime, " +
                "`k3`  char(20), `k4`  varchar(20), `k5`  boolean, `k6`  tinyint, " +
                "`k7`  smallint, `k8`  int, `k9`  bigint, `k10` largeint, " +
                "`k11` float, `k12` double, `k13` decimal(27,9) ) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "COMMENT \"OLAP\" DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) " +
                "BUCKETS 3 PROPERTIES ( \"replication_num\" = \"1\", " +
                "\"storage_format\" = \"v2\" );");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW bitmap_mv\n" +
                "                             AS\n" +
                "                             SELECT k1,k2,k3,k4, bitmap_union(to_bitmap(k7)), " +
                "bitmap_union(to_bitmap(k8)) FROM duplicate_table_with_null group by k1,k2,k3,k4");
        FeConstants.runningUnitTest = true;
    }

    @Before
    public void before() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    private static final String V1 = "v1";
    private static final String V2 = "v2";
    private static final String V3 = "v3";

    @Test
    public void testAggWithLowCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };

        String sql = "select sum(v2) from t0 group by v2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(4: sum(2: v2))\n"
                + "  |  group by: 2: v2"));
        Assert.assertTrue(planFragment.contains("  1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: sum(2: v2)\n"
                + "  |  group by: 2: v2"));
    }

    @Test
    public void testAggWithHighCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };

        String sql = "select sum(v2) from t0 group by v2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: sum(2: v2)\n"
                + "  |  group by: 2: v2"));
        Assert.assertFalse(planFragment.contains("  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2"));
    }

    @Test
    public void testSortWithLowCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0"));
    }

    @Test
    public void testSortWithHighCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0"));
    }

    @Test
    public void testTopNWithLowCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1 limit 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1"));
    }

    @Test
    public void testTopNWithHighCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1 limit 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1"));
    }

    @Test
    public void testDistinctWithoutGroupByWithLowCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage)
            throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select count(distinct v2), sum(v1) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge serialize)\n"
                + "  |  output: sum(5: sum(1: v1))\n"
                + "  |  group by: 2: v2"));
        Assert.assertTrue(planFragment.contains("  6:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count(distinct 2: v2)), sum(5: sum(1: v1))\n"
                + "  |  group by: \n"
                + "  |  use vectorized: true"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 05\n"
                + "    UNPARTITIONED"));
        Assert.assertFalse(planFragment.contains("PLAN FRAGMENT 3"));

    }

    @Test
    public void testDistinctWithoutGroupByWithHighCardinalityForceOneStage(
            @Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select count(distinct v2), sum(v1) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2), sum(1: v1)\n"
                + "  |  group by:"));

    }

    @Test
    public void testDistinctWithGroupByWithLowCardinalityForceThreeStage(
            @Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 50),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 50));
            }
        };
        String sql = "select count(distinct v2) from t0 group by v3";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge serialize)\n"
                + "  |  group by: 2: v2, 3: v3"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 06\n"
                + "    UNPARTITIONED"));

    }

    @Test
    public void testDistinctWithGroupByWithHighCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage)
            throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select count(distinct v2) from t0 group by v3";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2)\n"
                + "  |  group by: 3: v3"));
    }

    @Test
    public void testPredicateRewrittenByProjectWithLowCardinality(@Mocked CachedStatisticStorage mockedStatisticStorage)
            throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 10),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 10));
            }
        };
        String sql = "SELECT -v3 from t0 group by v3, v2 having -v3 < 63;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:Project\n"
                + "  |  <slot 4> : -1 * 3: v3"));
        Assert.assertTrue(planFragment.contains("PREDICATES: -1 * 3: v3 < 63"));
    }

    @Test
    public void testPredicateRewrittenByProjectWithHighCardinality(
            @Mocked CachedStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((OlapTable) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "SELECT -v3 from t0 group by v3, v2 having -v3 < 63;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:Project\n"
                + "  |  <slot 4> : -1 * 3: v3"));
    }

    @Test
    public void testShuffleInnerJoin() throws Exception {
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        Catalog catalog = connectContext.getCatalog();
        OlapTable table1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(table1, 10000);
        OlapTable table2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(table2, 5000);
        connectContext.getSessionVariable().setPreferJoinMethod("shuffle");
        String sql = "SELECT v2,t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:HASH JOIN\n"
                + "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  1:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("    EXCHANGE ID: 03\n"
                + "    HASH_PARTITIONED: 7: t1d\n"
                + "\n"
                + "  2:OlapScanNode"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 01\n"
                + "    HASH_PARTITIONED: 2: v2\n"
                + "\n"
                + "  0:OlapScanNode"));
        Catalog.getCurrentSystemInfo().dropBackend(10002);
        Catalog.getCurrentSystemInfo().dropBackend(10003);
    }

    @Test
    public void testBroadcastInnerJoin() throws Exception {
        String sql = "SELECT v1, t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));
    }

    @Test
    public void testBroadcastInnerJoinWithCommutativity() throws Exception {
        Catalog catalog = connectContext.getCatalog();
        OlapTable table = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(table, 1000);
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 7: t1d = 1: v1\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: test_all_type\n"));
        setTableStatistics(table, 10000);
    }

    @Test
    public void testColocateJoin() throws Exception {
        String sql = "SELECT * from t0 join t0 as b on t0.v1 = b.v1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (COLOCATE)"));
    }

    @Test
    public void testColocateAgg() throws Exception {
        String sql = "SELECT count(*) from t0 group by t0.v1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update finalize)"));
    }

    @Test
    public void testDistinctExpr() throws Exception {
        String sql = "SELECT DISTINCT - - v1 DIV - 98 FROM t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 4>\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:Project"));
        Assert.assertTrue(planFragment.contains("EXCHANGE"));
    }

    @Test
    public void testRollUp() throws Exception {
        String sql = "select event_day from test_mv group by event_day;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select count(*) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select count(*), event_day from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select event_day from test_mv where citycode = 1 group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r2"));

        sql = "select siteid from test_mv where event_day  = 1 group by siteid;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r1"));

        sql = "select siteid from test_mv group by siteid, event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r1"));

        sql = "select siteid from test_mv group by siteid, username;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select siteid,sum(pv) from test_mv group by siteid, username;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select sum(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r4"));

        sql = "select max(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select max(event_day) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select max(event_day), sum(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r4"));

        sql = "select max(event_day), max(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));
    }

    @Test
    public void testMV() throws Exception {
        String sql = "select count(distinct k7), count(distinct k8) from duplicate_table_with_null;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("OUTPUT EXPRS:16: count(distinct 7: k7) | 17: count(distinct 8: k8)"));
        Assert.assertTrue(planFragment.contains("  |  <slot 7> : 14: mv_bitmap_union_k7\n" +
                "  |  <slot 8> : 15: mv_bitmap_union_k8"));
        Assert.assertTrue(planFragment.contains("rollup: bitmap_mv"));
    }

    @Test
    public void testVipKidQuery() throws Exception {
        starRocksAssert.withTable("create table if not exists `rl_vk_dwd_online_class` (\n" +
                "  `id` bigint(20) null ,\n" +
                "  `student_id` bigint(20) null ,\n" +
                "  `teacher_id` bigint(20) null ,\n" +
                "  `scheduled_date_time` datetime null ,\n" +
                "  `status` varchar(100) null ,\n" +
                "  `finish_type` varchar(100) null ,\n" +
                "  `book_type` bigint(20) null ,\n" +
                "  `book_date_time` datetime null ,\n" +
                "  `update_time` datetime null ,\n" +
                "  `course_id` bigint(20) null ,\n" +
                "  `create_time` datetime null ,\n" +
                "  `student_changed_tag` bigint(20) null ,\n" +
                "  `lesson_id` bigint(20) null ,\n" +
                "  `curriculum_version` varchar(100) null ,\n" +
                "  `book_user_id` bigint(20) null ,\n" +
                "  `finish_user_id` bigint(20) null ,\n" +
                "  `finish_date_time` datetime null ,\n" +
                "  `can_undo_finish` bigint(20) null ,\n" +
                "  `attatchdocumentsucess` bigint(20) null ,\n" +
                "  `dby_document` varchar(1000) null ,\n" +
                "  `short_notice` bigint(20) null ,\n" +
                "  `classroom` varchar(200) null ,\n" +
                "  `student_enter_classroom_date_time` datetime null ,\n" +
                "  `teacher_enter_classroom_date_time` datetime null ,\n" +
                "  `able_to_enter_classroom_date_time` datetime null ,\n" +
                "  `consume_class_hour` bigint(20) null ,\n" +
                "  `supplier_code` varchar(10) null ,\n" +
                "  `version` bigint(20) null ,\n" +
                "  `wxt_course_id` varchar(100) null ,\n" +
                "  `last_editor_id` bigint(20) null ,\n" +
                "  `comments` varchar(1000) null ,\n" +
                "  `creater_id` bigint(20) null ,\n" +
                "  `class_type` bigint(20) null ,\n" +
                "  `archived` bigint(20) null ,\n" +
                "  `backup` bigint(20) null ,\n" +
                "  `last_edit_date_time` datetime null ,\n" +
                "  `max_student_number` bigint(20) null ,\n" +
                "  `min_student_number` bigint(20) null ,\n" +
                "  `serial_number` varchar(200) null ,\n" +
                "  `unit_price` decimal(18, 4) null ,\n" +
                "  `is_paid_trail` bigint(20) null ,\n" +
                "  `class_mode` bigint(20) null ,\n" +
                "  `related_class_id` bigint(20) null ,\n" +
                "  `is_vailue` varchar(10) null ,\n" +
                "  `time_gap` varchar(10) null ,\n" +
                "  `is_minor` varchar(10) null ,\n" +
                "  `is_vail` varchar(10) null \n" +
                ") engine=olap\n" +
                "unique key(`id`)\n" +
                "comment \"olap\"\n" +
                "distributed by hash(`id`) buckets 30\n" +
                "properties (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");");
        starRocksAssert.withTable("create table if not exists `vk_dw_pty_teacher_info_da` (\n" +
                "  `teacher_id` bigint(20) null ,\n" +
                "  `teacher_show_name` varchar(500) null ,\n" +
                "  `birthday` varchar(30) null ,\n" +
                "  `gender` varchar(30) null ,\n" +
                "  `timezone` varchar(120) null ,\n" +
                "  `first_name` varchar(500) null ,\n" +
                "  `middle_name` varchar(500) null ,\n" +
                "  `last_name` varchar(500) null ,\n" +
                "  `maiden_name` varchar(500) null ,\n" +
                "  `teacher_address` varchar(500) null ,\n" +
                "  `nationality` varchar(120) null ,\n" +
                "  `entry_date` date null ,\n" +
                "  `life_cycle` varchar(60) null ,\n" +
                "  `register_date_time` datetime null ,\n" +
                "  `job` varchar(500) null ,\n" +
                "  `work_exp` bigint(20) null ,\n" +
                "  `vipkid_work_exp` bigint(20) null ,\n" +
                "  `quit_or_toc` bigint(20) null ,\n" +
                "  `job_type` varchar(30) null ,\n" +
                "  `certificates` varchar(500) null ,\n" +
                "  `account_type` varchar(30) null ,\n" +
                "  `age` bigint(20) null ,\n" +
                "  `highest_degree` varchar(60) null ,\n" +
                "  `university` varchar(500) null ,\n" +
                "  `major` varchar(500) null ,\n" +
                "  `referee_name` varchar(120) null ,\n" +
                "  `referee_entry_date` date null ,\n" +
                "  `contract_start_date` date null ,\n" +
                "  `contract_end_date` date null ,\n" +
                "  `base_salary` varchar(30) null ,\n" +
                "  `contract_type` varchar(60) null ,\n" +
                "  `country` varchar(200) null ,\n" +
                "  `state` varchar(200) null ,\n" +
                "  `city` varchar(500) null ,\n" +
                "  `skype` varchar(30) null ,\n" +
                "  `partner_id` bigint(20) null ,\n" +
                "  `partner_name` varchar(200) null ,\n" +
                "  `other_channel` varchar(500) null ,\n" +
                "  `channel` varchar(200) null ,\n" +
                "  `channel_team` varchar(30) null ,\n" +
                "  `referee_id` bigint(20) null ,\n" +
                "  `course_id` varchar(500) null ,\n" +
                "  `if_normal_course` varchar(30) null ,\n" +
                "  `first_online_class_id` bigint(20) null ,\n" +
                "  `first_online_class_time` datetime null ,\n" +
                "  `vipkid_remarks` varchar(3000) null ,\n" +
                "  `introduction_zh` varchar(3000) null ,\n" +
                "  `quit_date_time` datetime null ,\n" +
                "  `nts_tag` bigint(20) null ,\n" +
                "  `fp_tag` bigint(20) null ,\n" +
                "  `apple_score` varchar(30) null ,\n" +
                "  `teaching_label` varchar(1000) null ,\n" +
                "  `course_type` varchar(2000) null ,\n" +
                "  `student_favorite` bigint(20) null ,\n" +
                "  `basic_date_time` datetime null ,\n" +
                "  `manager` bigint(20) null ,\n" +
                "  `referral_code` varchar(30) null ,\n" +
                "  `plat_from` varchar(60) null ,\n" +
                "  `last_online_class_id` bigint(20) null ,\n" +
                "  `last_online_class_time` datetime null ,\n" +
                "  `silent_days` bigint(20) null ,\n" +
                "  `fp_code_submit_time` datetime null ,\n" +
                "  `job_hrs_per_week` bigint(20) null ,\n" +
                "  `last_open_date` date null ,\n" +
                "  `skip_interview_tag` bigint(20) null ,\n" +
                "  `first_refer_time` datetime null ,\n" +
                "  `refer_times` bigint(20) null ,\n" +
                "  `refer_entry_num` bigint(20) null ,\n" +
                "  `speak_language` varchar(500) null ,\n" +
                "  `k12_teaching_experience` varchar(500) null ,\n" +
                "  `state_issued_license` bigint(20) null ,\n" +
                "  `other_esl_license` bigint(20) null ,\n" +
                "  `softmedium_class_cnt` bigint(20) null ,\n" +
                "  `token_cnt_all` bigint(20) null ,\n" +
                "  `token_cnt_no_softmedium` bigint(20) null ,\n" +
                "  `type` bigint(20) null \n" +
                ") engine=olap\n" +
                "duplicate key(`teacher_id`)\n" +
                "comment \" : cuilingfeng@vipkid.com.cn\"\n" +
                "distributed by hash(`teacher_id`) buckets 4\n" +
                "properties (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");");

        String query =
                "SELECT worksheet_2881_.`oc2scheduled_date_time` AS `oc2scheduled_date_time`, worksheet_2881_.`slot` AS `slot`, COUNT(DISTINCT worksheet_2881_.`1v2online_class_id`) AS `1v2_booked`\n" +
                        "  , COUNT(DISTINCT worksheet_2881_.`1v2tc`) AS `1v2_tc`, COUNT(DISTINCT worksheet_2881_.`1v2tns`) AS `1v2_tns`\n" +
                        "  , COUNT(DISTINCT worksheet_2881_.`1v2tit`) AS `1v2_tit`, COUNT(DISTINCT worksheet_2881_.`1v2sp`) AS `1v2_sys`\n" +
                        "  , COUNT(DISTINCT worksheet_2881_.`yc_online_class_id`) AS `yc_amount`, COUNT(DISTINCT worksheet_2881_.`yc_tc`) AS `yc_tc`\n" +
                        "  , COUNT(DISTINCT worksheet_2881_.`yc_tns`) AS `yc_tns`\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM (\n" +
                        "    SELECT *\n" +
                        "    FROM (\n" +
                        "      SELECT teacher_id AS oc2tid, substr(scheduled_date_time, 12, 8) AS slot\n" +
                        "        , to_date(scheduled_date_time) AS oc2scheduled_date_time\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 1 THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS 1v2online_class_id\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 1\n" +
                        "          AND finish_type IN ('teacher_cancellation', 'teacher_cancellation_24h', 'teacher_no_show_2h') THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS 1v2tc\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 1\n" +
                        "          AND finish_type = 'teacher_no_show' THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS 1v2tns\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 1\n" +
                        "          AND finish_type = 'teacher_it_problem' THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS 1v2tit\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 1\n" +
                        "          AND finish_type = 'system_problem' THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS 1v2sp\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 0 THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS yc_online_class_id\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 0\n" +
                        "          AND finish_type IN ('teacher_cancellation', 'teacher_cancellation_24h', 'teacher_no_show_2h') THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS yc_tc\n" +
                        "        , CASE \n" +
                        "          WHEN class_mode = 0\n" +
                        "          AND finish_type = 'teacher_no_show' THEN id\n" +
                        "          ELSE NULL\n" +
                        "        END AS yc_tns\n" +
                        "      FROM rl_vk_dwd_online_class\n" +
                        "      WHERE to_date(scheduled_date_time) >= '2021-01-01'\n" +
                        "        AND status NOT IN (\n" +
                        "          'available', \n" +
                        "          'expired', \n" +
                        "          'discard', \n" +
                        "          'obsolete', \n" +
                        "          'refused', \n" +
                        "          'require_cancel', \n" +
                        "          'removed', \n" +
                        "          'required', \n" +
                        "          'pre_book', \n" +
                        "          'pre_force_book', \n" +
                        "          'pre_require'\n" +
                        "        )\n" +
                        "        AND (finish_type <> 'student_no_show_24h'\n" +
                        "          OR finish_type IS NULL)\n" +
                        "        AND book_date_time IS NOT NULL\n" +
                        "        AND class_mode IN (0, 1)\n" +
                        "        AND course_id = '101153579'\n" +
                        "        AND ((((pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (1, 2, 3, 4, 5)\n" +
                        "                AND substr(scheduled_date_time, 12, 5) IN ('19:00', '20:00'))\n" +
                        "              OR (pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (6, 7)\n" +
                        "                AND substr(scheduled_date_time, 12, 5) IN ('19:00', '10:00', '18:00', '20:00')))\n" +
                        "            AND to_date(scheduled_date_time) < '2021-02-12')\n" +
                        "          OR (((pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (1, 2, 3, 4)\n" +
                        "                AND substr(scheduled_date_time, 12, 5) IN ('19:00', '20:00'))\n" +
                        "              OR (pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 = 5\n" +
                        "                AND substr(scheduled_date_time, 12, 5) IN ('18:30', '19:30', '20:30', '19:00', '20:00'))\n" +
                        "              OR (pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (6, 7)\n" +
                        "                AND substr(scheduled_date_time, 12, 5) IN (\n" +
                        "                  '18:30', \n" +
                        "                  '19:30', \n" +
                        "                  '20:30', \n" +
                        "                  '19:00', \n" +
                        "                  '10:00', \n" +
                        "                  '18:00', \n" +
                        "                  '20:00'\n" +
                        "                )))\n" +
                        "            AND to_date(scheduled_date_time) >= '2021-02-12'\n" +
                        "            AND to_date(scheduled_date_time) < '2021-03-01')\n" +
                        "          OR ((pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (1, 2, 3, 4)\n" +
                        "              AND substr(scheduled_date_time, 12, 5) IN (\n" +
                        "                '18:00', \n" +
                        "                '18:30', \n" +
                        "                '19:00', \n" +
                        "                '19:30', \n" +
                        "                '20:00', \n" +
                        "                '20:30'\n" +
                        "              ))\n" +
                        "            OR (pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 = 5\n" +
                        "              AND substr(scheduled_date_time, 12, 5) IN (\n" +
                        "                '18:00', \n" +
                        "                '18:30', \n" +
                        "                '19:30', \n" +
                        "                '20:30', \n" +
                        "                '19:00', \n" +
                        "                '20:00'\n" +
                        "              ))\n" +
                        "            OR (pmod(datediff(scheduled_date_time, '1900-01-08'), 7) + 1 IN (6, 7)\n" +
                        "              AND substr(scheduled_date_time, 12, 5) IN (\n" +
                        "                '18:30', \n" +
                        "                '19:30', \n" +
                        "                '20:30', \n" +
                        "                '19:00', \n" +
                        "                '10:00', \n" +
                        "                '18:00', \n" +
                        "                '20:00'\n" +
                        "              ))\n" +
                        "            AND to_date(scheduled_date_time) >= '2021-03-01'))\n" +
                        "    ) oc2\n" +
                        "      INNER JOIN (\n" +
                        "        SELECT teacher_id\n" +
                        "        FROM vk_dw_pty_teacher_info_da\n" +
                        "        WHERE life_cycle IN (\n" +
                        "          'regular', \n" +
                        "          'pending', \n" +
                        "          'suspend', \n" +
                        "          'toc_by_company', \n" +
                        "          'expired', \n" +
                        "          'quit'\n" +
                        "        )\n" +
                        "      ) t\n" +
                        "      ON t.teacher_id = oc2.oc2tid\n" +
                        "  ) temp\n" +
                        ") worksheet_2881_\n" +
                        "GROUP BY worksheet_2881_.`oc2scheduled_date_time`, worksheet_2881_.`slot`\n" +
                        "ORDER BY `oc2scheduled_date_time` ASC, `slot` ASC\n" +
                        "LIMIT 0, 10";

        Catalog catalog = connectContext.getCatalog();
        OlapTable smallerTable =
                (OlapTable) catalog.getDb("default_cluster:test").getTable("vk_dw_pty_teacher_info_da");
        setTableStatistics(smallerTable, 4656602);

        OlapTable biggerTable = (OlapTable) catalog.getDb("default_cluster:test").getTable("rl_vk_dwd_online_class");
        setTableStatistics(biggerTable, 333581032);

        String planFragment = getFragmentPlan(query);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (BROADCAST)"));
    }
}
