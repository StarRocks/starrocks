// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExternalTableTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create external table test.jdbc_key_words_test\n" +
                "(a int, `schema` varchar(20))\n" +
                "ENGINE=jdbc\n" +
                "PROPERTIES (\n" +
                "\"resource\"=\"jdbc_test\",\n" +
                "\"table\"=\"test_table\"\n" +
                ");");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testKeyWordWhereCaluse() throws Exception {
        String sql = "select * from test.jdbc_key_words_test where `schema` = \"test\"";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "`schema` = 'test'");
    }

    @Test
    public void testMysqlTableFilter() throws Exception {
        String sql = "select * from ods_order where order_dt = '2025-08-07' and order_no = 'p' limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, `up_trade_no`, " +
                "`mchnt_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07') AND (order_no = 'p')\n" +
                "     limit: 10"));

        sql = "select * from ods_order where order_dt = '2025-08-07' and length(order_no) > 10 limit 10;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:SELECT\n" +
                        "  |  predicates: length(order_no) > 10\n" +
                        "  |  limit: 10\n" +
                        "  |  \n" +
                        "  0:SCAN MYSQL\n" +
                        "     TABLE: `ods_order`\n" +
                        "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));

        sql = "select * from ods_order where order_dt = '2025-08-08' or length(order_no) > 10 limit 10;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:SELECT\n" +
                        "  |  predicates: (order_dt = '2025-08-08') OR (length(order_no) > 10)\n" +
                        "  |  limit: 10\n" +
                        "  |  \n" +
                        "  0:SCAN MYSQL\n" +
                        "     TABLE: `ods_order`\n" +
                        "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order`"));

        sql = "select * from ods_order where order_dt = '2025-08-07' and (length(order_no) > 10 or order_no = 'p');";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:SELECT\n" +
                        "  |  predicates: (length(order_no) > 10) OR (order_no = 'p')\n" +
                        "  |  \n" +
                        "  0:SCAN MYSQL\n" +
                        "     TABLE: `ods_order`\n" +
                        "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));

        sql = "select * from ods_order where not (order_dt = '2025-08-07' and length(order_no) > 10)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:SELECT\n" +
                        "  |  predicates: (order_dt != '2025-08-07') OR (length(order_no) <= 10)\n" +
                        "  |  \n" +
                        "  0:SCAN MYSQL\n" +
                        "     TABLE: `ods_order`\n" +
                        "    " +
                        " Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order`"));

        sql = "select * from ods_order where order_dt in ('2025-08-08','2025-08-08') " +
                "or order_dt between '2025-08-01' and '2025-09-05';";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  0:SCAN MYSQL\n" +
                        "     TABLE: `ods_order`\n" +
                        "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` " +
                        "WHERE ((order_dt IN ('2025-08-08', '2025-08-08')) " +
                        "OR ((order_dt >= '2025-08-01') AND (order_dt <= '2025-09-05')))"));

        sql =
                "select * from ods_order where (order_dt = '2025-08-07' and length(order_no) > 10) and org_order_no = 'p';";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:SELECT\n" +
                "  |  predicates: length(order_no) > 10\n" +
                "  |  \n" +
                "  0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` " +
                "WHERE (order_dt = '2025-08-07') AND (org_order_no = 'p')"));

    }

    @Test
    public void testMysqlTableAggregateSort() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order where order_dt = '2025-08-07' group by " +
                "order_dt,order_no order by order_no limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:TOP-N\n" +
                "  |  order by: <slot 2> 2: order_no ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(pay_st)\n" +
                "  |  group by: order_dt, order_no\n" +
                "  |  \n" +
                "  0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));
    }

    @Test
    public void testMysqlTableJoin() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order join test_all_type on order_no = t1a where " +
                "order_dt = '2025-08-07' group by order_dt,order_no order by order_no limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: order_no = 8: t1a\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));
    }

    @Test
    public void testMysqlPredicateWithoutCast() throws Exception {
        String sql = "select * from ods_order where pay_st = 214748364;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, " +
                        "`up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` WHERE (pay_st = 214748364)"));
    }

    @Test
    public void testJDBCTableFilter() throws Exception {
        String sql = "select * from test.jdbc_test where a > 10 and b < 'abc' limit 10";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:SCAN JDBC\n" +
                "     TABLE: `test_table`\n" +
                "     QUERY: SELECT `a`, `b`, `c` FROM `test_table` WHERE (`a` > 10) AND (`b` < 'abc')\n" +
                "     limit: 10"));
        sql = "select * from test.jdbc_test where a > 10 and length(b) < 20 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:SELECT\n" +
                        "  |  predicates: length(b) < 20\n" +
                        "  |  limit: 10\n" +
                        "  |  \n" +
                        "  0:SCAN JDBC\n" +
                        "     TABLE: `test_table`\n" +
                        "     QUERY: SELECT `a`, `b`, `c` FROM `test_table` WHERE (`a` > 10)"));

    }

    @Test
    public void testJDBCTableAggregation() throws Exception {
        String sql = "select b, sum(a) from test.jdbc_test group by b";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: sum(a)\n" +
                        "  |  group by: b\n" +
                        "  |  \n" +
                        "  0:SCAN JDBC\n" +
                        "     TABLE: `test_table`\n" +
                        "     QUERY: SELECT `a`, `b` FROM `test_table`"));
    }

    @Test
    public void testMysqlTableWithPredicate() throws Exception {
        String sql = "select max(order_dt) over (partition by order_no) from ods_order where order_no > 1";
        String plan = getThriftPlan(sql);
        Assert.assertFalse(plan.contains("use_vectorized:false"));
    }

    @Test
    public void testMysqlJoinSelf() throws Exception {
        String sql = "SELECT ref_0.order_dt AS c0\n" +
                "  FROM ods_order ref_0\n" +
                "    LEFT JOIN ods_order ref_1 ON ref_0.order_dt = ref_1.order_dt\n" +
                "  WHERE ref_1.order_no IS NOT NULL;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)"));
    }

    @Test
    public void testJoinWithMysqlTable() throws Exception {
        // set data size and row count for the olap table
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        replica.updateRowCount(2, 200000, 10000);
                    }
                }
            }
        }

        String queryStr = "select * from mysql_table t2, jointest t1 where t1.k1 = t2.k1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BUCKET_SHUFFLE)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));

        queryStr = "select * from jointest t1, mysql_table t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BUCKET_SHUFFLE)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));

        queryStr = "select * from jointest t1, mysql_table t2, mysql_table t3 where t1.k1 = t3.k1";
        explainString = getFragmentPlan(queryStr);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("INNER JOIN (BUCKET_SHUFFLE))"));
        Assert.assertTrue(explainString.contains("4:SCAN MYSQL"));
    }

}
