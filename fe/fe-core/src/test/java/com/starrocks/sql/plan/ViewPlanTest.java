// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ViewPlanTest extends PlanTestBase {
    private static final AtomicInteger INDEX = new AtomicInteger(0);

    private void testView(String sql) throws Exception {
        String viewName = "view" + INDEX.getAndIncrement();
        String createView = "create view " + viewName + " as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from " + viewName);
        Assert.assertEquals(sqlPlan, viewPlan);

        starRocksAssert.dropView(viewName);
    }

    private void testViewIgnoreObjectCountDistinct(String sql) throws Exception {
        String viewName = "view" + INDEX.getAndIncrement();
        String createView = "create view " + viewName + " as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from " + viewName);

        System.out.println(sqlPlan);
        System.out.println(viewPlan);

        sqlPlan = sqlPlan.replaceAll("bitmap_union_count\\(", "count");
        viewPlan = viewPlan.replaceAll("bitmap_union_count\\(", "count");
        sqlPlan = sqlPlan.replaceAll("bitmap_union_count", "count");
        viewPlan = viewPlan.replaceAll("bitmap_union_count", "count");
        sqlPlan = sqlPlan.replaceAll("hll_union_agg\\(", "count");
        viewPlan = viewPlan.replaceAll("hll_union_agg\\(", "count");
        sqlPlan = sqlPlan.replaceAll("hll_union_agg", "count");
        viewPlan = viewPlan.replaceAll("hll_union_agg", "count");
        Assert.assertEquals(sqlPlan, viewPlan);
    }

    @Test
    public void testSql0() throws Exception {
        String sql = "select [][1]";
        testView(sql);
    }

    @Test
    public void testSql1() throws Exception {
        String sql = "select [][1] + 1, [1,2,3][1] + [[1,2,3],[1,1,1]][2][2]";
        testView(sql);
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "select [1,2][1]";
        testView(sql);
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "select [[1,2],[3,4]][1][2]";
        testView(sql);
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "select 1 as b, MIN(v1) from t0 having (b + 1) != b;";
        testView(sql);
    }

    @Test
    public void testSql5() throws Exception {
        String sql = "SELECT 1 AS z, MIN(a.x) FROM (select 1 as x) a WHERE abs(1) = 2";
        testView(sql);
    }

    @Test
    public void testSql6() throws Exception {
        String sql = "select 1 from (select 1, 3 from t0 except select 2, 3 ) as a limit 3";
        testView(sql);
    }

    @Test
    public void testSql7() throws Exception {
        String sql = "select 1 from (select 1, 3 from t0 intersect select 2, 3 ) as a limit 3";
        testView(sql);
    }

    @Test
    public void testSql8() throws Exception {
        String sql = "select 1 from (select 4, 3 from t0 union all select 2, 3 ) as a limit 3";
        testView(sql);
    }

    @Test
    public void testSql9() throws Exception {
        String sql = "SELECT 1 FROM (SELECT COUNT(1) FROM t0 WHERE false) t;";
        testView(sql);
    }

    @Test
    public void testSql10() throws Exception {
        String sql = "select 499 union select 670 except select 499";
        testView(sql);
    }

    @Test
    public void testSql11() throws Exception {
        String sql = "SELECT 8 from t0 group by v1 having avg(v2) < 63;";
        testView(sql);
    }

    @Test
    public void testSql12() throws Exception {
        String sql = "select 'a', 'b' from t0 group by 'a', 'b'; ";
        testView(sql);
    }

    @Test
    public void testSql13() throws Exception {
        String sql = "select a.* from join1 as a join join1 as b ;";
        testView(sql);
    }

    @Test
    public void testSql14() throws Exception {
        String sql = "select a.* from join1 as a join (select sum(id) from join1 group by dt) as b ;";
        testView(sql);
    }

    @Test
    public void testSql15() throws Exception {
        String sql =
                "select a.id_datetime from test_all_type as a join test_all_type as b where a.id_date in (b.id_date)";
        testView(sql);
    }

    @Test
    public void testSql16() throws Exception {
        String sql = "select a.v1, a.v2 from (select v1, v2 from t0 limit 1) a ";
        testView(sql);
    }

    @Test
    public void testSql17() throws Exception {
        String sql = "select a.v1 from (select v1, v2 from t0 limit 10) a group by a.v1";
        testView(sql);
    }

    @Test
    public void testSql18() throws Exception {
        String sql = "select a.v2 from (select v1, v2 from t0 limit 10) a group by a.v2";
        testView(sql);
    }

    @Test
    public void testSql19() throws Exception {
        String sql = "select 'a', v2, sum(v1) from t0 group by 'a', v2; ";
        testView(sql);
    }

    @Test
    public void testSql20() throws Exception {
        String sql = "select AVG(DATEDIFF(curdate(),DATE_ADD(curdate(),interval -day(curdate())+1 day))) as a FROM t0";
        testView(sql);
    }

    @Test
    public void testSql21() throws Exception {
        String sql = "select avg(t1c), count(distinct id_decimal) from test_all_type;";
        testView(sql);
    }

    @Test
    public void testSql22() throws Exception {
        String sql = "select avg(v1), count(distinct v1) from t0 group by v1";
        testView(sql);
    }

    @Test
    public void testSql23() throws Exception {
        String sql = "select avg(v2) from t0 group by v2";
        testView(sql);
    }

    @Test
    public void testSql24() throws Exception {
        String sql = "select avg(x1) from (select avg(v1) as x1 from t0) as q";
        testView(sql);
    }

    @Test
    public void testSql25() throws Exception {
        String sql = "select BITOR(825279661, 1960775729) as a from test_all_type group by a";
        testView(sql);
    }

    @Test
    public void testSql26() throws Exception {
        String sql = "SELECT c2, count(*) FROM db1.tbl3 WHERE c1<10 GROUP BY c2;";
        testView(sql);
    }

    @Test
    public void testSql27() throws Exception {
        String sql = "SELECT c2, count(*) FROM db1.tbl5 GROUP BY c2;";
        testView(sql);
    }

    @Test
    public void testSql28() throws Exception {
        String sql = "SELECT c3, count(*) FROM db1.tbl4 GROUP BY c3;";
        testView(sql);
    }

    @Test
    public void testSql29() throws Exception {
        String sql =
                "select case '10000' when 10000 THEN 'TEST1' WHEN NULL THEN 'TEST2' WHEN 40000 THEN 'TEST4' END FROM t1;";
        testView(sql);
    }

    @Test
    public void testSql32() throws Exception {
        String sql = "select case 'a' when 1 then 'a' when 'a' then 'b' else 'other' end as col212;";
        testView(sql);
    }

    @Test
    public void testSql33() throws Exception {
        String sql = "select case 'a' when 1 then 'a' when 'b' then 'b' end as col22;";
        testView(sql);
    }

    @Test
    public void testSql34() throws Exception {
        String sql =
                "select case 'a' when 'b' then 'a' when substr(k7,2,1) " +
                        "then 2 when false then 3 else 0 end as col23 from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql35() throws Exception {
        String sql = "select case 'a' when null then 'a' else 'other' end as col421";
        testView(sql);
    }

    @Test
    public void testSql36() throws Exception {
        String sql =
                "select case 'a' when substr(k7,2,1) then 2 when 1 then 'a' " +
                        "when false then 3 else 0 end as col231 from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql38() throws Exception {
        String sql =
                "select case k1 when substr(k7,2,1) then 2 when 1 then 'a' " +
                        "when false then 3 else 0 end as col232 from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql41() throws Exception {
        String sql =
                "select case when case when substr(k7,2,1) then true else false end " +
                        "then 2 when false then 3 else 0 end as col from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql42() throws Exception {
        String sql =
                "select case when (case when true then true else false end) then 2 when false then 3 else 0 end as col";
        testView(sql);
    }

    @Test
    public void testSql43() throws Exception {
        String sql = "select case when false then 1 when true then 2 when false then 3 else 'other' end as col124";
        testView(sql);
    }

    @Test
    public void testSql44() throws Exception {
        String sql = "select case when false then 2 end as col3";
        testView(sql);
    }

    @Test
    public void testSql45() throws Exception {
        String sql = "select case when false then 2 when false then 3 else 4 end as col131";
        testView(sql);
    }

    @Test
    public void testSql46() throws Exception {
        String sql = "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql47() throws Exception {
        String sql = "select case when false then 2 when true then 3 else 0 end as col11;";
        testView(sql);
    }

    @Test
    public void testSql48() throws Exception {
        String sql = "select case when null then 1 else 2 end as col16;";
        testView(sql);
    }

    @Test
    public void testSql49() throws Exception {
        String sql = "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122 from test.baseall";
        testView(sql);
    }

    @Test
    public void testSql50() throws Exception {
        String sql = "select CAST(CAST(CAST(t1e AS DATE) AS BOOLEAN) AS BOOLEAN) from test_all_type;";
        testView(sql);
    }

    @Test
    public void testSql51() throws Exception {
        String sql = "select cast(v1 as decimal128(10,5)) * cast(v2 as decimal64(9,7)) from t0";
        testView(sql);
    }

    @Test
    public void testSql52() throws Exception {
        String sql = "select cast(v1 as decimal128(18,5)) / cast(v2 as decimal32(9,7)) from t0";
        testView(sql);
    }

    @Test
    public void testSql53() throws Exception {
        String sql = "select cast(v1 as decimal128(27,2)) - cast(v2 as decimal64(10,3)) from t0";
        testView(sql);
    }

    @Test
    public void testSql54() throws Exception {
        String sql = "select cast(v1 as decimal64(18,5)) % cast(v2 as decimal32(9,7)) from t0";
        testView(sql);
    }

    @Test
    public void testSql55() throws Exception {
        String sql = "select cast(v1 as decimal64(7,2)) + cast(v2 as decimal64(9,3)) from t0";
        testView(sql);
    }

    @Test
    public void testSql56() throws Exception {
        String sql = "select column_name from information_schema.columns limit 1;";
        testView(sql);
    }

    @Test
    public void testSql57() throws Exception {
        String sql = "select column_name, UPPER(DATA_TYPE) from information_schema.columns;";
        testView(sql);
    }

    @Test
    public void testSql58() throws Exception {
        String sql = "select connection_id()";
        testView(sql);
    }

    @Test
    public void testSql59() throws Exception {
        String sql = "select count(*) as count1 from test_all_type having count1 > 1";
        testView(sql);
    }

    @Test
    public void testSql60() throws Exception {
        String sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a";
        testView(sql);
    }

    @Test
    public void testSql61() throws Exception {
        String sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a group by a.v2";
        testView(sql);
    }

    @Test
    public void testSql63() throws Exception {
        String sql = "select count(distinct id2) from test.bitmap_table";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void testSql65() throws Exception {
        String sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void testSql67() throws Exception {
        String sql = "select count(distinct id2) from test.hll_table";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void testSql68() throws Exception {
        String sql = "select count(distinct id) from test.bitmap_table";
        testView(sql);
    }

    @Test
    public void testSql69() throws Exception {
        String sql = "select count(distinct k1) from baseall";
        testView(sql);
    }

    @Test
    public void testSql70() throws Exception {
        String sql = "select count(distinct k1) from baseall group by k3";
        testView(sql);
    }

    @Test
    public void testSql72() throws Exception {
        String sql = "select count(distinct k1, k2) from baseall group by k3";
        testView(sql);
    }

    @Test
    public void testSql73() throws Exception {
        String sql = "select count(distinct t1a,t1b), avg(t1c) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql74() throws Exception {
        String sql = "select count(distinct t1a,t1b) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql75() throws Exception {
        String sql = "select count(distinct t1b,t1c) as x1, count(distinct t1b,t1c) as x2 from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql76() throws Exception {
        String sql =
                "select count(distinct t1b,t1c) as x1, count(distinct t1b,t1c) as x2 from test_all_type group by t1d";
        testView(sql);
    }

    @Test
    public void testSql78() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql79() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type group by t1d";
        testView(sql);
    }

    @Test
    public void testSql80() throws Exception {
        String sql = "select count(distinct t1b,t1c,t1d) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql81() throws Exception {
        String sql = "select count(distinct t1b,t1c,t1d,t1e) from test_all_type group by t1f";
        testView(sql);
    }

    @Test
    public void testSql82() throws Exception {
        String sql = "select count(*) from (select v1 from t0 order by v2 limit 10,20) t;";
        testView(sql);
    }

    @Test
    public void testSql83() throws Exception {
        String sql = "select count(*) from t0 limit 0";
        testView(sql);
    }

    @Test
    public void testSql84() throws Exception {
        String sql = "select count(*) from test_all_type group by null";
        testView(sql);
    }

    @Test
    public void testSql85() throws Exception {
        String sql = "SELECT COUNT(*) FROM test_all_type WHERE CAST(CAST(t1e AS DATE) AS BOOLEAN);";
        testView(sql);
    }

    @Test
    public void testSql88() throws Exception {
        String sql =
                "select count(*) FROM  test.join1 WHERE  EXISTS (select max(id) from test.join2 where join2.id = join1.id)";
        testView(sql);
    }

    @Test
    public void testSql89() throws Exception {
        String sql = "select count(v1) from (select v1 from t0 order by v2 limit 10) t";
        testView(sql);
    }

    @Test
    public void testSql90() throws Exception {
        String sql = "select database();";
        testView(sql);
    }

    @Test
    public void testSql91() throws Exception {
        String sql = "select distinct k1 from (select distinct k1 from test.pushdown_test) t where k1 > 1";
        testView(sql);
    }

    @Test
    public void testSql97() throws Exception {
        String sql = "SELECT DISTINCT + + v1, v1 AS col2 FROM t0;";
        testView(sql);
    }

    @Test
    public void testSql99() throws Exception {
        String sql = "select distinct x1 from (select distinct v1 as x1 from t0) as q";
        testView(sql);
    }

    @Test
    public void testSql101() throws Exception {
        String sql = "select * from  baseall where (k1 > 1 and k2 < 1) or  (k1 > 1)";
        testView(sql);
    }

    @Test
    public void testSql102() throws Exception {
        String sql = "select * from  baseall where (k1 > 1) or (k1 > 1)";
        testView(sql);
    }

    @Test
    public void testSql103() throws Exception {
        String sql = "select * from baseall where (k1 > 1) or (k1 > 1 and k2 < 1)";
        testView(sql);
    }

    @Test
    public void testSql104() throws Exception {
        String sql = "select * from db1.tbl1 join [BROADCAST] db1.tbl2 on tbl1.k1 = tbl2.k3";
        testView(sql);
    }

    @Test
    public void testSql105() throws Exception {
        String sql = "select * from db1.tbl1 join [SHUFFLE] db1.tbl2 on tbl1.k1 = tbl2.k3";
        testView(sql);
    }

    @Test
    public void testSql107() throws Exception {
        String sql = "select * from information_schema.columns";
        testView(sql);
    }

    @Test
    public void testSql117() throws Exception {
        String sql = "select * from ods_order where order_dt = '2025-08-07' and order_no = 'p' limit 10;";
        testView(sql);
    }

    @Test
    public void testSql118() throws Exception {
        String sql = "select * from ods_order where pay_st = 214748364;";
        testView(sql);
    }

    @Test
    public void testSql121() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1";
        testView(sql);
    }

    @Test
    public void testSql122() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1000";
        testView(sql);
    }

    @Test
    public void testSql123() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 1) a order by a.v1 limit 10,1";
        testView(sql);
    }

    @Test
    public void testSql124() throws Exception {
        String sql = "select * from t0";
        testView(sql);
    }

    @Test
    public void testSql128() throws Exception {
        String sql = "select * from t0 as x0 inner join[shuffle] t1 as x1 on x0.v1 = x1.v4;";
        testView(sql);
    }

    @Test
    public void testSql130() throws Exception {
        String sql = "select * from t0 as x0 inner join t1 as x1 on x0.v1 = x1.v4;";
        testView(sql);
    }

    @Test
    public void testSql133() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 limit 10";
        testView(sql);
    }

    @Test
    public void testSql134() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 where abs(1) > 2;";
        testView(sql);
    }

    @Test
    public void testSql135() throws Exception {
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 limit 10";
        testView(sql);
    }

    @Test
    public void testSql136() throws Exception {
        String sql = "select * from t0 join t1 on t0.v1 = t1.v4 and cast(t0.v1 as STRING) = t0.v1";
        testView(sql);
    }

    @Test
    public void testSql137() throws Exception {
        String sql = "select * from t0 join t1 on t0.v2 = t1.v4 limit 2";
        testView(sql);
    }

    @Test
    public void testSql138() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 <=> t1.v4";
        testView(sql);
    }

    @Test
    public void testSql139() throws Exception {
        String sql = "SELECT * from t0 join test_all_type;";
        testView(sql);
    }

    @Test
    public void testSql140() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on NOT NULL >= NULL";
        testView(sql);
    }

    @Test
    public void testSql141() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        testView(sql);
    }

    @Test
    public void testSql142() throws Exception {
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        testView(sql);
    }

    @Test
    public void testSql143() throws Exception {
        String sql = "select * from t0 left anti join t1 on t0.v1 = t1.v4 limit 10";
        testView(sql);
    }

    @Test
    public void testSql144() throws Exception {
        String sql = "select * from t0 left join t1 on t0.v3 <=> t1.v4";
        testView(sql);
    }

    @Test
    public void testSql145() throws Exception {
        String sql = "SELECT * from t0 left join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 > 1;";
        testView(sql);
    }

    @Test
    public void testSql146() throws Exception {
        String sql = "select * from t0 left outer join t1 on t0.v1 = t1.v4 limit 10";
        testView(sql);
    }

    @Test
    public void testSql147() throws Exception {
        String sql = "select * from t0 left semi join t1 on v1 = v4 where v1 = 2";
        testView(sql);
    }

    @Test
    public void testSql148() throws Exception {
        String sql = "select * from t0 right semi join t1 on t0.v1 = t1.v4 limit 10";
        testView(sql);
    }

    @Test
    public void testSql149() throws Exception {
        String sql = "select * from t0, t1 limit 10";
        testView(sql);
    }

    @Test
    public void testSql150() throws Exception {
        String sql = "SELECT * FROM t0 where v1 = (select v1 from t0 limit 1);";
        testView(sql);
    }

    @Test
    public void testSql151() throws Exception {
        String sql = "select * from t1 inner join t3 on t1.v4 = t3.v10 right semi " +
                "join test_all_type as a on t3.v10 = a.t1a and 1 > 2;";
        testView(sql);
    }

    @Test
    public void testSql152() throws Exception {
        String sql = "select * from t1 union all select * from t2;";
        testView(sql);
    }

    @Test
    public void testSql153() throws Exception {
        String sql = "select * from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql154() throws Exception {
        String sql = "select * from test_all_type limit 20000";
        testView(sql);
    }

    @Test
    public void testSql155() throws Exception {
        String sql = "select * from test_all_type where id_date between '2020-12-12' and '2021-12-12'";
        testView(sql);
    }

    @Test
    public void testSql156() throws Exception {
        String sql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        testView(sql);
    }

    @Test
    public void testSql171() throws Exception {
        String sql =
                "select * from test.join1 left anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        testView(sql);
    }

    @Test
    public void testSql173() throws Exception {
        String sql =
                "select * from test.join1 left semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        testView(sql);
    }

    @Test
    public void testSql174() throws Exception {
        String sql =
                "select * from test.join1 right anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        testView(sql);
    }

    @Test
    public void testSql176() throws Exception {
        String sql =
                "select * from test.join1 right semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        testView(sql);
    }

    @Test
    public void testSql177() throws Exception {
        String sql = "select * from test.join1 where round(2.0, 0) > 3.0";
        testView(sql);
    }

    @Test
    public void testSql178() throws Exception {
        String sql =
                "select * from  test.join1 where ST_Contains(\"\", APPEND_TRAILING_CHAR_IF_ABSENT(-1338745708, \"RDBLIQK\") )";
        testView(sql);
    }

    @Test
    public void testSql182() throws Exception {
        String sql =
                "select id_datetime from test_all_type WHERE CAST(IF(true, 0.38542880072101215, '-Inf')  AS BOOLEAN )";
        testView(sql);
    }

    @Test
    public void testSql183() throws Exception {
        String sql = "select if(3, 4, 5);";
        testView(sql);
    }

    @Test
    public void testSql184() throws Exception {
        String sql = "select if(3, date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        testView(sql);
    }

    @Test
    public void testSql185() throws Exception {
        String sql = "select ifnull(1234, 'kks');";
        testView(sql);
    }

    @Test
    public void testSql186() throws Exception {
        String sql = "select ifnull(date('2021-01-12'), 123);";
        testView(sql);
    }

    @Test
    public void testSql187() throws Exception {
        String sql = "select ifnull(date('2021-01-12'), 'kks');";
        testView(sql);
    }

    @Test
    public void testSql189() throws Exception {
        String sql = "select join1.id from join1, join2 group by join1.id";
        testView(sql);
    }

    @Test
    public void testSql190() throws Exception {
        String sql = "select join2.id from baseall, join2 group by join2.id";
        testView(sql);
    }

    @Test
    public void testSql191() throws Exception {
        String sql = "select k10 = 20200812 from baseall;";
        testView(sql);
    }

    @Test
    public void testSql196() throws Exception {
        String sql = "select k2 from baseall group by ((10800861)/(((NULL)%(((-1114980787)+(-1182952114)))))), " +
                        "((10800861)*(-9223372036854775808)), k2";
        testView(sql);
    }

    @Test
    public void testSql197() throws Exception {
        String sql = "select k2, sum(k9) from baseall join join2 on k1 = id group by k2";
        testView(sql);
    }

    @Test
    public void testSql198() throws Exception {
        String sql = "select k2, sum(k9) from join2 join [broadcast] baseall on k1 = id group by k2";
        testView(sql);
    }

    @Test
    public void testSql199() throws Exception {
        String sql = "SELECT k3, avg(k3) OVER (partition by k3 order by k3) AS sum FROM baseall;";
        testView(sql);
    }

    @Test
    public void testSql201() throws Exception {
        String sql = "select lag(id_datetime, 1, '2020-01-01') over(partition by t1c) from test_all_type;";
        testView(sql);
    }

    @Test
    public void testSql203() throws Exception {
        String sql = "select lag(id_decimal, 1, 10000) over(partition by t1c) from test_all_type;";
        testView(sql);
    }

    @Test
    public void testSql205() throws Exception {
        String sql = "select lag(null, 1,1) OVER () from t0";
        testView(sql);
    }

    @Test
    public void testSql206() throws Exception {
        String sql = "select lag(v1, 1,1) OVER () from t0 limit 1";
        testView(sql);
    }

    @Test
    public void testSql210() throws Exception {
        String sql = "select max(order_dt) over (partition by order_no) from ods_order where order_no > 1";
        testView(sql);
    }

    @Test
    public void testSql211() throws Exception {
        String sql = "select max(v3) over (partition by v2,v2,v2 order by v2,v2) from t0;";
        testView(sql);
    }

    @Test
    public void testSql212() throws Exception {
        String sql = "select MIN(v1) from t0 having abs(1) = 2";
        testView(sql);
    }

    @Test
    public void testSql213() throws Exception {
        String sql = "select months_diff(\"2074-03-04T17:43:24\", \"2074-03-04T17:43:24\") from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql214() throws Exception {
        String sql = "select nullif(date('2021-01-12'), date('2021-01-11'));";
        testView(sql);
    }

    @Test
    public void testSql215() throws Exception {
        String sql = "select nullif(date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        testView(sql);
    }

    @Test
    public void testSql216() throws Exception {
        String sql = "select null+null as c3 from test.join2;";
        testView(sql);
    }

    @Test
    public void testSql217() throws Exception {
        String sql = "select S_COMMENT from supplier;";
        testView(sql);
    }

    @Test
    public void testSql219() throws Exception {
        String sql = "select sin(v1) + cos(v2) as a from t0";
        testView(sql);
    }

    @Test
    public void testSql220() throws Exception {
        String sql = "select stddev_pop(1222) from (select 1) t;";
        testView(sql);
    }

    @Test
    public void testSql221() throws Exception {
        String sql = "select str_to_date('11/09/2011', '%m/%d/%Y');";
        testView(sql);
    }

    @Test
    public void testSql222() throws Exception {
        String sql = "select sum(2), sum(distinct 2) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql223() throws Exception {
        String sql = "select sum(a) from (select v1 as a from t0 limit 0) t";
        testView(sql);
    }

    @Test
    public void testSql224() throws Exception {
        String sql = "select sum(a.v1) over() from (select v1, v2 from t0 limit 10) a";
        testView(sql);
    }

    @Test
    public void testSql225() throws Exception {
        String sql = "select sum(a.v1) over(partition by a.v2) from (select v1, v2 from t0 limit 10) a";
        testView(sql);
    }

    @Test
    public void testSql226() throws Exception {
        String sql = "select sum(a.v1) over(partition by a.v2 order by a.v1) from (select v1, v2 from t0 limit 10) a";
        testView(sql);
    }

    @Test
    public void testSql227() throws Exception {
        String sql = "select sum(distinct x1) from (select v2, sum(v2) as x1 from t0 group by v2) as q";
        testView(sql);
    }

    @Test
    public void testSql228() throws Exception {
        String sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void testSql229() throws Exception {
        String sql = "select sum(id) / count(distinct id2) from test.hll_table";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void testSql230() throws Exception {
        String sql = "select sum(id_decimal - ifnull(id_decimal, 0)) over (partition by t1c) from test_all_type";
        testView(sql);
    }

    @Test
    public void testSql231() throws Exception {
        String sql = "select SUM(S_NATIONKEY) from supplier;";
        testView(sql);
    }

    @Test
    public void testSql232() throws Exception {
        String sql = "select sum(v1) from t0 group by v2 having sum(v1) > 0";
        testView(sql);
    }

    @Test
    public void testSql233() throws Exception {
        String sql = "select sum(v1) from t0 group by v2 having v2 > 0";
        testView(sql);
    }

    @Test
    public void testSql234() throws Exception {
        String sql = "select sum(v1 + v2) from t0";
        testView(sql);
    }

    @Test
    public void testSql235() throws Exception {
        String sql = "select SUM(v2) from (select v2, sum(distinct v2) as x1 from t0 group by v2) as q";
        testView(sql);
    }

    @Test
    public void testSql236() throws Exception {
        String sql = "select SUM(v2) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        testView(sql);
    }

    @Test
    public void testSql237() throws Exception {
        String sql = "select sum(x1) from (select sum(v1) as x1 from t0) as q";
        testView(sql);
    }

    @Test
    public void testSql238() throws Exception {
        String sql = "select SUM(x1) from (select v2, sum(distinct v1), sum(v3) as x1 from t0 group by v2) as q";
        testView(sql);
    }

    @Test
    public void testSql239() throws Exception {
        String sql = "select SUM(x1) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        testView(sql);
    }

    @Test
    public void testSql240() throws Exception {
        String sql = "select t0.v1, case when true then t0.v1 else t0.v1 end from t0;";
        testView(sql);
    }

    @Test
    public void testSql241() throws Exception {
        String sql = "SELECT t0.v1 from t0 join test_all_type on t0.v1 = test_all_type.t1c";
        testView(sql);
    }

    @Test
    public void testSql242() throws Exception {
        String sql =
                "SELECT t0.v1 from t0 join test_all_type on t0.v2 = test_all_type.t1d where t0.v1 = test_all_type.t1d";
        testView(sql);
    }

    @Test
    public void testSql243() throws Exception {
        String sql = "SELECT t0.v1 from t0, test_all_type where t0.v1 = test_all_type.t1d";
        testView(sql);
    }

    @Test
    public void testSql244() throws Exception {
        String sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2";
        testView(sql);
    }

    @Test
    public void testSql245() throws Exception {
        String sql = "select t1.k2, sum(t1.k9) from baseall t1 " +
                "join baseall t2 on t1.k1 = t2.k1 where t1.k9 + t2.k9 = 1 group by t1.k2";
        testView(sql);
    }

    @Test
    public void testSql246() throws Exception {
        String sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k9 = t2.k9 group by t1.k2";
        testView(sql);
    }

    @Test
    public void testSql247() throws Exception {
        String sql = "select t1.k2, sum(t1.k9) from baseall t1 " +
                "join join2 t2 on t1.k1 = t2.id join baseall t3 on t1.k1 = t3.k1 group by t1.k2";
        testView(sql);
    }

    @Test
    public void testSql250() throws Exception {
        String sql = "select t1.v4 from t0 right semi join t1 on t0.v1 = t1.v4 and t0.v1 > 1 ";
        testView(sql);
    }

    @Test
    public void testSql251() throws Exception {
        String sql = "select t1.v5 from (select * from t0 limit 1) as x inner join t1 on x.v1 = t1.v4";
        testView(sql);
    }

    @Test
    public void testSql252() throws Exception {
        String sql = "select t1.v5 from t0 inner join[broadcast] t1 on cast(t0.v1 as int) = cast(t1.v4 as int)";
        testView(sql);
    }

    @Test
    public void testSql253() throws Exception {
        String sql =
                "select t2.k2, sum(t2.k9) from baseall t1 join [broadcast] baseall t2 on t1.k1 = t2.k1 group by t2.k2";
        testView(sql);
    }

    @Test
    public void testSql254() throws Exception {
        String sql = "select t3.k2, sum(t3.k9) from baseall t1 " +
                "join [broadcast] join2 t2 on t1.k1 = t2.id " +
                "join [broadcast] baseall t3 on t1.k1 = t3.k1 group by t3.k2";
        testView(sql);
    }

    @Test
    public void testSql255() throws Exception {
        String sql =
                "select t3.v10 from t3 inner join test_all_type on t3.v11 = test_all_type.id_decimal and t3.v11 > true";
        testView(sql);
    }

    @Test
    public void testSql256() throws Exception {
        String sql = "select v1+20, case v2 when v3 then 1 else 0 end from t0 where v1 is null";
        testView(sql);
    }

    @Test
    public void testSql257() throws Exception {
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        String sql = "select v1 as v2 from t0 group by v1, v2;";
        testView(sql);
        connectContext.getSessionVariable().setEnableGroupbyUseOutputAlias(false);
    }

    @Test
    public void testSql258() throws Exception {
        String sql = "select v1 from (select * from t0 limit 0) t";
        testView(sql);
    }

    @Test
    public void testSql259() throws Exception {
        String sql = "select v1 from t0";
        testView(sql);
    }

    @Test
    public void testSql260() throws Exception {
        String sql = "select v1 from t0 limit 1";
        testView(sql);
    }

    @Test
    public void testSql262() throws Exception {
        String sql = "select v1 from t0 where v1 in (v1 + v2, sin(v2))";
        testView(sql);
    }

    @Test
    public void testSql263() throws Exception {
        String sql = "select v1 from t0 where v2 > 1";
        testView(sql);
    }

    @Test
    public void testSql264() throws Exception {
        String sql = "select v1 from t0 where v2 < null group by v1 HAVING NULL IS NULL;";
        testView(sql);
    }

    @Test
    public void testSql266() throws Exception {
        String sql = "select v1, sum(v2) over (partition by v1, v2 order by v2 desc) as sum1 from t0";
        testView(sql);
    }

    @Test
    public void testSql267() throws Exception {
        String sql = "select v1, sum(v3[1]) from tarray group by v1";
        testView(sql);
    }

    @Test
    public void testSql268() throws Exception {
        String sql = "SELECT v1, sum(v3) as v from t0 where v2 = 0 group by v1 having sum(v3) > 0 limit 10";
        testView(sql);
    }

    @Test
    public void testSql269() throws Exception {
        String sql = "select v1, v2 from t0";
        testView(sql);
    }

    @Test
    public void testSql270() throws Exception {
        String sql = "select v1, v2 from t0 limit 1 ";
        testView(sql);
    }

    @Test
    public void testSql271() throws Exception {
        String sql = "select v1 + v2, v1 + v2 + v3, v1 + v2 + v3 + 1 from t0";
        testView(sql);
    }

    @Test
    public void testSql273() throws Exception {
        String sql = "select v2 from t0 group by v2 having v2 > 0";
        testView(sql);
    }

    @Test
    public void testSql274() throws Exception {
        String sql = "select v2, SUM(x1) from (select v2, v3, sum(v1) as x1 from t0 group by v2, v3) as q group by v2";
        testView(sql);
    }

    @Test
    public void testSql275() throws Exception {
        String sql = "select v3[1] from tarray";
        testView(sql);
    }

    @Test
    public void testSql276() throws Exception {
        String sql = "select count(distinct v1), count(distinct v2), count(distinct v3), count(distinct v4), " +
                "count(distinct b1), count(distinct b2), count(distinct b3), count(distinct b4) from test_object;";
        testViewIgnoreObjectCountDistinct(sql);
    }

    @Test
    public void test278() throws Exception {
        String sql = "select id_date + interval '3' month," +
                "id_date + interval '1' day," +
                "id_date + interval '2' year," +
                "id_date - interval '3' day from test_all_type";
        testView(sql);
    }

    @Test
    public void test280() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 " +
                " where (NOT (t0.v2 IS NOT NULL))";
        testView(sql);
    }

    @Test
    public void test284() throws Exception {
        String sql = "select t1.* from t0, t2, t3, t1 where t1.v4 = t2.v7 " +
                "and t1.v4 = t3.v10 and t3.v10 = t0.v1";
        testView(sql);
    }

    @Test
    public void test285() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        testView(sql);
    }

    @Test
    public void test286() throws Exception {
        String sql = "select sum(t1c) from (select t1c, lag(id_datetime, 1, '2020-01-01') over( partition by t1c)" +
                " from test_all_type) a ;";
        testView(sql);
    }

    @Test
    public void test287() throws Exception {
        String sql = "select b from (select t1a as a, t1b as b, t1c as c, t1d as d from test_all_type " +
                "union all select 1 as a, 2 as b, 3 as c, 4 as d) t1;";
        testView(sql);
    }

    @Test
    public void test293() throws Exception {
        String sql = "select * from (select v1, v2 from t0) a join [shuffle] " +
                "(select v4 from t1 limit 1) b on a.v1 = b.v4";
        testView(sql);
    }

    @Test
    public void test294() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v4 from t1 ) b on a.v1 = b.v4";
        testView(sql);
    }

    @Test
    public void test295() throws Exception {
        String sql = "select avg(null) over (order by ref_0.v1) as c2 "
                + "from t0 as ref_0 left join t1 as ref_1 on (ref_0.v1 = ref_1.v4 );";
        testView(sql);
    }

    @Test
    public void test296() throws Exception {
        String sql = "select * from (select v3, rank() over (partition by v1 order by v2) as j1 from t0) as x0 "
                + "join t1 on x0.v3 = t1.v4 order by x0.v3, t1.v4 limit 100;";
        testView(sql);
    }

    @Test
    public void test297() throws Exception {
        String sql = "select case when v1 then 2 else 2 end from (select v1, case when true then v1 else v1 end as c2"
                + " from t0 limit 1) as x where c2 > 2 limit 2;";
        testView(sql);
    }

    @Test
    public void test298() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order where order_dt = '2025-08-07' group by " +
                "order_dt,order_no order by order_no limit 10;";
        testView(sql);
    }

    @Test
    public void test299() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order join test_all_type on order_no = t1a where " +
                "order_dt = '2025-08-07' group by order_dt,order_no order by order_no limit 10;";
        testView(sql);
    }

    @Test
    public void test300() throws Exception {
        String sql = "SELECT ref_0.order_dt AS c0\n" +
                "  FROM ods_order ref_0\n" +
                "    LEFT JOIN ods_order ref_1 ON ref_0.order_dt = ref_1.order_dt\n" +
                "  WHERE ref_1.order_no IS NOT NULL;";
        testView(sql);
    }

    @Test
    public void test301() throws Exception {
        String sql = "select * from test_all_type where t1a = 123 AND t1b = 999999999 AND t1d = 999999999 "
                + "AND id_datetime = '2020-12-20 20:20:20' AND id_date = '2020-12-11' AND id_datetime = 'asdlfkja';";
        testView(sql);
    }

    @Test
    public void test302() throws Exception {
        String sql = "SELECT\n" +
                "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                "    k4\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT\n" +
                "        k1,\n" +
                "        k2,\n" +
                "        k3,\n" +
                "        SUM(k4) AS k4\n" +
                "    FROM  db1.tbl6\n" +
                "    WHERE k1 = 0\n" +
                "        AND k4 = 1\n" +
                "        AND k3 = 'foo'\n" +
                "    GROUP BY \n" +
                "    GROUPING SETS (\n" +
                "        (k1),\n" +
                "        (k1, k2),\n" +
                "        (k1, k3),\n" +
                "        (k1, k2, k3)\n" +
                "    )\n" +
                ") t\n" +
                "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        testView(sql);
    }

    @Test
    public void test303() throws Exception {
        String sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        testView(sql);
    }

    @Test
    public void test304() throws Exception {
        String sql = "select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id" +
                " and 1 > 2 group by join2.id" +
                " union select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id " +
                " and 1 > 2 WHERE (NOT (true)) group by join2.id ";
        testView(sql);
    }

    @Test
    public void test306() throws Exception {
        String sql = "SELECT *\n" +
                "FROM test.pushdown_test\n" +
                "WHERE 0 < (\n" +
                "    SELECT MAX(k9)\n" +
                "    FROM test.pushdown_test);";
        testView(sql);
    }

    @Test
    public void test308() throws Exception {
        String sql = "select\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "    c_acctbal,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_phone,\n" +
                "    c_comment\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem,\n" +
                "    nation\n" +
                "where\n" +
                "        c_custkey = o_custkey\n" +
                "  and l_orderkey = o_orderkey\n" +
                "  and o_orderdate >= date '1994-05-01'\n" +
                "  and o_orderdate < date '1994-08-01'\n" +
                "  and l_returnflag = 'R'\n" +
                "  and c_nationkey = n_nationkey\n" +
                "group by\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    c_acctbal,\n" +
                "    c_phone,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_comment\n" +
                "order by\n" +
                "    revenue desc limit 20;";
        testView(sql);
    }

    @Test
    public void test309() throws Exception {
        String sql = "SELECT COUNT(*)\n" +
                "FROM lineitem JOIN [shuffle] orders o1 ON l_orderkey = o1.o_orderkey\n" +
                "JOIN [shuffle] orders o2 ON l_orderkey = o2.o_orderkey";
        testView(sql);
    }

    @Test
    public void test310() throws Exception {
        String sql = "select count(1) from lineitem t1 join [shuffle] orders t2 on " +
                "t1.l_orderkey = t2.o_orderkey and t2.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t3 " +
                "on t1.l_orderkey = t3.o_orderkey and t3.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t4 on\n" +
                "t1.l_orderkey = t4.o_orderkey and t4.O_ORDERDATE = t1.L_SHIPDATE;";
        testView(sql);
    }

    @Test
    public void test311() throws Exception {
        String sql = "SELECT v1 FROM t0 WHERE NOT ((v2 > 93 AND v1 < 27) OR v1 >= 22);";
        testView(sql);
    }

    @Test
    public void test312() throws Exception {
        String sql = "SELECT * FROM (VALUES(1,2,3),(4,5,6),(7,8,9)) T";
        testView(sql);
    }

    @Test
    public void test313() throws Exception {
        String sql = "select * from t0,t1 inner join t2 on v4 = v7";
        testView(sql);
    }

    @Test
    public void test314() throws Exception {
        String sql = "select split('1,2,3', ',') from t1;";
        testView(sql);

        sql = "select v3 from tarray";
        testView(sql);

        sql = "select array_sum(v3) from tarray";
        testView(sql);

        sql = "select v1,unnest from tarray,unnest(v3)";
        testView(sql);

        sql = "select * from tarray,unnest(v3)";
        testView(sql);

        sql = "select * from tarray,unnest(v3) t";
        testView(sql);
    }

    @Test
    public void test315() throws Exception {
        String sql = "select * from t0 where case when true then (v1 is null ) in (true,false) else true end";
        testView(sql);
    }

    @Test
    public void testAliasView() throws Exception {
        String sql = "select * from (select a.A, count(*) as cnt from ( SELECT 1 AS A UNION ALL SELECT 2 UNION ALL " +
                "SELECT 2 UNION ALL SELECT 5 UNION ALL SELECT 3 UNION ALL SELECT 0 AS A ) a group by a.A order by a.A ) b;";
        String createView = "create view alias_view(col1, col2) as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from alias_view");
        Assert.assertEquals(sqlPlan, viewPlan);
        starRocksAssert.dropView("alias_view");
    }

    @Test
    public void testAliasView2() throws Exception {
        String sql = "select SUM(A.v1), SUM(A.v2) from t0 A inner join t1 B on A.v1 = B.v4";
        String createView = "create view alias_view(col1, col2) as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from alias_view");
        Assert.assertEquals(sqlPlan, viewPlan);
        starRocksAssert.dropView("alias_view");
    }

    @Test
    public void testAliasView3() throws Exception {
        String createView = "create view alias_view(a, b) as select v1,v2 from test.t0";
        starRocksAssert.withView(createView);

        String plan = getFragmentPlan("select test.t.a from test.alias_view t");
        assertContains(plan, "OUTPUT EXPRS:1: v1");
        starRocksAssert.dropView("alias_view");
    }

    @Test
    public void testExpressionRewriteView() throws Exception {
        String sql =
                "select from_unixtime(unix_timestamp(id_datetime, 'yyyy-MM-dd'), 'yyyy-MM-dd') as x1, sum(t1c) as x3 " +
                        "from test_all_type " +
                        "group by from_unixtime(unix_timestamp(id_datetime, 'yyyy-MM-dd'), 'yyyy-MM-dd')";
        testView(sql);
    }

    @Test
    public void testUnionView() throws Exception {
        String sql = "select count(c_1) c1 ,cast(c_1 as string) c2 from (" +
                "select 1 c_1 union all select 2) a group by cast(c_1 as string)  union all  " +
                "select count(c_1) c1 ,cast(c_1 as string) c2 from (select 1 c_1 union all select 2) a " +
                "group by cast(c_1 as string);";
        String createView = "create view test_view15 (col_1,col_2) as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from test_view15");
        Assert.assertEquals(sqlPlan, viewPlan);
        starRocksAssert.dropView("test_view15");
    }

    @Test
    public void testGroupByView() throws Exception {
        String sql = "select case  when c1=1 then 1 end from " +
                "(select '1' c1  union  all select '2') a group by case  when c1=1 then 1 end;";
        testView(sql);

        sql = "select case  when c1=1 then 1 end from " +
                "(select '1' c1  union  all select '2') a group by rollup(case  when c1=1 then 1 end, a.c1);";
        testView(sql);

        sql = "select case when c1=1 then 1 end from " +
                "(select '1' c1  union  all select '2') a " +
                "group by grouping sets((case when c1=1 then 1 end, c1), (case  when c1=1 then 1 end));";

        testView(sql);
    }

    @Test
    public void testGroupByView2() throws Exception {
        String sql = "select case  when c1=1 then 1 end from " +
                "(select '1' c1  union  all select '2') a group by cube(case when c1=1 then 1 end, a.c1);";
        testView(sql);
    }

    @Test
    public void testUnionWith() throws Exception {
        String sql = "with a as (select 1 c1 union all select 2 ), b as (select 1 c1 union all select 2 ) " +
                "select * from a union all select * from b;";

        String createView = "create view test_view16 (c_1) as with a as (select 1 c1 union all select 2 ), " +
                "b as (select 1 c1 union all select 2 ) select * from a union all select * from b;";
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from test_view16;");
        Assert.assertEquals(sqlPlan, viewPlan);

        starRocksAssert.dropView("test_view16");
    }

    @Test
    public void testWith() throws Exception {
        String sql = "With x0 AS (SELECT DISTINCT v1, v2 FROM t0)\n" +
                " SELECT v1, v2 from x0";
        testView(sql);
    }

    @Test
    public void testWithMore() throws Exception {
        String sql = "With x0 AS (SELECT DISTINCT v1, v2 FROM t0)\n" +
                " SELECT v1, v2 from x0";
        testView(sql);
    }

    @Test
    public void testEscapeString() throws Exception {
        String sql = "select concat('123123', 'abc', '\\\\zx')";
        testView(sql);

        sql = "select replace('123123', 'abc', '\\\\\\\\zx')";
        testView(sql);

        sql = "select replace('123123', 'abc', '\\\\\\\\zx')";
        testView(sql);
    }

    @Test
    public void testAlter() throws Exception {
        String sql = "select v1 as c1, sum(v2) as c2 from t0 group by v1";
        String viewName = "view" + INDEX.getAndIncrement();
        String createView = "create view " + viewName + " as " + sql;
        starRocksAssert.withView(createView);

        String sqlPlan = getFragmentPlan(sql);
        String viewPlan = getFragmentPlan("select * from " + viewName);
        Assert.assertEquals(sqlPlan, viewPlan);

        String alterStmt = "with testTbl_cte (w1, w2) as (select v1, v2 from t0) " +
                "select w1 as c1, sum(w2) as c2 from testTbl_cte where w1 > 10 group by w1";
        String alterView = "alter view " + viewName + " as " + alterStmt;

        AlterViewStmt alterViewStmt =
                (AlterViewStmt) UtFrameUtils.parseStmtWithNewParser(alterView, starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().alterView(alterViewStmt);

        sqlPlan = getFragmentPlan(alterStmt);
        viewPlan = getFragmentPlan("select * from " + viewName);
        Assert.assertEquals(sqlPlan, viewPlan);
    }


    @Test
    public void testLateralJoin() throws Exception {
        starRocksAssert.withTable("CREATE TABLE json_test (" +
                " v_id INT," +
                " v_json json, " +
                " v_SMALLINT SMALLINT" +
                ") DUPLICATE KEY (v_id) " +
                "DISTRIBUTED BY HASH (v_id) " +
                "properties(\"replication_num\"=\"1\") ;"
        );
        String sql = "    SELECT\n" +
                "         v_id\n" +
                "        , get_json_string(ie.value,'$.b') as b\n" +
                "        ,get_json_string(ie.value,'$.c') as c\n" +
                "    FROM\n" +
                "      json_test ge\n" +
                "      ,lateral json_each(cast (ge.v_json as json) -> '$.')ie";

        testView(sql);

        sql = "    SELECT\n" +
                "         v_id\n" +
                "        , get_json_string(ie.value,'$.b') as b\n" +
                "        ,get_json_string(ie.value,'$.c') as c\n" +
                "    FROM\n" +
                "      json_test ge\n" +
                "      ,lateral json_each(cast (ge.v_json as json) -> '$.') ie(`key`, `value`)";
        testView(sql);
    }

    @Test
    public void testBackquoteAlias() throws Exception {
        String sql = "select `select` from (select v1 `select` from t0) `abc.bcd`;";
        testView(sql);
    }
}