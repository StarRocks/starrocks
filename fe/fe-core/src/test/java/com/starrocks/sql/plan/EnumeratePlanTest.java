// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnumeratePlanTest extends DistributedEnvPlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        connectContext.getSessionVariable().setCboPruneShuffleColumnRate(0);
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setUseNthExecPlan(0);
    }

    @Test
    public void testThreeTableJoinEnumPlan() throws Exception {
        runFileUnitTest("enumerate-plan/three-join");
    }

    @Test
    public void testTPCHQ1EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q1");
    }

    @Test
    public void testTPCHQ2EnumPlan() throws Exception {
        String sql = "select\n" +
                "    s_acctbal,\n" +
                "    s_name,\n" +
                "    n_name,\n" +
                "    p_partkey,\n" +
                "    p_mfgr,\n" +
                "    s_address,\n" +
                "    s_phone,\n" +
                "    s_comment\n" +
                "from\n" +
                "    part,\n" +
                "    supplier,\n" +
                "    partsupp,\n" +
                "    nation,\n" +
                "    region\n" +
                "where\n" +
                "        p_partkey = ps_partkey\n" +
                "  and s_suppkey = ps_suppkey\n" +
                "  and p_size = 12\n" +
                "  and p_type like '%COPPER'\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_regionkey = r_regionkey\n" +
                "  and r_name = 'AMERICA'\n" +
                "  and ps_supplycost = (\n" +
                "    select\n" +
                "        min(ps_supplycost)\n" +
                "    from\n" +
                "        partsupp,\n" +
                "        supplier,\n" +
                "        nation,\n" +
                "        region\n" +
                "    where\n" +
                "            p_partkey = ps_partkey\n" +
                "      and s_suppkey = ps_suppkey\n" +
                "      and s_nationkey = n_nationkey\n" +
                "      and n_regionkey = r_regionkey\n" +
                "      and r_name = 'AMERICA'\n" +
                ")\n" +
                "order by\n" +
                "    s_acctbal desc,\n" +
                "    n_name,\n" +
                "    s_name,\n" +
                "    p_partkey limit 100;\n" +
                "\n";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(45, planCount);
    }

    @Test
    public void testTPCHQ3EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q3");
    }

    @Test
    public void testTPCHQ4EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q4");
    }

    @Test
    public void testTPCHQ5EnumPlan() throws Exception {
        String sql = "select\n" +
                "    n_name,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem,\n" +
                "    supplier,\n" +
                "    nation,\n" +
                "    region\n" +
                "where\n" +
                "        c_custkey = o_custkey\n" +
                "  and l_orderkey = o_orderkey\n" +
                "  and l_suppkey = s_suppkey\n" +
                "  and c_nationkey = s_nationkey\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_regionkey = r_regionkey\n" +
                "  and r_name = 'AFRICA'\n" +
                "  and o_orderdate >= date '1995-01-01'\n" +
                "  and o_orderdate < date '1996-01-01'\n" +
                "group by\n" +
                "    n_name\n" +
                "order by\n" +
                "    revenue desc ;";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(77, planCount);
    }

    @Test
    public void testTPCHQ6EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q6");
    }

    @Test
    public void testTPCHQ7EnumPlan() throws Exception {
        String sql = "select\n" +
                "    o_year,\n" +
                "    sum(case\n" +
                "            when nation = 'IRAN' then volume\n" +
                "            else 0\n" +
                "        end) / sum(volume) as mkt_share\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            extract(year from o_orderdate) as o_year,\n" +
                "            l_extendedprice * (1 - l_discount) as volume,\n" +
                "            n2.n_name as nation\n" +
                "        from\n" +
                "            part,\n" +
                "            supplier,\n" +
                "            lineitem,\n" +
                "            orders,\n" +
                "            customer,\n" +
                "            nation n1,\n" +
                "            nation n2,\n" +
                "            region\n" +
                "        where\n" +
                "                p_partkey = l_partkey\n" +
                "          and s_suppkey = l_suppkey\n" +
                "          and l_orderkey = o_orderkey\n" +
                "          and o_custkey = c_custkey\n" +
                "          and c_nationkey = n1.n_nationkey\n" +
                "          and n1.n_regionkey = r_regionkey\n" +
                "          and r_name = 'MIDDLE EAST'\n" +
                "          and s_nationkey = n2.n_nationkey\n" +
                "          and o_orderdate between date '1995-01-01' and date '1996-12-31'\n" +
                "          and p_type = 'ECONOMY ANODIZED STEEL'\n" +
                "    ) as all_nations\n" +
                "group by\n" +
                "    o_year\n" +
                "order by\n" +
                "    o_year ;";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(27, planCount);
    }

    @Test
    public void testTPCHQ8EnumPlan() throws Exception {
        String sql = "select\n" +
                "    o_year,\n" +
                "    sum(case\n" +
                "            when nation = 'IRAN' then volume\n" +
                "            else 0\n" +
                "        end) / sum(volume) as mkt_share\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            extract(year from o_orderdate) as o_year,\n" +
                "            l_extendedprice * (1 - l_discount) as volume,\n" +
                "            n2.n_name as nation\n" +
                "        from\n" +
                "            part,\n" +
                "            supplier,\n" +
                "            lineitem,\n" +
                "            orders,\n" +
                "            customer,\n" +
                "            nation n1,\n" +
                "            nation n2,\n" +
                "            region\n" +
                "        where\n" +
                "                p_partkey = l_partkey\n" +
                "          and s_suppkey = l_suppkey\n" +
                "          and l_orderkey = o_orderkey\n" +
                "          and o_custkey = c_custkey\n" +
                "          and c_nationkey = n1.n_nationkey\n" +
                "          and n1.n_regionkey = r_regionkey\n" +
                "          and r_name = 'MIDDLE EAST'\n" +
                "          and s_nationkey = n2.n_nationkey\n" +
                "          and o_orderdate between date '1995-01-01' and date '1996-12-31'\n" +
                "          and p_type = 'ECONOMY ANODIZED STEEL'\n" +
                "    ) as all_nations\n" +
                "group by\n" +
                "    o_year\n" +
                "order by\n" +
                "    o_year ;";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(27, planCount);
    }

    @Test
    public void testTPCHQ9EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q9");
    }

    @Test
    public void testTPCHQ10EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q10");
    }

    @Test
    public void testTPCHQ11EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q11");
    }

    @Test
    public void testTPCHQ12EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q12");
    }

    @Test
    public void testTPCHQ13EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q13");
    }

    @Test
    public void testTPCHQ14EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q14");
    }

    @Test
    public void testTPCHQ15EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q15");
    }

    @Test
    public void testTPCHQ16EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q16");
    }

    @Test
    public void testTPCHQ17EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q17");
    }

    @Test
    public void testTPCHQ18EnumPlan() throws Exception {
        String sql = "select\n" +
                "    c_name,\n" +
                "    c_custkey,\n" +
                "    o_orderkey,\n" +
                "    o_orderdate,\n" +
                "    o_totalprice,\n" +
                "    sum(l_quantity)\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem\n" +
                "where\n" +
                "        o_orderkey in (\n" +
                "        select\n" +
                "            l_orderkey\n" +
                "        from\n" +
                "            lineitem\n" +
                "        group by\n" +
                "            l_orderkey having\n" +
                "                sum(l_quantity) > 315\n" +
                "    )\n" +
                "  and c_custkey = o_custkey\n" +
                "  and o_orderkey = l_orderkey\n" +
                "group by\n" +
                "    c_name,\n" +
                "    c_custkey,\n" +
                "    o_orderkey,\n" +
                "    o_orderdate,\n" +
                "    o_totalprice\n" +
                "order by\n" +
                "    o_totalprice desc,\n" +
                "    o_orderdate limit 100;";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(10, planCount);
    }

    @Test
    public void testTPCHQ19EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q19");
    }

    @Test
    public void testTPCHQ20EnumPlan() throws Exception {
        String sql = "select\n" +
                "    s_name,\n" +
                "    s_address\n" +
                "from\n" +
                "    supplier,\n" +
                "    nation\n" +
                "where\n" +
                "        s_suppkey in (\n" +
                "        select\n" +
                "            ps_suppkey\n" +
                "        from\n" +
                "            partsupp\n" +
                "        where\n" +
                "                ps_partkey in (\n" +
                "                select\n" +
                "                    p_partkey\n" +
                "                from\n" +
                "                    part\n" +
                "                where\n" +
                "                        p_name like 'sienna%'\n" +
                "            )\n" +
                "          and ps_availqty > (\n" +
                "            select\n" +
                "                    0.5 * sum(l_quantity)\n" +
                "            from\n" +
                "                lineitem\n" +
                "            where\n" +
                "                    l_partkey = ps_partkey\n" +
                "              and l_suppkey = ps_suppkey\n" +
                "              and l_shipdate >= date '1993-01-01'\n" +
                "              and l_shipdate < date '1994-01-01'\n" +
                "        )\n" +
                "    )\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_name = 'ARGENTINA'\n" +
                "order by\n" +
                "    s_name ;";
        int planCount = getPlanCount(sql);
        Assert.assertEquals(12, planCount);
    }

    @Test
    public void testTPCHQ21EnumPlan() throws Exception {
        String sql = "select\n" +
                "    s_name,\n" +
                "    count(*) as numwait\n" +
                "from\n" +
                "    supplier,\n" +
                "    lineitem l1,\n" +
                "    orders,\n" +
                "    nation\n" +
                "where\n" +
                "        s_suppkey = l1.l_suppkey\n" +
                "  and o_orderkey = l1.l_orderkey\n" +
                "  and o_orderstatus = 'F'\n" +
                "  and l1.l_receiptdate > l1.l_commitdate\n" +
                "  and exists (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            lineitem l2\n" +
                "        where\n" +
                "                l2.l_orderkey = l1.l_orderkey\n" +
                "          and l2.l_suppkey <> l1.l_suppkey\n" +
                "    )\n" +
                "  and not exists (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            lineitem l3\n" +
                "        where\n" +
                "                l3.l_orderkey = l1.l_orderkey\n" +
                "          and l3.l_suppkey <> l1.l_suppkey\n" +
                "          and l3.l_receiptdate > l3.l_commitdate\n" +
                "    )\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_name = 'CANADA'\n" +
                "group by\n" +
                "    s_name\n" +
                "order by\n" +
                "    numwait desc,\n" +
                "    s_name limit 100;";
        connectContext.getSessionVariable().setJoinImplementationMode("hash");
        int planCount = getPlanCount(sql);
        Assert.assertEquals("planCount is " + planCount, 21, planCount);
        connectContext.getSessionVariable().setJoinImplementationMode("auto");
    }

    @Test
    public void testTPCHQ22EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q22");
    }
}
