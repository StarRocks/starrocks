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

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import kotlin.text.Charsets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;
import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.starRocksAssert;

public class LosslessLogicalViewTest {
    private static ConnectContext ctx;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        ctx.getSessionVariable().setEnableQueryCache(true);
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("lossless_view_db").useDatabase("lossless_view_db");
        getSsbCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });



        String view1 = "create view lineorder_flat_v1 as\n" +
                "select\n" +
                "   `lo_orderdate`,\n" +
                "   `lo_orderkey`,\n" +
                "   `lo_linenumber`,\n" +
                "   `lo_custkey`,\n" +
                "   `lo_partkey`,\n" +
                "   `lo_suppkey`,\n" +
                "   `lo_orderpriority`,\n" +
                "   `lo_shippriority`,\n" +
                "   `lo_quantity`,\n" +
                "   `lo_extendedprice`,\n" +
                "   `lo_ordtotalprice`,\n" +
                "   `lo_discount`,\n" +
                "   `lo_revenue`,\n" +
                "   `lo_supplycost`,\n" +
                "   `lo_tax`,\n" +
                "   `lo_commitdate`,\n" +
                "   `lo_shipmode`,\n" +
                "   `c_name`,\n" +
                "   `c_address`,\n" +
                "   `c_city`,\n" +
                "   `c_nation`,\n" +
                "   `c_region`,\n" +
                "   `c_phone`,\n" +
                "   `c_mktsegment`,\n" +
                "   `s_name`,\n" +
                "   `s_address`,\n" +
                "   `s_city`,\n" +
                "   `s_nation`,\n" +
                "   `s_region`,\n" +
                "   `s_phone`,\n" +
                "   `p_name`,\n" +
                "   `p_mfgr`,\n" +
                "   `p_category`,\n" +
                "   `p_brand`,\n" +
                "   `p_color`,\n" +
                "   `p_type`,\n" +
                "   `p_size`,\n" +
                "   `p_container`\n" +
                "from\n" +
                "   lineorder l\n" +
                "   inner join customer [droppable] c on (c.c_custkey = l.lo_custkey)\n" +
                "   inner join supplier [droppable] s  on (s.s_suppkey = l.lo_suppkey)\n" +
                "   inner join part [droppable] p on (p.p_partkey = l.lo_partkey);";
        starRocksAssert.withView(view1);


        String createDeptsSql = "CREATE TABLE `depts` (\n" +
                "  `deptno` int(11) NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(25) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`deptno`, `name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"unique_constraints\" = \"deptno\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")";

        String createEmpsSql = "CREATE TABLE `emps` (\n" +
                "  `empid` int(11) NOT NULL COMMENT \"\",\n" +
                "  `deptno` int(11) NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(25) NOT NULL COMMENT \"\",\n" +
                "  `salary` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`, `deptno`, `name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"foreign_key_constraints\" = \"(deptno) REFERENCES depts(deptno)\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")";

        String createView1 = "create view emps_flat_view as \n" +
                "select \n" +
                "\tempid, \n" +
                "\temps.deptno as deptno, \n" +
                "\tdepts.deptno as deptno2, \n" +
                "\temps.name as name, \n" +
                "\tsalary, \n" +
                "\tdepts.name as dept_name\n" +
                "from emps inner join depts on emps.deptno = depts.deptno";

        starRocksAssert.withTable(createDeptsSql);
        starRocksAssert.withTable(createEmpsSql);
        starRocksAssert.withView(createView1);
    }

    public static List<String> getSsbCreateTableSqlList() {
        List<String> ssbTableNames = Lists.newArrayList("customer", "dates", "supplier", "part", "lineorder");
        ClassLoader loader = LosslessLogicalViewTest.class.getClassLoader();
        List<String> createTableSqlList = ssbTableNames.stream().map(n -> {
            try {
                return CharStreams.toString(
                        new InputStreamReader(
                                Objects.requireNonNull(loader.getResourceAsStream("sql/ssb_pk_fk/" + n + ".sql")),
                                Charsets.UTF_8));
            } catch (Throwable e) {
                return null;
            }
        }).collect(Collectors.toList());
        Assert.assertFalse(createTableSqlList.contains(null));
        return createTableSqlList;
    }

    @Test
    public void showCreateViewWithDroppableHint() throws Exception {
        System.out.println("OK");
        List<List<String>> result = starRocksAssert.show("show create view lossless_view_db.lineorder_flat_v1");
        for (List<String>row: result) {
            for(String cell:row) {
                System.out.println(cell);
            }
        }
    }

    @Test
    public void testBasic() throws Exception {
        ctx.getSessionVariable().setQueryTimeoutS(100000000);
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, "select empid from emps_flat_view");
        System.out.println(plan);
    }

    @Test
    public void testBasic2() throws Exception {
        ctx.getSessionVariable().setQueryTimeoutS(100000000);
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, "select empid, deptno2 from emps_flat_view");
        System.out.println(plan);
    }

    @Test
    public void testJoin() throws Exception {
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, "select * from lineorder l inner join customer c on (c.c_custkey = l.lo_custkey)");
        System.out.println(plan);
    }

}