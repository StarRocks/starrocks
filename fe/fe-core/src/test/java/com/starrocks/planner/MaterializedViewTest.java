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
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(false);
        connectContext.getSessionVariable().setEnableOptimizerTraceLog(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        // connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);

        String deptsTable = "" +
                "CREATE TABLE depts(    \n" +
                "   deptno INT NOT NULL,\n" +
                "   name VARCHAR(20)    \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`deptno`)\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String locationsTable = "" +
                "CREATE TABLE locations(\n" +
                "    locationid INT NOT NULL,\n" +
                "    state CHAR(2), \n" +
                "   name VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`locationid`)\n" +
                "DISTRIBUTED BY HASH(`locationid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String ependentsTable = "" +
                "CREATE TABLE dependents(\n" +
                "   empid INT NOT NULL,\n" +
                "   name VARCHAR(20)   \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String empsTable = "" +
                "CREATE TABLE emps\n" +
                "(\n" +
                "    empid      INT         NOT NULL,\n" +
                "    deptno     INT         NOT NULL,\n" +
                "    locationid INT         NOT NULL,\n" +
                "    commission INT         NOT NULL,\n" +
                "    name       VARCHAR(20) NOT NULL,\n" +
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
                .withTable(ependentsTable)
                .withTable(deptsTable);
    }

    @AfterClass
    public static void afterClass() {
        try {
            starRocksAssert.dropDatabase(MATERIALIZED_DB_NAME);
        } catch (Exception e) {
            LOG.warn("drop database failed:", e);
        }
    }

    class MaterializedViewTestFixture {
        private final String mv;
        private final String query;

        private String rewritePlan;
        private Exception exception;
        public MaterializedViewTestFixture(String mv, String query) {
            this.mv = mv;
            this.query = query;
        }
        public MaterializedViewTestFixture rewrite() {
            // Get a faked distribution name
            this.exception = null;
            this.rewritePlan = "";

            try {
                LOG.info("start to create mv:" + mv);
                ExecPlan mvPlan = getExecPlan(mv);
                List<String> outputNames = mvPlan.getColNames();
                String mvSQL = "CREATE MATERIALIZED VIEW mv0 \n" +
                        "   DISTRIBUTED BY HASH(`"+ outputNames.get(0) +"`) BUCKETS 12\n" +
                        " AS " +
                        mv;
                starRocksAssert.withMaterializedView(mvSQL);
                this.rewritePlan = getFragmentPlan(query);
                System.out.println(rewritePlan);
            } catch (Exception e) {
                LOG.warn("test rewwrite failed:", e);
                this.exception = e;
            } finally {
                try {
                    starRocksAssert.dropMaterializedView("mv0");
                } catch (Exception e) {
                    LOG.warn("drop materialized view failed:", e);
                }
            }
            return this;
        }

        public MaterializedViewTestFixture ok() {
            Assert.assertTrue(this.exception == null);
            Assert.assertTrue(this.rewritePlan.contains("TABLE: mv0"));
            return this;
        }

        public MaterializedViewTestFixture nonMatch() {
            Assert.assertTrue(!this.rewritePlan.contains("TABLE: mv0"));
            return this;
        }

        public MaterializedViewTestFixture contains(String expect) {
            Assert.assertTrue(this.rewritePlan.contains(expect));
            return this;
        }
    }

    private MaterializedViewTestFixture testRewriteOK(String mv, String query) {
        MaterializedViewTestFixture fixture = new MaterializedViewTestFixture(mv, query);
        return fixture.rewrite().ok();
    }

    private MaterializedViewTestFixture testRewriteFail(String mv, String query) {
        MaterializedViewTestFixture fixture = new MaterializedViewTestFixture(mv, query);
        return fixture.rewrite().nonMatch();
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
        String mv = "select empid, locationid * 2 as col2 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select empid from emps where deptno = 10 and (locationid * 2) < 10");
    }

    @Test
    public void testSwapInnerJoin() {
        String mv = "select count(*) as col1 from emps join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*) from locations join emps on emps.locationid = locations.locationid");
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
        for (String joinType : outerJoinTypes) {
            String mv = "select count(*) from " +
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
    public void testAggregateBasic() {
        String mv = "select deptno as col1, locationid + 1 as b, count(*) as c, sum(empid) as s " +
                "from emps group by locationid + 1, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select count(*) as c, locationid + 1 from emps group by locationid + 1");
    }

    @Test
    public void testAggregate0() {
        testRewriteOK("select deptno, count(*) as c, empid + 2 as d, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testAggregate1() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregate2() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by deptno");
    }

    @Test
    public void testAggregate3() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    public void testAggregate4() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate5() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate6() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate7() {

        testRewriteFail("select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate8() {
        // TODO: support rewrite query by using mv's binary predicate later
        testRewriteFail("select empid, deptno + 1, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate9() {
        testRewriteOK("select sum(salary), count(salary) + 1 from emps",
                "select sum(salary), count(salary) + 1 from emps");
        testRewriteFail("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select sum(salary), count(salary) + 1 from emps");
    }

    @Test
    public void testAggregateWithAggExpr() {
        // support agg expr: empid -> abs(empid)
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by empid");
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by empid, deptno");
    }

    @Test
    public void testAggregateWithGroupByKeyExpr() {
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by abs(empid), deptno")
                .contains("  0:OlapScanNode\n" +
                        "     TABLE: mv0")
                .contains("  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(11: total)\n" +
                        "  |  group by: 14: abs, 10: deptno");
    }

    @Test
    @Ignore
    // TODO: Remove const group by keys
    public void testAggregateWithContGroupByKey() {
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
    }

    @Test
    public void testAggregateRollup() {
        String mv = "select deptno, count(*) as c, sum(empid) as s from emps group by locationid, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select sum(empid), count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select count(*) + 1 as c, deptno from emps group by deptno");
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
    public void testAggregateProject() {
        testRewriteOK("select deptno, count(*) as c, empid + 2, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK("select deptno, count(*) as c, empid + 2, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs1() {
        testRewriteOK("select empid, deptno from emps group by empid, deptno",
                "select empid, deptno from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs2() {
        testRewriteOK("select empid, deptno from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs3() {
        testRewriteFail("select deptno from emps group by deptno",
                "select empid, deptno from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs4() {
        testRewriteOK("select empid, deptno\n"
                        + "from emps where deptno = 10 group by empid, deptno",
                "select deptno from emps where deptno = 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs5() {
        testRewriteFail("select empid, deptno\n"
                        + "from emps where deptno = 5 group by empid, deptno",
                "select deptno from emps where deptno = 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs6() {
        testRewriteOK("select empid, deptno\n"
                        + "from emps where deptno > 5 group by empid, deptno",
                "select deptno from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs7() {
        testRewriteFail("select empid, deptno\n"
                        + "from emps where deptno > 5 group by empid, deptno",
                "select deptno from emps where deptno < 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs8() {
        testRewriteFail("select empid from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs9() {
        testRewriteFail("select empid, deptno from emps\n"
                        + "where salary > 1000 group by name, empid, deptno",
                "select empid from emps\n"
                        + "where salary > 2000 group by name, empid");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs1() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs2() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs3() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs4() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs5() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs6() {
        testRewriteFail("select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs7() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Ignore
    // TODO: Support deptno + 1 rewrite to deptno
    public void testAggregateMaterializationAggregateFuncs8() {
        testRewriteOK("select empid, deptno + 1, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    @Ignore
    // TODO: Remove const group by keys
    public void testAggregateMaterializationAggregateFuncsWithConstGroupByKeys() {
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) + 1 as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME) ), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME) )");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, cast('1997-01-20 12:34:56' as DATETIME), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, cast('1997-01-20 12:34:56' as DATETIME)",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select eventid, floor(cast(ts as DATETIME)), "
                        + "count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, floor(cast(ts as DATETIME))",
                "select floor(cast(ts as DATETIME) ), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME) )");
        testRewriteOK("select eventid, cast(ts as DATETIME), count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, cast(ts as DATETIME)",
                "select floor(cast(ts as DATETIME)), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME))");
        testRewriteOK("select eventid, floor(cast(ts as DATETIME)), "
                        + "count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, floor(cast(ts as DATETIME))",
                "select floor(cast(ts as DATETIME)), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME))");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs18() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select empid*deptno, sum(empid) as s\n"
                        + "from emps group by empid*deptno");
    }


    @Test
    public void testAggregateMaterializationAggregateFuncs19() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select empid + 10, count(*) + 1 as c\n"
                        + "from emps group by empid + 10");
    }


    // TODO: Don't support group by position.
    @Ignore
    public void testAggregateMaterializationAggregateFuncs20() {
        testRewriteOK("select 11 as empno, 22 as sal, count(*) from emps group by 11",
                "select * from\n"
                        + "(select 11 as empno, 22 as sal, count(*)\n"
                        + "from emps group by 11, 22 ) tmp\n"
                        + "where sal = 33");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs1() {
        // If agg push down is open, cannot rewrite.
        testRewriteOK("select empid, depts.deptno from emps\n"
                        + "join depts using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs2() {
        testRewriteOK("select depts.deptno, empid from depts\n"
                        + "join emps using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs3() {
        // It does not match, Project on top of query
        testRewriteFail("select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs4() {
        testRewriteOK("select empid, depts.deptno from emps\n"
                        + "join depts using (deptno) where emps.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs5() {
        testRewriteOK("select depts.deptno, emps.empid from depts\n"
                        + "join emps using (deptno) where emps.empid > 10\n"
                        + "group by depts.deptno, emps.empid",
                "select depts.deptno from depts\n"
                        + "join emps using (deptno) where emps.empid > 15\n"
                        + "group by depts.deptno, emps.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs6() {
        testRewriteOK("select depts.deptno, emps.empid from depts\n"
                        + "join emps using (deptno) where emps.empid > 10\n"
                        + "group by depts.deptno, emps.empid",
                "select depts.deptno from depts\n"
                        + "join emps using (deptno) where emps.empid > 15\n"
                        + "group by depts.deptno");
    }

    @Test
    @Ignore
    // TODO: union all support
    public void testJoinAggregateMaterializationNoAggregateFuncs7() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs8() {
        testRewriteFail("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 20\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    @Ignore
    // TODO: union all support
    public void testJoinAggregateMaterializationNoAggregateFuncs9() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs10() {
        testRewriteOK("select depts.name, dependents.name as name2, "
                        + "emps.deptno, depts.deptno as deptno2, "
                        + "dependents.empid\n"
                        + "from depts, dependents, emps\n"
                        + "where depts.deptno > 10\n"
                        + "group by depts.name, dependents.name, "
                        + "emps.deptno, depts.deptno, "
                        + "dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10\n"
                        + "group by dependents.empid");
    }


    @Test
    @Ignore
    // TODO: This test relies on FK-UK relationship
    public void testJoinAggregateMaterializationAggregateFuncs1() {
        testRewriteOK("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, depts.deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs2() {
        testRewriteOK("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, emps.deptno",
                "select depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by depts.deptno");
    }

    @Ignore
    @Test
    // TODO: This test relies on FK-UK relationship
    public void testJoinAggregateMaterializationAggregateFuncs3() {
        testRewriteOK("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, depts.deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs4() {
        testRewriteOK("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where emps.deptno >= 10 group by empid, emps.deptno",
                "select depts.deptno, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where emps.deptno > 10 group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs5() {
        testRewriteOK("select empid, depts.deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.deptno >= 10 group by empid, depts.deptno",
                "select depts.deptno, sum(empid) + 1 as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.deptno > 10 group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs6() {
        final String m = "select depts.name, sum(salary) as s\n"
                + "from emps\n"
                + "join depts on (emps.deptno = depts.deptno)\n"
                + "group by depts.name";
        final String q = "select dependents.empid, sum(salary) as s\n"
                + "from emps\n"
                + "join depts on (emps.deptno = depts.deptno)\n"
                + "join dependents on (depts.name = dependents.name)\n"
                + "group by dependents.empid";
        testRewriteOK(m, q);
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs7() {
        testRewriteOK("select dependents.empid, emps.deptno, sum(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select dependents.empid, sum(salary) as s\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs8() {
        testRewriteOK("select dependents.empid, emps.deptno, sum(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select depts.name, sum(salary) as s\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by depts.name");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs9() {
        testRewriteOK("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs10() {
        testRewriteFail("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by emps.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs11() {
        testRewriteOK("select depts.deptno, dependents.empid, count(emps.salary) as s\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid, count(emps.salary) + 1\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs12() {
        testRewriteFail("select depts.deptno, dependents.empid, "
                        + "count(distinct emps.salary) as s\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid, count(distinct emps.salary) + 1\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs13() {
        testRewriteFail("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs14() {
        testRewriteOK("select empid, emps.name, emps.deptno, depts.name, "
                        + "count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b')\n"
                        + "group by empid, emps.name, depts.name, emps.deptno",
                "select depts.deptno, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.name is not null and emps.name = 'a'\n"
                        + "group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs15() {
        final String m = ""
                + "SELECT deptno,\n"
                + "  COUNT(*) AS dept_size,\n"
                + "  SUM(salary) AS dept_budget\n"
                + "FROM emps\n"
                + "GROUP BY deptno";
        final String q = ""
                + "SELECT FLOOR(CREATED_AT TO YEAR) AS by_year,\n"
                + "  COUNT(*) AS num_emps\n"
                + "FROM (SELECTdeptno\n"
                + "    FROM emps) AS t\n"
                + "JOIN (SELECT deptno,\n"
                + "        inceptionDate as CREATED_AT\n"
                + "    FROM depts2) using (deptno)\n"
                + "GROUP BY FLOOR(CREATED_AT TO YEAR)";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization1() {
        String q = "select depts.deptno, empid, locationid, t.name \n"
                + "from (select * from emps where empid < 300) t \n"
                + "join depts using (deptno)";
        testRewriteOK("select deptno, empid, locationid, name from emps where empid < 500", q);
    }

    @Test
    public void testJoinMaterialization2() {
        String q = "select *\n"
                + "from emps\n"
                + "join depts using (deptno)";
        String m = "select deptno, empid, locationid, name,\n"
                + "salary, commission from emps";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterializationStar() {
        String q = "select *\n"
                + "from emps\n"
                + "join depts using (deptno)";
        String m = "select * from emps";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization3() {
        String q = "select empid deptno from emps\n"
                + "join depts using (deptno) where empid = 1";
        String m = "select empid deptno from emps\n"
                + "join depts using (deptno)";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization4() {
        testRewriteOK("select empid deptno from emps\n"
                        + "join depts using (deptno)",
                "select empid deptno from emps\n"
                        + "join depts using (deptno) where empid = 1");
    }

    @Test
    @Ignore
    // TODO: Need add no-loss-cast in lineage factory.
    public void testJoinMaterialization5() {
        testRewriteOK("select cast(empid as BIGINT) as a from emps\n"
                        + "join depts using (deptno)",
                "select empid from emps\n"
                        + "join depts using (deptno) where empid > 1");
    }

    @Test
    @Ignore
    // TODO: Need add no-loss-cast in lineage factory.
    public void testJoinMaterialization6() {
        testRewriteOK("select cast(empid as BIGINT) as a from emps\n"
                        + "join depts using (deptno)",
                "select empid deptno from emps\n"
                        + "join depts using (deptno) where empid = 1");
    }

    @Test
    public void testJoinMaterialization7() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (depts.name = dependents.name)");
    }

    @Test
    public void testJoinMaterialization8() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)");
    }

    @Test
    public void testJoinMaterialization9() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)");
    }


    @Test
    @Ignore
    public void testJoinMaterialization10() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 30",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10");
    }

    @Test
    public void testJoinMaterialization11() {
        testRewriteFail("select empid from emps\n"
                        + "join depts using (deptno)",
                "select empid from emps\n"
                        + "where deptno in (select deptno from depts)");
    }

    @Test
    @Ignore
    public void testJoinMaterialization12() {
        testRewriteOK("select empid, emps.name as a, emps.deptno, depts.name as b \n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b') or "
                        + "(depts.name is not null and emps.name = 'c')",
                "select depts.deptno, depts.name\n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b')");
    }


    @Test
    @Ignore
    // TODO: agg push down below Join
    public void testAggregateOnJoinKeys() {
        testRewriteOK("select deptno, empid, salary "
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select empid, depts.deptno "
                        + "from emps\n"
                        + "join depts on depts.deptno = empid group by empid, depts.deptno");
    }


    @Test
    @Ignore
    // TODO: agg need push down below join
    public void testAggregateOnJoinKeys2() {
        testRewriteOK("select deptno, empid, salary, sum(1) as c "
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select sum(1) "
                        + "from emps\n"
                        + "join depts on depts.deptno = empid group by empid, depts.deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery1() {
        // The column empid is already unique, thus DISTINCT is not
        // in the COUNT of the resulting rewriting
        testRewriteOK("select deptno, empid, salary\n"
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select deptno, count(distinct empid) as c from (\n"
                        + "select deptno, empid\n"
                        + "from emps\n"
                        + "group by deptno, empid) t\n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery2() {
        // The column empid is already unique, thus DISTINCT is not
        // in the COUNT of the resulting rewriting
        testRewriteOK("select deptno, salary, empid\n"
                        + "from emps\n"
                        + "group by deptno, salary, empid",
                "select deptno, count(distinct empid) as c from (\n"
                        + "select deptno, empid\n"
                        + "from emps\n"
                        + "group by deptno, empid) t \n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery3() {
        // The column salary is not unique, thus we end up with
        // a different rewriting
        testRewriteOK("select deptno, empid, salary\n"
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select deptno, count(distinct salary) from (\n"
                        + "select deptno, salary\n"
                        + "from emps\n"
                        + "group by deptno, salary) t \n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery4() {
        // Although there is no DISTINCT in the COUNT, this is
        // equivalent to previous query
        testRewriteOK("select deptno, salary, empid\n"
                        + "from emps\n"
                        + "group by deptno, salary, empid",
                "select deptno, count(salary) from (\n"
                        + "select deptno, salary\n"
                        + "from emps\n"
                        + "group by deptno, salary) t\n"
                        + "group by deptno");
    }
}
