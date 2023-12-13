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

package com.starrocks.sql.plan;

import com.starrocks.common.profile.Tracers;
import com.starrocks.planner.MaterializedViewTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TracerMVTest extends MaterializedViewTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.useTable("depts");
        starRocksAssert.useTable("locations");
        starRocksAssert.useTable("emps");

        String mv = "CREATE MATERIALIZED VIEW `test_distinct_mv1`\n" +
                "DISTRIBUTED BY HASH(`deptno`, `locationid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS \n" +
                "SELECT \n" +
                "  `locationid`,\n" +
                "  `deptno`,\n" +
                "  count(DISTINCT `empid`) AS `order_num`\n" +
                "FROM `emps`\n" +
                "GROUP BY `locationid`, `deptno`;";
        starRocksAssert.withMaterializedView(mv);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        starRocksAssert.dropMaterializedView("test_distinct_mv1");
        MaterializedViewTestBase.afterClass();
    }

    @Test
    public void testTracerTimerMV() {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertContains(pr, "-- Planner");
    }

    @Test
    public void testTracerVarMV() {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.VARS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printVars();
        Tracers.close();
        assertContains(pr, "mv0: Rewrite Succeed");
    }

    @Test
    public void testTracerLogMV1() {
        connectContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "[MV TRACE]");
    }

    @Test
    public void testTracerLogMV2() {
        connectContext.getSessionVariable().setTraceLogMode("File");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertNotContains(pr, "[MV TRACE]");
        connectContext.getSessionVariable().setTraceLogMode("command");
    }

    @Test
    public void testTracerLogMV_Success1() {
        connectContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "[MV TRACE]");
        assertContains(pr, "Query has already been successfully rewritten by: mv0.");
    }

    @Test
    public void testTracerLogMV_Success2() {
        connectContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteOK(mv, "select emps.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid = 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "[MV TRACE]");
        assertContains(pr, "Query input tables");
    }

    @Test
    public void testTracerLogMV_Fail1() {
        connectContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteFail(mv, "select emps.locationid, empid, sum(emps.deptno + 1) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid  > 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "[MV TRACE]");
        assertNotContains(pr, "Query has already been successfully rewritten by: mv0.");
    }

    @Test
    public void testTracerLogMV_Fail2() {
        connectContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        String mv = "select locations.locationid, empid, sum(emps.deptno) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid group by empid,locations.locationid";
        testRewriteFail(mv, "select emps.locationid, empid, sum(emps.deptno + 1) as col3 from emps " +
                "join locations on emps.locationid = locations.locationid where empid  > 10 group by empid,emps.locationid");
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "[MV TRACE]");
        assertContains(pr, "has related materialized views");
        assertContains(pr, "Rewrite aggregate group-by/agg expr failed");
    }
}
