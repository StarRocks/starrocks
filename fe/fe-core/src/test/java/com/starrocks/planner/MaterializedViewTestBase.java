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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.utframe.UtFrameUtils.CREATE_STATISTICS_TABLE_STMT;

public class MaterializedViewTestBase extends PlanTestBase {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewTestBase.class);

    protected static final String MATERIALIZED_DB_NAME = "test_mv";

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(false);
        // connectContext.getSessionVariable().setEnableOptimizerTraceLog(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        // connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);

        new MockUp<MaterializedView>() {
            @Mock
            Set<String> getPartitionNamesToRefreshForMv() {
                return Sets.newHashSet();
            }
        };

        new MockUp<UtFrameUtils>() {
            @Mock
            boolean isPrintPlanTableNames() {
                return true;
            }
        };

        new MockUp<PlanTestBase>() {
            @Mock
            boolean isIgnoreExplicitColRefIds() {
                return true;
            }
        };

        if (!starRocksAssert.databaseExist("_statistics_")) {
            starRocksAssert.withDatabaseWithoutAnalyze(StatsConstants.STATISTICS_DB_NAME)
                    .useDatabase(StatsConstants.STATISTICS_DB_NAME);
            starRocksAssert.withTable(CREATE_STATISTICS_TABLE_STMT);
        }

        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME);


        String deptsTable = "" +
                "CREATE TABLE depts(    \n" +
                "   deptno INT NOT NULL,\n" +
                "   name VARCHAR(20)    \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`deptno`)\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"unique_constraints\" = \"deptno\"\n," +
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
                "    \"foreign_key_constraints\" = \"(deptno) REFERENCES depts(deptno)\"," +
                "    \"replication_num\" = \"1\"\n" +
                ");";

        String empsTableWithoutConstraints = "" +
                "CREATE TABLE emps_no_constraint\n" +
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

        String empsWithBigintTable = "" +
                "CREATE TABLE emps_bigint\n" +
                "(\n" +
                "    empid      BIGINT        NOT NULL,\n" +
                "    deptno     BIGINT         NOT NULL,\n" +
                "    locationid BIGINT         NOT NULL,\n" +
                "    commission BIGINT         NOT NULL,\n" +
                "    name       VARCHAR(20) NOT NULL,\n" +
                "    salary     DECIMAL(18, 2)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String nullableEmps = "create table emps_null (\n" +
                "empid int null,\n" +
                "deptno int null,\n" +
                "name varchar(25) null,\n" +
                "salary double\n" +
                ")\n" +
                "distributed by hash(`empid`) buckets 10\n" +
                "properties (\"replication_num\" = \"1\");";
        String nullableDepts = "create table depts_null (\n" +
                "deptno int null,\n" +
                "name varchar(25) null\n" +
                ")\n" +
                "distributed by hash(`deptno`) buckets 10\n" +
                "properties (\"replication_num\" = \"1\");";

        starRocksAssert
                .withTable(deptsTable)
                .withTable(empsTable)
                .withTable(locationsTable)
                .withTable(ependentsTable)
                .withTable(empsWithBigintTable)
                .withTable(empsTableWithoutConstraints)
                .withTable(nullableEmps)
                .withTable(nullableDepts);

    }

    @AfterClass
    public static void afterClass() {
        try {
            starRocksAssert.dropDatabase(MATERIALIZED_DB_NAME);
        } catch (Exception e) {
            LOG.warn("drop database failed:", e);
        }
    }

    protected class MVRewriteChecker {
        private String mv;
        private final String query;
        private String rewritePlan;
        private Exception exception;
        private String properties;

        public MVRewriteChecker(String query) {
            this.query = query;
        }

        public MVRewriteChecker(String mv, String query) {
            this(mv, query, null);
        }

        public MVRewriteChecker(String mv, String query, String properties) {
            this.mv = mv;
            this.query = query;
            this.properties = properties;
        }

        public MVRewriteChecker rewrite() {
            // Get a faked distribution name
            this.exception = null;
            this.rewritePlan = "";

            try {
                // create mv if needed
                if (mv != null && !mv.isEmpty()) {
                    LOG.info("start to create mv:" + mv);
                    ExecPlan mvPlan = getExecPlan(mv);
                    List<String> outputNames = mvPlan.getColNames();
                    String properties = this.properties != null ? "PROPERTIES (\n" +
                            this.properties + ")" : "";
                    String mvSQL = "CREATE MATERIALIZED VIEW mv0 \n" +
                            "   DISTRIBUTED BY HASH(`"+ outputNames.get(0) +"`) BUCKETS 12\n" +
                            properties + " AS " +
                            mv;
                    starRocksAssert.withMaterializedView(mvSQL);
                }

                this.rewritePlan = getFragmentPlan(query);
            } catch (Exception e) {
                LOG.warn("test rewwrite failed:", e);
                this.exception = e;
            } finally {
                if (mv != null && !mv.isEmpty()) {
                    try {
                        starRocksAssert.dropMaterializedView("mv0");
                    } catch (Exception e) {
                        LOG.warn("drop materialized view failed:", e);
                    }
                }
            }
            return this;
        }

        public MVRewriteChecker ok() {
            return match("TABLE: mv0");
        }

        public MVRewriteChecker match(String targetMV) {
            contains(targetMV);
            Assert.assertTrue(this.exception == null);
            return this;
        }

        public MVRewriteChecker nonMatch() {
            return nonMatch("TABLE: mv0");
        }

        public MVRewriteChecker nonMatch(String targetMV) {
            Assert.assertTrue(this.rewritePlan != null);
            Assert.assertFalse(this.rewritePlan.contains(targetMV));
            return this;
        }

        public MVRewriteChecker contains(String expect) {
            Assert.assertTrue(this.rewritePlan != null);
            boolean contained = this.rewritePlan.contains(expect);
            if (!contained) {
                LOG.warn("rewritePlan: \n{}", rewritePlan);
                LOG.warn("expect: \n{}", expect);
            }
            Assert.assertTrue(contained);
            return this;
        }

        public MVRewriteChecker contains(String... expects) {
            for (String expect: expects) {
                Assert.assertTrue(this.rewritePlan.contains(expect));
            }
            return this;
        }

        public MVRewriteChecker contains(List<String> expects) {
            for (String expect: expects) {
                Assert.assertTrue(this.rewritePlan.contains(expect));
            }
            return this;
        }
    }

    protected MVRewriteChecker sql(String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(query);
        return fixture.rewrite();
    }

    protected MVRewriteChecker testRewriteOK(String mv, String query) {
        return testRewriteOK(mv, query, null);
    }

    protected MVRewriteChecker testRewriteOK(String mv, String query, String properties) {
        MVRewriteChecker fixture = new MVRewriteChecker(mv, query, properties);
        return fixture.rewrite().ok();
    }

    protected MVRewriteChecker testRewriteFail(String mv, String query, String properties) {
        MVRewriteChecker fixture = new MVRewriteChecker(mv, query, properties);
        return fixture.rewrite().nonMatch();
    }

    protected MVRewriteChecker testRewriteFail(String mv, String query) {
        return testRewriteFail(mv, query, null);
    }

    protected static Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    protected static MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    protected static void refreshMaterializedView(String dbName, String mvName) throws Exception {
        MaterializedView mv = getMv(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        final String mvTaskName = TaskBuilder.getMvTaskName(mv.getId());
        Task task = taskManager.getTask(mvTaskName);
        if (task == null) {
            task = TaskBuilder.buildMvTask(mv, dbName);
            TaskBuilder.updateTaskInfo(task, mv);
            taskManager.createTask(task, false);
        }
        taskManager.executeTaskSync(task);
    }

    protected static void createAndRefreshMV(String db, String sql) throws Exception {
        Pattern createMvPattern = Pattern.compile("^create materialized view (\\w+) .*");
        Matcher matcher = createMvPattern.matcher(sql.toLowerCase(Locale.ROOT));
        if (!matcher.find()) {
            throw new Exception("create materialized view syntax error.");
        }
        String tableName = matcher.group(1);
        starRocksAssert.withMaterializedView(sql);
        refreshMaterializedView(db, tableName);
    }
}
