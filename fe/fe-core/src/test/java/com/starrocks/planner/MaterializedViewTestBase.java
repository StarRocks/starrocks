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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaterializedViewTestBase extends PlanTestBase {
    protected static final Logger LOG = LogManager.getLogger(MaterializedViewTestBase.class);

    protected static final String MATERIALIZED_DB_NAME = "test_mv";
    protected static final String MATERIALIZED_VIEW_NAME = "mv0";

    // You can set it in each unit test for trace mv log: mv/all/"", default is "" which will output nothing.
    private  String traceLogModule = "";

    public void setTracLogModule(String module) {
        this.traceLogModule = module;
    }

    public void resetTraceLogModule() {
        this.traceLogModule = "";
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        // set default config for timeliness mvs
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);

        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME);
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
        private final String query;
        private String mvDBName;
        private String rewritePlan;
        private String mv;
        private Exception exception;
        private String properties;
        private String traceLog;
        private boolean isLogical;

        public MVRewriteChecker(String query) {
            this(query, false);
        }

        public MVRewriteChecker(String query, boolean isLogical) {
            this.query = query;
            this.isLogical = isLogical;
        }

        public MVRewriteChecker(String mv, String query) {
            this(null, mv, query, null);
        }

        public MVRewriteChecker(String mvDBName, String mv, String query, String properties) {
            this.mvDBName = mvDBName;
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
                    String properties = Strings.isNullOrEmpty(this.properties) ? "" : "PROPERTIES (\n" +
                            this.properties + ")";
                    String mvName = getMVName();
                    String mvSQL = String.format("CREATE MATERIALIZED VIEW %s \n" +
                            " REFRESH MANUAL \n%s AS %s", mvName, properties, mv);
                    System.out.println(mvSQL);
                    starRocksAssert.withMaterializedView(mvSQL);
                }

                Pair<ExecPlan, String> planAndTrace =
                        UtFrameUtils.getFragmentPlanWithTrace(connectContext, query, traceLogModule).second;
                if (isLogical) {
                    this.rewritePlan = planAndTrace.first.getExplainString(StatementBase.ExplainLevel.LOGICAL);
                } else {
                    this.rewritePlan = planAndTrace.first.getExplainString(TExplainLevel.NORMAL);
                }
                if (!Strings.isNullOrEmpty(traceLogModule)) {
                    System.out.println(this.rewritePlan);
                }
                this.traceLog = planAndTrace.second;
            } catch (Exception e) {
                LOG.warn("test rewrite failed:", e);
                this.exception = e;
            } finally {
                if (!Strings.isNullOrEmpty(traceLogModule)) {
                    System.out.println(traceLog);
                }
                if (mv != null && !mv.isEmpty()) {
                    try {
                        starRocksAssert.dropMaterializedView(getMVName());
                    } catch (Exception e) {
                        LOG.warn("drop materialized view failed:", e);
                    }
                }
            }
            return this;
        }

        private String getMVName() {
            return Strings.isNullOrEmpty(mvDBName) ? MATERIALIZED_VIEW_NAME
                    : String.format("%s.%s", mvDBName, MATERIALIZED_VIEW_NAME);
        }

        public String getTraceLog() {
            return this.traceLog;
        }

        Exception getException() {
            return this.exception;
        }

        public MVRewriteChecker ok() {
            return match("TABLE: mv0");
        }

        public MVRewriteChecker match(String targetMV) {
            contains(targetMV);
            Assert.assertTrue(this.exception == null);
            return this;
        }

        // there may be an exception
        public MVRewriteChecker failed() {
            return nonMatch("TABLE: mv0");
        }

        // check plan result without any exception
        public MVRewriteChecker nonMatch() {
            Preconditions.checkState(exception == null);
            return nonMatch("TABLE: mv0");
        }

        public MVRewriteChecker nonMatch(String targetMV) {
            Assert.assertTrue(this.rewritePlan != null);
            Assert.assertFalse(this.rewritePlan, this.rewritePlan.contains(targetMV));
            return this;
        }

        public MVRewriteChecker contains(String expect) {
            Assert.assertTrue(this.rewritePlan != null);
            String normalizedExpect = normalizeNormalPlan(expect);
            String actual = normalizeNormalPlan(this.rewritePlan);
            boolean contained = actual.contains(normalizedExpect);

            if (!contained) {
                LOG.warn("rewritePlan: \n{}", rewritePlan);
                LOG.warn("expect: \n{}", expect);
                LOG.warn("normalized rewritePlan: \n{}", actual);
                LOG.warn("normalized expect: \n{}", normalizedExpect);
            }
            Assert.assertTrue(contained);
            return this;
        }

        public MVRewriteChecker notContain(String expect) {
            Assert.assertTrue(this.rewritePlan != null);
            boolean contained = this.rewritePlan.contains(expect);
            if (contained) {
                LOG.warn("rewritePlan: \n{}", rewritePlan);
                LOG.warn("expect: \n{}", expect);
            }
            Assert.assertFalse(contained);
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

        public String getExecPlan() {
            return this.rewritePlan;
        }
    }

    protected MVRewriteChecker sql(String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(query);
        return fixture.rewrite();
    }

    protected MVRewriteChecker sql(String query, boolean isLogical) {
        MVRewriteChecker fixture = new MVRewriteChecker(query, isLogical);
        return fixture.rewrite();
    }

    protected MVRewriteChecker testRewriteOK(String mv, String query) {
        return testRewriteOK(mv, query, null);
    }

    protected MVRewriteChecker testRewriteOK(String mv, String query, String properties) throws RuntimeException {
        return testRewriteOK(null, mv, query, properties);
    }

    protected MVRewriteChecker testRewriteOK(String mvDBName, String mv, String query, String properties) throws RuntimeException {
        MVRewriteChecker fixture = new MVRewriteChecker(mvDBName, mv, query, properties);
        MVRewriteChecker checker = fixture.rewrite();
        if (checker.getException() != null) {
            throw new RuntimeException(checker.getException());
        }
        return checker.ok();
    }

    protected MVRewriteChecker testRewriteFail(String mv, String query, String properties) {
        MVRewriteChecker fixture = new MVRewriteChecker(null, mv, query, properties);
        return fixture.rewrite().failed();
    }

    protected MVRewriteChecker testRewriteFail(String mv, String query) {
        return testRewriteFail(mv, query, null);
    }

    protected MVRewriteChecker testRewriteNonmatch(String mv, String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(null, mv, query, null);
        return fixture.rewrite().nonMatch();
    }

    protected MVRewriteChecker rewrite(String mv, String query, String properties) throws Exception {
        MVRewriteChecker fixture = new MVRewriteChecker(null, mv, query, properties);
        MVRewriteChecker checker = fixture.rewrite();
        if (checker.getException() != null) {
            throw checker.getException();
        }
        return checker;
    }

    protected static Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), mvName);
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

    public static Pair<Table, Column> getRefBaseTablePartitionColumn(MaterializedView mv) {
        Map<Table, List<Column>> result = mv.getRefBaseTablePartitionColumns();
        Assert.assertTrue(result.size() == 1);
        Map.Entry<Table, List<Column>> e = result.entrySet().iterator().next();
        Assert.assertEquals(1, e.getValue().size());
        return Pair.create(e.getKey(), e.getValue().get(0));
    }

    public String getQueryPlan(String query) {
        return getQueryPlan(query, TExplainLevel.NORMAL);
    }

    public String getQueryPlan(String query, TExplainLevel level) {
        try {
            Pair<ExecPlan, String> planAndTrace =
                    UtFrameUtils.getFragmentPlanWithTrace(connectContext, query, traceLogModule).second;
            return planAndTrace.first.getExplainString(level);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return null;
    }
}

