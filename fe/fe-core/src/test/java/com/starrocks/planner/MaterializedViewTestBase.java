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
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;

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
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        // connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
                starRocksAssert.withDatabaseWithoutAnalyze(StatsConstants.STATISTICS_DB_NAME)
                        .useDatabase(StatsConstants.STATISTICS_DB_NAME);
                starRocksAssert.withTable(CREATE_STATISTICS_TABLE_STMT);
        }
        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
            .useDatabase(MATERIALIZED_DB_NAME);
    }

    protected class MVRewriteChecker {
        private String mv;
        private final String query;
        private String rewritePlan;
        private Exception exception;

        public MVRewriteChecker(String query) {
            this.query = query;
        }

        public MVRewriteChecker(String mv, String query) {
            this.mv = mv;
            this.query = query;
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
                    String mvSQL = "CREATE MATERIALIZED VIEW mv0 \n" +
                            "   DISTRIBUTED BY HASH(`"+ outputNames.get(0) +"`) BUCKETS 12\n" +
                            " AS " +
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
            Assert.assertTrue(this.exception == null);
            if (mv != null && !mv.isEmpty()) {
                Assert.assertTrue(this.rewritePlan.contains("TABLE: mv0"));
            }
            return this;
        }

        public MVRewriteChecker nonMatch() {
            if (mv != null && !mv.isEmpty()) {
                Assert.assertTrue(!this.rewritePlan.contains("TABLE: mv0"));
            }
            return this;
        }

        public MVRewriteChecker contains(String expect) {
            Assert.assertTrue(this.rewritePlan.contains(expect));
            return this;
        }

        public MVRewriteChecker notContains(String expect) {
            Assert.assertTrue(!this.rewritePlan.contains(expect));
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

    protected MVRewriteChecker testRewriteOK(String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(query);
        return fixture.rewrite().ok();
    }

    protected MVRewriteChecker testRewriteFail(String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(query);
        return fixture.rewrite().nonMatch();
    }

    protected MVRewriteChecker testRewriteOK(String mv, String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(mv, query);
        return fixture.rewrite().ok();
    }

    protected MVRewriteChecker testRewriteFail(String mv, String query) {
        MVRewriteChecker fixture = new MVRewriteChecker(mv, query);
        return fixture.rewrite().nonMatch();
    }

    protected Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    protected MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    protected void refreshMaterializedView(String dbName, String mvName) throws Exception {
        MaterializedView mv = getMv(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        final String mvTaskName = TaskBuilder.getMvTaskName(mv.getId());
        if (!taskManager.containTask(mvTaskName)) {
            Task task = TaskBuilder.buildMvTask(mv, "test");
            TaskBuilder.updateTaskInfo(task, mv);
            taskManager.createTask(task, false);
        }
        taskManager.executeTaskSync(mvTaskName);
    }
}
