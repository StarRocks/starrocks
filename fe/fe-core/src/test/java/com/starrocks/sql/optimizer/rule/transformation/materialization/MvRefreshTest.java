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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRefreshTest extends MVTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(500);
        cluster = PseudoCluster.getInstance();
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "test_base_part");
        starRocksAssert.withTable(cluster, "table_with_partition");
    }

    @Test
    public void testMvWithComplexNameRefresh() throws Exception {
        createAndRefreshMv("create materialized view `cc`" +
                    " partition by id_date" +
                    " distributed by hash(`t1a`)" +
                    " as" +
                    " select t1a, id_date, t1b from table_with_partition");

        createAndRefreshMv("create materialized view `luchen_order_8e2c65ba_1c30`" +
                    " partition by id_date" +
                    " distributed by hash(`t1a`)" +
                    " as" +
                    " select t1a, id_date, t1b from table_with_partition");
    }

    @Test
    public void testMVRefreshWithQueryRewrite1() throws Exception {
        createAndRefreshMv("create materialized view `test_mv1` \n" +
                    " partition by id_date\n" +
                    " distributed by hash(`t1a`)\n" +
                    " refresh deferred manual \n" +
                    " as\n" +
                    " select t1a, id_date, t1b, sum(t1c) as sum1 from table_with_partition group by t1a, id_date, t1b");
        starRocksAssert.withMaterializedView("create materialized view `test_mv2` \n" +
                                " partition by id_date\n" +
                                " distributed by hash(`t1a`)\n" +
                                " refresh deferred manual \n" +
                                " as\n" +
                                " select t1a, id_date, sum(t1c) as sum1 from table_with_partition group by t1a, id_date",
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView materializedView =
                                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                .getTable(testDb.getFullName(), "test_mv2"));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        // If the base table has been refreshed and test_mv1 has been refreshed, test_mv2 refresh can use
                        // test_mv1.
                        {
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assert.assertNotNull("exec plan should not null", execPlan);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "test_mv1");
                        }

                        // If the base table has changed and test_mv1 has not been refreshed, test_mv2 refresh should
                        // not use test_mv1.
                        {
                            executeInsertSql(connectContext, "insert into table_with_partition partition(p1991)" +
                                        " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assert.assertNotNull("exec plan should not null", execPlan);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertNotContains(plan, "test_mv1");
                        }
                    });
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testMVRefreshWithQueryRewrite2() throws Exception {
        createAndRefreshMv("create materialized view `test_mv1` \n" +
                    " partition by id_date\n" +
                    " distributed by hash(`t1a`)\n" +
                    " refresh deferred manual \n" +
                    " as\n" +
                    " select t1a, id_date, t1b, sum(t1c) as sum1 from table_with_partition group by t1a, id_date, t1b");
        starRocksAssert.withMaterializedView("create materialized view `test_mv2` \n" +
                                "partition by id_date\n" +
                                "distributed by hash(`t1a`)\n" +
                                "refresh deferred manual \n" +
                                "PROPERTIES (\n" +
                                "   'replication_num' = '1', \n" +
                                "   'session.enable_materialized_view_rewrite' = 'false' \n" +
                                ")\n" +
                                " as\n" +
                                " select t1a, id_date, sum(t1c) as sum1 from table_with_partition group by t1a, id_date",
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView materializedView =
                                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                .getTable(testDb.getFullName(), "test_mv2"));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        // If the base table has been refreshed and test_mv1 has been refreshed, test_mv2 refresh can use
                        // test_mv1.
                        {
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assert.assertNotNull("exec plan should not null", execPlan);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertNotContains(plan, "test_mv1");
                        }
                    });
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testMVRefreshWithQueryRewrite3() throws Exception {
        createAndRefreshMv("create materialized view `test_mv1` \n" +
                    " partition by id_date\n" +
                    " distributed by hash(`t1a`)\n" +
                    " refresh deferred manual \n" +
                    " as\n" +
                    " select t1a, id_date, t1b, sum(t1c) as sum1 from table_with_partition group by t1a, id_date, t1b");
        Config.enable_mv_refresh_query_rewrite = false;
        starRocksAssert.withMaterializedView("create materialized view `test_mv2` \n" +
                                "partition by id_date\n" +
                                "distributed by hash(`t1a`)\n" +
                                "refresh deferred manual \n" +
                                "PROPERTIES (\n" +
                                "   'replication_num' = '1', \n" +
                                "   'session.enable_materialized_view_rewrite' = 'true', \n" +
                                "   'session.enable_materialized_view_for_insert' = 'true' \n" +
                                ")\n" +
                                " as\n" +
                                " select t1a, id_date, sum(t1c) as sum1 from table_with_partition group by t1a, id_date",
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView materializedView =
                                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                .getTable(testDb.getFullName(), "test_mv2"));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        // If the base table has been refreshed and test_mv1 has been refreshed, test_mv2 refresh can use
                        // test_mv1.
                        {
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assert.assertNotNull("exec plan should not null", execPlan);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "test_mv1");
                        }

                        // If the base table has changed and test_mv1 has not been refreshed, test_mv2 refresh should
                        // not use test_mv1.
                        {
                            executeInsertSql(connectContext, "insert into table_with_partition partition(p1991)" +
                                        " values(\"varchar12\", '1991-03-01', 2, 1, 1)");
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assert.assertNotNull("exec plan should not null", execPlan);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertNotContains(plan, "test_mv1");
                        }
                    });
        Config.enable_mv_refresh_query_rewrite = true;
        starRocksAssert.dropMaterializedView("test_mv1");
    }
}
