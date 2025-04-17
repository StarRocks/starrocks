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

package com.starrocks.qe.scheduler.warehouse;

import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.thrift.TWorkGroup;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

public class WarehouseJobSpecTest extends SchedulerTestBase {
    private static final TWorkGroup QUERY_RESOURCE_GROUP = new TWorkGroup();
    private static final TWorkGroup LOAD_RESOURCE_GROUP = new TWorkGroup();

    private static final DefaultCoordinator.Factory COORDINATOR_FACTORY = new DefaultCoordinator.Factory();

    static {
        QUERY_RESOURCE_GROUP.setId(0L);
        LOAD_RESOURCE_GROUP.setId(1L);
    }

    private boolean prevEnablePipelineLoad;

    /**
     * Mock {@link ResourceGroupMgr#chooseResourceGroup(ConnectContext, ResourceGroupClassifier.QueryType, Set)}.
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        SchedulerTestBase.beforeClass();

        new MockUp<ResourceGroupMgr>() {
            @Mock
            public TWorkGroup chooseResourceGroup(ConnectContext ctx, ResourceGroupClassifier.QueryType queryType,
                                                  Set<Long> databases) {
                if (queryType != ResourceGroupClassifier.QueryType.SELECT) {
                    return LOAD_RESOURCE_GROUP;
                } else {
                    return QUERY_RESOURCE_GROUP;
                }
            }
        };
    }

    @Before
    public void before() {
        prevEnablePipelineLoad = Config.enable_pipeline_load;
    }

    @After
    public void after() {
        Config.enable_pipeline_load = prevEnablePipelineLoad;
    }

    @Test
    public void testEnableQueue1() throws Exception {
        // in shared-data mode, behavior is not the same as shared-nothing mode
        {
            // 1. Load type.
            GlobalVariable.setEnableQueryQueueLoad(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueLoad(true);
            coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueLoad(false);
        }

        {
            // 2. Query for select.
            GlobalVariable.setEnableQueryQueueSelect(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueSelect(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueSelect(false);
        }

        {
            // 3. Query for statistic.
            connectContext.setStatisticsContext(false);
            connectContext.setStatisticsJob(true); // Mock statistics job.
            GlobalVariable.setEnableQueryQueueStatistic(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsJob(false);
            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsContext(true);
            GlobalVariable.setEnableQueryQueueStatistic(false);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());
            connectContext.setStatisticsContext(false);
            GlobalVariable.setEnableQueryQueueStatistic(false);
        }
    }

    @Test
    public void testEnableQueue2() throws Exception {
        // in shared-data mode, behavior is not the same as shared-nothing mode
        // alter warehouse
        String sql = "ALTER WAREHOUSE default_warehouse \n" +
                "SET (\n" +
                "    'enable_query_queue' = 'true',\n" +
                "    'enable_query_queue_load' = 'true',\n" +
                "    'enable_query_queue_statistic' = 'true',\n" +
                "    'query_queue_max_queued_queries' = '100',\n" +
                "    'query_queue_pending_timeout_second' = '600'\n" +
                ")";
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
        DDLStmtExecutor.execute(stmt, connectContext);

        {
            // 1. Load type.
            GlobalVariable.setEnableQueryQueueLoad(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueLoad(true);
            coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueLoad(false);
        }

        {
            // 2. Query for select.
            GlobalVariable.setEnableQueryQueueSelect(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueSelect(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueSelect(false);
        }

        {
            // 3. Query for statistic.
            connectContext.setStatisticsContext(false);
            connectContext.setStatisticsJob(true); // Mock statistics job.
            GlobalVariable.setEnableQueryQueueStatistic(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsJob(false);
            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsContext(true);
            GlobalVariable.setEnableQueryQueueStatistic(false);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            connectContext.setStatisticsContext(false);
            GlobalVariable.setEnableQueryQueueStatistic(false);

            // alter warehouse
            sql = "ALTER WAREHOUSE default_warehouse \n" +
                    "SET (\n" +
                    "    'enable_query_queue' = 'false'\n" +
                    ")";
            stmt = com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, connectContext);
            Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
            DDLStmtExecutor.execute(stmt, connectContext);
        }
    }
}

