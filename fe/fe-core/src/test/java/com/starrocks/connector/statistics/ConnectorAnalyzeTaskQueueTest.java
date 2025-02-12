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

package com.starrocks.connector.statistics;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectorAnalyzeTaskQueueTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @Test
    public void testAddPendingTaskWithMerge() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assert.assertEquals(1, queue.getPendingTaskSize());
        // merge task
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);
        Assert.assertEquals(1, queue.getPendingTaskSize());
    }


    @Test
    public void testAddPendingTaskExceedLimit() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        Config.connector_table_query_trigger_analyze_max_pending_task_num = 1;
        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assert.assertEquals(1, queue.getPendingTaskSize());
        // add task exceed limit
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        Assert.assertFalse(queue.addPendingTask(tableUUID, task2));
        Config.connector_table_query_trigger_analyze_max_pending_task_num = 100;
    }

    @Test
    public void testScheduledPendingTask() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assert.assertEquals(1, queue.getPendingTaskSize());

        queue.schedulePendingTask();
        Assert.assertEquals(0, queue.getPendingTaskSize());

        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_custkey", "o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);

        Config.connector_table_query_trigger_analyze_max_running_task_num = 0;
        queue.schedulePendingTask();
        Assert.assertEquals(1, queue.getPendingTaskSize());
        Config.connector_table_query_trigger_analyze_max_running_task_num = 2;
        queue.schedulePendingTask();
        Assert.assertEquals(0, queue.getPendingTaskSize());
    }
}
