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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Optional;

public class ConnectorAnalyzeTaskQueueTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
    }

    @Test
    public void testAddPendingTaskWithMerge() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        // merge task
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
    }

    @Test
    public void testAddPendingTaskExceedLimit() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        Config.connector_table_query_trigger_analyze_max_pending_task_num = 1;
        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        // add task exceed limit
        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderstatus"));
        Assertions.assertFalse(queue.addPendingTask(tableUUID, task2));
        Config.connector_table_query_trigger_analyze_max_pending_task_num = 100;
    }

    @Test
    public void testScheduledPendingTask() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);
        ConnectorAnalyzeTask task1 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey", "o_custkey"));

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.addPendingTask(tableUUID, task1);
        Assertions.assertEquals(1, queue.getPendingTaskSize());

        queue.schedulePendingTask();
        Assertions.assertEquals(0, queue.getPendingTaskSize());

        ConnectorAnalyzeTask task2 = new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_custkey", "o_orderstatus"));
        queue.addPendingTask(tableUUID, task2);

        Config.connector_table_query_trigger_analyze_max_running_task_num = 0;
        queue.schedulePendingTask();
        Assertions.assertEquals(1, queue.getPendingTaskSize());
        Config.connector_table_query_trigger_analyze_max_running_task_num = 2;
        queue.schedulePendingTask();
        Assertions.assertEquals(0, queue.getPendingTaskSize());
    }

    private static AnalyzeStatus statusWith(StatsConstants.ScheduleStatus scheduleStatus, String tableUUID) {
        AnalyzeStatus status = new ExternalAnalyzeStatus(1L, "hive0", "partitioned_db", "orders", tableUUID,
                Lists.newArrayList("o_orderkey"), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
        status.setStatus(scheduleStatus);
        return status;
    }

    @Test
    public void testFailedAnalyzeIsNotRetriedOnEveryTick() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);

        // The statistics this job failed to collect are still missing, so the next query asks for them
        // again right away. Without a cooldown that request would be accepted every schedule tick.
        Assertions.assertFalse(queue.addPendingTask(tableUUID,
                new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey"))));
        Assertions.assertEquals(0, queue.getPendingTaskSize());
    }

    @Test
    public void testCooldownLengthensWhileFailuresContinue() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);
        long afterFirst = queue.cooldownRemainingMs(tableUUID);
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);
        long afterSecond = queue.cooldownRemainingMs(tableUUID);

        Assertions.assertTrue(afterFirst > 0);
        Assertions.assertTrue(afterSecond > afterFirst,
                "expected the wait to grow, got " + afterFirst + " then " + afterSecond);
        Assertions.assertTrue(afterSecond <=
                Config.connector_table_query_trigger_analyze_failure_backoff_max_second * 1000L);
    }

    @Test
    public void testSuccessClearsTheCooldown() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FINISH, tableUUID)), null);

        Assertions.assertTrue(queue.cooldownRemainingMs(tableUUID) <= 0);
        Assertions.assertTrue(queue.addPendingTask(tableUUID,
                new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey"))));
    }

    @Test
    public void testAnAttemptThatFoundNothingToDoIsNeitherFailureNorSuccess() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);
        long beforeEmpty = queue.cooldownRemainingMs(tableUUID);
        // An analyze that was skipped because one was already running says nothing either way, so it must
        // not clear the cooldown a real failure just set.
        queue.recordOutcome(tableUUID, Optional.empty(), null);

        Assertions.assertTrue(queue.cooldownRemainingMs(tableUUID) > 0);
        Assertions.assertTrue(queue.cooldownRemainingMs(tableUUID) <= beforeEmpty);
    }

    @Test
    public void testCooldownCanBeTurnedOff() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, "hive0",
                "partitioned_db", "orders");
        String tableUUID = table.getUUID();
        Triple<String, Database, Table> tableTriple = StatisticsUtils.getTableTripleByUUID(ctx, tableUUID);

        ConnectorAnalyzeTaskQueue queue = new ConnectorAnalyzeTaskQueue();
        queue.recordOutcome(tableUUID, Optional.of(statusWith(StatsConstants.ScheduleStatus.FAILED, tableUUID)), null);
        long saved = Config.connector_table_query_trigger_analyze_failure_backoff_max_second;
        try {
            Config.connector_table_query_trigger_analyze_failure_backoff_max_second = 0;
            Assertions.assertTrue(queue.addPendingTask(tableUUID,
                    new ConnectorAnalyzeTask(tableTriple, Sets.newHashSet("o_orderkey"))));
        } finally {
            Config.connector_table_query_trigger_analyze_failure_backoff_max_second = saved;
        }
    }
}
