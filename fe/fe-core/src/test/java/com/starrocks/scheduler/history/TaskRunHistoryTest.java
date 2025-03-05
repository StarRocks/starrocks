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

package com.starrocks.scheduler.history;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TaskRunHistoryTest {

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testTaskRunStatusSerialization() {
        TaskRunStatus status = new TaskRunStatus();
        String json = status.toJSON();
        assertEquals("{\"taskId\":0,\"createTime\":0,\"expireTime\":0,\"priority\":0,\"mergeRedundant\":false," +
                "\"source\":\"CTAS\",\"errorCode\":0,\"finishTime\":0,\"processStartTime\":0,\"state\":\"PENDING\"," +
                "\"progress\":0,\"mvExtraMessage\":{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[]," +
                "\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{},\"processStartTime\":0," +
                "\"executeOption\":{\"priority\":0,\"taskRunProperties\":{},\"isMergeRedundant\":false,\"isManual\":false," +
                "\"isSync\":false,\"isReplay\":false},\"planBuilderMessage\":{}}}", json);

        TaskRunStatus b = TaskRunStatus.fromJson(json);
        assertEquals(status.toJSON(), b.toJSON());
    }

    @Test
    public void testCRUD(@Mocked RepoExecutor repo) {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId("aaa");
        status.setTaskName("t1");
        status.setState(Constants.TaskRunState.SUCCESS);
        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };
        String jsonString = StringEscapeUtils.escapeJava(GsonUtils.GSON.toJson(status));
        new Expectations() {
            {
                repo.executeDML(String.format("INSERT INTO _statistics_.task_run_history (task_id, task_run_id, task_name, " +
                        "task_state, create_time, finish_time, expire_time, history_content_json) " +
                        "VALUES(0, 'aaa', 't1', 'SUCCESS', '1970-01-01 08:00:00', " +
                        "'1970-01-01 08:00:00', '1970-01-01 08:00:00', " +
                        "'%s')", jsonString));
            }
        };

        TaskRunHistoryTable history = new TaskRunHistoryTable();
        history.addHistory(status);

        // lookup by params
        TGetTasksParams params = new TGetTasksParams();
        new Expectations() {
            {
                repo.executeDQL("SELECT history_content_json FROM _statistics_.task_run_history WHERE TRUE AND  " +
                        "get_json_string(history_content_json, 'dbName') = 'default_cluster:d1'");
            }
        };
        params.setDb("d1");
        history.lookup(params);

        new Expectations() {
            {
                repo.executeDQL("SELECT history_content_json FROM _statistics_.task_run_history WHERE TRUE AND  " +
                        "task_state = 'SUCCESS'");
            }
        };
        params.setDb(null);
        params.setState("SUCCESS");
        history.lookup(params);

        new Expectations() {
            {
                repo.executeDQL("SELECT history_content_json FROM _statistics_.task_run_history WHERE TRUE AND  " +
                        "task_name = 't1'");
            }
        };
        params.setDb(null);
        params.setState(null);
        params.setTask_name("t1");
        history.lookup(params);

        new Expectations() {
            {
                repo.executeDQL("SELECT history_content_json FROM _statistics_.task_run_history WHERE TRUE AND  " +
                        "task_run_id = 'q1'");
            }
        };
        params.setDb(null);
        params.setState(null);
        params.setTask_name(null);
        params.setQuery_id("q1");
        history.lookup(params);

        // lookup by task names
        String dbName = "";
        Set<String> taskNames = Set.of("t1", "t2");
        new Expectations() {
            {
                repo.executeDQL("SELECT history_content_json FROM _statistics_.task_run_history WHERE TRUE AND  " +
                        "task_name IN ('t1','t2')");
            }
        };
        history.lookupByTaskNames(dbName, taskNames);
    }

    @Test
    public void testKeeper(@Mocked RepoExecutor repo) {
        TableKeeper keeper = TaskRunHistoryTable.createKeeper();
        assertEquals(StatsConstants.STATISTICS_DB_NAME, keeper.getDatabaseName());
        assertEquals(TaskRunHistoryTable.TABLE_NAME, keeper.getTableName());
        assertEquals(TaskRunHistoryTable.CREATE_TABLE, keeper.getCreateTableSql());

        // database not exists
        new Expectations() {
            {
                keeper.checkDatabaseExists();
                result = false;
            }
        };
        keeper.run();
        assertFalse(keeper.checkTableExists());

        // create table
        new StatisticsMetaManager().createStatisticsTablesForTest();
        new Expectations() {
            {
                repo.executeDDL("CREATE TABLE IF NOT EXISTS _statistics_.task_run_history (task_id bigint NOT NULL, " +
                        "task_run_id string NOT NULL, create_time datetime NOT NULL, " +
                        "task_name string NOT NULL, task_state STRING NOT NULL, finish_time datetime NOT NULL, " +
                        "expire_time datetime NOT NULL, history_content_json JSON NOT NULL)PRIMARY KEY " +
                        "(task_id, task_run_id, create_time) PARTITION BY date_trunc('DAY', create_time) " +
                        "DISTRIBUTED BY HASH(task_id) BUCKETS 8 PROPERTIES( 'replication_num' = '1', " +
                        "'partition_live_number' = '7')");
            }
        };
        keeper.run();
        assertFalse(keeper.isTableCorrected());

        new MockUp<SystemInfoService>() {
            @Mock
            public int getTotalBackendNumber() {
                return 3;
            }
        };
        // correct table replicas
        keeper.run();
    }

    @Test
    public void testHistoryVacuum(@Mocked RepoExecutor repo) {
        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        TaskRunHistory history = new TaskRunHistory();

        // prepare
        long currentTime = 1720165070904L;
        TaskRunStatus run1 = new TaskRunStatus();
        run1.setExpireTime(currentTime - 1);
        run1.setQueryId("q1");
        run1.setTaskName("t1");
        run1.setState(Constants.TaskRunState.RUNNING);
        history.addHistory(run1);
        TaskRunStatus run2 = new TaskRunStatus();
        run2.setExpireTime(currentTime + 10000);
        run2.setQueryId("q2");
        run2.setTaskName("t2");
        run2.setState(Constants.TaskRunState.SUCCESS);
        history.addHistory(run2);
        assertEquals(2, history.getInMemoryHistory().size());

        history.vacuum();
        assertEquals(1, history.getInMemoryHistory().size());

        // vacuum failed
        new Expectations() {
            {
                repo.executeDML(anyString);
                result = new RuntimeException("insert failed");
            }
        };
        TaskRunStatus run3 = new TaskRunStatus();
        run3.setExpireTime(System.currentTimeMillis() + 10000);
        run3.setQueryId("q3");
        run3.setTaskName("t3");
        run3.setState(Constants.TaskRunState.SUCCESS);
        history.addHistory(run3);
        history.vacuum();
        assertEquals(2, history.getInMemoryHistory().size());
    }

    @Test
    public void testDisableArchiveHistory() {
        Config.enable_task_history_archive = false;
        TaskRunHistory history = new TaskRunHistory();

        // prepare
        long currentTime = 1720165070904L;
        TaskRunStatus run1 = new TaskRunStatus();
        run1.setExpireTime(currentTime - 1);
        run1.setQueryId("q1");
        run1.setTaskName("t1");
        run1.setState(Constants.TaskRunState.RUNNING);
        history.addHistory(run1);
        TaskRunStatus run2 = new TaskRunStatus();
        run2.setExpireTime(currentTime + 10000);
        run2.setQueryId("q2");
        run2.setTaskName("t2");
        run2.setState(Constants.TaskRunState.SUCCESS);
        history.addHistory(run2);
        assertEquals(2, history.getInMemoryHistory().size());

        // run, trigger the expiration
        history.vacuum();
        Assert.assertEquals(0, history.getInMemoryHistory().size());

        Config.enable_task_history_archive = true;
    }

    @Test
    public void testLookBadHistory() {
        try {
            String jsonString = "{\"data\":[{\"createTime\": 1723679736257, \"dbName\": " +
                    "\"default_cluster:test_rename_columnb190692a_5a98_11ef_aa5e_00163e0e489a\", \"errorCode\": 0, " +
                    "\"expireTime\": 1723765136257, \"finishTime\": 1723679737239, \"mergeRedundant\": true, " +
                    "\"mvExtraMessage\": {\"basePartitionsToRefreshMap\": {\"t1\": [\"p3\"]}, \"executeOption\": " +
                    "{\"isManual\": false, \"isMergeRedundant\": true, \"isReplay\": false, \"isSync\": false, " +
                    "\"priority\": 100, \"taskRunProperties\": {\"PARTITION_END\": \"4\", " +
                    "\"PARTITION_START\": \"3\", \"START_TASK_RUN_ID\": \"b1f755a8-5a98-11ef-9acf-00163e08a129\", " +
                    "\"compression\": \"LZ4\", \"datacache\": {\"enable\": \"true\"}, \"enable_async_write_back\": " +
                    "\"false\", \"mvId\": \"490884\", \"replicated_storage\": \"true\", \"replication_num\": \"1\"}}, " +
                    "\"forceRefresh\": false, \"mvPartitionsToRefresh\": [\"p3\"], \"partitionEnd\": \"4\", " +
                    "\"partitionStart\": \"3\", \"processStartTime\": 1723679736946, \"refBasePartitionsToRefreshMap\": " +
                    "{\"t1\": [\"p3\"]}}, \"postRun\": \"ANALYZE SAMPLE TABLE mv1 WITH ASYNC MODE\", \"priority\": 100, " +
                    "\"processStartTime\": 1723679736946, \"progress\": 100, \"properties\": " +
                    "{\"PARTITION_END\": \"4\", \"PARTITION_START\": \"3\", \"START_TASK_RUN_ID\": " +
                    "\"b1f755a8-5a98-11ef-9acf-00163e08a129\", \"compression\": \"LZ4\", \"datacache\": " +
                    "{\"enable\": \"true\"}, \"enable_async_write_back\": \"false\", \"mvId\": \"490884\", " +
                    "\"replicated_storage\": \"true\", \"replication_num\": \"1\"}, \"queryId\": " +
                    "\"b35dc350-5a98-11ef-9acf-00163e08a129\", \"source\": \"MV\", \"startTaskRunId\": " +
                    "\"b1f755a8-5a98-11ef-9acf-00163e08a129\", \"state\": \"SUCCESS\", \"taskId\": 490886, " +
                    "\"taskName\": \"mv-490884\", \"user\": \"root\", \"userIdentity\": {\"host\": \"%\", " +
                    "\"isDomain\": false, \"user\": \"root\"}}]}";
            List<TaskRunStatus> ans = TaskRunStatus.TaskRunStatusJSONRecord.fromJson(jsonString).data;
            Preconditions.checkArgument(ans == null);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Expected a string but was BEGIN_OBJECT at line 1 column 568"));
        }
    }

    @Test
    public void testTaskRunStatusSerDer() {
        TaskRunStatus status = new TaskRunStatus();
        Map<String, String> properties = new HashMap<>();
        properties.put("datacache", "{\"enable\": \"true\"}");
        status.setProperties(properties);
        String json = GsonUtils.GSON.toJson(status);
        System.out.println(json);
        TaskRunStatus dst = GsonUtils.GSON.fromJson(json, TaskRunStatus.class);
        System.out.println(dst);
        Assert.assertEquals(json, GsonUtils.GSON.toJson(status));
    }
    @Test
    public void testCase1() {
        TaskRunStatus status = new TaskRunStatus();
        Map<String, String> properties = new HashMap<>();
        properties.put("datacache", "{\"enable\": \"true\"}");
        status.setProperties(properties);
        String json = GsonUtils.GSON.toJson(status);
        System.out.println(json);
        String res = MessageFormat.format("{0}", Strings.quote(status.toJSON()));
        System.out.println(res);
        Assert.assertTrue(res.contains("\"datacache\":\"{\\\"enable\\\": \\\"true\\\"}\""));
    }

    private TaskRunStatus createTaskRunStatus(long createdTime) {
        TaskRunStatus status = new TaskRunStatus();
        status.setCreateTime(createdTime);
        status.setExpireTime(createdTime + 10000);
        status.setQueryId("q1");
        status.setTaskName("t1");
        status.setState(Constants.TaskRunState.SUCCESS);
        return status;
    }

    @Test
    public void testLookByTaskNamesOrder(@Mocked RepoExecutor repo) {
        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        TaskRunHistoryTable history = new TaskRunHistoryTable();
        List<TaskRunStatus> taskRuns = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            TaskRunStatus status = createTaskRunStatus(i);
            taskRuns.add(status);
        }
        // shuffle the taskRuns' order
        Collections.shuffle(taskRuns);
        new MockUp<RepoExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                TaskRunStatus.TaskRunStatusJSONRecord record = new TaskRunStatus.TaskRunStatusJSONRecord();
                record.data = taskRuns;
                String json = GsonUtils.GSON.toJson(record);

                TResultBatch resultBatch = new TResultBatch();
                ByteBuffer buffer = ByteBuffer.wrap(json.getBytes());
                resultBatch.setRows(Lists.newArrayList(buffer));
                return Lists.newArrayList(resultBatch);
            }
        };
        // lookup by task names
        String dbName = "";
        Set<String> taskNames = Set.of("t1", "t2");
        List<TaskRunStatus> result = history.lookupByTaskNames(dbName, taskNames);
        Assert.assertEquals(10, result.size());
        // result always sorted by createTime desc
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(9 - i, result.get(i).getCreateTime());
        }
    }

    @Test
    public void testLookOrder(@Mocked RepoExecutor repo) {
        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };

        TaskRunHistoryTable history = new TaskRunHistoryTable();
        List<TaskRunStatus> taskRuns = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            TaskRunStatus status = createTaskRunStatus(i);
            taskRuns.add(status);
        }

        // shuffle the taskRuns' order
        Collections.shuffle(taskRuns);

        new MockUp<RepoExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                TaskRunStatus.TaskRunStatusJSONRecord record = new TaskRunStatus.TaskRunStatusJSONRecord();
                record.data = taskRuns;
                String json = GsonUtils.GSON.toJson(record);
                TResultBatch resultBatch = new TResultBatch();
                ByteBuffer buffer = ByteBuffer.wrap(json.getBytes());
                resultBatch.setRows(Lists.newArrayList(buffer));
                return Lists.newArrayList(resultBatch);
            }
        };
        // lookup by params
        TGetTasksParams params = new TGetTasksParams();
        params.setDb(null);
        params.setState(null);
        params.setTask_name("t1");
        List<TaskRunStatus> result = history.lookup(params);
        Assert.assertEquals(10, result.size());
        // result always sorted by createTime desc
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(9 - i, result.get(i).getCreateTime());
        }
    }
}
