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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResultBatch;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableBasedTaskRunHistoryTest {

    @BeforeAll
    public static void beforeAll() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testTaskRunStatusSerialization() {
        TaskRunStatus status = new TaskRunStatus();
        String json = status.toJSON();
        assertEquals("{\"taskId\":0,\"createTime\":0,\"finishTime\":0,\"processStartTime\":0," +
                "\"state\":\"PENDING\",\"progress\":0,\"errorCode\":0,\"expireTime\":0,\"priority\":0," +
                "\"mergeRedundant\":false,\"source\":\"CTAS\",\"mvExtraMessage\":" +
                "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[],\"refBasePartitionsToRefreshMap\":{}," +
                "\"basePartitionsToRefreshMap\":{},\"executeOption\":{\"priority\":0,\"isMergeRedundant\":false," +
                "\"isManual\":false,\"isSync\":false,\"isReplay\":false}}}", json);

        TaskRunStatus b = TaskRunStatus.fromJson(json);
        assertEquals(status.toJSON(), b.toJSON());
    }

    @Test
    public void testCRUD(@Mocked RepoExecutor repo) {
        new Expectations() {{
            repo.executeDML(
                    "INSERT INTO _statistics_.task_run_history (task_id, task_run_id, task_name, create_time, " +
                            "finish_time, expire_time, history_content_json) VALUES(0, 'aaa', 't1', " +
                            "'1970-01-01 08:00:00', '1970-01-01 08:00:00', '1970-01-01 08:00:00', " +
                            "'{\"startTaskRunId\":\"aaa\",\"taskId\":0,\"taskName\":\"t1\",\"createTime\":0," +
                            "\"finishTime\":0,\"processStartTime\":0,\"state\":\"PENDING\",\"progress\":0," +
                            "\"errorCode\":0,\"expireTime\":0,\"priority\":0,\"mergeRedundant\":false," +
                            "\"source\":\"CTAS\",\"mvExtraMessage\":{\"forceRefresh\":false,\"mvPartitionsToRefresh\":" +
                            "[],\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{}," +
                            "\"executeOption\":{\"priority\":0,\"isMergeRedundant\":false,\"isManual\":false," +
                            "\"isSync\":false,\"isReplay\":false}}}')");

        }};

        TableBasedTaskRunHistory history = new TableBasedTaskRunHistory();
        TaskRunStatus status = new TaskRunStatus();
        status.setStartTaskRunId("aaa");
        status.setTaskName("t1");
        history.addHistory(status);

        // getTaskByName
        new Expectations() {{
            repo.executeDQL("SELECT task_id, task_run_id, task_name, create_time, finish_time, expire_time, " +
                    "history_content_json  FROM _statistics_.task_run_history WHERE task_name = 't1'");
        }};
        history.getTaskByName("t1");

        // getTask
        new Expectations() {{
            repo.executeDQL("SELECT task_id, task_run_id, task_name, create_time, finish_time, expire_time, " +
                    "history_content_json  FROM _statistics_.task_run_history WHERE task_run_id = 't1'");
        }};
        history.getTask("t1");

        // getAllHistory
        new Expectations() {{
            repo.executeDQL("SELECT task_id, task_run_id, task_name, create_time, finish_time, expire_time, " +
                    "history_content_json  FROM _statistics_.task_run_history");
        }};
        history.getAllHistory();

        // getTaskRunCount
        TResultBatch batch = new TResultBatch();
        batch.setRows(Lists.newArrayList(ByteBuffer.wrap("[123]".getBytes())));
        List<TResultBatch> resultBatch = Lists.newArrayList(batch);
        new Expectations() {{
            repo.executeDQL("SELECT count(*) as cnt FROM _statistics_.task_run_history");
            result = resultBatch;
        }};
        assertEquals(123, history.getTaskRunCount());
    }

    @Test
    public void testKeeper(@Mocked RepoExecutor repo) {
        TableKeeper keeper = TableBasedTaskRunHistory.createKeeper();
        assertEquals(StatsConstants.STATISTICS_DB_NAME, keeper.getDatabaseName());
        assertEquals(TableBasedTaskRunHistory.TABLE_NAME, keeper.getTableName());
        assertEquals(TableBasedTaskRunHistory.CREATE_TABLE, keeper.getCreateTableSql());
        assertEquals(TableBasedTaskRunHistory.TABLE_REPLICAS, keeper.getTableReplicas());

        // database not exists
        new Expectations() {{
            keeper.checkDatabaseExists();
            result = false;
        }};
        keeper.run();
        assertFalse(keeper.isDatabaseExisted());

        // create table
        keeper.setDatabaseExisted(true);
        new Expectations() {{
            repo.executeDDL(
                    "CREATE TABLE task_run_history (id BIGINT NOT NULL auto_increment, task_id bigint NOT NULL, task_run_id string NOT NULL, task_name string NOT NULL, create_time datetime NOT NULL, finish_time datetime NOT NULL, expire_time datetime NOT NULL, history_content_json JSON NOT NULL)PRIMARY KEY (task_id, task_run_id) PARTITION BY RANGE(create_time) DISTRIBUTED BY HASH(id) BUCKETS 8 PROPERTIES('replication_num' = '1','dynamic_partition.time_unit' = 'DAY', 'dynamic_partition.start' = '-7', 'dynamic_partition.end' = '3', 'dynamic_partition.prefix' = 'p' ) ");
        }};
        keeper.run();
        assertTrue(keeper.isTableExisted());
        assertFalse(keeper.isTableCorrected());

        new MockUp<SystemInfoService>() {
            @Mock
            public int getTotalBackendNumber() {
                return 3;
            }
        };
        // correct table replicas
        keeper.run();
        assertTrue(keeper.isTableCorrected());
    }

}