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

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.starrocks.common.util.DateUtils;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.util.Strings;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.util.List;

/**
 * History storage that leverage a regular starrocks table to store the data.
 * By default, using the time-based garbage collection strategy, which keep 7days' history
 */
public class TableBasedTaskRunHistory implements TaskRunHistory {

    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "task_run_history";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;
    public static final int TABLE_REPLICAS = 3;
    public static final String CREATE_TABLE =
            String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    // identifiers
                    "task_id bigint NOT NULL, " +
                    "task_run_id string NOT NULL, " +
                    "create_time datetime NOT NULL, " +
                    "task_name string NOT NULL, " +

                    // times
                    "finish_time datetime NOT NULL, " +
                    "expire_time datetime NOT NULL, " +

                    // content in JSON format
                    "history_content_json JSON NOT NULL" +
                    ")" +

                    // properties
                    "PRIMARY KEY (task_id, task_run_id, create_time) " +
                    "PARTITION BY RANGE(create_time)() " +
                    "DISTRIBUTED BY HASH(task_id) BUCKETS 8 " +
                    "PROPERTIES(" +
                    "'replication_num' = '1'," +
                    "'dynamic_partition.time_unit' = 'DAY', " +
                    "'dynamic_partition.start' = '-7', " +
                    "'dynamic_partition.end' = '3', " +
                    "'dynamic_partition.prefix' = 'p' " +
                    ") ", TABLE_FULL_NAME);

    private static final String COLUMN_LIST = "task_id, task_run_id, task_name, " +
            "create_time, finish_time, expire_time, " +
            "history_content_json ";
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO {0} " +
            "(task_id, task_run_id, task_name, " +
            "create_time, finish_time, expire_time, " +
            "history_content_json) " +
            "VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})";
    private static final String SELECT_BY_TASK_NAME =
            "SELECT history_content_json FROM " + TABLE_FULL_NAME + " WHERE task_name = {0}";
    private static final String SELECT_ALL = "SELECT history_content_json FROM " + TABLE_FULL_NAME;
    private static final String COUNT_TASK_RUNS = "SELECT count(*) as cnt FROM " + TABLE_FULL_NAME;

    public static TableKeeper createKeeper() {
        return new TableKeeper(DATABASE_NAME, TABLE_NAME, CREATE_TABLE, TABLE_REPLICAS);
    }

    @Override
    public void addHistory(TaskRunStatus status, boolean isReplay) {
        if (isReplay) {
            return;
        }

        String createTime =
                Strings.quote(DateUtils.formatTimeStampInMill(status.getCreateTime(), ZoneId.systemDefault()));
        String finishTime =
                Strings.quote(DateUtils.formatTimeStampInMill(status.getFinishTime(), ZoneId.systemDefault()));
        String expireTime =
                Strings.quote(DateUtils.formatTimeStampInMill(status.getExpireTime(), ZoneId.systemDefault()));

        final String sql = MessageFormat.format(INSERT_SQL_TEMPLATE, TABLE_FULL_NAME,
                String.valueOf(status.getTaskId()), Strings.quote(status.getStartTaskRunId()),
                Strings.quote(status.getTaskName()),
                createTime, finishTime, expireTime, Strings.quote(status.toJSON()));
        RepoExecutor.getInstance().executeDML(sql);
    }

    @Override
    public List<TaskRunStatus> getTaskByName(String taskName) {
        final String sql = MessageFormat.format(SELECT_BY_TASK_NAME, Strings.quote(taskName));
        List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(sql);
        return TaskRunStatus.fromResultBatch(batch);
    }

    @Override
    public void replayTaskRunChange(String queryId, TaskRunStatusChange change) {
    }

    @Override
    public void gc() {
        // TableBasedHistory could do the gc by itself
    }

    @Override
    public void forceGC() {
        // TODO
    }

    // TODO: don't return all history, which is very expensive
    @Override
    public List<TaskRunStatus> getAllHistory() {
        List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(SELECT_ALL);
        return TaskRunStatus.fromResultBatch(batch);
    }

    @Override
    public long getTaskRunCount() {
        List<TResultBatch> batches = RepoExecutor.getInstance().executeDQL(COUNT_TASK_RUNS);
        if (CollectionUtils.isNotEmpty(batches)) {
            try {
                ByteBuffer buffer = batches.get(0).getRows().get(0);
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                JsonArray obj = (JsonArray) JsonParser.parseString(jsonString);
                return obj.get(0).getAsInt();
            } catch (Exception e) {
                throw new IllegalStateException("failed to parse json result: " + batches);
            }
        }
        return 0;
    }

}
