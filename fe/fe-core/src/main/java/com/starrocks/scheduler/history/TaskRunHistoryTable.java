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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TResultBatch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * History storage that leverage a regular starrocks table to store the data.
 * By default, using the time-based garbage collection strategy, which keep 7days' history
 */
public class TaskRunHistoryTable {

    public static final int INSERT_BATCH_SIZE = 128;
    private static final int DEFAULT_RETENTION_DAYS = 7;
    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;
    public static final String TABLE_NAME = "task_run_history";
    public static final String TABLE_FULL_NAME = DATABASE_NAME + "." + TABLE_NAME;
    public static final String CREATE_TABLE =
            String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    // identifiers
                    "task_id bigint NOT NULL, " +
                    "task_run_id string NOT NULL, " +
                    "create_time datetime NOT NULL, " +

                    // indexed columns
                    "task_name string NOT NULL, " +
                            "task_state STRING NOT NULL, " +

                    // times
                    "finish_time datetime NOT NULL, " +
                    "expire_time datetime NOT NULL, " +

                    // content in JSON format
                    "history_content_json JSON NOT NULL" +
                    ")" +

                    // properties
                    "PRIMARY KEY (task_id, task_run_id, create_time) " +
                    "PARTITION BY date_trunc('DAY', create_time) " +
                    "DISTRIBUTED BY HASH(task_id) BUCKETS 8 " +
                            "PROPERTIES( " +
                            "'replication_num' = '1', " +
                            "'partition_live_number' = '" + DEFAULT_RETENTION_DAYS + "'" +
                            ")",
                    TABLE_FULL_NAME);

    private static final String CONTENT_COLUMN = "history_content_json";
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO {0} " +
            "(task_id, task_run_id, task_name, task_state, " +
            "create_time, finish_time, expire_time, " +
            "history_content_json) " +
            "VALUES";
    private static final String INSERT_SQL_VALUE = "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})";
    private static final String LOOKUP =
            "SELECT history_content_json " + "FROM " + TABLE_FULL_NAME + " WHERE ";

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private static final TableKeeper KEEPER =
            new TableKeeper(DATABASE_NAME, TABLE_NAME, CREATE_TABLE,
                    () -> Math.max(1, Config.task_runs_ttl_second / 3600 / 24));

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    private void checkTableReady() {
        if (!FeConstants.runningUnitTest && !KEEPER.isReady()) {
            throw new IllegalStateException("The table is not ready: " + TABLE_NAME);
        }
    }

    public void addHistory(TaskRunStatus status) {
        addHistories(Lists.newArrayList(status));
    }

    public void addHistories(List<TaskRunStatus> histories) {
        checkTableReady();

        List<List<TaskRunStatus>> batches = ListUtils.partition(histories, INSERT_BATCH_SIZE);
        for (var batch : batches) {
            String insert = MessageFormat.format(INSERT_SQL_TEMPLATE, TABLE_FULL_NAME);
            String values = batch.stream().map(status -> {
                String createTime =
                        Strings.quote(DateUtils.formatTimeStampInMill(status.getCreateTime(), ZoneId.systemDefault()));
                String finishTime =
                        Strings.quote(DateUtils.formatTimeStampInMill(status.getFinishTime(), ZoneId.systemDefault()));
                String expireTime =
                        Strings.quote(DateUtils.formatTimeStampInMill(status.getExpireTime(), ZoneId.systemDefault()));

                // To be compatible with old task runs, filter out unnecessary properties to avoid json content too large
                // and deserialize error.
                removeUnnecessaryProperties(status);

                // Since the content is stored in JSON format and insert into the starrocks olap table, we need to escape the
                // content to make it safer in deserializing the json.
                return MessageFormat.format(INSERT_SQL_VALUE,
                        String.valueOf(status.getTaskId()),
                        Strings.quote(status.getQueryId()),
                        Strings.quote(status.getTaskName()),
                        Strings.quote(status.getState().toString()),
                        createTime,
                        finishTime,
                        expireTime,
                        Strings.quote(StringEscapeUtils.escapeJava(status.toJSON())));
            }).collect(Collectors.joining(", "));

            String sql = insert + values;
            SimpleExecutor.getRepoExecutor().executeDML(sql);
        }
    }

    /**
     * Normalize the task run status, remove unnecessary properties
     * @param status
     */
    private void removeUnnecessaryProperties(TaskRunStatus status) {
        //  remove unnecessary properties
        if (status.getProperties() != null) {
            removeUnnecessaryProperties(status.getProperties());
        }
        // remove unnecessary properties in mvTaskRunExtraMessage
        if (status.getMvTaskRunExtraMessage() != null) {
            ExecuteOption executeOption = status.getMvTaskRunExtraMessage().getExecuteOption();
            if (executeOption != null) {
                removeUnnecessaryProperties(executeOption.getTaskRunProperties());
            }
        }
    }

    /**
     * Only keep the properties that are necessary for task run
     * @param properties
     */
    private void removeUnnecessaryProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }
        Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            if (!TaskRun.TASK_RUN_PROPERTIES.contains(iterator.next().getKey())) {
                iterator.remove();
            }
        }
    }

    public List<TaskRunStatus> lookup(TGetTasksParams params) {
        if (params == null) {
            return Lists.newArrayList();
        }
        String sql = LOOKUP;
        List<String> predicates = Lists.newArrayList("TRUE");
        if (StringUtils.isNotEmpty(params.getDb())) {
            predicates.add(" get_json_string(" + CONTENT_COLUMN + ", 'dbName') = "
                    + Strings.quote(ClusterNamespace.getFullName(params.getDb())));
        }
        if (StringUtils.isNotEmpty(params.getTask_name())) {
            predicates.add(" task_name = " + Strings.quote(params.getTask_name()));
        }
        if (StringUtils.isNotEmpty(params.getQuery_id())) {
            predicates.add(" task_run_id = " + Strings.quote(params.getQuery_id()));
        }
        if (StringUtils.isNotEmpty(params.getState())) {
            predicates.add(" task_state = " + Strings.quote(params.getState()));
        }
        sql += Joiner.on(" AND ").join(predicates);

        List<TResultBatch> batch = SimpleExecutor.getRepoExecutor().executeDQL(sql);
        List<TaskRunStatus> result = TaskRunStatus.fromResultBatch(batch);
        // sort results by create time desc to make the result more stable.
        Collections.sort(result, TaskRunStatus.COMPARATOR_BY_CREATE_TIME_DESC);
        return result;
    }

    public List<TaskRunStatus> lookupByTaskNames(String dbName, Set<String> taskNames) {
        List<String> predicates = Lists.newArrayList("TRUE");
        if (StringUtils.isNotEmpty(dbName)) {
            predicates.add(" get_json_string(" + CONTENT_COLUMN + ", 'dbName') = "
                    + Strings.quote(ClusterNamespace.getFullName(dbName)));
        }
        if (CollectionUtils.isNotEmpty(taskNames)) {
            String values = taskNames.stream().sorted().map(Strings::quote).collect(Collectors.joining(","));
            predicates.add(" task_name IN (" + values + ")");
        }

        String sql = LOOKUP + Joiner.on(" AND ").join(predicates);
        List<TResultBatch> batch = SimpleExecutor.getRepoExecutor().executeDQL(sql);
        List<TaskRunStatus> result = TaskRunStatus.fromResultBatch(batch);
        // sort results by create time desc to make the result more stable.
        Collections.sort(result, TaskRunStatus.COMPARATOR_BY_CREATE_TIME_DESC);
        return result;
    }

    public List<TaskRunStatus> lookupLastJobOfTasks(String dbName, Set<String> taskNames) {
        final String template =
                "WITH MaxStartRunID AS (" +
                        "    SELECT " +
                        "       task_name, " +
                        "       cast(history_content_json->'startTaskRunId' as string) start_run_id" +
                        "    FROM $tableName " +
                        "    WHERE (task_name, create_time) IN (" +
                        "            SELECT task_name, MAX(create_time)" +
                        "            FROM $tableName" +
                        "            WHERE $taskFilter" +
                        "            GROUP BY task_name" +
                        "    )" +
                        " )" +
                        " SELECT t.history_content_json" +
                        " FROM $tableName t" +
                        " JOIN MaxStartRunID msr" +
                        "   ON t.task_name = msr.task_name" +
                        "   AND cast(t.history_content_json->'startTaskRunId' as string) = msr.start_run_id" +
                        " ORDER BY t.create_time DESC";

        List<String> predicates = Lists.newArrayList("TRUE");
        if (StringUtils.isNotEmpty(dbName)) {
            predicates.add(" get_json_string(" + CONTENT_COLUMN + ", 'dbName') = "
                    + Strings.quote(ClusterNamespace.getFullName(dbName)));
        }
        if (CollectionUtils.isNotEmpty(taskNames)) {
            String values = taskNames.stream().sorted().map(Strings::quote).collect(Collectors.joining(","));
            predicates.add(" task_name IN (" + values + ")");
        }
        String where = Joiner.on(" AND ").join(predicates);

        VelocityContext context = new VelocityContext();
        context.put("tableName", TABLE_FULL_NAME);
        context.put("taskFilter", where);

        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        String sql = sw.toString();

        List<TResultBatch> batch = SimpleExecutor.getRepoExecutor().executeDQL(sql);
        return TaskRunStatus.fromResultBatch(batch);
    }
}
