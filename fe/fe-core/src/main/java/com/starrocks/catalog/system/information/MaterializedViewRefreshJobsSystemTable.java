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

package com.starrocks.catalog.system.information;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.qe.ShowMaterializedViewStatus.RefreshJobStatus;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TListMaterializedViewRefreshJobsResult;
import com.starrocks.thrift.TMaterializedViewRefreshJobInfo;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TException;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MaterializedViewRefreshJobsSystemTable extends SystemTable {
    public static final String NAME = "materialized_view_refresh_jobs";

    public MaterializedViewRefreshJobsSystemTable() {
        super(SystemId.MATERIALIZED_VIEW_REFRESH_JOBS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("JOB_ID", TypeFactory.createVarcharType(64))
                        .column("MATERIALIZED_VIEW_ID", IntegerType.BIGINT)
                        .column("TABLE_SCHEMA", TypeFactory.createVarcharType(64))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(64))
                        .column("TASK_ID", IntegerType.BIGINT)
                        .column("WAREHOUSE", TypeFactory.createVarcharType(128))
                        .column("RESOURCE_GROUP", TypeFactory.createVarcharType(128))
                        .column("CREATOR", TypeFactory.createVarcharType(64))
                        .column("SUBMIT_USER", TypeFactory.createVarcharType(64))
                        .column("RUN_AS_USER", TypeFactory.createVarcharType(128))
                        .column("SUBMIT_TIME", DateType.DATETIME)
                        .column("REFRESH_STATE", TypeFactory.createVarcharType(20))
                        .column("FINISH_TIME", DateType.DATETIME)
                        .column("DURATION_TIME", FloatType.DOUBLE)
                        .column("REFRESH_TRIGGER", TypeFactory.createVarcharType(24))
                        .column("REFRESH_MODE", TypeFactory.createVarcharType(16))
                        .column("IMV_SOURCE_VERSION_RANGE", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("IMV_SOURCE_TIMESTAMP_RANGE", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("FAILED_TASK_RUN_ID", TypeFactory.createVarcharType(64))
                        .column("FAILED_QUERY_ID", TypeFactory.createVarcharType(64))
                        .column("ERROR_CODE", TypeFactory.createVarcharType(20))
                        .column("ERROR_MESSAGE", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .build(), TSchemaTableType.SCH_MATERIALIZED_VIEW_REFRESH_JOBS);
    }

    public static SystemTable create() {
        return new MaterializedViewRefreshJobsSystemTable();
    }

    public static TListMaterializedViewRefreshJobsResult query(TGetTasksParams params, ConnectContext context)
            throws TException {
        TListMaterializedViewRefreshJobsResult result = new TListMaterializedViewRefreshJobsResult();
        List<TMaterializedViewRefreshJobInfo> jobs = Lists.newArrayList();
        result.setJobs(jobs);

        if (params.isSetCurrent_user_ident()) {
            UserIdentityUtils.setAuthInfoFromThrift(context, params.current_user_ident);
        }

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        List<TaskRunStatus> runs = taskManager.getMatchedTaskRunStatus(params);

        Map<String, List<TaskRunStatus>> jobToRuns = new LinkedHashMap<>();
        for (TaskRunStatus run : runs) {
            if (run == null || run.getSource() != Constants.TaskSource.MV
                    || run.getState() == Constants.TaskRunState.MERGED || run.getDbName() == null) {
                continue;
            }
            String key = run.getStartTaskRunId();
            if (Strings.isNullOrEmpty(key)) {
                key = run.getTaskRunId();
            }
            if (Strings.isNullOrEmpty(key)) {
                continue;
            }
            jobToRuns.computeIfAbsent(key, k -> Lists.newArrayList()).add(run);
        }

        Map<Long, MaterializedView> mvCache = Maps.newHashMap();
        for (Map.Entry<String, List<TaskRunStatus>> entry : jobToRuns.entrySet()) {
            String key = entry.getKey();
            List<TaskRunStatus> batch = entry.getValue();
            // getMatchedTaskRunStatus concatenates pending/running/history and history is newest-first,
            // so encounter order is not chronological; sort ascending so "latest run wins" is well-defined.
            batch.sort(Comparator.comparingLong(TaskRunStatus::getProcessStartTime));
            TaskRunStatus anyRun = batch.get(0);

            Map<String, String> props = anyRun.getProperties();
            String mvIdStr = props == null ? null : props.get(TaskRun.MV_ID);
            if (mvIdStr == null) {
                continue;
            }
            long mvId;
            try {
                mvId = Long.parseLong(mvIdStr);
            } catch (NumberFormatException e) {
                continue;
            }
            String dbName = anyRun.getDbName();

            try {
                Authorizer.checkAnyActionOnOrInDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName);
            } catch (AccessDeniedException e) {
                continue;
            }

            MaterializedView mv = mvCache.computeIfAbsent(mvId, id -> lookupMv(dbName, id));
            RefreshJobStatus rjs = ShowMaterializedViewStatus.fromTaskRuns(batch);

            TMaterializedViewRefreshJobInfo info = new TMaterializedViewRefreshJobInfo();
            info.setJob_id(key);
            info.setMaterialized_view_id(String.valueOf(mvId));
            info.setTable_schema(dbName);
            if (mv != null) {
                info.setTable_name(mv.getName());
                info.setResource_group(mv.getResourceGroupString());
                info.setRefresh_mode(mv.getRefreshMode().name());
            }
            info.setTask_id(String.valueOf(anyRun.getTaskId()));
            if (anyRun.getWarehouseName() != null) {
                info.setWarehouse(anyRun.getWarehouseName());
            }
            info.setCreator(anyRun.getUser());
            info.setSubmit_user(anyRun.getSubmitUser());
            if (anyRun.getUserIdentity() != null) {
                info.setRun_as_user(anyRun.getUserIdentity().toString());
            }
            info.setSubmit_time(TimeUtils.longToTimeString(rjs.getMvRefreshStartTime()));
            info.setRefresh_state(String.valueOf(rjs.getRefreshState()));
            info.setRefresh_trigger(mv == null ? "UNKNOWN" : mv.getRefreshTriggerString());

            if (rjs.isRefreshFinished()) {
                info.setFinish_time(TimeUtils.longToTimeString(rjs.getMvRefreshEndTime()));
                // Wall-clock span of the whole job, not the per-run totalProcessDuration sum.
                long startBasis = rjs.getMvRefreshProcessTime() > 0
                        ? rjs.getMvRefreshProcessTime() : rjs.getMvRefreshStartTime();
                long wallMs = rjs.getMvRefreshEndTime() - startBasis;
                info.setDuration_time(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(wallMs / 1000D));
            }

            info.setImv_source_version_range(mergeToJson(batch, MVTaskRunExtraMessage::getImvSourceVersionRange));
            info.setImv_source_timestamp_range(mergeToJson(batch, MVTaskRunExtraMessage::getImvSourceTimestampRange));
            info.setImv_source_pinned_snapshot_id_map(mergeToJson(batch, MVTaskRunExtraMessage::getPinnedSnapshotIdMap));

            TaskRunStatus failed = batch.stream()
                    .filter(run -> run.getState() == Constants.TaskRunState.FAILED)
                    .reduce((first, second) -> second)   // latest failure: batch is sorted by processStartTime ascending
                    .orElse(null);
            if (failed != null) {
                info.setFailed_task_run_id(failed.getTaskRunId());
                info.setFailed_query_id(failed.getQueryId());
                info.setError_code(String.valueOf(failed.getErrorCode()));
                info.setError_message(failed.getErrorMessage());
            }

            jobs.add(info);
        }
        return result;
    }

    private static MaterializedView lookupMv(String dbName, long mvId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            return null;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId);
        return (table instanceof MaterializedView) ? (MaterializedView) table : null;
    }

    private static <V> String mergeToJson(List<TaskRunStatus> batch,
                                           Function<MVTaskRunExtraMessage, Map<String, V>> extractor) {
        Map<String, V> merged = Maps.newHashMap();
        for (TaskRunStatus run : batch) {
            MVTaskRunExtraMessage extra = run.getMvTaskRunExtraMessage();
            if (extra == null) {
                continue;
            }
            Map<String, V> part = extractor.apply(extra);
            if (part != null) {
                // batch is time-sorted, so a later run's value wins on key collision (latest run wins)
                merged.putAll(part);
            }
        }
        return GsonUtils.GSON.toJson(merged);
    }
}
