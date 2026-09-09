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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class LoadsSystemTable {
    public static final String NAME = "loads";
    private static final Logger LOG = LogManager.getLogger(LoadsSystemTable.class);

    public static SystemTable create() {
        return new SystemTable(SystemId.LOADS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("ID", IntegerType.BIGINT)
                        .column("LABEL", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PROFILE_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DB_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("USER", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("WAREHOUSE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PROGRESS", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PRIORITY", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SCAN_ROWS", IntegerType.BIGINT)
                        .column("SCAN_BYTES", IntegerType.BIGINT)
                        .column("FILTERED_ROWS", IntegerType.BIGINT)
                        .column("UNSELECTED_ROWS", IntegerType.BIGINT)
                        .column("SINK_ROWS", IntegerType.BIGINT)
                        .column("RUNTIME_DETAILS", JsonType.JSON)
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("LOAD_START_TIME", DateType.DATETIME)
                        .column("LOAD_COMMIT_TIME", DateType.DATETIME)
                        .column("LOAD_FINISH_TIME", DateType.DATETIME)
                        .column("PROPERTIES", JsonType.JSON)
                        .column("ERROR_MSG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("REJECTED_RECORD_PATH", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("JOB_ID", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_LOADS);
    }

    public static TGetLoadsResult query(TGetLoadsParams request) {
        TGetLoadsResult result = new TGetLoadsResult();
        List<TLoadInfo> loads = Lists.newArrayList();
        try {
            LoadRequestFilter filter = LoadRequestFilter.from(request);

            boolean paged = request.isSetStart_job_id_offset();
            long startJobIdOffset = paged ? request.getStart_job_id_offset() : 0;

            // Load job ids and stream load task ids are both drawn from the one global id
            // allocator, so merging the two managers and sorting by id gives the stable total
            // order a cursor needs.
            List<Candidate> candidates = Lists.newArrayList();
            collectLoadJobs(candidates, request, filter, startJobIdOffset);
            collectStreamLoadTasks(candidates, request, filter, startJobIdOffset);
            candidates.sort(Comparator.comparingLong(Candidate::id));

            for (Candidate candidate : candidates) {
                loads.addAll(candidate.rows().get());
                // Cut the page at a job boundary so one job's rows never straddle two pages.
                if (paged && loads.size() >= Config.max_get_loads_result_count) {
                    result.setNext_job_id_offset(candidate.id() + 1);
                    LOG.info("getLoads page is full, returned rows: {}, next_job_id_offset: {}",
                            loads.size(), candidate.id() + 1);
                    break;
                }
            }
            result.setLoads(loads);
        } catch (Exception e) {
            LOG.warn("Failed to query information_schema.loads", e);
            throw e;
        }
        return result;
    }

    public interface Job {
        long getDbId();

        String getUser();

        String getStateName();

        boolean matchTableName(String tableName);

        Long getCreateTimeMs();

        Long getLoadStartTimeMs();

        Long getLoadFinishTimeMs();
    }

    /**
     * One page unit: a job id to order and resume by, plus a lazy generator for its rows.
     * The rows stay unmaterialized so a candidate past the page cut costs nothing.
     */
    private record Candidate(long id, Supplier<List<TLoadInfo>> rows) {
    }

    private static void collectLoadJobs(List<Candidate> candidates, TGetLoadsParams request,
                                        LoadRequestFilter filter, long startJobIdOffset) {
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        if (request.isSetJob_id()) {
            addLoadJob(candidates, loadMgr.getLoadJob(request.getJob_id()), filter, startJobIdOffset);
        } else if (filter.dbId != null) {
            List<LoadJob> loadJobs;
            if (request.isSetLabel()) {
                loadJobs = loadMgr.getLoadJobsByDb(filter.dbId, request.getLabel(), true);
            } else {
                loadJobs = loadMgr.getLoadJobsByDb(filter.dbId, null, false);
            }
            for (LoadJob loadJob : loadJobs) {
                addLoadJob(candidates, loadJob, filter, startJobIdOffset);
            }
        } else {
            String label = request.isSetLabel() ? request.getLabel() : null;
            for (LoadJob loadJob : loadMgr.getLoadJobs(label)) {
                addLoadJob(candidates, loadJob, filter, startJobIdOffset);
            }
        }
    }

    private static void addLoadJob(List<Candidate> candidates, LoadJob loadJob,
                                   LoadRequestFilter filter, long startJobIdOffset) {
        if (loadJob == null || loadJob.getId() < startJobIdOffset || !matchFilter(loadJob, filter)) {
            return;
        }
        candidates.add(new Candidate(loadJob.getId(), () -> Lists.newArrayList(loadJob.toThrift())));
    }

    private static void collectStreamLoadTasks(List<Candidate> candidates, TGetLoadsParams request,
                                               LoadRequestFilter filter, long startJobIdOffset) {
        StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        if (request.isSetJob_id()) {
            addStreamLoadTask(candidates, streamLoadMgr.getTaskById(request.getJob_id()), filter, startJobIdOffset);
            return;
        }
        List<AbstractStreamLoadTask> streamLoadTaskList = streamLoadMgr.getTaskByName(request.getLabel());
        if (streamLoadTaskList == null) {
            return;
        }
        for (AbstractStreamLoadTask streamLoadTask : streamLoadTaskList) {
            addStreamLoadTask(candidates, streamLoadTask, filter, startJobIdOffset);
        }
    }

    private static void addStreamLoadTask(List<Candidate> candidates, AbstractStreamLoadTask task,
                                          LoadRequestFilter filter, long startJobIdOffset) {
        if (task == null || task.getId() < startJobIdOffset) {
            return;
        }
        if (!(task instanceof StreamLoadMultiStmtTask multiTask)) {
            if (matchFilter(task, filter)) {
                candidates.add(new Candidate(task.getId(), task::toThrift));
            }
            return;
        }
        // Children are still filtered one by one, but the multi-statement task is a single
        // page unit keyed by the parent id. Children are allocated after their parent, so
        // resuming at parentId + 1 clears the parent and every child it owns.
        List<AbstractStreamLoadTask> matched = Lists.newArrayList();
        for (AbstractStreamLoadTask childTask : multiTask.getTasks()) {
            if (matchFilter(childTask, filter)) {
                matched.add(childTask);
            }
        }
        if (matched.isEmpty()) {
            return;
        }
        candidates.add(new Candidate(task.getId(), () -> {
            List<TLoadInfo> rows = Lists.newArrayList();
            for (AbstractStreamLoadTask childTask : matched) {
                rows.addAll(childTask.toThrift());
            }
            return rows;
        }));
    }

    private static boolean matchFilter(Job job, LoadRequestFilter filter) {
        if (filter.dbId != null && job.getDbId() != filter.dbId) {
            return false;
        }
        if (filter.user != null && !filter.user.equals(job.getUser())) {
            return false;
        }
        if (filter.state != null) {
            String stateName = job.getStateName();
            if (stateName != null && !filter.state.equalsIgnoreCase(stateName)) {
                return false;
            }
        }
        if (filter.tableName != null && !job.matchTableName(filter.tableName)) {
            return false;
        }
        if (!matchTimeRange(job.getLoadStartTimeMs(), filter.loadStartTimeFrom, filter.loadStartTimeTo)) {
            return false;
        }
        if (!matchTimeRange(job.getLoadFinishTimeMs(), filter.loadFinishTimeFrom, filter.loadFinishTimeTo)) {
            return false;
        }
        return matchTimeRange(job.getCreateTimeMs(), filter.createTimeFrom, filter.createTimeTo);
    }

    private static boolean matchTimeRange(Long value, Long lowerBound, Long upperBound) {
        if (lowerBound == null && upperBound == null) {
            return true;
        }
        if (value == null || value < 0) {
            return true;
        }
        if (lowerBound != null && value < lowerBound) {
            return false;
        }
        return upperBound == null || value <= upperBound;
    }

    @VisibleForTesting
    static class LoadRequestFilter {
        Long dbId;
        String tableName;
        String user;
        String state;
        Long loadStartTimeFrom;
        Long loadStartTimeTo;
        Long loadFinishTimeFrom;
        Long loadFinishTimeTo;
        Long createTimeFrom;
        Long createTimeTo;

        @VisibleForTesting
        static LoadRequestFilter from(TGetLoadsParams request) {
            LoadRequestFilter filter = new LoadRequestFilter();

            if (request.isSetDb() && !Strings.isNullOrEmpty(request.getDb())) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(request.getDb());
                if (db != null) {
                    filter.dbId = db.getId();
                }
            }
            if (request.isSetTable_name() && !Strings.isNullOrEmpty(request.getTable_name())) {
                filter.tableName = request.getTable_name();
            }
            if (request.isSetUser()) {
                filter.user = request.getUser();
            }
            if (request.isSetState()) {
                filter.state = request.getState();
            }

            // Prefer the new UTC epoch-ms fields when the BE sets them - those are
            // already comparable to LoadJob.finishTimestamp (System.currentTimeMillis).
            // Fall back to parsing the legacy wall-clock string fields only when an
            // older BE hasn't been upgraded yet; parse those in the fixed cluster
            // storage zone (+08:00) to match how the legacy BE produced them,
            // regardless of the current session time_zone.
            filter.loadStartTimeFrom = pickTime(
                    request.isSetLoad_start_time_from_ms() ? request.getLoad_start_time_from_ms() : null,
                    request.isSetLoad_start_time_from() ? request.getLoad_start_time_from() : null);
            filter.loadStartTimeTo = pickTime(
                    request.isSetLoad_start_time_to_ms() ? request.getLoad_start_time_to_ms() : null,
                    request.isSetLoad_start_time_to() ? request.getLoad_start_time_to() : null);
            filter.loadFinishTimeFrom = pickTime(
                    request.isSetLoad_finish_time_from_ms() ? request.getLoad_finish_time_from_ms() : null,
                    request.isSetLoad_finish_time_from() ? request.getLoad_finish_time_from() : null);
            filter.loadFinishTimeTo = pickTime(
                    request.isSetLoad_finish_time_to_ms() ? request.getLoad_finish_time_to_ms() : null,
                    request.isSetLoad_finish_time_to() ? request.getLoad_finish_time_to() : null);
            filter.createTimeFrom = pickTime(
                    request.isSetCreate_time_from_ms() ? request.getCreate_time_from_ms() : null,
                    request.isSetCreate_time_from() ? request.getCreate_time_from() : null);
            filter.createTimeTo = pickTime(
                    request.isSetCreate_time_to_ms() ? request.getCreate_time_to_ms() : null,
                    request.isSetCreate_time_to() ? request.getCreate_time_to() : null);
            return filter;
        }

        @VisibleForTesting
        static Long pickTime(Long msField, String legacyStringField) {
            if (msField != null) {
                return msField;
            }
            if (legacyStringField == null) {
                return null;
            }
            long ts = TimeUtils.timeStringToLongInStorageZone(legacyStringField);
            return ts >= 0 ? ts : null;
        }
    }

}
