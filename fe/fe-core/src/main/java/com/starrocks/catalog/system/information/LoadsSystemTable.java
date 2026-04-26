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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
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

import java.util.List;

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

            LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
            if (request.isSetJob_id()) {
                LoadJob loadJob = loadMgr.getLoadJob(request.getJob_id());
                appendLoadJob(loads, loadJob, request, filter);
            } else if (filter.dbId != null) {
                List<LoadJob> loadJobs;
                if (request.isSetLabel()) {
                    loadJobs = loadMgr.getLoadJobsByDb(filter.dbId, request.getLabel(), true);
                } else {
                    loadJobs = loadMgr.getLoadJobsByDb(filter.dbId, null, false);
                }
                for (LoadJob loadJob : loadJobs) {
                    appendLoadJob(loads, loadJob, request, filter);
                }
            } else {
                String label = request.isSetLabel() ? request.getLabel() : null;
                List<LoadJob> loadJobs = loadMgr.getLoadJobs(label);
                for (LoadJob loadJob : loadJobs) {
                    appendLoadJob(loads, loadJob, request, filter);
                }
            }

            StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
            if (request.isSetJob_id()) {
                AbstractStreamLoadTask streamLoadTask = streamLoadMgr.getTaskById(request.getJob_id());
                appendStreamLoadTask(loads, streamLoadTask, request, filter);
            } else {
                List<AbstractStreamLoadTask> streamLoadTaskList = streamLoadMgr.getTaskByName(request.getLabel());
                if (streamLoadTaskList != null) {
                    for (AbstractStreamLoadTask streamLoadTask : streamLoadTaskList) {
                        appendStreamLoadTask(loads, streamLoadTask, request, filter);
                    }
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

    private static void appendLoadJob(List<TLoadInfo> loads, LoadJob loadJob, TGetLoadsParams request,
                                      LoadRequestFilter filter) {
        if (loadJob == null || !matchFilter(loadJob, filter)) {
            return;
        }
        loads.add(loadJob.toThrift());
    }

    private static void appendStreamLoadTask(List<TLoadInfo> loads, AbstractStreamLoadTask task, TGetLoadsParams request,
                                             LoadRequestFilter filter) {
        if (task == null) {
            return;
        }

        if (!(task instanceof StreamLoadMultiStmtTask)) {
            if (matchFilter(task, filter)) {
                loads.addAll(task.toThrift());
            }
        } else {
            StreamLoadMultiStmtTask multiTask = (StreamLoadMultiStmtTask) task;
            for (AbstractStreamLoadTask childTask : multiTask.getTasks()) {
                if (matchFilter(childTask, filter)) {
                    loads.addAll(childTask.toThrift());
                }
            }
        }
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

    private static class LoadRequestFilter {
        private Long dbId;
        private String tableName;
        private String user;
        private String state;
        private Long loadStartTimeFrom;
        private Long loadStartTimeTo;
        private Long loadFinishTimeFrom;
        private Long loadFinishTimeTo;
        private Long createTimeFrom;
        private Long createTimeTo;

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
            filter.loadStartTimeFrom = parseTime(request.isSetLoad_start_time_from() ? request.getLoad_start_time_from() : null);
            filter.loadStartTimeTo = parseTime(request.isSetLoad_start_time_to() ? request.getLoad_start_time_to() : null);
            filter.loadFinishTimeFrom =
                    parseTime(request.isSetLoad_finish_time_from() ? request.getLoad_finish_time_from() : null);
            filter.loadFinishTimeTo = parseTime(request.isSetLoad_finish_time_to() ? request.getLoad_finish_time_to() : null);
            filter.createTimeFrom = parseTime(request.isSetCreate_time_from() ? request.getCreate_time_from() : null);
            filter.createTimeTo = parseTime(request.isSetCreate_time_to() ? request.getCreate_time_to() : null);
            return filter;
        }

        private static Long parseTime(String value) {
            if (value == null) {
                return null;
            }
            long ts = TimeUtils.timeStringToLong(value);
            return ts >= 0 ? ts : null;
        }
    }

}
