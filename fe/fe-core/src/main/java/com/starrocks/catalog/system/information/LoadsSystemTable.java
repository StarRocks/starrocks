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

import com.google.common.collect.Lists;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TSchemaTableType;
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
                        .column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("LABEL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PROFILE_ID", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DB_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("USER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("WAREHOUSE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PROGRESS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIORITY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SCAN_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("SCAN_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("FILTERED_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("UNSELECTED_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("SINK_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("RUNTIME_DETAILS", ScalarType.createJsonType())
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LOAD_START_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LOAD_COMMIT_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LOAD_FINISH_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("PROPERTIES", ScalarType.createJsonType())
                        .column("ERROR_MSG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REJECTED_RECORD_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("JOB_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_LOADS);
    }

    public static TGetLoadsResult query(TGetLoadsParams request) {
        TGetLoadsResult result = new TGetLoadsResult();
        List<TLoadInfo> loads = Lists.newArrayList();
        try {
            boolean paged = request.isSetStart_job_id_offset();
            long startJobIdOffset = paged ? request.getStart_job_id_offset() : 0;

            // Load job ids and stream load task ids are both drawn from the one global id
            // allocator, so merging the two managers and sorting by id gives the stable total
            // order a cursor needs.
            List<Candidate> candidates = Lists.newArrayList();
            collectLoadJobs(candidates, request, startJobIdOffset);
            collectStreamLoadTasks(candidates, request, startJobIdOffset);
            candidates.sort(Comparator.comparingLong(Candidate::id));

            for (Candidate candidate : candidates) {
                loads.add(candidate.row().get());
                // Cut the page at a job boundary so one job's rows never straddle two pages.
                if (paged && loads.size() >= Config.max_get_loads_result_count) {
                    result.setNext_job_id_offset(candidate.id() + 1);
                    LOG.debug("getLoads page is full, returned rows: {}, next_job_id_offset: {}",
                            loads.size(), candidate.id() + 1);
                    break;
                }
            }
            result.setLoads(loads);
        } catch (Exception e) {
            LOG.warn("Failed to getLoads", e);
            throw e;
        }
        return result;
    }

    /**
     * One page unit: a job id to order and resume by, plus a lazy generator for its row.
     * The row stays unmaterialized so a candidate past the page cut costs nothing.
     */
    private record Candidate(long id, Supplier<TLoadInfo> row) {
    }

    private static void collectLoadJobs(List<Candidate> candidates, TGetLoadsParams request,
                                        long startJobIdOffset) {
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        if (request.isSetJob_id()) {
            addLoadJob(candidates, loadMgr.getLoadJob(request.getJob_id()), startJobIdOffset);
        } else if (request.isSetDb()) {
            long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(request.getDb()).getId();
            List<LoadJob> loadJobs;
            if (request.isSetLabel()) {
                loadJobs = loadMgr.getLoadJobsByDb(dbId, request.getLabel(), true);
            } else {
                loadJobs = loadMgr.getLoadJobsByDb(dbId, null, false);
            }
            for (LoadJob loadJob : loadJobs) {
                addLoadJob(candidates, loadJob, startJobIdOffset);
            }
        } else {
            String label = request.isSetLabel() ? request.getLabel() : null;
            for (LoadJob loadJob : loadMgr.getLoadJobs(label)) {
                addLoadJob(candidates, loadJob, startJobIdOffset);
            }
        }
    }

    private static void addLoadJob(List<Candidate> candidates, LoadJob loadJob, long startJobIdOffset) {
        if (loadJob == null || loadJob.getId() < startJobIdOffset) {
            return;
        }
        candidates.add(new Candidate(loadJob.getId(), loadJob::toThrift));
    }

    private static void collectStreamLoadTasks(List<Candidate> candidates, TGetLoadsParams request,
                                               long startJobIdOffset) {
        StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        if (request.isSetJob_id()) {
            addStreamLoadTask(candidates, streamLoadMgr.getTaskById(request.getJob_id()), startJobIdOffset);
            return;
        }
        List<StreamLoadTask> streamLoadTaskList = streamLoadMgr.getTaskByName(request.getLabel());
        if (streamLoadTaskList == null) {
            return;
        }
        for (StreamLoadTask streamLoadTask : streamLoadTaskList) {
            addStreamLoadTask(candidates, streamLoadTask, startJobIdOffset);
        }
    }

    private static void addStreamLoadTask(List<Candidate> candidates, StreamLoadTask task, long startJobIdOffset) {
        if (task == null || task.getId() < startJobIdOffset) {
            return;
        }
        candidates.add(new Candidate(task.getId(), task::toThrift));
    }
}
