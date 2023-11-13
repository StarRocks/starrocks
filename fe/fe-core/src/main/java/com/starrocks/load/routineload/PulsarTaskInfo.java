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


package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TPulsarLoadInfo;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfo extends RoutineLoadTaskInfo {
    private RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();

    private List<String> partitions;
    private Map<String, Long> initialPositions = Maps.newHashMap();

    public PulsarTaskInfo(UUID id, long jobId, long taskScheduleIntervalMs, long timeToExecuteMs,
                          List<String> partitions, Map<String, Long> initialPositions, long tastTimeoutMs) {
        super(id, jobId, taskScheduleIntervalMs, timeToExecuteMs, tastTimeoutMs);
        this.partitions = partitions;
        this.initialPositions.putAll(initialPositions);
    }

    public PulsarTaskInfo(long timeToExecuteMs, PulsarTaskInfo pulsarTaskInfo, Map<String, Long> initialPositions) {
        super(UUID.randomUUID(), pulsarTaskInfo.getJobId(), pulsarTaskInfo.getTaskScheduleIntervalMs(),
                timeToExecuteMs, pulsarTaskInfo.getBeId(), pulsarTaskInfo.getTimeoutMs());
        this.partitions = pulsarTaskInfo.getPartitions();
        this.initialPositions.putAll(initialPositions);
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public Map<String, Long> getInitialPositions() {
        return initialPositions;
    }

    @Override
    public boolean readyToExecute() throws UserException {
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        if (routineLoadJob == null) {
            return false;
        }

        // Got initialPositions, we need to execute even there's no backlogs
        if (!initialPositions.isEmpty()) {
            return true;
        }

        PulsarRoutineLoadJob pulsarRoutineLoadJob = (PulsarRoutineLoadJob) routineLoadJob;
        Map<String, Long> backlogNums = PulsarUtil.getBacklogNums(pulsarRoutineLoadJob.getServiceUrl(),
                pulsarRoutineLoadJob.getTopic(), pulsarRoutineLoadJob.getSubscription(),
                ImmutableMap.copyOf(pulsarRoutineLoadJob.getConvertedCustomProperties()),
                partitions);
        for (String partition : partitions) {
            Long backlogNum = backlogNums.get(partition);
            if (backlogNum != null && backlogNum > 0) {
                return true;
            }
        }

        return false;
    }

    // TODO(chen9t) there's no way to find out how many backlogs have been consumed in this round,
    // the bellowing method will preempt the slots of BEs. So return ture until we find a better way.
    @Override
    public boolean isProgressKeepUp(RoutineLoadProgress progress) {
        // PulsarProgress pProgress = (PulsarProgress) progress;
        // for (Long backLogNum : pProgress.getBacklogNums()) {
        //     if (backLogNum > 0) {
        //         return false;
        //     }
        // }
        return true;
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        PulsarRoutineLoadJob routineLoadJob = (PulsarRoutineLoadJob) routineLoadManager.getJob(jobId);

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(jobId);
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = GlobalStateMgr.getCurrentState().getDb(routineLoadJob.getDbId());
        if (database == null) {
            throw new MetaNotFoundException("database " + routineLoadJob.getDbId() + " does not exist");
        }
        tRoutineLoadTask.setDb(database.getFullName());
        Table tbl = database.getTable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        tRoutineLoadTask.setTbl(tbl.getName());
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TPulsarLoadInfo tPulsarLoadInfo = new TPulsarLoadInfo();
        tPulsarLoadInfo.setService_url((routineLoadJob).getServiceUrl());
        tPulsarLoadInfo.setTopic((routineLoadJob).getTopic());
        tPulsarLoadInfo.setSubscription((routineLoadJob).getSubscription());
        tPulsarLoadInfo.setPartitions(getPartitions());
        if (!initialPositions.isEmpty()) {
            tPulsarLoadInfo.setInitial_positions(getInitialPositions());
        }
        tPulsarLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        tRoutineLoadTask.setPulsar_load_info(tPulsarLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.PULSAR);
        tRoutineLoadTask.setParams(plan(routineLoadJob));
        tRoutineLoadTask.setMax_interval_s(routineLoadJob.getTaskConsumeSecond());
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(Config.max_routine_load_batch_size);
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        if (Math.abs(routineLoadJob.getMaxFilterRatio() - 1) > 0.001) {
            tRoutineLoadTask.setMax_filter_ratio(routineLoadJob.getMaxFilterRatio());
        }
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        StringBuilder result = new StringBuilder();

        Gson gson = new Gson();
        result.append("Partitions: " + gson.toJson(partitions));
        if (!initialPositions.isEmpty()) {
            result.append("InitialPositisons: " + gson.toJson(initialPositions));
        }

        return result.toString();
    }

    @Override
    public String toString() {
        return "Task id: " + getId() + ", partitions: " + partitions + ", initial positions: " + initialPositions;
    }

    private TExecPlanFragmentParams plan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(loadId, txnId, label);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(txnId);
        return tExecPlanFragmentParams;
    }
}
