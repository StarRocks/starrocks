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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
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
import com.starrocks.thrift.TPulsarMessageId;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfo extends RoutineLoadTaskInfo {
    private RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
    private Map<String, TPulsarMessageId> beginPositions;
    private Map<String, TPulsarMessageId> latestPartMessageId;

    public PulsarTaskInfo(UUID id, long jobId, long taskScheduleIntervalMs, long timeToExecuteMs,
                          Map<String, TPulsarMessageId> beginPositions, long tastTimeoutMs) {
        super(id, jobId, taskScheduleIntervalMs, timeToExecuteMs, tastTimeoutMs);
        this.beginPositions.putAll(beginPositions);
    }

    public PulsarTaskInfo(long timeToExecuteMs, PulsarTaskInfo pulsarTaskInfo,
                          Map<String, TPulsarMessageId> beginPositions) {
        super(UUID.randomUUID(), pulsarTaskInfo.getJobId(), pulsarTaskInfo.getTaskScheduleIntervalMs(),
                timeToExecuteMs, pulsarTaskInfo.getBeId());
        this.beginPositions.putAll(beginPositions);
    }

    public List<String> getPartitions() {
        return new ArrayList<>(beginPositions.keySet());
    }

    @Override
    public boolean readyToExecute() throws UserException {
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        if (routineLoadJob == null) {
            return false;
        }

        PulsarRoutineLoadJob pulsarRoutineLoadJob = (PulsarRoutineLoadJob) routineLoadJob;
        Map<String, TPulsarMessageId> lastMessageIds = PulsarUtil.getLatestMessageIds(pulsarRoutineLoadJob.getServiceUrl(),
                pulsarRoutineLoadJob.getTopic(), pulsarRoutineLoadJob.getSubscription(),
                ImmutableMap.copyOf(pulsarRoutineLoadJob.getConvertedCustomProperties()),
                getPartitions());

        for (Map.Entry<String, TPulsarMessageId> entry : beginPositions.entrySet()) {
            String partition = entry.getKey();
            TPulsarMessageId latestPosition = lastMessageIds.get(partition);
            TPulsarMessageId consumedPosition = entry.getValue();

            if (latestMessageId != null && PulsarUtil.isMessageValid(latestMessageId)) {
                if (PulsarUtil.messageIdGt(latestPosition, consumedPosition)) {
                    this.latestPartMessageId = lastMessageIds;
                    return true;
                } else if (PulsarUtil.messageIdLt(latestPosition, consumedPosition)) {
                    LOG.warn("partition: {}, latest message id: {} less then consumed message id {}, it shouldn't be happened",
                            partition, latestMessageId, consumeMessageId);
                    throw new RoutineLoadPauseException(
                            "partition " + partition + " position " + consumedPosition + " has no data");
                }
            }
        }

        return false;
    }

    @Override
    public boolean isProgressKeepUp(RoutineLoadProgress progress) {
        PulsarProgress pProgress = (PulsarProgress) progress;
        if (latestPartMessageId == null) {
            return true;
        }

        for (Map.Entry<String, TPulsarMessageId> entry : latestPartMessageId.entrySet()) {
            String part = entry.getKey();
            TPulsarMessageId latestPosition = entry.getValue();
            TPulsarMessageId consumedPosition = pProgress.getPositionByPartition(part);
            if (consumedPosition != null && PulsarUtil.messageIdLt(consumedPosition, latestPosition)) {
                return false;
            }
        }
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
        if (!beginPositions.isEmpty()) {
            tPulsarLoadInfo.setBegin_positions(beginPositions);
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
        result.append("Progress:").append(gson.toJson(beginPositions));
        result.append(",");
        result.append("LatestMessageId:").append(gson.toJson(latestPartMessageId));
        return result.toString();
    }

    @Override
    public String toString() {
        return "Task id: " + getId() + ", begin positions: " + beginPositions;
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
