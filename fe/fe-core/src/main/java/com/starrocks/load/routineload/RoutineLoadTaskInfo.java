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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadTaskInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

/**
 * Routine load task info is the task info include the only id (signature).
 * For the kafka type of task info, it also include partitions which will be obtained data in this task.
 * The routine load task info and routine load task are the same thing logically.
 * Differently, routine load task is a agent task include backendId which will execute this task.
 */
public abstract class RoutineLoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskInfo.class);

    public static final long INVALID_BE_ID = -1L;

    private RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();

    protected UUID id;
    protected long txnId = -1L;
    protected long jobId;

    private final long createTimeMs;
    // The time when the task is actually executed
    private long executeStartTimeMs = -1L;

    // The time when the task should be submitted to executed, this is advise time,
    // The actual execution time may be later than this time
    private final long timeToExecuteMs;

    // the be id of previous task
    protected long previousBeId = INVALID_BE_ID;
    // the be id of this task
    protected long beId = INVALID_BE_ID;

    // last time this task being scheduled by RoutineLoadTaskScheduler
    protected long lastScheduledTime = -1;

    protected long taskScheduleIntervalMs;
    protected long timeoutMs;

    // this status will be set when corresponding transaction's status is changed.
    // so that user or other logic can know the status of the corresponding txn.
    protected TransactionStatus txnStatus = TransactionStatus.UNKNOWN;

    // record task schedule info
    protected String msg;

    protected String label;

    protected StreamLoadTask streamLoadTask = null;

    public RoutineLoadTaskInfo(UUID id, long jobId, long taskScheduleIntervalMs,
                               long timeToExecuteMs, long taskTimeoutMs) {
        this.id = id;
        this.jobId = jobId;
        this.createTimeMs = System.currentTimeMillis();
        this.taskScheduleIntervalMs = taskScheduleIntervalMs;
        this.timeoutMs = taskTimeoutMs;
        this.timeToExecuteMs = timeToExecuteMs;
    }

    public RoutineLoadTaskInfo(UUID id, long jobId, long taskSchedulerIntervalMs,
                               long timeToExecuteMs, long previousBeId, long taskTimeoutMs) {
        this(id, jobId, taskSchedulerIntervalMs, timeToExecuteMs, taskTimeoutMs);
        this.previousBeId = previousBeId;
    }

    public UUID getId() {
        return id;
    }

    public long getJobId() {
        return jobId;
    }

    public void setExecuteStartTimeMs(long executeStartTimeMs) {
        this.executeStartTimeMs = executeStartTimeMs;
    }

    public long getPreviousBeId() {
        return previousBeId;
    }

    public void setBeId(long beId) {
        this.beId = beId;
    }

    public long getBeId() {
        return beId;
    }

    public long getTxnId() {
        return txnId;
    }

    public String getLabel() {
        return label;
    }

    public boolean isRunning() {
        return executeStartTimeMs > 0;
    }

    public long getLastScheduledTime() {
        return lastScheduledTime;
    }

    public void setLastScheduledTime(long lastScheduledTime) {
        this.lastScheduledTime = lastScheduledTime;
    }

    public long getTaskScheduleIntervalMs() {
        return taskScheduleIntervalMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTxnStatus(TransactionStatus txnStatus) {
        this.txnStatus = txnStatus;
    }

    public TransactionStatus getTxnStatus() {
        return txnStatus;
    }

    public long getTimeToExecuteMs() {
        return timeToExecuteMs;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isRunningTimeout() {
        if (txnStatus == TransactionStatus.COMMITTED || txnStatus == TransactionStatus.VISIBLE) {
            // the corresponding txn is already finished, this task can not be treated as timeout.
            return false;
        }

        if (isRunning() && System.currentTimeMillis() - executeStartTimeMs > timeoutMs) {
            LOG.info("task {} is timeout. start: {}, timeout: {}", DebugUtil.printId(id),
                    executeStartTimeMs, timeoutMs);
            return true;
        }
        return false;
    }

    abstract TRoutineLoadTask createRoutineLoadTask() throws UserException;

    abstract boolean readyToExecute() throws UserException;

    public abstract boolean isProgressKeepUp(RoutineLoadProgress progress);

    // begin the txn of this task
    // throw exception if unrecoverable errors happen.
    public void beginTxn() throws Exception {
        // begin a txn for task
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);

        //  label = job_name+job_id+task_id
        label = Joiner.on("-").join(routineLoadJob.getName(), routineLoadJob.getId(), DebugUtil.printId(id));
        txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                routineLoadJob.getDbId(), Lists.newArrayList(routineLoadJob.getTableId()), label, null,
                new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, routineLoadJob.getId(),
                timeoutMs / 1000);
    }

    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        // StreamLoadTask is null, if not specify session variable `enable_profile = true`
        if (streamLoadTask != null) {
            streamLoadTask.afterCommitted(txnState, txnOperated);
        }
    }

    public void afterVisible(TransactionState txnState, boolean txnOperated) throws UserException {
        // StreamLoadTask is null, if not specify session variable `enable_profile = true`
        if (streamLoadTask != null) {
            streamLoadTask.afterVisible(txnState, txnOperated);
        }
    }

    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason) throws UserException {
        // StreamLoadTask is null, if not specify session variable `enable_profile = true`
        if (streamLoadTask != null) {
            streamLoadTask.afterAborted(txnState, txnOperated, txnStatusChangeReason);
        }
    }

    public void setStreamLoadTask(StreamLoadTask streamLoadTask) {
        this.streamLoadTask = streamLoadTask;
    }

    public List<String> getTaskShowInfo() {
        List<String> row = Lists.newArrayList();
        row.add(DebugUtil.printId(id));
        row.add(String.valueOf(txnId));
        row.add(txnStatus.name());
        row.add(String.valueOf(jobId));
        row.add(TimeUtils.longToTimeString(createTimeMs));
        if (lastScheduledTime != -1L) {
            row.add(TimeUtils.longToTimeString(lastScheduledTime));
        } else {
            row.add("NULL");
        }
        if (executeStartTimeMs != -1L) {
            row.add(TimeUtils.longToTimeString(executeStartTimeMs));
        } else {
            row.add("NULL");
        }
        row.add(String.valueOf(timeoutMs / 1000));
        row.add(String.valueOf(beId));
        row.add(getTaskDataSourceProperties());
        if (msg == null) {
            row.add("NULL");
        } else {
            row.add(msg);
        }
        return row;
    }

    abstract String getTaskDataSourceProperties();

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RoutineLoadTaskInfo) {
            RoutineLoadTaskInfo routineLoadTaskInfo = (RoutineLoadTaskInfo) obj;
            return this.id.toString().equals(routineLoadTaskInfo.getId().toString());
        } else {
            return false;
        }
    }
}
