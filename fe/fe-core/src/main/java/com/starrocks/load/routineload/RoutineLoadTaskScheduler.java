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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadTaskScheduler.java

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.routineload.RoutineLoadJob.JobState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Routine load task scheduler is a function which allocate task to be.
 * Step1: update backend slot if interval more than BACKEND_SLOT_UPDATE_INTERVAL_MS
 * Step2: submit beIdToBatchTask when queue is empty
 * Step3: take a task from queue and schedule this task
 * <p>
 * The scheduler will be blocked in step3 till the queue receive a new task
 */
public class RoutineLoadTaskScheduler extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);

    private static final long BACKEND_SLOT_UPDATE_INTERVAL_MS = 10000; // 10s
    private static final long SLOT_FULL_SLEEP_MS = 10000; // 10s

    private final RoutineLoadMgr routineLoadManager;
    private final LinkedBlockingQueue<RoutineLoadTaskInfo> needScheduleTasksQueue = Queues.newLinkedBlockingQueue();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    private long lastBackendSlotUpdateTime = -1;

    @VisibleForTesting
    public RoutineLoadTaskScheduler() {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
    }

    public RoutineLoadTaskScheduler(RoutineLoadMgr routineLoadManager) {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler", e);
        }
    }

    private void process() throws InterruptedException {
        updateBackendSlotIfNecessary();

        int idleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        // scheduler will be blocked when there is no slot for task in cluster
        if (idleSlotNum <= 0) {
            LOG.warn("no available be slot to scheduler tasks, wait for {} seconds to scheduler again, " +
                            "you can set max_routine_load_task_num_per_be bigger in fe.conf, current value is {}",
                    SLOT_FULL_SLEEP_MS / 1000, Config.max_routine_load_task_num_per_be);
            Thread.sleep(SLOT_FULL_SLEEP_MS);
            return;
        }

        try {
            // This step will be blocked when queue is empty
            RoutineLoadTaskInfo routineLoadTaskInfo = needScheduleTasksQueue.take();

            if (routineLoadTaskInfo.getTimeToExecuteMs() > System.currentTimeMillis()) {
                // delay adding to queue to avoid endless loop
                delayPutToQueue(routineLoadTaskInfo, null);
                return;
            }

            // try to delay scheduling this task for scheduleInterval, to void too many failure
            if (System.currentTimeMillis() - routineLoadTaskInfo.getLastScheduledTime() <
                    routineLoadTaskInfo.getTaskScheduleIntervalMs()) {
                // delay adding to queue to avoid endless loop
                delayPutToQueue(routineLoadTaskInfo, null);
                return;
            }

            submitToSchedule(routineLoadTaskInfo);
        } catch (Exception e) {
            LOG.warn("Taking routine load task from queue has been interrupted", e);
            return;
        }
    }

    private synchronized void delayPutToQueue(RoutineLoadTaskInfo routineLoadTaskInfo, String msg) {
        if (msg != null) {
            routineLoadTaskInfo.setMsg(msg);
        }
        scheduledExecutorService.schedule(() -> {
            try {
                needScheduleTasksQueue.put(routineLoadTaskInfo);
            } catch (InterruptedException exception) {
                LOG.warn("put task to queue failed", exception);
            }
        }, 1L, TimeUnit.SECONDS);
    }

    private void submitToSchedule(RoutineLoadTaskInfo routineLoadTaskInfo) {
        threadPool.submit(() -> {
            try {
                scheduleOneTask(routineLoadTaskInfo);
            } catch (Exception e) {
                LOG.warn("schedule routine load task failed", e);
            }
        });
    }

    private void scheduleOneTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws Exception {
        routineLoadTaskInfo.setLastScheduledTime(System.currentTimeMillis());
        // check if task has been abandoned
        if (!routineLoadManager.checkTaskInJob(routineLoadTaskInfo.getId())) {
            // task has been abandoned while renew task has been added in queue
            // or database has been deleted
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                    .add("error_msg", "task has been abandoned when scheduling task")
                    .build());
            return;
        }

        try {
            // for kafka/pulsar routine load, readyToExecute means there is new data in kafka/pulsar stream
            if (!routineLoadTaskInfo.readyToExecute()) {
                String msg = "";
                if (routineLoadTaskInfo instanceof KafkaTaskInfo || routineLoadTaskInfo instanceof PulsarTaskInfo) {
                    msg = String.format("there is no new data in kafka/pulsar, wait for %d seconds to schedule again",
                            routineLoadTaskInfo.getTaskScheduleIntervalMs() / 1000);
                }
                // The job keeps up with source.
                routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateSubstateStable();
                delayPutToQueue(routineLoadTaskInfo, msg);
                return;
            }
            // Update the job state is the job is too slow.
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateSubstate();

        } catch (RoutineLoadPauseException e) {
            String msg = "fe abort task with reason: check task ready to execute failed, " + e.getMessage();
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(
                    JobState.PAUSED, new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                    .add("error_msg", msg)
                    .build());
            return;
        } catch (Exception e) {
            LOG.warn("check task ready to execute failed", e);
            delayPutToQueue(routineLoadTaskInfo, "check task ready to execute failed, err: " + e.getMessage());
            return;
        }

        // allocate BE slot for this task.
        // this should be done before txn begin, or the txn may be begun successfully but failed to be allocated.
        if (!allocateTaskToBe(routineLoadTaskInfo)) {
            // allocate failed, push it back to the queue to wait next scheduling
            delayPutToQueue(routineLoadTaskInfo,
                    String.format("failed to allocate be slot, wait for %s second to schedule again, " +
                                    "you can set max_routine_load_task_num_per_be bigger in fe.conf, " +
                                    "current value is %d",
                            routineLoadTaskInfo.getTaskScheduleIntervalMs() / 1000,
                            Config.max_routine_load_task_num_per_be));
            return;
        }

        // begin txn
        try {
            routineLoadTaskInfo.beginTxn();
        } catch (Exception e) {
            releaseBeSlot(routineLoadTaskInfo);
            // exception happens, PAUSE the job
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR,
                            "failed to begin txn for task :" + e.getMessage()), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId()).add("error_msg",
                    "begin task txn encounter exception: " + e.getMessage()).build());
            throw e;
        }

        // create thrift object
        TRoutineLoadTask tRoutineLoadTask = null;
        try {
            long startTime = System.currentTimeMillis();
            tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
            LOG.debug("create routine load task cost(ms): {}, job id: {}",
                    (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
        } catch (MetaNotFoundException e) {
            releaseBeSlot(routineLoadTaskInfo);
            // this means database or table has been dropped, just stop this routine load job.
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.META_NOT_FOUND_ERR, "meta not found: " + e.getMessage()),
                            false);
            throw e;
        } catch (UserException e) {
            releaseBeSlot(routineLoadTaskInfo);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.PAUSED,
                            new ErrorReason(e.getErrorCode(), "failed to create task: " + e.getMessage()),
                            false);
            throw e;
        }

        try {
            long startTime = System.currentTimeMillis();
            submitTask(routineLoadTaskInfo.getBeId(), tRoutineLoadTask);
            LOG.debug("send routine load task cost(ms): {}, job id: {}",
                    (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
            if (tRoutineLoadTask.isSetKafka_load_info()) {
                LOG.debug("send kafka routine load task {} with partition offset: {}, job: {}",
                        tRoutineLoadTask.label, tRoutineLoadTask.kafka_load_info.partition_begin_offset,
                        tRoutineLoadTask.getJob_id());
            } else if (tRoutineLoadTask.isSetPulsar_load_info()) {
                LOG.debug("send pulsar routine load task {} with partitions: {}, job: {}",
                        tRoutineLoadTask.label, tRoutineLoadTask.pulsar_load_info.partitions,
                        tRoutineLoadTask.getJob_id());
            }
        } catch (LoadException e) {
            // submit task failed (such as TOO_MANY_TASKS error), but txn has already begun.
            // Here we will still set the ExecuteStartTime of this task, which means
            // we "assume" that this task has been successfully submitted.
            // And this task will then be aborted because of a timeout.
            // In this way, we can prevent the entire job from being paused due to submit errors,
            // and we can also relieve the pressure on BE by waiting for the timeout period.
            LOG.warn("failed to submit routine load task {} to BE: {}",
                    DebugUtil.printId(routineLoadTaskInfo.getId()),
                    routineLoadTaskInfo.getBeId());
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).setOtherMsg(e.getMessage());
            // fall through to set ExecuteStartTime
        }

        // set the executeStartTimeMs of task
        routineLoadTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis());
        routineLoadTaskInfo.setMsg("task submitted to execute");
    }

    private void releaseBeSlot(RoutineLoadTaskInfo routineLoadTaskInfo) {
        // release the BE slot
        routineLoadManager.releaseBeTaskSlot(routineLoadTaskInfo.getBeId());
        // set beId to INVALID_BE_ID to avoid release slot repeatedly,
        // when job set to paused/cancelled, the slot will be release again if beId is not INVALID_BE_ID
        routineLoadTaskInfo.setBeId(RoutineLoadTaskInfo.INVALID_BE_ID);
    }

    private void updateBackendSlotIfNecessary() {
        long currentTime = System.currentTimeMillis();
        if (lastBackendSlotUpdateTime == -1
                || (currentTime - lastBackendSlotUpdateTime > BACKEND_SLOT_UPDATE_INTERVAL_MS)) {
            routineLoadManager.updateBeTaskSlot();
            lastBackendSlotUpdateTime = currentTime;
            LOG.debug("update backend max slot for routine load task scheduling. current task num per BE: {}",
                    Config.max_routine_load_task_num_per_be);
        }
    }

    public void addTaskInQueue(RoutineLoadTaskInfo routineLoadTaskInfo) {
        needScheduleTasksQueue.add(routineLoadTaskInfo);
        LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
    }

    public void addTasksInQueue(List<RoutineLoadTaskInfo> routineLoadTaskInfoList) {
        needScheduleTasksQueue.addAll(routineLoadTaskInfoList);
        LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
    }

    private void submitTask(long beId, TRoutineLoadTask tTask) throws LoadException {
        // TODO: need to refactor after be split into cn + dn
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(beId);
        if (node == null) {
            throw new LoadException("failed to send tasks to backend " + beId + " because not exist");
        }

        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBePort());

        boolean ok = false;
        BackendService.Client client = null;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TStatus tStatus = client.submit_routine_load_task(Lists.newArrayList(tTask));
            ok = true;

            if (tStatus.getStatus_code() != TStatusCode.OK) {
                throw new LoadException("failed to submit task. error code: " + tStatus.getStatus_code()
                        + ", msg: " + (tStatus.getError_msgsSize() > 0 ? tStatus.getError_msgs().get(0) : "NaN"));
            }
            LOG.debug("send routine load task {} to BE: {}", DebugUtil.printId(tTask.id), beId);
        } catch (Exception e) {
            throw new LoadException("failed to send task: " + e.getMessage(), e);
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    // try to allocate a task to BE which has idle slot.
    // 1. First is to check if the previous allocated BE is available. If yes, allocate task to previous BE.
    // 2. If not, try to find a better one with most idle slots.
    // return true if allocate successfully. return false if failed.
    // throw exception if unrecoverable errors happen.
    private boolean allocateTaskToBe(RoutineLoadTaskInfo routineLoadTaskInfo) {
        if (routineLoadTaskInfo.getPreviousBeId() != -1L) {
            if (routineLoadManager.takeBeTaskSlot(routineLoadTaskInfo.getPreviousBeId()) != -1L) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                            .add("job_id", routineLoadTaskInfo.getJobId())
                            .add("previous_be_id", routineLoadTaskInfo.getPreviousBeId())
                            .add("msg", "task use the previous be id")
                            .build());
                }
                routineLoadTaskInfo.setBeId(routineLoadTaskInfo.getPreviousBeId());
                return true;
            }
        }

        // the previous BE is not available, try to find a better one
        long beId = routineLoadManager.takeBeTaskSlot();
        if (beId < 0) {
            return false;
        }

        routineLoadTaskInfo.setBeId(beId);
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                    .add("job_id", routineLoadTaskInfo.getJobId())
                    .add("be_id", routineLoadTaskInfo.getBeId())
                    .add("msg", "task has been allocated to be")
                    .build());
        }
        return true;
    }
}
