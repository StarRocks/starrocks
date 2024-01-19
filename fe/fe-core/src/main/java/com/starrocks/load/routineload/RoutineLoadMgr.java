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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadManager.java

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoadMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadMgr.class);

    // be => running tasks num
    private Map<Long, Integer> beTasksNum = Maps.newHashMap();
    private ReentrantLock slotLock = new ReentrantLock();

    // routine load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public RoutineLoadMgr() {
    }

    // returns -1 if there is no available be
    public long takeBeTaskSlot() {
        slotLock.lock();
        try {
            long beId = -1L;
            int minTasksNum = Integer.MAX_VALUE;
            for (Map.Entry<Long, Integer> entry : beTasksNum.entrySet()) {
                if (entry.getValue() < Config.max_routine_load_task_num_per_be
                        && entry.getValue() < minTasksNum) {
                    beId = entry.getKey();
                    minTasksNum = entry.getValue();
                }
            }
            if (beId != -1) {
                beTasksNum.put(beId, minTasksNum + 1);
            }
            return beId;
        } finally {
            slotLock.unlock();
        }
    }

    public long takeBeTaskSlot(long beId) {
        slotLock.lock();
        try {
            Integer taskNum = beTasksNum.get(beId);
            if (taskNum != null && taskNum < Config.max_routine_load_task_num_per_be) {
                beTasksNum.put(beId, taskNum + 1);
                return beId;
            } else {
                return -1L;
            }
        } finally {
            slotLock.unlock();
        }
    }

    public void releaseBeTaskSlot(long beId) {
        slotLock.lock();
        try {
            if (beTasksNum.containsKey(beId)) {
                int tasksNum = beTasksNum.get(beId);
                if (tasksNum > 0) {
                    beTasksNum.put(beId, tasksNum - 1);
                } else {
                    beTasksNum.put(beId, 0);
                }
            }
        } finally {
            slotLock.unlock();
        }
    }

    public void updateBeTaskSlot() {
        slotLock.lock();
        try {
            List<Long> aliveNodeIds = new ArrayList<>();
            // TODO: need to refactor after be split into cn + dn
            if (RunMode.isSharedDataMode()) {
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
                for (long nodeId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                    ComputeNode node =
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                    if (node != null && node.isAlive()) {
                        aliveNodeIds.add(nodeId);
                    }
                }
            } else {
                aliveNodeIds.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true));
            }

            // add new nodes
            for (Long nodeId : aliveNodeIds) {
                if (!beTasksNum.containsKey(nodeId)) {
                    beTasksNum.put(nodeId, 0);
                }
            }

            // remove not alive be
            List<Long> finalAliveNodeIds = aliveNodeIds;
            beTasksNum.keySet().removeIf(nodeId -> !finalAliveNodeIds.contains(nodeId));
        } finally {
            slotLock.unlock();
        }
    }

    public void createRoutineLoadJob(CreateRoutineLoadStmt createRoutineLoadStmt) throws UserException {
        RoutineLoadJob routineLoadJob = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(createRoutineLoadStmt.getTypeName());
        switch (type) {
            case KAFKA:
                routineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
                break;
            case PULSAR:
                routineLoadJob = PulsarRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
                break;
            default:
                throw new UserException("Unknown data source type: " + type);
        }

        routineLoadJob.setOrigStmt(createRoutineLoadStmt.getOrigStmt());
        addRoutineLoadJob(routineLoadJob, createRoutineLoadStmt.getDBName());
    }

    @VisibleForTesting
    public Map<Long, Integer> getBeTasksNum() {
        return beTasksNum;
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob, String dbName) throws DdlException {
        int beNum = 0;
        slotLock.lock();
        try {
            beNum = getBeTasksNum().size();
        } finally {
            slotLock.unlock();
        }
        writeLock();
        try {
            // check if db.routineLoadName has been used
            if (isNameUsed(routineLoadJob.getDbId(), routineLoadJob.getName())) {
                throw new DdlException("Name " + routineLoadJob.getName() + " already used in db "
                        + dbName);
            }
            long maxConcurrentTasks = beNum * Config.max_routine_load_task_num_per_be;
            // When calculating the used slots, we only consider Jobs in the NEED_SCHEDULE and RUNNING states.
            List<RoutineLoadJob> jobs = getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE,
                    RoutineLoadJob.JobState.RUNNING));
            long curTaskNum = 0;
            for (RoutineLoadJob job : jobs) {
                curTaskNum += job.calculateCurrentConcurrentTaskNum();
            }
            int newTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
            if (curTaskNum + newTaskNum > maxConcurrentTasks) {
                throw new DdlException("Current routine load tasks is " + curTaskNum + ". "
                        + "The new job need " + newTaskNum + " tasks. "
                        + "But we only support " + beNum + "*" + Config.max_routine_load_task_num_per_be + " tasks."
                        + "Please modify FE config max_routine_load_task_num_per_be if you want more job");
            }
            unprotectedAddJob(routineLoadJob);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateRoutineLoadJob(routineLoadJob);
            LOG.info("create routine load job: id: {}, name: {}", routineLoadJob.getId(), routineLoadJob.getName());
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    private void unprotectedAddJob(RoutineLoadJob routineLoadJob) {
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);

        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId());
        if (nameToRoutineLoadJob == null) {
            nameToRoutineLoadJob = Maps.newConcurrentMap();
            dbToNameToRoutineLoadJob.put(routineLoadJob.getDbId(), nameToRoutineLoadJob);
        }
        List<RoutineLoadJob> routineLoadJobList = nameToRoutineLoadJob.get(routineLoadJob.getName());
        if (routineLoadJobList == null) {
            routineLoadJobList = Lists.newArrayList();
            nameToRoutineLoadJob.put(routineLoadJob.getName(), routineLoadJobList);
        }
        routineLoadJobList.add(routineLoadJob);
        // add txn state callback in factory
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
        if (!Config.enable_dict_optimize_routine_load) {
            IDictManager.getInstance().disableGlobalDict(routineLoadJob.getTableId());
        }
    }

    // TODO(ml): Idempotency
    private boolean isNameUsed(Long dbId, String name) {
        if (dbToNameToRoutineLoadJob.containsKey(dbId)) {
            Map<String, List<RoutineLoadJob>> labelToRoutineLoadJob = dbToNameToRoutineLoadJob.get(dbId);
            if (labelToRoutineLoadJob.containsKey(name)) {
                List<RoutineLoadJob> routineLoadJobList = labelToRoutineLoadJob.get(name);
                Optional<RoutineLoadJob> optional = routineLoadJobList.parallelStream()
                        .filter(entity -> entity.getName().equals(name))
                        .filter(entity -> !entity.getState().isFinalState()).findFirst();
                if (optional.isPresent()) {
                    return true;
                }
            }
        }
        return false;
    }

    private RoutineLoadJob checkPrivAndGetJob(String dbName, String jobName)
            throws MetaNotFoundException, DdlException {
        RoutineLoadJob routineLoadJob = getJob(dbName, jobName);
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + jobName);
        }

        return routineLoadJob;
    }

    public void pauseRoutineLoadJob(PauseRoutineLoadStmt pauseRoutineLoadStmt)
            throws UserException {
        RoutineLoadJob routineLoadJob = checkPrivAndGetJob(pauseRoutineLoadStmt.getDbFullName(),
                pauseRoutineLoadStmt.getName());

        routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR,
                        "User " + ConnectContext.get().getQualifiedUser() + " pauses routine load job"),
                false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("current_state",
                routineLoadJob.getState()).add("user", ConnectContext.get().getQualifiedUser()).add("msg",
                "routine load job has been paused by user").build());
    }

    public void resumeRoutineLoadJob(ResumeRoutineLoadStmt resumeRoutineLoadStmt) throws UserException {
        RoutineLoadJob routineLoadJob = checkPrivAndGetJob(resumeRoutineLoadStmt.getDbFullName(),
                resumeRoutineLoadStmt.getName());

        routineLoadJob.autoResumeCount = 0;
        routineLoadJob.firstResumeTimestamp = 0;
        routineLoadJob.autoResumeLock = false;
        routineLoadJob.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("current_state", routineLoadJob.getState())
                .add("user", ConnectContext.get().getQualifiedUser())
                .add("msg", "routine load job has been resumed by user")
                .build());
    }

    public void stopRoutineLoadJob(StopRoutineLoadStmt stopRoutineLoadStmt)
            throws UserException {
        RoutineLoadJob routineLoadJob = checkPrivAndGetJob(stopRoutineLoadStmt.getDbFullName(),
                stopRoutineLoadStmt.getName());
        routineLoadJob.updateState(RoutineLoadJob.JobState.STOPPED,
                new ErrorReason(InternalErrorCode.MANUAL_STOP_ERR,
                        "User  " + ConnectContext.get().getQualifiedUser() + " stop routine load job"),
                false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("current_state", routineLoadJob.getState())
                .add("user", ConnectContext.get().getQualifiedUser())
                .add("msg", "routine load job has been stopped by user")
                .build());
    }

    public int getSizeOfIdToRoutineLoadTask() {
        readLock();
        try {
            int sizeOfTasks = 0;
            for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
                sizeOfTasks += routineLoadJob.getSizeOfRoutineLoadTaskInfoList();
            }
            return sizeOfTasks;
        } finally {
            readUnlock();
        }
    }

    public int getClusterIdleSlotNum() {
        slotLock.lock();
        try {
            return beTasksNum.values()
                    .stream()
                    .reduce(0,
                            (acc, num) -> acc + Config.max_routine_load_task_num_per_be - num);
        } finally {
            slotLock.unlock();
        }
    }

    public RoutineLoadJob getJob(long jobId) {
        readLock();
        try {
            return idToRoutineLoadJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    public RoutineLoadJob getJob(String dbFullName, String jobName) throws MetaNotFoundException {
        List<RoutineLoadJob> routineLoadJobList = getJob(dbFullName, jobName, false);
        if (routineLoadJobList == null || routineLoadJobList.size() == 0) {
            return null;
        } else {
            return routineLoadJobList.get(0);
        }
    }

    /*
      if dbFullName is null, result = all of routine load job in all of db
      else if jobName is null, result =  all of routine load job in dbFullName

      if includeHistory is false, filter not running job in result
      else return all of result
     */
    public List<RoutineLoadJob> getJob(String dbFullName, String jobName, boolean includeHistory)
            throws MetaNotFoundException {
        readLock();
        try {
            // return all of routine load job
            List<RoutineLoadJob> result;
            RESULT:
            {
                if (dbFullName == null) {
                    result = new ArrayList<>(idToRoutineLoadJob.values());
                    sortRoutineLoadJob(result);
                    break RESULT;
                }

                long dbId = 0L;
                Database database = GlobalStateMgr.getCurrentState().getDb(dbFullName);
                if (database == null) {
                    throw new MetaNotFoundException("failed to find database by dbFullName " + dbFullName);
                }
                dbId = database.getId();
                if (!dbToNameToRoutineLoadJob.containsKey(dbId)) {
                    result = new ArrayList<>();
                    break RESULT;
                }
                if (jobName == null) {
                    result = Lists.newArrayList();
                    for (List<RoutineLoadJob> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.get(dbId).values()) {
                        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob);
                        sortRoutineLoadJob(routineLoadJobList);
                        result.addAll(routineLoadJobList);
                    }
                    break RESULT;
                }
                if (dbToNameToRoutineLoadJob.get(dbId).containsKey(jobName)) {
                    result = new ArrayList<>(dbToNameToRoutineLoadJob.get(dbId).get(jobName));
                    sortRoutineLoadJob(result);
                    break RESULT;
                }
                return null;
            }

            if (!includeHistory) {
                result = result.stream().filter(entity -> !entity.getState().isFinalState())
                        .collect(Collectors.toList());
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // return all of routine load job named jobName in all of db
    public List<RoutineLoadJob> getJobByName(String jobName) {
        List<RoutineLoadJob> result = Lists.newArrayList();
        readLock();
        try {
            for (Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.values()) {
                if (nameToRoutineLoadJob.containsKey(jobName)) {
                    List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob.get(jobName));
                    sortRoutineLoadJob(routineLoadJobList);
                    result.addAll(routineLoadJobList);
                }
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    // put history job in the end
    private void sortRoutineLoadJob(List<RoutineLoadJob> routineLoadJobList) {
        if (routineLoadJobList == null) {
            return;
        }
        int i = 0;
        int j = routineLoadJobList.size() - 1;
        while (i < j) {
            while (!routineLoadJobList.get(i).isFinal() && (i < j)) {
                i++;
            }
            while (routineLoadJobList.get(j).isFinal() && (i < j)) {
                j--;
            }
            if (i < j) {
                RoutineLoadJob routineLoadJob = routineLoadJobList.get(i);
                routineLoadJobList.set(i, routineLoadJobList.get(j));
                routineLoadJobList.set(j, routineLoadJob);
            }
        }
    }

    public boolean checkTaskInJob(UUID taskId) {
        readLock();
        try {
            for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
                if (routineLoadJob.containsTask(taskId)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(Set<RoutineLoadJob.JobState> desiredStates) {
        return idToRoutineLoadJob.values().stream()
                .filter(entity -> desiredStates.contains(entity.getState()))
                .collect(Collectors.toList());
    }

    // RoutineLoadScheduler will run this method at fixed interval, and renew the timeout tasks
    public void processTimeoutTasks() {
        readLock();
        try {
            for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
                routineLoadJob.processTimeoutTasks();
            }
        } finally {
            readUnlock();
        }
    }

    // Remove old routine load jobs from idToRoutineLoadJob
    // This function is called periodically.
    // Cancelled and stopped job will be remove after Configure.label_keep_max_second seconds
    public void cleanOldRoutineLoadJobs() {
        LOG.debug("begin to clean old routine load jobs ");
        writeLock();
        try {
            Iterator<Map.Entry<Long, RoutineLoadJob>> iterator = idToRoutineLoadJob.entrySet().iterator();
            long currentTimestamp = System.currentTimeMillis();
            while (iterator.hasNext()) {
                RoutineLoadJob routineLoadJob = iterator.next().getValue();
                if (routineLoadJob.needRemove()) {
                    unprotectedRemoveJobFromDb(routineLoadJob);
                    iterator.remove();

                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("end_timestamp", routineLoadJob.getEndTimestamp())
                            .add("current_timestamp", currentTimestamp)
                            .add("job_state", routineLoadJob.getState())
                            .add("msg", "old job has been cleaned")
                    );
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayRemoveOldRoutineLoad(RoutineLoadOperation operation) {
        writeLock();
        try {
            RoutineLoadJob job = idToRoutineLoadJob.remove(operation.getId());
            if (job != null) {
                unprotectedRemoveJobFromDb(job);
            }
            LOG.info("replay remove routine load job: {}", operation.getId());
        } finally {
            writeUnlock();
        }
    }

    private void unprotectedRemoveJobFromDb(RoutineLoadJob routineLoadJob) {
        dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).remove(routineLoadJob);
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).isEmpty()) {
            dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).remove(routineLoadJob.getName());
        }
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).isEmpty()) {
            dbToNameToRoutineLoadJob.remove(routineLoadJob.getDbId());
        }
    }

    public void updateRoutineLoadJob() throws UserException {
        readLock();
        try {
            for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
                if (!routineLoadJob.state.isFinalState()) {
                    routineLoadJob.update();
                }
            }
        } finally {
            readUnlock();
        }
    }

    public void replayCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        unprotectedAddJob(routineLoadJob);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("msg", "replay create routine load job")
                .build());
    }

    public void replayChangeRoutineLoadJob(RoutineLoadOperation operation) {
        RoutineLoadJob job = getJob(operation.getId());
        try {
            job.updateState(operation.getJobState(), null, true /* is replay */);
        } catch (UserException e) {
            LOG.error("should not happened", e);
        }
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, operation.getId())
                .add("current_state", operation.getJobState())
                .add("msg", "replay change routine load job")
                .build());
    }

    public void setRoutineLoadJobOtherMsg(String reason, TxnCommitAttachment txnCommitAttachment) {
        if (!(txnCommitAttachment instanceof RLTaskTxnCommitAttachment)) {
            return;
        }
        RLTaskTxnCommitAttachment rLTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
        long jobId = rLTaskTxnCommitAttachment.getJobId();
        if (jobId == 0) {
            return;
        }
        RoutineLoadJob routineLoadJob = getJob(jobId);
        if (routineLoadJob != null && reason != null) {
            String otherMsg = String.format("The %s task failed due to: %s",
                    DebugUtil.printId(rLTaskTxnCommitAttachment.getTaskId()), reason);
            routineLoadJob.setOtherMsg(otherMsg);
        }
    }

    /**
     * Enter of altering a routine load job
     */
    public void alterRoutineLoadJob(AlterRoutineLoadStmt stmt) throws UserException {
        RoutineLoadJob job = checkPrivAndGetJob(stmt.getDbName(), stmt.getLabel());
        if (job.getState() != RoutineLoadJob.JobState.PAUSED) {
            throw new DdlException("Only supports modification of PAUSED jobs");
        }
        if (stmt.hasDataSourceProperty()
                && !stmt.getDataSourceProperties().getType().equalsIgnoreCase(job.dataSourceType.name())) {
            throw new DdlException("The specified job type is not: " + stmt.getDataSourceProperties().getType());
        }
        job.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), stmt.getOrigStmt(), false);
    }

    public void replayAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) throws UserException, IOException {
        RoutineLoadJob job = getJob(log.getJobId());
        Preconditions.checkNotNull(job, log.getJobId());

        // NOTE: we use the origin statement to get the RoutineLoadDesc
        RoutineLoadDesc routineLoadDesc = null;
        if (log.getOriginStatement() != null) {
            routineLoadDesc = CreateRoutineLoadStmt.getLoadDesc(
                    log.getOriginStatement(), job.getSessionVariables());
        }
        job.modifyJob(routineLoadDesc, log.getJobProperties(),
                log.getDataSourceProperties(), log.getOriginStatement(), true);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(idToRoutineLoadJob.size());
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            RoutineLoadJob routineLoadJob = RoutineLoadJob.read(in);
            if (routineLoadJob.needRemove()) {
                LOG.info("discard expired job [{}]", routineLoadJob.getId());
                continue;
            }

            putJob(routineLoadJob);
        }
    }

    private void putJob(RoutineLoadJob routineLoadJob) {
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
        Map<String, List<RoutineLoadJob>> map =
                dbToNameToRoutineLoadJob.computeIfAbsent(routineLoadJob.getDbId(), k -> Maps.newConcurrentMap());

        List<RoutineLoadJob> jobs = map.computeIfAbsent(routineLoadJob.getName(), k -> Lists.newArrayList());
        jobs.add(routineLoadJob);
        if (!routineLoadJob.getState().isFinalState()) {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
        }
    }

    public long loadRoutineLoadJobs(DataInputStream dis, long checksum) throws IOException {
        readFields(dis);
        LOG.info("finished replay routineLoadJobs from image");
        return checksum;
    }

    public long saveRoutineLoadJobs(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    public void saveRoutineLoadJobsV2(DataOutputStream dos) throws IOException, SRMetaBlockException {
        final int cnt = 1 + idToRoutineLoadJob.size();
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.ROUTINE_LOAD_MGR, cnt);
        writer.writeJson(idToRoutineLoadJob.size());
        for (RoutineLoadJob loadJob : idToRoutineLoadJob.values()) {
            writer.writeJson(loadJob);
        }
        writer.close();
    }

    public void loadRoutineLoadJobsV2(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int size = reader.readInt();
        while (size-- > 0) {
            RoutineLoadJob routineLoadJob = reader.readJson(RoutineLoadJob.class);

            if (routineLoadJob.needRemove()) {
                LOG.info("discard expired job [{}]", routineLoadJob.getId());
                continue;
            }

            putJob(routineLoadJob);
        }
    }
}
