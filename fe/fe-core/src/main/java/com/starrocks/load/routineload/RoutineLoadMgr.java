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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseLoadInfoBuilder;
import com.starrocks.warehouse.WarehouseLoadStatusInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoadMgr implements Writable, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadMgr.class);
    private static final int MEMORY_JOB_SAMPLES = 10;

    // warehouse ==> {be : running tasks num}
    private Map<Long, Map<Long, Integer>> warehouseNodeTasksNum = Maps.newHashMap();
    private ReentrantLock slotLock = new ReentrantLock();

    // warehouse ==> {nodeId : {jobId}}
    private Map<Long, Map<Long, Set<Long>>> warehouseNodeToJobs = Maps.newHashMap();

    // routine load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();

    protected final WarehouseLoadInfoBuilder warehouseLoadStatusInfoBuilder =
            new WarehouseLoadInfoBuilder();

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
        warehouseNodeTasksNum.put(WarehouseManager.DEFAULT_WAREHOUSE_ID, Maps.newHashMap());
        warehouseNodeToJobs.put(WarehouseManager.DEFAULT_WAREHOUSE_ID, Maps.newHashMap());
    }

    // returns -1 if there is no available be
    // find the node with the fewest tasks
    public long takeBeTaskSlot(long warehouseId, long jobId) {
        slotLock.lock();
        try {
            long nodeId = -1L;
            int minTasksNum = Integer.MAX_VALUE;
            Map<Long, Integer> nodeMap = warehouseNodeTasksNum.get(warehouseId);
            Map<Long, Set<Long>> nodeToJobs = warehouseNodeToJobs.get(warehouseId);
            if (nodeMap != null && nodeToJobs != null) {
                // find the node with the fewest tasks and does not contain the job
                for (Map.Entry<Long, Integer> entry : nodeMap.entrySet()) {
                    if (entry.getValue() < Config.max_routine_load_task_num_per_be
                            && entry.getValue() < minTasksNum
                            && (nodeToJobs.get(entry.getKey()) == null
                            || !nodeToJobs.get(entry.getKey()).contains(jobId))) {
                        nodeId = entry.getKey();
                        minTasksNum = entry.getValue();
                    }
                }
                // if there is no available be, find the node with the fewest tasks
                if (nodeId == -1) {
                    for (Map.Entry<Long, Integer> entry : nodeMap.entrySet()) {
                        if (entry.getValue() < Config.max_routine_load_task_num_per_be
                                && entry.getValue() < minTasksNum) {
                            nodeId = entry.getKey();
                            minTasksNum = entry.getValue();
                        }
                    }
                }
                if (nodeId != -1) {
                    nodeMap.put(nodeId, minTasksNum + 1);
                    nodeToJobs.computeIfAbsent(nodeId, k -> Sets.newHashSet()).add(jobId);
                    warehouseNodeTasksNum.put(warehouseId, nodeMap);
                    warehouseNodeToJobs.put(warehouseId, nodeToJobs);
                }
            }
            return nodeId;
        } finally {
            slotLock.unlock();
        }
    }

    public long takeNodeById(long warehouseId, long jobId, long nodeId) {
        slotLock.lock();
        try {
            Map<Long, Integer> nodeMap = warehouseNodeTasksNum.get(warehouseId);
            Map<Long, Set<Long>> nodeToJobs = warehouseNodeToJobs.get(warehouseId);
            Integer taskNum = nodeMap.get(nodeId);
            Set<Long> jobs = nodeToJobs.computeIfAbsent(nodeId, k -> Sets.newHashSet());
            if (taskNum != null && taskNum < Config.max_routine_load_task_num_per_be) {
                nodeMap.put(nodeId, taskNum + 1);
                jobs.add(jobId);
                return nodeId;
            } else {
                return -1L;
            }
        } finally {
            slotLock.unlock();
        }
    }

    public void releaseBeTaskSlot(long warehouseId, long jobId, long nodeId) {
        slotLock.lock();
        try {
            Map<Long, Integer> nodeMap = warehouseNodeTasksNum.get(warehouseId);
            Map<Long, Set<Long>> nodeToJobs = warehouseNodeToJobs.get(warehouseId);
            if (nodeMap.containsKey(nodeId)) {
                int tasksNum = nodeMap.get(nodeId);
                if (tasksNum > 0) {
                    nodeMap.put(nodeId, tasksNum - 1);
                } else {
                    nodeMap.put(nodeId, 0);
                }
            }
            if (nodeToJobs.containsKey(nodeId)) {
                Set<Long> jobs = nodeToJobs.get(nodeId);
                jobs.remove(jobId);
            }
        } finally {
            slotLock.unlock();
        }
    }

    public void updateBeTaskSlot() {
        slotLock.lock();
        try {
            // TODO: need to refactor after be split into cn + dn
            List<Long> finalAliveNodeIds = new ArrayList<>();
            // collect all nodes group by warehouse
            if (RunMode.isSharedDataMode()) {
                for (Warehouse warehouse : GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouses()) {
                    List<Long> allComputeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                            .getAllComputeNodeIds(warehouse.getId());
                    List<Long> aliveNodeIds = new ArrayList<>();
                    for (long nodeId : allComputeNodeIds) {
                        ComputeNode node =
                                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                        if (node != null && node.isAlive()) {
                            aliveNodeIds.add(nodeId);
                        }
                    }

                    finalAliveNodeIds.addAll(aliveNodeIds);

                    // add new nodes
                    Map<Long, Integer> nodesInfo = warehouseNodeTasksNum.get(warehouse.getId());
                    if (nodesInfo == null) {
                        nodesInfo = new HashMap<>();
                        warehouseNodeTasksNum.put(warehouse.getId(), nodesInfo);
                    }
                    Map<Long, Set<Long>> nodeToJobs = warehouseNodeToJobs.get(warehouse.getId());
                    if (nodeToJobs == null) {
                        nodeToJobs = new HashMap<>();
                        warehouseNodeToJobs.put(warehouse.getId(), nodeToJobs);
                    }
                    for (Long nodeId : aliveNodeIds) {
                        if (!nodesInfo.containsKey(nodeId)) {
                            nodesInfo.put(nodeId, 0);
                            nodeToJobs.put(nodeId, Sets.newHashSet());
                        }
                    }
                }
            } else {
                finalAliveNodeIds.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true));
                // add new nodes
                for (Long nodeId : finalAliveNodeIds) {
                    Map<Long, Integer> nodesInfo = warehouseNodeTasksNum.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
                    Map<Long, Set<Long>> nodeToJobs = warehouseNodeToJobs.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
                    if (!nodesInfo.containsKey(nodeId)) {
                        nodesInfo.put(nodeId, 0);
                        nodeToJobs.put(nodeId, Sets.newHashSet());
                    }
                }
            }

            // remove not alive be
            for (Map<Long, Integer> nodesInfo : warehouseNodeTasksNum.values()) {
                nodesInfo.keySet().removeIf(nodeId -> !finalAliveNodeIds.contains(nodeId));
            }
            for (Map<Long, Set<Long>> nodeToJobs : warehouseNodeToJobs.values()) {
                nodeToJobs.keySet().removeIf(nodeId -> !finalAliveNodeIds.contains(nodeId));
            }
        } finally {
            slotLock.unlock();
        }
    }

    public void createRoutineLoadJob(CreateRoutineLoadStmt createRoutineLoadStmt) throws StarRocksException {
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
                throw new StarRocksException("Unknown data source type: " + type);
        }

        routineLoadJob.setOrigStmt(createRoutineLoadStmt.getOrigStmt());
        addRoutineLoadJob(routineLoadJob, createRoutineLoadStmt.getDBName());
    }

    public Map<Long, Integer> getNodeTasksNum() {
        return warehouseNodeTasksNum.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    @VisibleForTesting
    public Map<Long, Integer> getNodeTasksNum(long warehouseId) {
        return warehouseNodeTasksNum.get(warehouseId);
    }

    public Map<Long, Set<Long>> getNodeToJobs(long warehouseId) {
        return warehouseNodeToJobs.get(warehouseId);
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob, String dbName) throws DdlException {
        int nodeNum = 0;
        slotLock.lock();
        try {
            Map<Long, Integer> nodeTasksNum = getNodeTasksNum(routineLoadJob.getWarehouseId());
            if (nodeTasksNum != null) {
                nodeNum = nodeTasksNum.size();
            }
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
            long maxConcurrentTasks = nodeNum * Config.max_routine_load_task_num_per_be;
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
                        + "But we only support " + nodeNum + "*" + Config.max_routine_load_task_num_per_be + " tasks."
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
            throws StarRocksException {
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

    public void resumeRoutineLoadJob(ResumeRoutineLoadStmt resumeRoutineLoadStmt) throws StarRocksException {
        RoutineLoadJob routineLoadJob = checkPrivAndGetJob(resumeRoutineLoadStmt.getDbFullName(),
                resumeRoutineLoadStmt.getName());

        routineLoadJob.autoResumeCount = 0;
        routineLoadJob.firstResumeTimestamp = 0;
        routineLoadJob.autoResumeLock = false;
        routineLoadJob.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, false /* not replay */);
        routineLoadJob.clearOtherMsg();
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("current_state", routineLoadJob.getState())
                .add("user", ConnectContext.get().getQualifiedUser())
                .add("msg", "routine load job has been resumed by user")
                .build());
    }

    public void stopRoutineLoadJob(StopRoutineLoadStmt stopRoutineLoadStmt)
            throws StarRocksException {
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
            int result = 0;
            for (Map<Long, Integer> nodeMap : warehouseNodeTasksNum.values()) {
                result += nodeMap.values()
                        .stream()
                        .reduce(0,
                                (acc, num) -> acc + Config.max_routine_load_task_num_per_be - num);
            }
            return result;
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

    /**
     * use dbFullName and jobName to filter routine load jobs.
     * if includeHistory is false, filter not running job in result else return all of result.
     */
    public List<RoutineLoadJob> getJob(String dbFullName, String jobName, boolean includeHistory)
            throws MetaNotFoundException {
        List<RoutineLoadJob> result = Lists.newArrayList();
        readLock();
        try {
            if (dbFullName == null && jobName == null) {
                result.addAll(idToRoutineLoadJob.values());
                sortRoutineLoadJob(result);
            } else if (dbFullName == null && jobName != null) {
                result = idToRoutineLoadJob.values().stream().filter(entity -> entity.getName().equals(jobName))
                        .collect(Collectors.toList());
                sortRoutineLoadJob(result);
            } else {
                long dbId = 0L;
                Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbFullName);
                if (database == null) {
                    throw new MetaNotFoundException("failed to find database by dbFullName " + dbFullName);
                }
                dbId = database.getId();

                Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob =
                        dbToNameToRoutineLoadJob.getOrDefault(dbId, Maps.newHashMap());
                if (jobName != null) {
                    result.addAll(nameToRoutineLoadJob.getOrDefault(jobName, Lists.newArrayList()));
                    sortRoutineLoadJob(result);
                } else {
                    for (List<RoutineLoadJob> jobs : nameToRoutineLoadJob.values()) {
                        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(jobs);
                        sortRoutineLoadJob(routineLoadJobList);
                        result.addAll(routineLoadJobList);
                    }
                }
            }

            if (!includeHistory) {
                result = result.stream().filter(entity -> !entity.isFinal()).collect(Collectors.toList());
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

    public boolean checkTaskInJob(long jobId, UUID taskId) {
        readLock();
        try {
            if (!idToRoutineLoadJob.containsKey(jobId)) {
                return false;
            }
            return idToRoutineLoadJob.get(jobId).containsTask(taskId);
        } finally {
            readUnlock();
        }
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(Set<RoutineLoadJob.JobState> desiredStates) {
        return idToRoutineLoadJob.values().stream()
                .filter(entity -> desiredStates.contains(entity.getState()))
                .collect(Collectors.toList());
    }

    public long numUnstableJobs() {
        return idToRoutineLoadJob.values().stream().filter(RoutineLoadJob::isUnstable).count();
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

        warehouseLoadStatusInfoBuilder.withRemovedJob(routineLoadJob);
    }

    public void updateRoutineLoadJob() throws StarRocksException {
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
        } catch (StarRocksException e) {
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
    public void alterRoutineLoadJob(AlterRoutineLoadStmt stmt) throws StarRocksException {
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

    public void replayAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) throws StarRocksException, IOException {
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

    public void saveRoutineLoadJobsV2(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        final int cnt = 1 + idToRoutineLoadJob.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.ROUTINE_LOAD_MGR, cnt);
        writer.writeInt(idToRoutineLoadJob.size());
        for (RoutineLoadJob loadJob : idToRoutineLoadJob.values()) {
            writer.writeJson(loadJob);
        }
        writer.close();
    }

    public void loadRoutineLoadJobsV2(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(RoutineLoadJob.class, routineLoadJob -> {
            if (routineLoadJob.needRemove()) {
                LOG.info("discard expired job [{}]", routineLoadJob.getId());
                return;
            }

            putJob(routineLoadJob);
        });
    }

    public Map<Long, WarehouseLoadStatusInfo> getWarehouseLoadInfo() {
        readLock();
        try {
            return warehouseLoadStatusInfoBuilder.buildFromJobs(idToRoutineLoadJob.values());
        } finally {
            readUnlock();
        }
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("RoutineLoad", (long) idToRoutineLoadJob.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> samples = idToRoutineLoadJob.values()
                .stream()
                .limit(MEMORY_JOB_SAMPLES)
                .collect(Collectors.toList());

        return Lists.newArrayList(Pair.create(samples, (long) idToRoutineLoadJob.size()));
    }

    public Map<Long, Long> getRunningRoutingLoadCount() {
        Map<Long, Long> result = new HashMap<>();
        readLock();
        try {
            for (RoutineLoadJob loadJob : idToRoutineLoadJob.values()) {
                if (!loadJob.isFinal()) {
                    result.compute(loadJob.getWarehouseId(), (key, value) -> value == null ? 1L : value + 1);
                }
            }
        } finally {
            readUnlock();
        }
        return result;
    }
}
