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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/AgentTaskQueue.java

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

package com.starrocks.task;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Task queue
 */
public class AgentTaskQueue {
    private static final Logger LOG = LogManager.getLogger(AgentTaskQueue.class);

    // backend id -> (task type -> (signature -> agent task))
    public static Table<Long, TTaskType, Map<Long, AgentTask>> tasks = HashBasedTable.create();
    private static int taskNum = 0;

    public static synchronized void addBatchTask(AgentBatchTask batchTask) {
        for (AgentTask task : batchTask.getAllTasks()) {
            addTask(task);
        }
    }

    public static synchronized void addTaskList(List<AgentTask> taskList) {
        taskList.forEach(AgentTaskQueue::addTask);
    }

    public static synchronized boolean addTask(AgentTask task) {
        long backendId = task.getBackendId();
        TTaskType type = task.getTaskType();

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (signatureMap == null) {
            signatureMap = Maps.newHashMap();
            tasks.put(backendId, type, signatureMap);
        }

        long signature = task.getSignature();
        if (signatureMap.containsKey(signature)) {
            return false;
        }
        signatureMap.put(signature, task);
        ++taskNum;
        LOG.debug("add task: type[{}], backend[{}], signature[{}]", type, backendId, signature);
        return true;
    }

    // remove all task in AgentBatchTask.
    // the caller should make sure all tasks in AgentBatchTask is type of 'type'
    public static synchronized void removeBatchTask(AgentBatchTask batchTask, TTaskType type) {
        for (AgentTask task : batchTask.getAllTasks()) {
            removeTask(task.getBackendId(), type, task.getSignature());
        }
    }

    public static synchronized void removeTask(long backendId, TTaskType type, long signature) {
        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (signatureMap == null) {
            return;
        }
        if (!signatureMap.containsKey(signature)) {
            return;
        }
        signatureMap.remove(signature);
        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", type, backendId, signature);
        --taskNum;
    }

    /*
     * we cannot define a push task with only 'backendId', 'signature' and 'TTaskType'
     * add version, and TPushType to help
     */
    public static synchronized void removePushTask(long backendId, long signature, long version,
                                                   TPushType pushType, TTaskType taskType) {
        if (!tasks.contains(backendId, taskType)) {
            return;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, taskType);
        AgentTask task = signatureMap.get(signature);
        if (task == null) {
            return;
        }

        PushTask pushTask = (PushTask) task;
        if (pushTask.getVersion() != version || pushTask.getPushType() != pushType) {
            return;
        }

        signatureMap.remove(signature);
        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", taskType, backendId, signature);
        --taskNum;
    }

    /*
     * we cannot define a push task with only 'backendId', 'signature' and 'TTaskType'
     * add version, and TPushType to help
     */
    public static synchronized void removePushTaskByTransactionId(long backendId, long transactionId,
                                                                  TPushType pushType, TTaskType taskType) {
        if (!tasks.contains(backendId, taskType)) {
            return;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, taskType);
        if (signatureMap == null) {
            return;
        }

        int numOfRemove = 0;
        Iterator<Long> signatureIt = signatureMap.keySet().iterator();
        while (signatureIt.hasNext()) {
            Long signature = signatureIt.next();
            AgentTask agentTask = signatureMap.get(signature);
            if (agentTask instanceof PushTask) {
                PushTask pushTask = (PushTask) agentTask;
                if (pushTask.getPushType() == pushType && pushTask.getTransactionId() == transactionId) {
                    signatureIt.remove();
                    --taskNum;
                    ++numOfRemove;
                }
            }
        }

        LOG.info("remove task: type[{}], backend[{}], transactionId[{}], numOfRemove[{}]",
                taskType, backendId, transactionId, numOfRemove);

    }

    public static synchronized void removeTaskOfType(TTaskType type, long signature) {
        // be id -> (signature -> task)
        Map<Long, Map<Long, AgentTask>> map = tasks.column(type);
        for (Map<Long, AgentTask> innerMap : map.values()) {
            innerMap.remove(signature);
        }
    }

    public static synchronized AgentTask getTask(long backendId, TTaskType type, long signature) {
        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (signatureMap == null) {
            return null;
        }
        return signatureMap.get(signature);
    }

    // this is just for unit test
    public static synchronized List<AgentTask> getTask(TTaskType type) {
        List<AgentTask> res = Lists.newArrayList();
        for (Map<Long, AgentTask> agentTasks : tasks.column(TTaskType.ALTER).values()) {
            res.addAll(agentTasks.values());
        }
        return res;
    }

    public static synchronized List<AgentTask> getDiffTasks(long backendId, Map<TTaskType, Set<Long>> runningTasks) {
        List<AgentTask> diffTasks = new ArrayList<AgentTask>();
        if (!tasks.containsRow(backendId)) {
            return diffTasks;
        }

        Map<TTaskType, Map<Long, AgentTask>> backendAllTasks = tasks.row(backendId);
        for (Map.Entry<TTaskType, Map<Long, AgentTask>> entry : backendAllTasks.entrySet()) {
            TTaskType taskType = entry.getKey();
            Map<Long, AgentTask> tasks = entry.getValue();
            Set<Long> excludeSignatures = new HashSet<Long>();
            if (runningTasks.containsKey(taskType)) {
                excludeSignatures = runningTasks.get(taskType);
            }

            for (Map.Entry<Long, AgentTask> taskEntry : tasks.entrySet()) {
                long signature = taskEntry.getKey();
                AgentTask task = taskEntry.getValue();
                if (!excludeSignatures.contains(signature)) {
                    diffTasks.add(task);
                }
            } // end for tasks
        } // end for backendAllTasks

        return diffTasks;
    }

    public static synchronized void removeReplicaRelatedTasks(long backendId, long tabletId) {
        if (!tasks.containsRow(backendId)) {
            return;
        }

        Map<TTaskType, Map<Long, AgentTask>> backendTasks = tasks.row(backendId);
        for (TTaskType type : TTaskType.values()) {
            if (backendTasks.containsKey(type)) {
                Map<Long, AgentTask> typeTasks = backendTasks.get(type);
                if (type == TTaskType.REALTIME_PUSH) {
                    Iterator<AgentTask> taskIterator = typeTasks.values().iterator();
                    while (taskIterator.hasNext()) {
                        PushTask realTimePushTask = (PushTask) taskIterator.next();
                        if (tabletId == realTimePushTask.getTabletId()) {
                            taskIterator.remove();
                        }
                    }
                } else {
                    if (typeTasks.containsKey(tabletId)) {
                        typeTasks.remove(tabletId);
                        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", type, backendId, tabletId);
                        --taskNum;
                    }
                }
            }
        } // end for types
    }

    // only for test now
    public static synchronized void clearAllTasks() {
        tasks.clear();
        taskNum = 0;
    }

    public static synchronized int getTaskNum() {
        return taskNum;
    }

    public static synchronized Multimap<Long, Long> getTabletIdsByType(TTaskType type) {
        Multimap<Long, Long> tabletIds = HashMultimap.create();
        Map<Long, Map<Long, AgentTask>> taskMap = tasks.column(type);
        if (taskMap != null) {
            for (Map<Long, AgentTask> signatureMap : taskMap.values()) {
                for (AgentTask task : signatureMap.values()) {
                    tabletIds.put(task.getDbId(), task.getTabletId());
                }
            }
        }
        return tabletIds;
    }

    public static synchronized int getTaskNum(long backendId, TTaskType type, boolean isFailed) {
        int taskNum = 0;
        if (backendId != -1) {
            Map<Long, AgentTask> taskMap = tasks.get(backendId, type);
            if (taskMap != null) {
                if (isFailed) {
                    for (AgentTask task : taskMap.values()) {
                        if (task.getFailedTimes() > 0) {
                            ++taskNum;
                        }
                    }
                } else {
                    taskNum += taskMap.size();
                }
            }
        } else {
            Map<Long, Map<Long, AgentTask>> taskMap = tasks.column(type);
            if (taskMap != null) {
                for (Map<Long, AgentTask> signatureMap : taskMap.values()) {
                    if (isFailed) {
                        for (AgentTask task : signatureMap.values()) {
                            if (task.getFailedTimes() > 0) {
                                ++taskNum;
                            }
                        }
                    } else {
                        taskNum += signatureMap.size();
                    }
                }
            }
        }

        LOG.debug("get task num with type[{}] in backend[{}]: {}. isFailed: {}",
                type.name(), backendId, taskNum, isFailed);
        return taskNum;
    }

    public static synchronized List<AgentTask> getFailedTask(long backendId, TTaskType type) {
        Map<Long, AgentTask> taskMap = tasks.get(backendId, type);
        List<AgentTask> tasks = Lists.newArrayList();
        if (taskMap != null) {
            for (AgentTask task : taskMap.values()) {
                if (task.getFailedTimes() > 0) {
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }

    public static synchronized List<Object> getSamplesForMemoryTracker() {
        List<Object> result = new ArrayList<>();
        // Get one task of each type
        for (TTaskType type : TTaskType.values()) {
            Map<Long, Map<Long, AgentTask>> tasksForType = tasks.column(type);
            Optional<Map<Long, AgentTask>> beTasks = tasksForType.values().stream().findAny();
            if (beTasks.isPresent()) {
                Optional<AgentTask> task = beTasks.get().values().stream().findAny();
                task.ifPresent(result::add);
            }
        }
        return result;
    }
}
