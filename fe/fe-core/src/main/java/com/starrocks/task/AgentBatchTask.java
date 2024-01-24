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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/AgentBatchTask.java

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

import com.google.common.collect.Lists;
import com.starrocks.common.ClientPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TAgentServiceVersion;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TCheckConsistencyReq;
import com.starrocks.thrift.TClearAlterTaskRequest;
import com.starrocks.thrift.TClearTransactionTaskRequest;
import com.starrocks.thrift.TCloneReq;
import com.starrocks.thrift.TCompactionReq;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TDownloadReq;
import com.starrocks.thrift.TDropAutoIncrementMapReq;
import com.starrocks.thrift.TDropTabletReq;
import com.starrocks.thrift.TMoveDirReq;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPublishVersionRequest;
import com.starrocks.thrift.TPushReq;
import com.starrocks.thrift.TReleaseSnapshotRequest;
import com.starrocks.thrift.TRemoteSnapshotRequest;
import com.starrocks.thrift.TReplicateSnapshotRequest;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStorageMediumMigrateReq;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateSchemaReq;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.thrift.TUploadReq;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*
 * This class group tasks by backend
 */
public class AgentBatchTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(AgentBatchTask.class);

    // backendId -> AgentTask List
    private final Map<Long, List<AgentTask>> backendIdToTasks;

    public AgentBatchTask() {
        this.backendIdToTasks = new HashMap<>();
    }

    public AgentBatchTask(AgentTask singleTask) {
        this();
        addTask(singleTask);
    }

    public void addTask(AgentTask agentTask) {
        if (agentTask == null) {
            return;
        }
        long backendId = agentTask.getBackendId();
        List<AgentTask> tasks = backendIdToTasks.computeIfAbsent(backendId, k -> new ArrayList<>());
        tasks.add(agentTask);
    }

    public void addTasks(Long backendId, List<AgentTask> agentTasks) {
        if (agentTasks == null) {
            return;
        }
        List<AgentTask> tasks = backendIdToTasks.computeIfAbsent(backendId, k -> new ArrayList<>());
        tasks.addAll(agentTasks);
    }

    public List<AgentTask> getAllTasks() {
        List<AgentTask> tasks = new ArrayList<>(getTaskNum());
        for (Map.Entry<Long, List<AgentTask>> entry : this.backendIdToTasks.entrySet()) {
            tasks.addAll(entry.getValue());
        }
        return tasks;
    }

    public int getTaskNum() {
        int num = 0;
        for (Map.Entry<Long, List<AgentTask>> entry : backendIdToTasks.entrySet()) {
            num += entry.getValue().size();
        }
        return num;
    }

    // return true only if all tasks are finished.
    // NOTICE that even if AgentTask.isFinished() return false, it does not mean that task is not finished.
    // this depends on caller's logic. See comments on 'isFinished' member.
    public boolean isFinished() {
        for (Map.Entry<Long, List<AgentTask>> entry : this.backendIdToTasks.entrySet()) {
            for (AgentTask agentTask : entry.getValue()) {
                if (!agentTask.isFinished()) {
                    return false;
                }
            }
        }
        return true;
    }

    // return the limit number of unfinished tasks.
    public List<AgentTask> getUnfinishedTasks(int limit) {
        List<AgentTask> res = Lists.newArrayList();
        if (limit == 0) {
            return res;
        }
        for (Map.Entry<Long, List<AgentTask>> entry : this.backendIdToTasks.entrySet()) {
            for (AgentTask agentTask : entry.getValue()) {
                if (!agentTask.isFinished()) {
                    res.add(agentTask);
                    if (res.size() >= limit) {
                        return res;
                    }
                }
            }
        }
        return res;
    }

    public int getFinishedTaskNum() {
        int count = 0;
        for (Map.Entry<Long, List<AgentTask>> entry : this.backendIdToTasks.entrySet()) {
            for (AgentTask agentTask : entry.getValue()) {
                count += agentTask.isFinished() ? 1 : 0;
            }
        }
        return count;
    }

    @Override
    public void run() {
        for (Long backendId : this.backendIdToTasks.keySet()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId);
                if (RunMode.isSharedDataMode() && computeNode == null) {
                    computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNode(backendId);
                }

                if (computeNode == null || !computeNode.isAlive()) {
                    continue;
                }

                String host = computeNode.getHost();
                int port = computeNode.getBePort();

                List<AgentTask> tasks = this.backendIdToTasks.get(backendId);
                // create AgentClient
                address = new TNetworkAddress(host, port);
                client = ClientPool.backendPool.borrowObject(address);
                List<TAgentTaskRequest> agentTaskRequests = new LinkedList<TAgentTaskRequest>();
                for (AgentTask task : tasks) {
                    agentTaskRequests.add(toAgentTaskRequest(task));
                }
                client.submit_tasks(agentTaskRequests);
                if (LOG.isDebugEnabled()) {
                    for (AgentTask task : tasks) {
                        LOG.debug("send task: type[{}], backend[{}], signature[{}]",
                                task.getTaskType(), backendId, task.getSignature());
                    }
                }
                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backendId, e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    // TODO: notify tasks rpc failed in trace
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        } // end for compute node
    }

    private TAgentTaskRequest toAgentTaskRequest(AgentTask task) {
        TAgentTaskRequest tAgentTaskRequest = new TAgentTaskRequest();
        tAgentTaskRequest.setProtocol_version(TAgentServiceVersion.V1);
        tAgentTaskRequest.setSignature(task.getSignature());

        TTaskType taskType = task.getTaskType();
        tAgentTaskRequest.setTask_type(taskType);
        switch (taskType) {
            case CREATE: {
                CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
                TCreateTabletReq request = createReplicaTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCreate_tablet_req(request);
                return tAgentTaskRequest;
            }
            case DROP: {
                DropReplicaTask dropReplicaTask = (DropReplicaTask) task;
                TDropTabletReq request = dropReplicaTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setDrop_tablet_req(request);
                return tAgentTaskRequest;
            }
            case REALTIME_PUSH: {
                PushTask pushTask = (PushTask) task;
                TPushReq request = pushTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPush_req(request);
                tAgentTaskRequest.setPriority(pushTask.getPriority());
                return tAgentTaskRequest;
            }
            case CLONE: {
                CloneTask cloneTask = (CloneTask) task;
                TCloneReq request = cloneTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setClone_req(request);
                return tAgentTaskRequest;
            }
            case ROLLUP:
            case SCHEMA_CHANGE:
                throw new RuntimeException("Old alter job is not supported.");
            case STORAGE_MEDIUM_MIGRATE: {
                StorageMediaMigrationTask migrationTask = (StorageMediaMigrationTask) task;
                TStorageMediumMigrateReq request = migrationTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setStorage_medium_migrate_req(request);
                return tAgentTaskRequest;
            }
            case CHECK_CONSISTENCY: {
                CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;
                TCheckConsistencyReq request = checkConsistencyTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setCheck_consistency_req(request);
                return tAgentTaskRequest;
            }
            case MAKE_SNAPSHOT: {
                SnapshotTask snapshotTask = (SnapshotTask) task;
                TSnapshotRequest request = snapshotTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setSnapshot_req(request);
                return tAgentTaskRequest;
            }
            case RELEASE_SNAPSHOT: {
                ReleaseSnapshotTask releaseSnapshotTask = (ReleaseSnapshotTask) task;
                TReleaseSnapshotRequest request = releaseSnapshotTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setRelease_snapshot_req(request);
                return tAgentTaskRequest;
            }
            case UPLOAD: {
                UploadTask uploadTask = (UploadTask) task;
                TUploadReq request = uploadTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setUpload_req(request);
                return tAgentTaskRequest;
            }
            case DOWNLOAD: {
                DownloadTask downloadTask = (DownloadTask) task;
                TDownloadReq request = downloadTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setDownload_req(request);
                return tAgentTaskRequest;
            }
            case PUBLISH_VERSION: {
                PublishVersionTask publishVersionTask = (PublishVersionTask) task;
                TPublishVersionRequest request = publishVersionTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setPublish_version_req(request);
                return tAgentTaskRequest;
            }
            case CLEAR_ALTER_TASK: {
                ClearAlterTask clearAlterTask = (ClearAlterTask) task;
                TClearAlterTaskRequest request = clearAlterTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setClear_alter_task_req(request);
                return tAgentTaskRequest;
            }
            case CLEAR_TRANSACTION_TASK: {
                ClearTransactionTask clearTransactionTask = (ClearTransactionTask) task;
                TClearTransactionTaskRequest request = clearTransactionTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setClear_transaction_task_req(request);
                return tAgentTaskRequest;
            }
            case MOVE: {
                DirMoveTask dirMoveTask = (DirMoveTask) task;
                TMoveDirReq request = dirMoveTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setMove_dir_req(request);
                return tAgentTaskRequest;
            }
            case UPDATE_TABLET_META_INFO: {
                UpdateTabletMetaInfoTask updateTabletMetaInfoTask = (UpdateTabletMetaInfoTask) task;
                TUpdateTabletMetaInfoReq request = updateTabletMetaInfoTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setUpdate_tablet_meta_info_req(request);
                return tAgentTaskRequest;
            }
            case DROP_AUTO_INCREMENT_MAP: {
                LOG.info("DROP_AUTO_INCREMENT_MAP begin");
                DropAutoIncrementMapTask dropAutoIncrementMapTask = (DropAutoIncrementMapTask) task;
                TDropAutoIncrementMapReq request = dropAutoIncrementMapTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setDrop_auto_increment_map_req(request);
                LOG.info("DROP_AUTO_INCREMENT_MAP end");
                return tAgentTaskRequest;
            }
            case ALTER: {
                AlterReplicaTask createRollupTask = (AlterReplicaTask) task;
                TAlterTabletReqV2 request = createRollupTask.toThrift();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(request.toString());
                }
                tAgentTaskRequest.setAlter_tablet_req_v2(request);
                return tAgentTaskRequest;
            }
            case COMPACTION: {
                CompactionTask compactionTask = (CompactionTask) task;
                TCompactionReq req = compactionTask.toThrift();
                tAgentTaskRequest.setCompaction_req(req);
                return tAgentTaskRequest;
            }
            case REMOTE_SNAPSHOT: {
                RemoteSnapshotTask remoteSnapshotTask = (RemoteSnapshotTask) task;
                TRemoteSnapshotRequest req = remoteSnapshotTask.toThrift();
                tAgentTaskRequest.setRemote_snapshot_req(req);
                return tAgentTaskRequest;
            }
            case REPLICATE_SNAPSHOT: {
                ReplicateSnapshotTask replicateSnapshotTask = (ReplicateSnapshotTask) task;
                TReplicateSnapshotRequest req = replicateSnapshotTask.toThrift();
                tAgentTaskRequest.setReplicate_snapshot_req(req);
                return tAgentTaskRequest;
            }
            case UPDATE_SCHEMA: {
                UpdateSchemaTask updateSchemaTask = (UpdateSchemaTask) task;
                TUpdateSchemaReq req = updateSchemaTask.toThrift();
                tAgentTaskRequest.setUpdate_schema_req(req);
                return tAgentTaskRequest;
            }
            default:
                LOG.debug("could not find task type for task [{}]", task);
                return null;
        }
    }
}
