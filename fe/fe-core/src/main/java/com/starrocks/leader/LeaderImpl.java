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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/master/MasterImpl.java

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

package com.starrocks.leader;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.alter.AlterJobV2.JobType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.DeleteJob;
import com.starrocks.load.OlapDeleteJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CheckConsistencyTask;
import com.starrocks.task.ClearAlterTask;
import com.starrocks.task.CloneTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.CreateReplicaTask.RecoverySource;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.DropAutoIncrementMapTask;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.task.PushTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UpdateTabletMetaInfoTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TBackendMeta;
import com.starrocks.thrift.TBasePartitionDesc;
import com.starrocks.thrift.TBeginRemoteTxnRequest;
import com.starrocks.thrift.TBeginRemoteTxnResponse;
import com.starrocks.thrift.TColumnMeta;
import com.starrocks.thrift.TCommitRemoteTxnRequest;
import com.starrocks.thrift.TCommitRemoteTxnResponse;
import com.starrocks.thrift.TDataProperty;
import com.starrocks.thrift.TDistributionDesc;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.thrift.THashDistributionInfo;
import com.starrocks.thrift.TIndexInfo;
import com.starrocks.thrift.TIndexMeta;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionInfo;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TRandomDistributionInfo;
import com.starrocks.thrift.TRange;
import com.starrocks.thrift.TRangePartitionDesc;
import com.starrocks.thrift.TReplicaMeta;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TSchemaMeta;
import com.starrocks.thrift.TSinglePartitionDesc;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableMeta;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletMeta;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

public class LeaderImpl {
    private static final Logger LOG = LogManager.getLogger(LeaderImpl.class);

    private ReportHandler reportHandler = new ReportHandler();

    public LeaderImpl() {
        reportHandler.start();
    }

    public TMasterResult finishTask(TFinishTaskRequest request) {
        // if current node is not master, reject the request
        TMasterResult result = new TMasterResult();
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }
        TStatus tStatus = new TStatus(TStatusCode.OK);
        result.setStatus(tStatus);
        // check task status
        // retry task by report process
        TStatus taskStatus = request.getTask_status();
        if (LOG.isDebugEnabled()) {
            LOG.debug("get task report: {}", request.toString());
        }
        if (taskStatus.getStatus_code() != TStatusCode.OK) {
            LOG.warn("finish task reports bad. request: {}", request.toString());
        }

        // get backend
        TBackend tBackend = request.getBackend();
        String host = tBackend.getHost();
        int bePort = tBackend.getBe_port();
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(host, bePort);
        if (backend == null) {
            tStatus.setStatus_code(TStatusCode.CANCELLED);
            List<String> errorMsgs = new ArrayList<>();
            errorMsgs.add("backend not exist.");
            tStatus.setError_msgs(errorMsgs);
            LOG.warn("backend does not found. host: {}, be port: {}. task: {}", host, bePort, request.toString());
            return result;
        }

        long backendId = backend.getId();
        TTaskType taskType = request.getTask_type();
        long signature = request.getSignature();
        AgentTask task = AgentTaskQueue.getTask(backendId, taskType, signature);
        if (task == null) {
            if (taskType != TTaskType.DROP && taskType != TTaskType.STORAGE_MEDIUM_MIGRATE
                    && taskType != TTaskType.RELEASE_SNAPSHOT && taskType != TTaskType.CLEAR_TRANSACTION_TASK) {
                String errMsg = "cannot find task. type: " + taskType + ", backendId: " + backendId
                        + ", signature: " + signature;
                LOG.warn(errMsg);
                tStatus.setStatus_code(TStatusCode.CANCELLED);
                List<String> errorMsgs = new ArrayList<String>();
                errorMsgs.add(errMsg);
                tStatus.setError_msgs(errorMsgs);
            }
            return result;
        } else {
            if (taskStatus.getStatus_code() != TStatusCode.OK) {
                task.failed();
                String taskErrMsg = taskStatus.getError_msgs() != null ? Joiner.on(",").join(taskStatus.getError_msgs()) : "";
                String errMsg = "task type: " + taskType + ", status_code: " + taskStatus.getStatus_code().toString() + ", " +
                        taskErrMsg + ", backendId: " + backend + ", signature: " + signature;
                task.setErrorMsg(errMsg);
                // We start to let FE perceive the task's error msg
                if (taskType != TTaskType.MAKE_SNAPSHOT && taskType != TTaskType.UPLOAD
                        && taskType != TTaskType.DOWNLOAD && taskType != TTaskType.MOVE
                        && taskType != TTaskType.CLONE && taskType != TTaskType.PUBLISH_VERSION
                        && taskType != TTaskType.CREATE && taskType != TTaskType.UPDATE_TABLET_META_INFO
                        && taskType != TTaskType.DROP_AUTO_INCREMENT_MAP
                        && taskType != TTaskType.STORAGE_MEDIUM_MIGRATE) {
                    return result;
                }
            }
        }

        try {
            List<TTabletInfo> finishTabletInfos;
            switch (taskType) {
                case CREATE:
                    Preconditions.checkState(request.isSetReport_version());
                    finishCreateReplica(task, request);
                    break;
                case REALTIME_PUSH:
                    checkHasTabletInfo(request);
                    Preconditions.checkState(request.isSetReport_version());
                    finishRealtimePush(task, request);
                    break;
                case PUBLISH_VERSION:
                    finishPublishVersion(task, request);
                    break;
                case CLEAR_ALTER_TASK:
                    finishClearAlterTask(task, request);
                    break;
                case DROP:
                    finishDropReplica(task);
                    break;
                case SCHEMA_CHANGE:
                case ROLLUP:
                    throw new RuntimeException("Old alter job is not supported.");
                case CLONE:
                    finishClone(task, request);
                    break;
                case STORAGE_MEDIUM_MIGRATE:
                    finishStorageMigration(task, request);
                    break;
                case CHECK_CONSISTENCY:
                    finishConsistenctCheck(task, request);
                    break;
                case MAKE_SNAPSHOT:
                    finishMakeSnapshot(task, request);
                    break;
                case UPLOAD:
                    finishUpload(task, request);
                    break;
                case DOWNLOAD:
                    finishDownloadTask(task, request);
                    break;
                case MOVE:
                    finishMoveDirTask(task, request);
                    break;
                case RECOVER_TABLET:
                    finishRecoverTablet(task);
                    break;
                case ALTER:
                    finishAlterTask(task);
                    break;
                case UPDATE_TABLET_META_INFO:
                    finishUpdateTabletMeta(task, request);
                    break;
                case DROP_AUTO_INCREMENT_MAP:
                    finishDropAutoIncrementMapTask(task, request);
                    break;
                default:
                    break;
            }
        } catch (RejectedExecutionException e) {
            tStatus.setStatus_code(TStatusCode.TOO_MANY_TASKS);
            String errMsg = "task queue full";
            List<String> errorMsgs = new ArrayList<String>();
            LOG.warn(errMsg, e);
            errorMsgs.add(errMsg);
            tStatus.setError_msgs(errorMsgs);
        } catch (Exception e) {
            tStatus.setStatus_code(TStatusCode.CANCELLED);
            String errMsg = "finish agent task error.";
            LOG.warn(errMsg, e);
            List<String> errorMsgs = new ArrayList<String>();
            errorMsgs.add(errMsg);
            tStatus.setError_msgs(errorMsgs);
        }

        if (tStatus.getStatus_code() == TStatusCode.OK) {
            LOG.debug("report task success. {}", request.toString());
        }

        return result;
    }

    private void checkHasTabletInfo(TFinishTaskRequest request) throws Exception {
        if (!request.isSetFinish_tablet_infos() || request.getFinish_tablet_infos().isEmpty()) {
            throw new Exception("tablet info is not set");
        }
    }

    private void finishCreateReplica(AgentTask task, TFinishTaskRequest request) {
        // if we get here, this task will be removed from AgentTaskQueue for certain.
        // because in this function, the only problem that cause failure is meta missing.
        // and if meta is missing, we no longer need to resend this task
        try {
            CreateReplicaTask createReplicaTask = (CreateReplicaTask) task;
            if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
                createReplicaTask.countDownToZero(
                        task.getBackendId() + ": " + request.getTask_status().getError_msgs().toString());
            } else {
                long tabletId = createReplicaTask.getTabletId();
                if (request.isSetFinish_tablet_infos()) {
                    Replica replica = GlobalStateMgr.getCurrentInvertedIndex()
                            .getReplica(tabletId, createReplicaTask.getBackendId());
                    if (replica != null) {
                        replica.setPathHash(request.getFinish_tablet_infos().get(0).getPath_hash());
                        if (createReplicaTask.getRecoverySource() == RecoverySource.REPORT) {
                            /*
                             * This creates replica task is created by ReportHanlder,
                             * (See comment of Config.recover_with_empty_tablet)
                             * So we set replica back to good.
                             */
                            replica.setBad(false);
                            LOG.info(
                                    "finish recover create replica task. set replica to good. tablet {}, replica {}, backend {}",
                                    tabletId, task.getBackendId(), replica.getId());
                        }
                    }
                }

                // this should be called before 'countDownLatch()'
                GlobalStateMgr.getCurrentSystemInfo()
                        .updateBackendReportVersion(task.getBackendId(), request.getReport_version(), task.getDbId());

                createReplicaTask.countDownLatch(task.getBackendId(), task.getSignature());
                LOG.debug("finish create replica. tablet id: {}, be: {}, report version: {}",
                        tabletId, task.getBackendId(), request.getReport_version());
            }

            if (createReplicaTask.getRecoverySource() == RecoverySource.SCHEDULER) {
                GlobalStateMgr.getCurrentState().getTabletScheduler()
                        .finishCreateReplicaTask(createReplicaTask, request);
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
        }
    }

    private void finishUpdateTabletMeta(AgentTask task, TFinishTaskRequest request) {
        // if we get here, this task will be removed from AgentTaskQueue for certain.
        // because in this function, the only problem that cause failure is meta missing.
        // and if meta is missing, we no longer need to resend this task
        try {
            UpdateTabletMetaInfoTask tabletTask = (UpdateTabletMetaInfoTask) task;
            if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
                tabletTask.countDownToZero(
                        task.getBackendId() + ": " + request.getTask_status().getError_msgs().toString());
            } else {
                tabletTask.countDownLatch(task.getBackendId(), tabletTask.getTablets());
                LOG.debug("finish update tablet meta. tablet id: {}, be: {}", tabletTask.getTablets(),
                        task.getBackendId());
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPDATE_TABLET_META_INFO, task.getSignature());
        }
    }

    private void finishRealtimePush(AgentTask task, TFinishTaskRequest request) {
        List<TTabletInfo> finishTabletInfos = request.getFinish_tablet_infos();
        Preconditions.checkState(finishTabletInfos != null && !finishTabletInfos.isEmpty());

        PushTask pushTask = (PushTask) task;

        long dbId = pushTask.getDbId();
        long backendId = pushTask.getBackendId();
        long signature = task.getSignature();
        long transactionId = ((PushTask) task).getTransactionId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            return;
        }

        long tableId = pushTask.getTableId();
        long partitionId = pushTask.getPartitionId();
        long pushIndexId = pushTask.getIndexId();
        long pushTabletId = pushTask.getTabletId();
        // push finish type:
        //                  numOfFinishTabletInfos  tabletId schemaHash
        // Normal:                     1                   /          /
        // SchemaChangeHandler         2                 same      diff
        // RollupHandler               2                 diff      diff
        // 
        // reuse enum 'PartitionState' here as 'push finish type'
        PartitionState pushState = null;
        if (finishTabletInfos.size() == 1) {
            pushState = PartitionState.NORMAL;
        } else if (finishTabletInfos.size() == 2) {
            if (finishTabletInfos.get(0).getTablet_id() == finishTabletInfos.get(1).getTablet_id()) {
                pushState = PartitionState.SCHEMA_CHANGE;
            } else {
                pushState = PartitionState.ROLLUP;
            }
        } else {
            LOG.warn("invalid push report infos. finishTabletInfos' size: " + finishTabletInfos.size());
            return;
        }
        LOG.debug("push report state: {}", pushState.name());

        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                throw new MetaNotFoundException("cannot find table[" + tableId + "] when push finished");
            }

            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("cannot find partition[" + partitionId + "] when push finished");
            }

            MaterializedIndex pushIndex = partition.getIndex(pushIndexId);
            if (pushIndex == null) {
                // yiguolei: if index is dropped during load, it is not a failure.
                // throw exception here and cause the job to cancel the task
                throw new MetaNotFoundException("cannot find index[" + pushIndex + "] when push finished");
            }

            // should be done before addReplicaPersistInfos and countDownLatch
            long reportVersion = request.getReport_version();
            GlobalStateMgr.getCurrentSystemInfo().updateBackendReportVersion(task.getBackendId(), reportVersion,
                    task.getDbId());

            List<Long> tabletIds = finishTabletInfos.stream().map(
                    tTabletInfo -> tTabletInfo.getTablet_id()).collect(Collectors.toList());
            List<TabletMeta> tabletMetaList = GlobalStateMgr.getCurrentInvertedIndex().getTabletMetaList(tabletIds);

            // handle load job
            // TODO yiguolei: why delete should check request version and task version?
            if (pushTask.getPushType() == TPushType.LOAD || pushTask.getPushType() == TPushType.LOAD_DELETE) {
                Preconditions.checkArgument(false, "LOAD and LOAD_DELETE not supported");
            } else if (pushTask.getPushType() == TPushType.DELETE) {
                DeleteJob deleteJob = GlobalStateMgr.getCurrentState().getDeleteMgr().getDeleteJob(transactionId);
                if (deleteJob == null) {
                    throw new MetaNotFoundException("cannot find delete job, job[" + transactionId + "]");
                }
                Preconditions.checkState(deleteJob instanceof OlapDeleteJob);
                OlapDeleteJob olapDeleteJob = (OlapDeleteJob) deleteJob;
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    long tabletId = tabletIds.get(i);
                    Replica replica = findRelatedReplica(olapTable, partition,
                            backendId, tabletId, tabletMeta.getIndexId());
                    if (replica != null) {
                        olapDeleteJob.addFinishedReplica(partitionId, pushTabletId, replica);
                        pushTask.countDownLatch(backendId, pushTabletId);
                    }
                }
            } else if (pushTask.getPushType() == TPushType.LOAD_V2) {
                long loadJobId = pushTask.getLoadJobId();
                com.starrocks.load.loadv2.LoadJob job =
                        GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJob(loadJobId);
                if (job == null) {
                    throw new MetaNotFoundException("cannot find load job, job[" + loadJobId + "]");
                }
                for (int i = 0; i < tabletMetaList.size(); i++) {
                    TabletMeta tabletMeta = tabletMetaList.get(i);
                    checkReplica(finishTabletInfos.get(i), tabletMeta);
                    long tabletId = tabletIds.get(i);
                    Replica replica =
                            findRelatedReplica(olapTable, partition, backendId, tabletId, tabletMeta.getIndexId());
                    // if the replica is under schema change, could not find the replica with aim schema hash
                    if (replica != null) {
                        ((SparkLoadJob) job).addFinishedReplica(replica.getId(), pushTabletId, backendId);
                    }
                }
            }

            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.debug("finish push replica. tabletId: {}, backendId: {}", pushTabletId, backendId);
        } catch (MetaNotFoundException e) {
            AgentTaskQueue.removeTask(backendId, TTaskType.REALTIME_PUSH, signature);
            LOG.warn("finish push replica error", e);
        } finally {
            db.writeUnlock();
        }
    }

    private void checkReplica(TTabletInfo tTabletInfo, TabletMeta tabletMeta)
            throws MetaNotFoundException {
        long tabletId = tTabletInfo.getTablet_id();
        int schemaHash = tTabletInfo.getSchema_hash();
        // during finishing stage, index's schema hash switched, when old schema hash finished
        // current index hash != old schema hash and alter job's new schema hash != old schema hash
        // the check replica will failed
        // should use tabletid not pushTabletid because in rollup state, the push tabletid != tabletid
        // and tablet meta will not contain rollupindex's schema hash
        if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
            // rollup may be dropped
            throw new MetaNotFoundException("tablet " + tabletId + " does not exist");
        }

        // lake tablet not need to compare schemaHash
        if (tabletMeta.isLakeTablet()) {
            return;
        }

        if (!tabletMeta.containsSchemaHash(schemaHash)) {
            throw new MetaNotFoundException("tablet[" + tabletId
                    + "] schemaHash is not equal to index's switchSchemaHash. "
                    + tabletMeta.toString() + " vs. " + schemaHash);
        }
    }

    private Replica findRelatedReplica(OlapTable olapTable, Partition partition,
                                       long backendId, long tabletId, long indexId)
            throws MetaNotFoundException {
        // both normal index and rollingup index are in inverted index
        // this means the index is dropped during load
        if (indexId == TabletInvertedIndex.NOT_EXIST_VALUE) {
            LOG.warn("tablet[{}] may be dropped. push index[{}]", tabletId, indexId);
            return null;
        }
        MaterializedIndex index = partition.getIndex(indexId);
        if (index == null) {
            if (olapTable.getState() == OlapTableState.ROLLUP) {
                // this happens when:
                // a rollup job is finish and a delete job is the next first job (no load job before)
                // and delete task is first send to base tablet, so it will return 2 tablets info.
                // the second tablet is rollup tablet and it is no longer exist in alterJobs queue.
                // just ignore the rollup tablet info. it will be handled in rollup tablet delete task report.

                // add log to observe
                LOG.warn("Cannot find table[{}].", olapTable.getId());
                return null;
            }
            throw new MetaNotFoundException("Could not find rollup index.");
        }

        Tablet tablet = index.getTablet(tabletId);
        if (tablet instanceof LakeTablet) {
            return new Replica(tabletId, backendId, -1, NORMAL);
        } else {
            if (tablet == null) {
                LOG.warn("could not find tablet {} in rollup index {} ", tabletId, indexId);
                return null;
            }
            Replica replica = ((LocalTablet) tablet).getReplicaByBackendId(backendId);
            if (replica == null) {
                LOG.warn("could not find replica with backend {} in tablet {} in rollup index {} ",
                        backendId, tabletId, indexId);
            }
            return replica;
        }
    }

    private void finishClearAlterTask(AgentTask task, TFinishTaskRequest request) {
        ClearAlterTask clearAlterTask = (ClearAlterTask) task;
        clearAlterTask.setFinished(true);
        AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
    }

    private void finishPublishVersion(AgentTask task, TFinishTaskRequest request) {
        List<Long> errorTabletIds = null;
        if (request.isSetError_tablet_ids()) {
            errorTabletIds = request.getError_tablet_ids();
        }

        if (request.isSetReport_version()) {
            // report version is required. here we check if set, for compatibility.
            long reportVersion = request.getReport_version();
            GlobalStateMgr.getCurrentSystemInfo()
                    .updateBackendReportVersion(task.getBackendId(), reportVersion, task.getDbId());
        }

        PublishVersionTask publishVersionTask = (PublishVersionTask) task;
        publishVersionTask.setErrorTablets(errorTabletIds);
        if (Config.enable_new_publish_mechanism) {
            if (request.isSetTablet_versions()) {
                publishVersionTask.updateReplicaVersions(request.getTablet_versions());
            } else {
                LOG.error(
                        "new publish mechanism require BE to report tablet version, maybe BE has not upgraded?" +
                                "db_id: {} tx_id: {} BE: {}",
                        publishVersionTask.getDbId(), publishVersionTask.getTransactionId(), publishVersionTask.getBackendId());
            }
        }
        publishVersionTask.setIsFinished(true);
        TransactionState txnState = publishVersionTask.getTxnState();
        if (txnState != null) {
            txnState.updatePublishTaskFinishTime();
        }

        if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
            // not remove the task from queue and be will retry
            return;
        }
        AgentTaskQueue.removeTask(publishVersionTask.getBackendId(),
                publishVersionTask.getTaskType(),
                publishVersionTask.getSignature());
    }

    private ReplicaPersistInfo updateReplicaInfo(OlapTable olapTable, Partition partition,
                                                 long backendId, long pushIndexId, long indexId,
                                                 TTabletInfo tTabletInfo, PartitionState pushState)
            throws MetaNotFoundException {
        long tabletId = tTabletInfo.getTablet_id();
        int schemaHash = tTabletInfo.getSchema_hash();
        long version = tTabletInfo.getVersion();
        long rowCount = tTabletInfo.getRow_count();
        long dataSize = tTabletInfo.getData_size();

        if (indexId != pushIndexId) {
            // this may be a rollup tablet
            if (pushState != PartitionState.ROLLUP && indexId != TabletInvertedIndex.NOT_EXIST_VALUE) {
                // this probably should not happend. add log to observe(cmy)
                LOG.warn("push task report tablet[{}] with different index[{}] and is not in ROLLUP. push index[{}]",
                        tabletId, indexId, pushIndexId);
                return null;
            }

            if (indexId == TabletInvertedIndex.NOT_EXIST_VALUE) {
                LOG.warn("tablet[{}] may be dropped. push index[{}]", tabletId, pushIndexId);
                return null;
            }

            return null;
        }

        int currentSchemaHash = olapTable.getSchemaHashByIndexId(pushIndexId);
        if (schemaHash != currentSchemaHash) {
            if (pushState != PartitionState.SCHEMA_CHANGE) {
                // this should not happen. observe(cmy)
                throw new MetaNotFoundException("Diff tablet[" + tabletId + "] schemaHash. index[" + pushIndexId + "]: "
                        + currentSchemaHash + " vs. " + schemaHash);
            }
        }

        MaterializedIndex materializedIndex = partition.getIndex(pushIndexId);
        if (materializedIndex == null) {
            throw new MetaNotFoundException("Cannot find index[" + pushIndexId + "]");
        }
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(tabletId);
        if (tablet == null) {
            throw new MetaNotFoundException("Cannot find tablet[" + tabletId + "]");
        }

        // update replica info
        Replica replica = tablet.getReplicaByBackendId(backendId);
        if (replica == null) {
            throw new MetaNotFoundException("cannot find replica in tablet[" + tabletId + "], backend[" + backendId
                    + "]");
        }
        replica.updateRowCount(version, dataSize, rowCount);

        LOG.debug("replica[{}] report schemaHash:{}", replica.getId(), schemaHash);
        return ReplicaPersistInfo.createForLoad(olapTable.getId(), partition.getId(), pushIndexId, tabletId,
                replica.getId(), version, schemaHash, dataSize, rowCount);
    }

    private void finishDropReplica(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DROP, task.getSignature());
    }

    private void finishClone(AgentTask task, TFinishTaskRequest request) {
        CloneTask cloneTask = (CloneTask) task;
        if (cloneTask.getTaskVersion() == CloneTask.VERSION_2) {
            GlobalStateMgr.getCurrentState().getTabletScheduler().finishCloneTask(cloneTask, request);
        } else {
            LOG.warn("invalid clone task, ignore it. {}", task);
        }

        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CLONE, task.getSignature());
    }

    private void finishStorageMigration(AgentTask task, TFinishTaskRequest request) {
        try {
            // check if task success
            if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
                LOG.warn("tablet migrate failed. signature: {}, error msg: {}", request.getSignature(),
                        request.getTask_status().error_msgs);
                return;
            }

            // check tablet info is set
            if (!request.isSetFinish_tablet_infos() || request.getFinish_tablet_infos().isEmpty()) {
                LOG.warn("migration finish tablet infos not set. signature: {}", request.getSignature());
                return;
            }

            TTabletInfo reportedTablet = request.getFinish_tablet_infos().get(0);
            long tabletId = reportedTablet.getTablet_id();
            TabletMeta tabletMeta = GlobalStateMgr.getCurrentInvertedIndex().getTabletMeta(tabletId);
            if (tabletMeta == null) {
                LOG.warn("tablet meta does not exist. tablet id: {}", tabletId);
                return;
            }

            long dbId = tabletMeta.getDbId();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                LOG.warn("db does not exist. db id: {}", dbId);
                return;
            }

            db.writeLock();
            try {
                // local migration just set path hash
                Replica replica = GlobalStateMgr.getCurrentInvertedIndex().getReplica(tabletId, task.getBackendId());
                Preconditions.checkArgument(reportedTablet.isSetPath_hash());
                replica.setPathHash(reportedTablet.getPath_hash());
            } finally {
                db.writeUnlock();
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.STORAGE_MEDIUM_MIGRATE, task.getSignature());
        }
    }

    private void finishConsistenctCheck(AgentTask task, TFinishTaskRequest request) {
        CheckConsistencyTask checkConsistencyTask = (CheckConsistencyTask) task;

        if (checkConsistencyTask.getVersion() != request.getRequest_version()) {
            LOG.warn("check consisteny task is not match. [{}-{}]",
                    checkConsistencyTask.getVersion(), request.getRequest_version());
            return;
        }

        GlobalStateMgr.getCurrentState().getConsistencyChecker().handleFinishedConsistencyCheck(checkConsistencyTask,
                request.getTablet_checksum());
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CHECK_CONSISTENCY, task.getSignature());
    }

    private void finishMakeSnapshot(AgentTask task, TFinishTaskRequest request) {
        SnapshotTask snapshotTask = (SnapshotTask) task;
        if (GlobalStateMgr.getCurrentState().getBackupHandler().handleFinishedSnapshotTask(snapshotTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MAKE_SNAPSHOT, task.getSignature());
        }

    }

    private void finishUpload(AgentTask task, TFinishTaskRequest request) {
        UploadTask uploadTask = (UploadTask) task;
        if (GlobalStateMgr.getCurrentState().getBackupHandler().handleFinishedSnapshotUploadTask(uploadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.UPLOAD, task.getSignature());
        }
    }

    private void finishDownloadTask(AgentTask task, TFinishTaskRequest request) {
        DownloadTask downloadTask = (DownloadTask) task;
        if (GlobalStateMgr.getCurrentState().getBackupHandler().handleDownloadSnapshotTask(downloadTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DOWNLOAD, task.getSignature());
        }
    }

    private void finishMoveDirTask(AgentTask task, TFinishTaskRequest request) {
        DirMoveTask dirMoveTask = (DirMoveTask) task;
        if (GlobalStateMgr.getCurrentState().getBackupHandler().handleDirMoveTask(dirMoveTask, request)) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.MOVE, task.getSignature());
        }
    }

    private void finishDropAutoIncrementMapTask(AgentTask task, TFinishTaskRequest request) {
        try {
            DropAutoIncrementMapTask dropAutoIncrementMapTask = (DropAutoIncrementMapTask) task;
            if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
                dropAutoIncrementMapTask.countDownToZero(
                        task.getBackendId() + ": " + request.getTask_status().getError_msgs().toString());
            } else {
                dropAutoIncrementMapTask.countDownLatch(task.getBackendId());
                LOG.debug("finish drop auto increment map. table id: {}, be: {}",
                        dropAutoIncrementMapTask.tableId(), task.getBackendId());
            }
        } finally {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.DROP_AUTO_INCREMENT_MAP, task.getSignature());
        }
    }

    private void finishRecoverTablet(AgentTask task) {
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.RECOVER_TABLET, task.getSignature());
    }

    public TMasterResult report(TReportRequest request) throws TException {
        // if current node is not master, reject the request
        TMasterResult result = new TMasterResult();
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }
        return reportHandler.handleReport(request);
    }

    private void finishAlterTask(AgentTask task) {
        AlterReplicaTask alterTask = (AlterReplicaTask) task;
        if (alterTask.getJobType() == JobType.ROLLUP) {
            GlobalStateMgr.getCurrentState().getRollupHandler().handleFinishAlterTask(alterTask);
        } else if (alterTask.getJobType() == JobType.SCHEMA_CHANGE) {
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().handleFinishAlterTask(alterTask);
        }
        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
    }

    public TGetTableMetaResponse getTableMeta(TGetTableMetaRequest request) {
        String dbName = request.getDb_name();
        String tableName = request.getTable_name();
        TTableMeta tableMeta;
        TGetTableMetaResponse response = new TGetTableMetaResponse();

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            TStatus status = new TStatus(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Lists.newArrayList("missing db or table name"));
            response.setStatus(status);
            return response;
        }

        // checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.SELECT);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.setError_msgs(Lists.newArrayList("db not exist"));
            response.setStatus(status);
            return response;
        }

        try {
            db.readLock();

            Table table = db.getTable(tableName);
            if (table == null) {
                TStatus status = new TStatus(TStatusCode.NOT_FOUND);
                status.setError_msgs(Lists.newArrayList("table " + tableName + " not exist"));
                response.setStatus(status);
                return response;
            }

            // just only support OlapTable, ignore others such as ESTable
            if (!(table instanceof OlapTable)) {
                TStatus status = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);
                status.setError_msgs(Lists.newArrayList("only olap table supported"));
                response.setStatus(status);
                return response;
            }

            OlapTable olapTable = (OlapTable) table;
            tableMeta = new TTableMeta();
            tableMeta.setTable_id(table.getId());
            tableMeta.setTable_name(tableName);
            tableMeta.setTable_type(TableType.serialize(table.getType()));
            tableMeta.setDb_id(db.getId());
            tableMeta.setDb_name(dbName);
            tableMeta.setCluster_id(GlobalStateMgr.getCurrentState().getClusterId());
            tableMeta.setState(olapTable.getState().name());
            tableMeta.setBloomfilter_fpp(olapTable.getBfFpp());
            if (olapTable.getCopiedBfColumns() != null) {
                for (String bfColumn : olapTable.getCopiedBfColumns()) {
                    tableMeta.addToBloomfilter_columns(bfColumn);
                }
            }
            tableMeta.setBase_index_id(olapTable.getBaseIndexId());
            tableMeta.setColocate_group(olapTable.getColocateGroup());
            tableMeta.setKey_type(olapTable.getKeysType().name());

            TDistributionDesc distributionDesc = new TDistributionDesc();
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            distributionDesc.setDistribution_type(distributionInfo.getType().name());
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                THashDistributionInfo tHashDistributionInfo = new THashDistributionInfo();
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                tHashDistributionInfo.setBucket_num(hashDistributionInfo.getBucketNum());
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    tHashDistributionInfo.addToDistribution_columns(column.getName());
                }
                distributionDesc.setHash_distribution(tHashDistributionInfo);
            } else {
                TRandomDistributionInfo tRandomDistributionInfo = new TRandomDistributionInfo();
                RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) distributionInfo;
                tRandomDistributionInfo.setBucket_num(randomDistributionInfo.getBucketNum());
                distributionDesc.setRandom_distribution(tRandomDistributionInfo);
            }
            tableMeta.setDistribution_desc(distributionDesc);

            TableProperty tableProperty = olapTable.getTableProperty();
            for (Map.Entry<String, String> property : tableProperty.getProperties().entrySet()) {
                tableMeta.putToProperties(property.getKey(), property.getValue());
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            TBasePartitionDesc basePartitionDesc = new TBasePartitionDesc();
            // fill partition meta info
            for (Partition partition : olapTable.getAllPartitions()) {
                TPartitionMeta partitionMeta = new TPartitionMeta();
                partitionMeta.setPartition_id(partition.getId());
                partitionMeta.setPartition_name(partition.getName());
                partitionMeta.setState(partition.getState().name());
                partitionMeta.setVisible_version(partition.getVisibleVersion());
                partitionMeta.setVisible_time(partition.getVisibleVersionTime());
                partitionMeta.setNext_version(partition.getNextVersion());
                partitionMeta.setIs_temp(olapTable.getPartition(partition.getName(), true) != null);
                tableMeta.addToPartitions(partitionMeta);
                Short replicaNum = partitionInfo.getReplicationNum(partition.getId());
                boolean inMemory = partitionInfo.getIsInMemory(partition.getId());
                basePartitionDesc.putToReplica_num_map(partition.getId(), replicaNum);
                basePartitionDesc.putToIn_memory_map(partition.getId(), inMemory);
                DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                TDataProperty thriftDataProperty = new TDataProperty();
                thriftDataProperty.setStorage_medium(dataProperty.getStorageMedium());
                thriftDataProperty.setCold_time(dataProperty.getCooldownTimeMs());
                basePartitionDesc.putToData_property(partition.getId(), thriftDataProperty);
            }

            TPartitionInfo tPartitionInfo = new TPartitionInfo();
            tPartitionInfo.setType(partitionInfo.getType().toThrift());
            if (partitionInfo.isRangePartition()) {
                TRangePartitionDesc rangePartitionDesc = new TRangePartitionDesc();
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    TColumnMeta columnMeta = new TColumnMeta();
                    columnMeta.setColumnName(column.getName());
                    columnMeta.setColumnType(column.getType().toThrift());
                    columnMeta.setKey(column.isKey());
                    if (column.getAggregationType() != null) {
                        columnMeta.setAggregationType(column.getAggregationType().name());
                    }
                    columnMeta.setComment(column.getComment());
                    rangePartitionDesc.addToColumns(columnMeta);
                }
                Map<Long, Range<PartitionKey>> ranges = Maps.newHashMap(rangePartitionInfo.getIdToRange(false));
                Map<Long, Range<PartitionKey>> tempRanges = rangePartitionInfo.getIdToRange(true);
                ranges.putAll(tempRanges);
                for (Map.Entry<Long, Range<PartitionKey>> range : ranges.entrySet()) {
                    TRange tRange = new TRange();
                    tRange.setPartition_id(range.getKey());
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    DataOutputStream stream = new DataOutputStream(output);
                    range.getValue().lowerEndpoint().write(stream);
                    tRange.setStart_key(output.toByteArray());

                    output = new ByteArrayOutputStream();
                    stream = new DataOutputStream(output);
                    range.getValue().upperEndpoint().write(stream);
                    tRange.setEnd_key(output.toByteArray());
                    tRange.setBase_desc(basePartitionDesc);
                    tRange.setIs_temp(tempRanges.containsKey(range.getKey()));
                    rangePartitionDesc.putToRanges(range.getKey(), tRange);
                }
                tPartitionInfo.setRange_partition_desc(rangePartitionDesc);
            } else if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                TSinglePartitionDesc singlePartitionDesc = new TSinglePartitionDesc();
                singlePartitionDesc.setBase_desc(basePartitionDesc);
                tPartitionInfo.setSingle_partition_desc(singlePartitionDesc);
            } else {
                LOG.info("invalid partition type {}", partitionInfo.getType());
                return null;
            }

            tableMeta.setPartition_info(tPartitionInfo);

            // fill index meta info
            for (Index index : olapTable.getIndexes()) {
                TIndexInfo indexInfo = new TIndexInfo();
                indexInfo.setIndex_name(index.getIndexName());
                indexInfo.setIndex_type(index.getIndexType().name());
                indexInfo.setComment(index.getComment());
                for (String column : index.getColumns()) {
                    indexInfo.addToColumns(column);
                }
                tableMeta.addToIndex_infos(indexInfo);
            }

            for (Partition partition : olapTable.getAllPartitions()) {
                List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.ALL);
                for (MaterializedIndex index : indexes) {
                    TIndexMeta indexMeta = new TIndexMeta();
                    indexMeta.setIndex_id(index.getId());
                    indexMeta.setPartition_id(partition.getId());
                    indexMeta.setIndex_state(index.getState().toThrift());
                    indexMeta.setRow_count(index.getRowCount());
                    indexMeta.setRollup_index_id(-1L);
                    indexMeta.setRollup_finished_version(-1L);
                    TSchemaMeta schemaMeta = new TSchemaMeta();
                    MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexMetaByIndexId(index.getId());
                    schemaMeta.setSchema_version(materializedIndexMeta.getSchemaVersion());
                    schemaMeta.setSchema_hash(materializedIndexMeta.getSchemaHash());
                    schemaMeta.setShort_key_col_count(materializedIndexMeta.getShortKeyColumnCount());
                    schemaMeta.setStorage_type(materializedIndexMeta.getStorageType());
                    schemaMeta.setKeys_type(materializedIndexMeta.getKeysType().name());
                    for (Column column : materializedIndexMeta.getSchema()) {
                        TColumnMeta columnMeta = new TColumnMeta();
                        columnMeta.setColumnName(column.getName());
                        columnMeta.setColumnType(column.getType().toThrift());
                        columnMeta.setKey(column.isKey());
                        columnMeta.setAllowNull(column.isAllowNull());
                        if (column.getAggregationType() != null) {
                            columnMeta.setAggregationType(column.getAggregationType().name());
                        }
                        columnMeta.setComment(column.getComment());
                        columnMeta.setDefaultValue(column.getDefaultValue());
                        schemaMeta.addToColumns(columnMeta);
                    }
                    indexMeta.setSchema_meta(schemaMeta);
                    // fill in tablet info
                    for (Tablet tablet : index.getTablets()) {
                        TTabletMeta tTabletMeta = new TTabletMeta();
                        if (tablet instanceof LocalTablet) {
                            LocalTablet localTablet = (LocalTablet) tablet;
                            tTabletMeta.setTablet_id(tablet.getId());
                            tTabletMeta.setChecked_version(localTablet.getCheckedVersion());
                            tTabletMeta.setConsistent(localTablet.isConsistent());
                            TabletMeta tabletMeta = GlobalStateMgr.getCurrentInvertedIndex().getTabletMeta(tablet.getId());
                            tTabletMeta.setDb_id(tabletMeta.getDbId());
                            tTabletMeta.setTable_id(tabletMeta.getTableId());
                            tTabletMeta.setPartition_id(tabletMeta.getPartitionId());
                            tTabletMeta.setIndex_id(tabletMeta.getIndexId());
                            tTabletMeta.setStorage_medium(tabletMeta.getStorageMedium());
                            tTabletMeta.setOld_schema_hash(tabletMeta.getOldSchemaHash());
                            tTabletMeta.setNew_schema_hash(tabletMeta.getNewSchemaHash());
                            // fill replica info
                            for (Replica replica : localTablet.getImmutableReplicas()) {
                                TReplicaMeta replicaMeta = new TReplicaMeta();
                                replicaMeta.setReplica_id(replica.getId());
                                replicaMeta.setBackend_id(replica.getBackendId());
                                replicaMeta.setSchema_hash(replica.getSchemaHash());
                                replicaMeta.setVersion(replica.getVersion());
                                replicaMeta.setData_size(replica.getDataSize());
                                replicaMeta.setRow_count(replica.getRowCount());
                                replicaMeta.setState(replica.getState().name());
                                replicaMeta.setLast_failed_version(replica.getLastFailedVersion());
                                replicaMeta.setLast_failed_time(replica.getLastFailedTimestamp());
                                replicaMeta.setLast_success_version(replica.getLastSuccessVersion());
                                replicaMeta.setVersion_count(replica.getVersionCount());
                                replicaMeta.setPath_hash(replica.getPathHash());
                                replicaMeta.setBad(replica.isBad());
                                // TODO(wulei) fill backend info
                                tTabletMeta.addToReplicas(replicaMeta);
                            }
                        } else if (tablet instanceof LakeTablet) {
                            LakeTablet lakeTablet = (LakeTablet) tablet;
                            tTabletMeta.setTablet_id(tablet.getId());
                            tTabletMeta.setChecked_version(0);
                            tTabletMeta.setConsistent(true);
                            TabletMeta tabletMeta = GlobalStateMgr.getCurrentInvertedIndex().getTabletMeta(tablet.getId());
                            tTabletMeta.setDb_id(tabletMeta.getDbId());
                            tTabletMeta.setTable_id(tabletMeta.getTableId());
                            tTabletMeta.setPartition_id(tabletMeta.getPartitionId());
                            tTabletMeta.setIndex_id(tabletMeta.getIndexId());
                            tTabletMeta.setStorage_medium(tabletMeta.getStorageMedium());
                            tTabletMeta.setOld_schema_hash(tabletMeta.getOldSchemaHash());
                            tTabletMeta.setNew_schema_hash(tabletMeta.getNewSchemaHash());
                            // fill replica info
                            List<Replica> replicas = new ArrayList<Replica>();
                            lakeTablet.getQueryableReplicas(replicas, null, 0, -1, 0);
                            for (Replica replica : replicas) {
                                TReplicaMeta replicaMeta = new TReplicaMeta();
                                replicaMeta.setReplica_id(replica.getId());
                                replicaMeta.setBackend_id(replica.getBackendId());
                                replicaMeta.setSchema_hash(replica.getSchemaHash());
                                replicaMeta.setVersion(replica.getVersion());
                                replicaMeta.setData_size(replica.getDataSize());
                                replicaMeta.setRow_count(replica.getRowCount());
                                replicaMeta.setState(replica.getState().name());
                                replicaMeta.setLast_failed_version(replica.getLastFailedVersion());
                                replicaMeta.setLast_failed_time(replica.getLastFailedTimestamp());
                                replicaMeta.setLast_success_version(replica.getLastSuccessVersion());
                                replicaMeta.setVersion_count(replica.getVersionCount());
                                replicaMeta.setPath_hash(replica.getPathHash());
                                replicaMeta.setBad(replica.isBad());
                                // TODO(wulei) fill backend info
                                tTabletMeta.addToReplicas(replicaMeta);
                            }
                        } else {
                            throw new NotImplementedException(tablet.getClass().getName() + " is not implemented");
                        }
                        indexMeta.addToTablets(tTabletMeta);
                    }
                    tableMeta.addToIndexes(indexMeta);
                }
            }

            List<TBackendMeta> backends = new ArrayList<>();
            for (Backend backend : GlobalStateMgr.getCurrentState().getCurrentSystemInfo().getBackends()) {
                TBackendMeta backendMeta = new TBackendMeta();
                backendMeta.setBackend_id(backend.getId());
                backendMeta.setHost(backend.getHost());
                backendMeta.setBe_port(backend.getBeRpcPort());
                backendMeta.setRpc_port(backend.getBrpcPort());
                backendMeta.setHttp_port(backend.getHttpPort());
                backendMeta.setAlive(backend.isAlive());
                backendMeta.setState(backend.getBackendState().ordinal());
                backends.add(backendMeta);
            }
            response.setStatus(new TStatus(TStatusCode.OK));
            response.setTable_meta(tableMeta);
            response.setBackends(backends);
        } catch (Exception e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            LOG.info("exception: {}", e.getStackTrace());
            response.setStatus(status);
        } finally {
            db.readUnlock();
        }
        return response;
    }

    public TNetworkAddress masterAddr() {
        Pair<String, Integer> ipAndPort = GlobalStateMgr.getCurrentState().getLeaderIpAndRpcPort();
        return new TNetworkAddress(ipAndPort.first, ipAndPort.second);
    }

    public TBeginRemoteTxnResponse beginRemoteTxn(TBeginRemoteTxnRequest request) throws TException {
        TBeginRemoteTxnResponse response = new TBeginRemoteTxnResponse();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        // if current node is follower, forward it to leader
        if (!globalStateMgr.isLeader()) {
            TNetworkAddress addr = masterAddr();
            try {
                LOG.info("beginRemoteTxn as follower, forward it to master. Label: {}, master: {}",
                        request.getLabel(), addr.toString());
                response = FrontendServiceProxy.call(addr,
                        Config.thrift_rpc_timeout_ms,
                        Config.thrift_rpc_retry_times,
                        client -> client.beginRemoteTxn(request));
            } catch (Exception e) {
                LOG.warn("create thrift client failed during beginRemoteTxn, label: {}, exception: {}",
                        request.getLabel(), e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList("forward request to fe master failed"));
                response.setStatus(status);
            }
            return response;
        }

        Database db = globalStateMgr.getDb(request.getDb_id());
        if (db == null) {
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.setError_msgs(Lists.newArrayList("db not exist"));
            response.setStatus(status);
            LOG.warn("begin remote txn failed, db: {} not exist, label: {}",
                    request.getDb_id(), request.getLabel());
            return response;
        }

        long txnId;
        try {
            txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                    request.getTable_ids(), request.getLabel(),
                    new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    LoadJobSourceType.valueOf(request.getSource_type()), request.getTimeout_second());
        } catch (Exception e) {
            LOG.warn("begin remote txn failed, label {}", request.getLabel(), e);
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            response.setStatus(status);
            return response;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        response.setStatus(status);
        response.setTxn_id(txnId);
        response.setTxn_label(request.getLabel());
        LOG.info("begin remote txn, label: {}, txn_id: {}", request.getLabel(), txnId);
        return response;
    }

    public TCommitRemoteTxnResponse commitRemoteTxn(TCommitRemoteTxnRequest request) throws TException {
        TCommitRemoteTxnResponse response = new TCommitRemoteTxnResponse();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        // if current node is follower, forward it to leader
        if (!globalStateMgr.isLeader()) {
            TNetworkAddress addr = masterAddr();
            try {
                LOG.info("commitRemoteTxn as follower, forward it to master. txn_id: {}, master: {}",
                        request.getTxn_id(), addr.toString());
                response = FrontendServiceProxy.call(addr,
                        // commit txn might take a while, so add transaction timeout
                        Config.thrift_rpc_timeout_ms + Config.external_table_commit_timeout_ms,
                        Config.thrift_rpc_retry_times,
                        client -> client.commitRemoteTxn(request));
            } catch (Exception e) {
                LOG.warn("create thrift client failed during commitRemoteTxn, txn_id: {}, exception: {}",
                        request.getTxn_id(), e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList("forward request to fe master failed"));
                response.setStatus(status);
            }
            return response;
        }

        Database db = globalStateMgr.getDb(request.getDb_id());
        if (db == null) {
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.setError_msgs(Lists.newArrayList("db not exist or already deleted"));
            response.setStatus(status);
            LOG.warn("commit remote txn failed, db: {} not exist, txn_id: {}",
                    request.getDb_id(), request.getTxn_id());
            return response;
        }

        try {
            TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.getCommit_attachment());
            long timeoutMs = request.isSetCommit_timeout_ms() ? request.getCommit_timeout_ms() :
                    Config.external_table_commit_timeout_ms;
            boolean ret = GlobalStateMgr.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                    db, request.getTxn_id(),
                    TabletCommitInfo.fromThrift(request.getCommit_infos()),
                    TabletFailInfo.fromThrift(request.getFail_infos()),
                    timeoutMs, attachment);
            if (!ret) { // timeout
                TStatus status = new TStatus(TStatusCode.TIMEOUT);
                status.setError_msgs(Lists.newArrayList("commit and publish txn timeout"));
                response.setStatus(status);
                return response;
            }
        } catch (UserException e) {
            LOG.warn("commit remote txn failed, txn_id: {}", request.getTxn_id(), e);
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            response.setStatus(status);
            return response;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        response.setStatus(status);
        LOG.info("commit remote transaction: {} success", request.getTxn_id());
        return response;
    }

    public TAbortRemoteTxnResponse abortRemoteTxn(TAbortRemoteTxnRequest request) throws TException {
        TAbortRemoteTxnResponse response = new TAbortRemoteTxnResponse();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        // if current node is follower, forward it to leader
        if (!globalStateMgr.isLeader()) {
            TNetworkAddress addr = masterAddr();
            try {
                LOG.info("abortRemoteTxn as follower, forward it to master. txn_id: {}, master: {}",
                        request.getTxn_id(), addr.toString());
                response = FrontendServiceProxy.call(addr,
                        Config.thrift_rpc_timeout_ms,
                        Config.thrift_rpc_retry_times,
                        client -> client.abortRemoteTxn(request));
            } catch (Exception e) {
                LOG.warn("create thrift client failed during abortRemoteTxn, txn_id: {}, exception: {}",
                        request.getTxn_id(), e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList("forward request to fe master failed"));
                response.setStatus(status);
            }
            return response;
        }

        Database db = globalStateMgr.getDb(request.getDb_id());
        if (db == null) {
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.setError_msgs(Lists.newArrayList("db not exist or already deleted"));
            response.setStatus(status);
            LOG.warn("abort remote txn failed, db: {} not exist, txn_id: {}",
                    request.getDb_id(), request.getTxn_id());
            return response;
        }

        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                    request.getDb_id(), request.getTxn_id(), request.getError_msg());
        } catch (Exception e) {
            LOG.warn("abort remote txn failed, txn_id: {}", request.getTxn_id(), e);
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            response.setStatus(status);
            return response;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        response.setStatus(status);
        return response;
    }
}
