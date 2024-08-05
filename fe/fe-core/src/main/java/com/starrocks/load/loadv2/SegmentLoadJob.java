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

package com.starrocks.load.loadv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PushTask;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPriority;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletQuorumFailedException;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SegmentLoadJob is the implementation of Spark Connector Bypass Write in the shard-nothing mode
 * There are 3 steps in SegmentLoadJob: SegmentLoadPendingTask, PushTask, CommitAndPublishTxn
 * Step1: SegmentLoadPendingTask will be created on method of unprotectedExecuteJob.
 * Step2: PushTask will be created by the method of onTaskFinished when SegmentLoadPendingTask is finished.
 * Step3: CommitAndPublicTxn will be called by the method of onTaskFinished when all of PushTask are finished.
 */
public class SegmentLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SegmentLoadJob.class);

    // --- members below not persist ---
    private Map<String, Pair<String, Long>> tabletMetaToDataFileInfo = null;
    private Map<String, String> tabletMetaToSchemaFilePath = null;
    private final Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();

    private final Map<Long, SegmentBrokerReaderParams> indexToPushBrokerReaderParams = Maps.newHashMap();
    private final Set<Long> quorumTablets = Sets.newHashSet();
    private final Set<Long> fullTablets = Sets.newHashSet();
    private final Set<Long> finishedReplicas = Sets.newHashSet();
    private final Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask = Maps.newHashMap();
    private long quorumFinishTimestamp = -1;

    // only for log replay
    public SegmentLoadJob() {
        super();
        this.jobType = EtlJobType.SEGMENT_LOAD;
    }

    public SegmentLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.brokerDesc = brokerDesc;
        this.jobType = EtlJobType.SEGMENT_LOAD;
    }

    @Override
    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException,
            DuplicatedRequestException {
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                        new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                                FrontendOptions.getLocalHostAddress()),
                        TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id, timeoutSecond);
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        LoadTask task = new SegmentLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc);
        submitTask(GlobalStateMgr.getCurrentState().getPendingLoadTaskScheduler(), task);
    }

    private SegmentBrokerReaderParams getPushBrokerReaderParams(OlapTable table, long indexId) throws LoadException {
        if (!indexToPushBrokerReaderParams.containsKey(indexId)) {
            SegmentBrokerReaderParams pushBrokerReaderParams = new SegmentBrokerReaderParams();
            pushBrokerReaderParams.init(brokerDesc);
            indexToPushBrokerReaderParams.put(indexId, pushBrokerReaderParams);
        }
        return indexToPushBrokerReaderParams.get(indexId);
    }

    private void pushTask(long backendId, long tableId, long partitionId, long indexId, long tabletId,
                          long replicaId, SegmentBrokerReaderParams params, AgentBatchTask batchTask,
                          String tabletMetaStr, ComputeNode backend, Replica replica,
                          Set<Long> tabletFinishedReplicas) throws UserException {
        if (!tabletToSentReplicaPushTask.containsKey(tabletId)
                || !tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId)) {
            long taskSignature = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .getTransactionIDGenerator().getNextTransactionId();
            // deep copy TBrokerScanRange because filePath and fileSize will be updated in different tablet push task
            // update filePath fileSize
            TBrokerScanRange tBrokerScanRange =
                    new TBrokerScanRange(params.tBrokerScanRange);
            TBrokerRangeDesc tBrokerRangeDesc = tBrokerScanRange.getRanges().get(0);
            tBrokerRangeDesc.setPath("");
            tBrokerRangeDesc.setStart_offset(0);
            tBrokerRangeDesc.setSize(0);
            tBrokerRangeDesc.setFile_size(-1);
            tBrokerRangeDesc.setSchema_path("");
            if (tabletMetaToDataFileInfo.containsKey(tabletMetaStr)
                    && tabletMetaToSchemaFilePath.containsKey(tabletMetaStr)) {
                Pair<String, Long> fileInfo = tabletMetaToDataFileInfo.get(tabletMetaStr);
                tBrokerRangeDesc.setPath(fileInfo.first);
                tBrokerRangeDesc.setStart_offset(0);
                tBrokerRangeDesc.setSize(fileInfo.second);
                tBrokerRangeDesc.setFile_size(fileInfo.second);
                tBrokerRangeDesc.setSchema_path(tabletMetaToSchemaFilePath.get(tabletMetaStr));
            }

            // update broker address
            if (brokerDesc.hasBroker()) {
                FsBroker fsBroker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(
                        brokerDesc.getName(), backend.getHost());
                tBrokerScanRange.getBroker_addresses().add(
                        new TNetworkAddress(fsBroker.ip, fsBroker.port));
                LOG.debug("push task for replica {}, broker {}:{}, backendId {}," +
                                "filePath {}, fileSize {}", replicaId, fsBroker.ip, fsBroker.port,
                        backend.getId(), tBrokerRangeDesc.path, tBrokerRangeDesc.file_size);
            } else {
                LOG.debug("push task for replica {}, backendId {}, filePath {}, fileSize {}",
                        replicaId, backend.getId(), tBrokerRangeDesc.path,
                        tBrokerRangeDesc.file_size);
            }

            PushTask pushTask = new PushTask(backendId, dbId, tableId, partitionId, indexId, tabletId, replicaId,
                    -1, 0, id, TPushType.LOAD_SEGMENT, TPriority.NORMAL, transactionId, taskSignature,
                    tBrokerScanRange, null, timezone, TTabletType.TABLET_TYPE_DISK, null);
            if (AgentTaskQueue.addTask(pushTask)) {
                batchTask.addTask(pushTask);
                if (!tabletToSentReplicaPushTask.containsKey(tabletId)) {
                    tabletToSentReplicaPushTask.put(tabletId, Maps.newHashMap());
                }
                tabletToSentReplicaPushTask.get(tabletId).put(replicaId, pushTask);
            }
        }

        if (finishedReplicas.contains(replicaId) && replica.getLastFailedVersion() < 0) {
            tabletFinishedReplicas.add(replicaId);
        }
    }

    /**
     * 1. Sends push tasks to Be
     * 2. Commit transaction after all push tasks execute successfully
     */
    public void updateLoadingStatus() throws UserException {
        if (!checkState(JobState.LOADING)) {
            return;
        }

        // submit push tasks
        Set<Long> totalTablets = submitPushTasks();
        if (totalTablets.isEmpty()) {
            LOG.warn("total tablets set is empty. job id: {}, state: {}", id, state);
            return;
        }

        // update status
        boolean canCommitJob = false;
        writeLock();
        try {
            // loading progress
            // 100: txn status is visible and load has been finished
            progress = fullTablets.size() * 100 / totalTablets.size();
            if (progress == 100) {
                progress = 99;
            }

            // quorum finish ts
            if (quorumFinishTimestamp < 0 && quorumTablets.containsAll(totalTablets)) {
                quorumFinishTimestamp = System.currentTimeMillis();
            }

            // if all replicas are finished or stay in quorum finished for long time, try to commit it.
            long stragglerTimeout = Config.load_straggler_wait_second * 1000L;
            if ((quorumFinishTimestamp > 0 && System.currentTimeMillis() - quorumFinishTimestamp > stragglerTimeout)
                    || fullTablets.containsAll(totalTablets)) {
                canCommitJob = true;
            }
        } finally {
            writeUnlock();
        }

        // try commit transaction
        if (canCommitJob) {
            tryCommitJob();
        }
    }

    private void tryCommitJob() throws UserException {
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                .add("txn_id", transactionId)
                .add("msg", "Load job try to commit txn")
                .build());
        Database db = getDb();
        db.writeLock();
        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().commitTransaction(
                    dbId, transactionId, commitInfos, Lists.newArrayList(),
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                            finishTimestamp, state, failMsg));
        } catch (TabletQuorumFailedException | CommitRateExceededException e) {
            // retry in next loop
            LOG.info("Failed commit for txn {}, will retry. Error: {}", transactionId, e.getMessage());
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof SegmentPendingTaskAttachment) {
            onPendingTaskFinished((SegmentPendingTaskAttachment) attachment);
        }
    }

    /**
     * create and submit push tasks
     * @param attachment SegmentPendingTaskAttachment
     */
    private void onPendingTaskFinished(SegmentPendingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of pending task "
                                + "when broker already has loading task")
                        .build());
                return;
            }

            // add task id into finishedTaskIds
            finishedTaskIds.add(attachment.getTaskId());
        } finally {
            writeUnlock();
        }

        try {
            unprotectedPrepareLoadingInfos(attachment);
            submitPushTasks();
            unprotectedUpdateState(JobState.LOADING);
        } catch (Exception e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to divide job into loading task.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
        }
    }

    private void unprotectedPrepareLoadingInfos(SegmentPendingTaskAttachment attachment) throws LoadException {
        this.tabletMetaToDataFileInfo = attachment.getTabletMetaToDataFileInfo();
        this.tabletMetaToSchemaFilePath = attachment.getTabletMetaToSchemaFilePath();

        for (String tabletMetaStr : tabletMetaToDataFileInfo.keySet()) {
            String[] fileNameArr = tabletMetaStr.split("/");
            // tableId/partitionId/indexId/tabletId
            Preconditions.checkState(fileNameArr.length == 4);
            Long tableId = Long.parseLong(fileNameArr[0]);
            Long partitionId = Long.parseLong(fileNameArr[1]);
            if (!tableToLoadPartitions.containsKey(tableId)) {
                tableToLoadPartitions.put(tableId, Sets.newHashSet());
            }
            tableToLoadPartitions.get(tableId).add(partitionId);
        }

        if (tableToLoadPartitions.keySet().size() > 1) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("label", label)
                    .add("error_msg", "Segment Load file path is invalid, Only supports load into one table.")
                    .build();
            throw new LoadException(errMsg);
        }
    }

    private Set<Long> submitPushTasks() throws LoadException {
        Database db;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("label", label)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build();
            throw new LoadException(errMsg);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        boolean hasLoadPartitions = false;
        Set<Long> totalTablets = Sets.newHashSet();
        db.readLock();
        try {
            writeLock();
            try {
                if (state != JobState.LOADING) {
                    LOG.warn("job state is not loading. job id: {}, state: {}", id, state);
                    return totalTablets;
                }

                for (Map.Entry<Long, Set<Long>> entry : tableToLoadPartitions.entrySet()) {
                    long tableId = entry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        LOG.warn("table does not exist. table id: {}, job id {}", tableId, id);
                        continue;
                    }

                    Set<Long> partitionIds = entry.getValue();
                    for (long partitionId : partitionIds) {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            LOG.warn("partition does not exist. partition id: {}, job id: {}", partitionId, id);
                            continue;
                        }

                        hasLoadPartitions = true;
                        int quorumReplicaNum = table.getPartitionInfo().getQuorumNum(partitionId, table.writeQuorum());
                        List<MaterializedIndex> indexes =
                                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);

                        for (MaterializedIndex index : indexes) {
                            long indexId = index.getId();
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                totalTablets.add(tabletId);
                                String tabletMetaStr = String.format("%d/%d/%d/%d", tableId, partitionId,
                                        indexId, tabletId);
                                Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                                Set<Long> tabletAllReplicas = Sets.newHashSet();
                                SegmentBrokerReaderParams params = getPushBrokerReaderParams(table, indexId);

                                if (tablet instanceof LocalTablet) {
                                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                        long replicaId = replica.getId();
                                        tabletAllReplicas.add(replicaId);
                                        long backendId = replica.getBackendId();
                                        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);

                                        pushTask(backendId, tableId, partitionId, indexId, tabletId, replicaId, params,
                                                batchTask, tabletMetaStr, backend, replica, tabletFinishedReplicas);
                                    }

                                    if (tabletAllReplicas.isEmpty()) {
                                        LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                                    }

                                    // check tablet push states
                                    if (tabletFinishedReplicas.size() >= quorumReplicaNum) {
                                        quorumTablets.add(tabletId);
                                        if (tabletFinishedReplicas.size() == tabletAllReplicas.size()) {
                                            fullTablets.add(tabletId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

                if (!hasLoadPartitions) {
                    String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("database_id", dbId)
                            .add("label", label)
                            .add("error_msg", "No partitions have data available for loading")
                            .build();
                    throw new LoadException(errMsg);
                }

                return totalTablets;
            } catch (UserException e) {
                String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("database_id", dbId)
                        .add("label", label)
                        .add("error_msg", e)
                        .build();
                throw new LoadException(errMsg);
            } finally {
                writeUnlock();
            }
        } finally {
            db.readUnlock();
        }
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        super.afterVisible(txnState, txnOperated);
        // collect table-level metrics after spark load job finished
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (null == db) {
            return;
        }
        loadingStatus.travelTableCounters(kv -> {
            TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(kv.getKey());
            if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_BYTES)) {
                entity.counterSegmentLoadBytesTotal.increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_BYTES));
            }
            if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_ROWS)) {
                entity.counterSegmentLoadRowsTotal.increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS));
            }
            if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_FINISHED)) {
                entity.counterSegmentLoadFinishedTotal
                        .increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_FINISHED));
            }
        });
        clearJob();
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        clearJob();
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        clearJob();
    }

    /**
     * load job already cancelled or finished, clear job below:
     * 1. clear push tasks and infos that not persist
     */
    private void clearJob() {
        Preconditions.checkState(state == JobState.FINISHED || state == JobState.CANCELLED);

        LOG.debug("clear push tasks and infos that not persist. id: {}, state: {}", id, state);
        writeLock();
        try {
            // clear push task first
            for (Map<Long, PushTask> sentReplicaPushTask : tabletToSentReplicaPushTask.values()) {
                for (PushTask pushTask : sentReplicaPushTask.values()) {
                    if (pushTask == null) {
                        continue;
                    }
                    AgentTaskQueue.removeTask(pushTask.getBackendId(), pushTask.getTaskType(), pushTask.getSignature());
                }
            }
            // clear job infos that not persist
            tableToLoadPartitions.clear();
            indexToPushBrokerReaderParams.clear();
            tabletToSentReplicaPushTask.clear();
            finishedReplicas.clear();
            quorumTablets.clear();
            fullTablets.clear();
        } finally {
            writeUnlock();
        }
    }

    public void addFinishedReplica(long replicaId, long tabletId, long backendId) {
        writeLock();
        try {
            if (finishedReplicas.add(replicaId)) {
                commitInfos.add(new TabletCommitInfo(tabletId, backendId));
                // set replica push task null
                Map<Long, PushTask> sentReplicaPushTask = tabletToSentReplicaPushTask.get(tabletId);
                if (sentReplicaPushTask != null) {
                    if (sentReplicaPushTask.containsKey(replicaId)) {
                        sentReplicaPushTask.put(replicaId, null);
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

}
