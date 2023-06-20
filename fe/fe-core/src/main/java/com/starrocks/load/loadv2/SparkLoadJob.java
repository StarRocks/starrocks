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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/SparkLoadJob.java

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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.load.loadv2.dpp.DppResult;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.system.Backend;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PushTask;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPriority;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletQuorumFailedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

/**
 * There are 4 steps in SparkLoadJob:
 * Step1: SparkLoadPendingTask will be created by unprotectedExecuteJob method and submit spark etl job.
 * Step2: LoadEtlChecker will check spark etl job status periodically and send push tasks to be when spark etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodically and commit transaction when push tasks are finished.
 * Step4: PublishVersionDaemon will send publish version tasks to be and finish transaction.
 */
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // --- members below need persist ---
    // create from resourceDesc when job created
    @SerializedName("spkr")
    private SparkResource sparkResource;
    // members below updated when job state changed to etl
    @SerializedName("etlt")
    private long etlStartTimestamp = -1;
    // for spark yarn
    @SerializedName("appi")
    private String appId = "";
    // spark job outputPath
    @SerializedName("etlo")
    private String etlOutputPath = "";
    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    @SerializedName("tbtm")
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();
    @SerializedName("spkh")
    private SparkLoadAppHandle sparkLoadAppHandle = new SparkLoadAppHandle();

    // --- members below not persist ---
    private ResourceDesc resourceDesc;
    // for straggler wait long time to commit transaction
    private long quorumFinishTimestamp = -1;
    // spark load wait yarn response timeout
    protected long sparkLoadSubmitTimeoutSecond = Config.spark_load_submit_timeout_second;
    // below for push task
    private final Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();
    private final Map<Long, PushBrokerReaderParams> indexToPushBrokerReaderParams = Maps.newHashMap();
    private final Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();
    private final Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask = Maps.newHashMap();
    private final Set<Long> finishedReplicas = Sets.newHashSet();
    private final Set<Long> quorumTablets = Sets.newHashSet();
    private final Set<Long> fullTablets = Sets.newHashSet();

    // only for log replay
    public SparkLoadJob() {
        super();
        jobType = EtlJobType.SPARK;
    }

    public SparkLoadJob(long dbId, String label, ResourceDesc resourceDesc, OriginStatement originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.resourceDesc = resourceDesc;
        timeoutSecond = Config.spark_load_default_timeout_second;
        jobType = EtlJobType.SPARK;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        if (properties.containsKey(LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT)) {
            try {
                sparkLoadSubmitTimeoutSecond = Long.parseLong(properties.get(LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT));
            } catch (NumberFormatException e) {
                throw new DdlException("spark_load_submit_timeout is not LONG", e);
            }
        }

        // set spark resource and broker desc
        setResourceInfo();
    }

    /**
     * merge system conf with load stmt
     */
    private void setResourceInfo() throws DdlException {
        // spark resource
        String resourceName = resourceDesc.getName();
        Resource oriResource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (oriResource == null) {
            throw new DdlException("Resource does not exist. name: " + resourceName);
        }
        sparkResource = ((SparkResource) oriResource).getCopiedResource();
        sparkResource.update(resourceDesc);

        // broker desc
        Map<String, String> brokerProperties = sparkResource.getBrokerPropertiesWithoutPrefix();
        if (sparkResource.hasBroker()) {
            brokerDesc = new BrokerDesc(sparkResource.getBroker(), brokerProperties);
        } else {
            brokerDesc = new BrokerDesc(brokerProperties);
        }
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
        transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        LoadJobSourceType.FRONTEND, id, timeoutSecond);
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        // create pending task
        LoadTask task = new SparkLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                sparkResource, brokerDesc);
        task.init();
        idToTasks.put(task.getSignature(), task);
        submitTask(GlobalStateMgr.getCurrentState().getPendingLoadTaskScheduler(), task);
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof SparkPendingTaskAttachment) {
            onPendingTaskFinished((SparkPendingTaskAttachment) attachment);
        }
    }

    private void onPendingTaskFinished(SparkPendingTaskAttachment attachment) {
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

            sparkLoadAppHandle = attachment.getHandle();
            appId = attachment.getAppId();
            etlOutputPath = attachment.getOutputPath();

            executeEtl();
            // log etl state
            unprotectedLogUpdateStateInfo();
        } finally {
            writeUnlock();
        }
    }

    /**
     * update etl start time and state in spark load job
     */
    private void executeEtl() {
        etlStartTimestamp = System.currentTimeMillis();
        state = JobState.ETL;
        LOG.info("update to {} state success. job id: {}", state, id);
    }

    private boolean checkState(JobState expectState) {
        readLock();
        try {
            return state == expectState;
        } finally {
            readUnlock();
        }
    }

    /**
     * Check the status of etl job regularly
     * 1. RUNNING, update etl job progress
     * 2. CANCELLED, cancel load job
     * 3. FINISHED, get the etl output file paths, update job state to LOADING and log job update info
     * <p>
     * Send push tasks if job state changed to LOADING
     */
    public void updateEtlStatus() throws Exception {
        if (!checkState(JobState.ETL)) {
            return;
        }

        // get etl status
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        EtlStatus status =
                handler.getEtlJobStatus(sparkLoadAppHandle, appId, id, etlOutputPath, sparkResource, brokerDesc);
        writeLock();
        try {
            switch (status.getState()) {
                case RUNNING:
                    unprotectedUpdateEtlStatusInternal(status);
                    break;
                case FINISHED:
                    unprotectedProcessEtlFinish(status, handler);
                    break;
                case CANCELLED:
                    throw new LoadException("spark etl job failed. msg: " + status.getFailMsg());
                default:
                    LOG.warn("unknown etl state: {}", status.getState().name());
                    break;
            }
        } finally {
            writeUnlock();
        }

        if (checkState(JobState.LOADING)) {
            // create and send push tasks
            submitPushTasks();
        }
    }

    private void unprotectedUpdateEtlStatusInternal(EtlStatus etlStatus) {
        loadingStatus = etlStatus;
        progress = etlStatus.getProgress();
        if (!sparkResource.isYarnMaster()) {
            loadingStatus.setTrackingUrl(appId);
        }

        DppResult dppResult = etlStatus.getDppResult();
        if (dppResult != null) {
            // update load statistic and counters when spark etl job finished
            // fe gets these infos from spark dpp, so we use dummy load id and dummy backend id here
            loadingStatus.setLoadFileInfo((int) dppResult.fileNumber, dppResult.fileSize);
            TUniqueId dummyId = new TUniqueId(0, 0);
            long dummyBackendId = -1L;
            loadingStatus.getLoadStatistic()
                    .initLoad(dummyId, Sets.newHashSet(dummyId), Lists.newArrayList(dummyBackendId));
            TReportExecStatusParams params = new TReportExecStatusParams();
            params.setDone(true);
            params.setSource_load_rows(dppResult.scannedRows);
            loadingStatus.getLoadStatistic()
                    .updateLoadProgress(params);

            Map<String, String> counters = loadingStatus.getCounters();
            counters.put(DPP_NORMAL_ALL, String.valueOf(dppResult.normalRows));
            counters.put(DPP_ABNORMAL_ALL, String.valueOf(dppResult.abnormalRows));
            counters.put(UNSELECTED_ROWS, String.valueOf(dppResult.unselectRows));

            // collect table-level metrics
            if (null == dppResult.tableCounters) {
                return;
            }
            for (Map.Entry<Long, Map<String, Long>> entry : dppResult.tableCounters.entrySet()) {
                for (String counterKey : new String[] {TableMetricsEntity.TABLE_LOAD_BYTES,
                        TableMetricsEntity.TABLE_LOAD_ROWS, TableMetricsEntity.TABLE_LOAD_FINISHED}) {
                    loadingStatus.increaseTableCounter(entry.getKey(), counterKey, entry.getValue().get(counterKey));
                }
            }
        }
    }

    private void unprotectedProcessEtlFinish(EtlStatus etlStatus, SparkEtlJobHandler handler) throws Exception {
        unprotectedUpdateEtlStatusInternal(etlStatus);
        // checkDataQuality
        if (!checkDataQuality()) {
            throw new DataQualityException(DataQualityException.QUALITY_FAIL_MSG);
        }

        // get etl output files and update loading state
        unprotectedUpdateToLoadingState(etlStatus, handler.getEtlFilePaths(etlOutputPath, brokerDesc));
        // log loading statedppResult
        unprotectedLogUpdateStateInfo();
        // prepare loading infos
        unprotectedPrepareLoadingInfos();
    }

    private void unprotectedUpdateToLoadingState(EtlStatus etlStatus, Map<String, Long> filePathToSize)
            throws LoadException {
        try {
            for (Map.Entry<String, Long> entry : filePathToSize.entrySet()) {
                String filePath = entry.getKey();
                if (!filePath.endsWith(EtlJobConfig.ETL_OUTPUT_FILE_FORMAT)) {
                    continue;
                }
                String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
                tabletMetaToFileInfo.put(tabletMetaStr, Pair.create(filePath, entry.getValue()));
            }

            loadingStatus = etlStatus;
            progress = 0;
            unprotectedUpdateState(JobState.LOADING);
            LOG.info("update to {} state success. job id: {}", state, id);
        } catch (Exception e) {
            LOG.warn("update to {} state failed. job id: {}", state, id, e);
            throw new LoadException(e.getMessage(), e);
        }
    }

    private void unprotectedPrepareLoadingInfos() {
        for (String tabletMetaStr : tabletMetaToFileInfo.keySet()) {
            String[] fileNameArr = tabletMetaStr.split("\\.");
            // tableId.partitionId.indexId.bucket.schemaHash
            Preconditions.checkState(fileNameArr.length == 5);
            long tableId = Long.parseLong(fileNameArr[0]);
            long partitionId = Long.parseLong(fileNameArr[1]);
            long indexId = Long.parseLong(fileNameArr[2]);
            int schemaHash = Integer.parseInt(fileNameArr[4]);

            if (!tableToLoadPartitions.containsKey(tableId)) {
                tableToLoadPartitions.put(tableId, Sets.newHashSet());
            }
            tableToLoadPartitions.get(tableId).add(partitionId);

            indexToSchemaHash.put(indexId, schemaHash);
        }
    }

    private PushBrokerReaderParams getPushBrokerReaderParams(OlapTable table, long indexId) throws UserException {
        if (!indexToPushBrokerReaderParams.containsKey(indexId)) {
            PushBrokerReaderParams pushBrokerReaderParams = new PushBrokerReaderParams();
            pushBrokerReaderParams.init(table.getSchemaByIndexId(indexId), brokerDesc);
            indexToPushBrokerReaderParams.put(indexId, pushBrokerReaderParams);
        }
        return indexToPushBrokerReaderParams.get(indexId);
    }

    private Set<Long> submitPushTasks() throws UserException {
        // check db exist
        Database db;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("label", label)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build();
            throw new MetaNotFoundException(errMsg);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        boolean hasLoadPartitions = false;
        Set<Long> totalTablets = Sets.newHashSet();
        db.readLock();
        try {
            writeLock();
            try {
                // check state is still loading. If state is cancelled or finished, return.
                // if state is cancelled or finished and not return, this would throw all partitions have no load data exception,
                // because tableToLoadPartitions was already cleaned up,
                if (state != JobState.LOADING) {
                    LOG.warn("job state is not loading. job id: {}, state: {}", id, state);
                    return totalTablets;
                }

                for (Map.Entry<Long, Set<Long>> entry : tableToLoadPartitions.entrySet()) {
                    long tableId = entry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        LOG.warn("table does not exist. id: {}", tableId);
                        continue;
                    }

                    Set<Long> partitionIds = entry.getValue();
                    for (long partitionId : partitionIds) {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            LOG.warn("partition does not exist. id: {}", partitionId);
                            continue;
                        }

                        hasLoadPartitions = true;
                        int quorumReplicaNum = table.getPartitionInfo().getQuorumNum(partitionId, table.writeQuorum());

                        List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.ALL);
                        for (MaterializedIndex index : indexes) {
                            long indexId = index.getId();
                            int schemaHash = indexToSchemaHash.get(indexId);

                            int bucket = 0;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                totalTablets.add(tabletId);
                                String tabletMetaStr = String.format("%d.%d.%d.%d.%d", tableId, partitionId,
                                        indexId, bucket++, schemaHash);

                                Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                                Set<Long> tabletAllReplicas = Sets.newHashSet();
                                PushBrokerReaderParams params = getPushBrokerReaderParams(table, indexId);

                                if (tablet instanceof LocalTablet) {
                                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                        long replicaId = replica.getId();
                                        tabletAllReplicas.add(replicaId);
                                        long backendId = replica.getBackendId();
                                        Backend backend = GlobalStateMgr.getCurrentState().getCurrentSystemInfo()
                                                .getBackend(backendId);

                                        pushTask(backendId, tableId, partitionId, indexId, tabletId,
                                                replicaId, schemaHash, params, batchTask, tabletMetaStr,
                                                backend, replica, tabletFinishedReplicas, TTabletType.TABLET_TYPE_DISK);
                                    }

                                    if (tabletAllReplicas.size() == 0) {
                                        LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                                    }

                                    // check tablet push states
                                    if (tabletFinishedReplicas.size() >= quorumReplicaNum) {
                                        quorumTablets.add(tabletId);
                                        if (tabletFinishedReplicas.size() == tabletAllReplicas.size()) {
                                            fullTablets.add(tabletId);
                                        }
                                    }

                                } else {
                                    // lake tablet
                                    long backendId = ((LakeTablet) tablet).getPrimaryBackendId();
                                    Backend backend = GlobalStateMgr.getCurrentSystemInfo().
                                            getBackend(backendId);
                                    if (backend == null) {
                                        LOG.warn("replica {} not exists", backendId);
                                        continue;
                                    }

                                    pushTask(backend.getId(), tableId, partitionId, indexId, tabletId,
                                            tabletId, schemaHash, params, batchTask, tabletMetaStr,
                                            backend, new Replica(tabletId, backendId, -1, NORMAL),
                                            tabletFinishedReplicas, TTabletType.TABLET_TYPE_LAKE);

                                    if (tabletFinishedReplicas.contains(tabletId)) {
                                        quorumTablets.add(tabletId);
                                        fullTablets.add(tabletId);
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
                            .add("error_msg", "all partitions have no load data")
                            .build();
                    throw new LoadException(errMsg);
                }

                return totalTablets;
            } finally {
                writeUnlock();
            }
        } finally {
            db.readUnlock();
        }
    }

    private void pushTask(long backendId, long tableId, long partitionId, long indexId,
                          long tabletId, long replicaId, int schemaHash,
                          PushBrokerReaderParams params,
                          AgentBatchTask batchTask,
                          String tabletMetaStr,
                          Backend backend, Replica replica, Set<Long> tabletFinishedReplicas,
                          TTabletType tabletType)
            throws AnalysisException {

        if (!tabletToSentReplicaPushTask.containsKey(tabletId)
                || !tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId)) {
            long taskSignature = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .getTransactionIDGenerator().getNextTransactionId();
            // deep copy TBrokerScanRange because filePath and fileSize will be updated
            // in different tablet push task
            // update filePath fileSize
            TBrokerScanRange tBrokerScanRange =
                    new TBrokerScanRange(params.tBrokerScanRange);
            TBrokerRangeDesc tBrokerRangeDesc = tBrokerScanRange.getRanges().get(0);
            tBrokerRangeDesc.setPath("");
            tBrokerRangeDesc.setStart_offset(0);
            tBrokerRangeDesc.setSize(0);
            tBrokerRangeDesc.setFile_size(-1);
            if (tabletMetaToFileInfo.containsKey(tabletMetaStr)) {
                Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
                tBrokerRangeDesc.setPath(fileInfo.first);
                tBrokerRangeDesc.setStart_offset(0);
                tBrokerRangeDesc.setSize(fileInfo.second);
                tBrokerRangeDesc.setFile_size(fileInfo.second);
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

            PushTask pushTask = new PushTask(backendId, dbId, tableId, partitionId,
                    indexId, tabletId, replicaId, schemaHash,
                    0, id, TPushType.LOAD_V2,
                    TPriority.NORMAL, transactionId, taskSignature,
                    tBrokerScanRange, params.tDescriptorTable,
                    timezone, tabletType);
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
        } catch (TabletQuorumFailedException e) {
            // retry in next loop
        } finally {
            db.writeUnlock();
        }
    }

    /**
     * load job already cancelled or finished, clear job below:
     * 1. kill etl job and delete etl files
     * 2. clear push tasks and infos that not persist
     */
    private void clearJob() {
        Preconditions.checkState(state == JobState.FINISHED || state == JobState.CANCELLED);

        LOG.debug("kill etl job and delete etl files. id: {}, state: {}", id, state);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        if (state == JobState.CANCELLED) {
            if ((!Strings.isNullOrEmpty(appId) && sparkResource.isYarnMaster()) || sparkLoadAppHandle != null) {
                try {
                    handler.killEtlJob(sparkLoadAppHandle, appId, id, sparkResource);
                } catch (Exception e) {
                    LOG.warn("kill etl job failed. id: {}, state: {}", id, state, e);
                }
            }
        }
        if (!Strings.isNullOrEmpty(etlOutputPath)) {
            try {
                // delete label dir, remove the last taskId dir
                String outputPath = etlOutputPath.substring(0, etlOutputPath.lastIndexOf("/"));
                handler.deleteEtlOutputPath(outputPath, brokerDesc);
            } catch (Exception e) {
                LOG.warn("delete etl files failed. id: {}, state: {}", id, state, e);
            }
        }

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
            resourceDesc = null;
            tableToLoadPartitions.clear();
            indexToPushBrokerReaderParams.clear();
            indexToSchemaHash.clear();
            tabletToSentReplicaPushTask.clear();
            finishedReplicas.clear();
            quorumTablets.clear();
            fullTablets.clear();
        } finally {
            writeUnlock();
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
                entity.counterSparkLoadBytesTotal.increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_BYTES));
            }
            if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_ROWS)) {
                entity.counterSparkLoadRowsTotal.increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS));
            }
            if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_FINISHED)) {
                entity.counterSparkLoadFinishedTotal
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

    @Override
    public String getResourceName() {
        return sparkResource.getName();
    }

    @Override
    protected long getEtlStartTimestamp() {
        return etlStartTimestamp;
    }

    public SparkLoadAppHandle getHandle() {
        return sparkLoadAppHandle;
    }

    public void clearSparkLauncherLog() {
        if (sparkLoadAppHandle == null) {
            return;
        }

        String logPath = sparkLoadAppHandle.getLogPath();
        if (!Strings.isNullOrEmpty(logPath)) {
            File file = new File(logPath);
            if (file.exists()) {
                if (!file.delete()) {
                    LOG.warn("Failed to delete file, filepath={}", file.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        sparkResource.write(out);
        sparkLoadAppHandle.write(out);
        out.writeLong(etlStartTimestamp);
        Text.writeString(out, appId);
        Text.writeString(out, etlOutputPath);
        out.writeInt(tabletMetaToFileInfo.size());
        for (Map.Entry<String, Pair<String, Long>> entry : tabletMetaToFileInfo.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().first);
            out.writeLong(entry.getValue().second);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        sparkResource = (SparkResource) Resource.read(in);
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_91) {
            sparkLoadAppHandle = SparkLoadAppHandle.read(in);
        }
        etlStartTimestamp = in.readLong();
        appId = Text.readString(in);
        etlOutputPath = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tabletMetaStr = Text.readString(in);
            Pair<String, Long> fileInfo = Pair.create(Text.readString(in), in.readLong());
            tabletMetaToFileInfo.put(tabletMetaStr, fileInfo);
        }
    }

    /**
     * log load job update info when job state changed to etl or loading
     */
    private void unprotectedLogUpdateStateInfo() {
        SparkLoadJobStateUpdateInfo info = new SparkLoadJobStateUpdateInfo(
                id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp, appId, etlOutputPath,
                loadStartTimestamp, tabletMetaToFileInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logUpdateLoadJob(info);
    }

    @Override
    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        super.replayUpdateStateInfo(info);
        SparkLoadJobStateUpdateInfo sparkJobStateInfo = (SparkLoadJobStateUpdateInfo) info;
        sparkLoadAppHandle = sparkJobStateInfo.getSparkLoadAppHandle();
        etlStartTimestamp = sparkJobStateInfo.getEtlStartTimestamp();
        appId = sparkJobStateInfo.getAppId();
        etlOutputPath = sparkJobStateInfo.getEtlOutputPath();
        tabletMetaToFileInfo = sparkJobStateInfo.getTabletMetaToFileInfo();

        switch (state) {
            case ETL:
                // nothing to do
                break;
            case LOADING:
                unprotectedPrepareLoadingInfos();
                break;
            default:
                LOG.warn("replay update load job state info failed. error: wrong state. job id: {}, state: {}",
                        id, state);
                break;
        }
    }

    /**
     * Used for spark load job journal log when job state changed to ETL or LOADING
     */
    public static class SparkLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {
        @SerializedName(value = "sparkLoadAppHandle")
        private SparkLoadAppHandle sparkLoadAppHandle;
        @SerializedName(value = "etlStartTimestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "appId")
        private String appId;
        @SerializedName(value = "etlOutputPath")
        private String etlOutputPath;
        @SerializedName(value = "tabletMetaToFileInfo")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;

        public SparkLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId,
                                           SparkLoadAppHandle sparkLoadAppHandle,
                                           long etlStartTimestamp, String appId, String etlOutputPath,
                                           long loadStartTimestamp,
                                           Map<String, Pair<String, Long>> tabletMetaToFileInfo) {
            super(jobId, state, transactionId, loadStartTimestamp);
            this.sparkLoadAppHandle = sparkLoadAppHandle;
            this.etlStartTimestamp = etlStartTimestamp;
            this.appId = appId;
            this.etlOutputPath = etlOutputPath;
            this.tabletMetaToFileInfo = tabletMetaToFileInfo;
        }

        public SparkLoadAppHandle getSparkLoadAppHandle() {
            return sparkLoadAppHandle;
        }

        public long getEtlStartTimestamp() {
            return etlStartTimestamp;
        }

        public String getAppId() {
            return appId;
        }

        public String getEtlOutputPath() {
            return etlOutputPath;
        }

        public Map<String, Pair<String, Long>> getTabletMetaToFileInfo() {
            return tabletMetaToFileInfo;
        }
    }

    /**
     * Params for be push broker reader
     * 1. TBrokerScanRange: file path and size, broker address, transform expr
     * 2. TDescriptorTable: src and dest SlotDescriptors, src and dest tupleDescriptors
     * <p>
     * These params are sent to Be through push task
     */
    private static class PushBrokerReaderParams {
        TBrokerScanRange tBrokerScanRange;
        TDescriptorTable tDescriptorTable;

        public PushBrokerReaderParams() {
            this.tBrokerScanRange = new TBrokerScanRange();
            this.tDescriptorTable = null;
        }

        public void init(List<Column> columns, BrokerDesc brokerDesc) throws UserException {
            // Generate tuple descriptor
            DescriptorTable descTable = new DescriptorTable();
            TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
            // use index schema to fill the descriptor table
            for (Column column : columns) {
                SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(destTupleDesc);
                destSlotDesc.setIsMaterialized(true);
                destSlotDesc.setColumn(column);
                destSlotDesc.setIsNullable(column.isAllowNull());
            }
            initTBrokerScanRange(descTable, destTupleDesc, columns, brokerDesc);
            initTDescriptorTable(descTable);
        }

        private void initTBrokerScanRange(DescriptorTable descTable, TupleDescriptor destTupleDesc,
                                          List<Column> columns, BrokerDesc brokerDesc) throws AnalysisException {
            // scan range params
            TBrokerScanRangeParams params = new TBrokerScanRangeParams();
            params.setStrict_mode(false);
            if (brokerDesc.hasBroker()) {
                params.setProperties(brokerDesc.getProperties());
                params.setUse_broker(true);
            } else {
                THdfsProperties hdfsProperties = new THdfsProperties();
                params.setHdfs_properties(hdfsProperties);
                params.setHdfs_read_buffer_size_kb(Config.hdfs_read_buffer_size_kb);
                params.setUse_broker(false);
            }
            TupleDescriptor srcTupleDesc = descTable.createTupleDescriptor();
            Map<String, SlotDescriptor> srcSlotDescByName = Maps.newHashMap();
            for (Column column : columns) {
                SlotDescriptor srcSlotDesc = descTable.addSlotDescriptor(srcTupleDesc);
                srcSlotDesc.setIsMaterialized(true);
                srcSlotDesc.setIsNullable(true);
                Type type = column.getType();
                if (type.isLargeIntType() || type.isBoolean() || type.isBitmapType() || type.isHllType()) {
                    // largeint, boolean, bitmap, hll type using varchar in spark dpp parquet file
                    srcSlotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                    srcSlotDesc.setColumn(new Column(column.getName(), Type.VARCHAR));
                } else {
                    srcSlotDesc.setType(type);
                    srcSlotDesc.setColumn(new Column(column.getName(), type));
                }
                params.addToSrc_slot_ids(srcSlotDesc.getId().asInt());
                srcSlotDescByName.put(column.getName(), srcSlotDesc);
            }

            Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
            for (SlotDescriptor destSlotDesc : destTupleDesc.getSlots()) {
                if (!destSlotDesc.isMaterialized()) {
                    continue;
                }

                SlotDescriptor srcSlotDesc = srcSlotDescByName.get(destSlotDesc.getColumn().getName());
                destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                Expr expr = new SlotRef(srcSlotDesc);
                expr = castToSlot(destSlotDesc, expr);
                params.putToExpr_of_dest_slot(destSlotDesc.getId().asInt(), expr.treeToThrift());
            }
            params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
            params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
            params.setDest_tuple_id(destTupleDesc.getId().asInt());
            tBrokerScanRange.setParams(params);

            // broker address updated for each replica
            tBrokerScanRange.setBroker_addresses(Lists.newArrayList());

            // broker range desc
            TBrokerRangeDesc tBrokerRangeDesc = new TBrokerRangeDesc();
            tBrokerRangeDesc.setFile_type(TFileType.FILE_BROKER);
            tBrokerRangeDesc.setFormat_type(TFileFormatType.FORMAT_PARQUET);
            tBrokerRangeDesc.setSplittable(false);
            tBrokerRangeDesc.setStart_offset(0);
            tBrokerRangeDesc.setSize(-1);
            // path and file size updated for each replica
            tBrokerScanRange.setRanges(Lists.newArrayList(tBrokerRangeDesc));
        }

        private Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
            Type dstType = slotDesc.getType();
            Type srcType = expr.getType();
            if (dstType.isBoolean() && srcType.isVarchar()) {
                // there is no cast VARCHAR to BOOLEAN function
                // so we cast VARCHAR to TINYINT first, then cast TINYINT to BOOLEAN
                return new CastExpr(Type.BOOLEAN, new CastExpr(Type.TINYINT, expr));
            } else if (dstType.isScalarType()) {
                if ((dstType.isBitmapType() || dstType.isHllType()) && srcType.isVarchar()) {
                    // there is no cast VARCHAR to BITMAP|HLL function,
                    // bitmap and hll data will be converted from varchar in be push.
                    return expr;
                }
                return dstType.getPrimitiveType() != srcType.getPrimitiveType() ? expr.castTo(dstType) : expr;
            } else {
                throw new AnalysisException("Spark-Load does not support complex types yet");
            }
        }

        private void initTDescriptorTable(DescriptorTable descTable) {
            descTable.computeMemLayout();
            tDescriptorTable = descTable.toThrift();
        }
    }
}
