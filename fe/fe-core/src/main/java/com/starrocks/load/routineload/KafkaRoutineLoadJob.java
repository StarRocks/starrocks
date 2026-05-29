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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaRoutineLoadJob.java

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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.Load;
import com.starrocks.metric.RoutineLoadLagTimeMetricMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.RoutineLoadDataSourceProperties;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    public static final String KAFKA_FILE_CATALOG = "kafka";

    private static final String PROPERTY_KAFKA_GROUP_ID = "group.id";

    @SerializedName("bkl")
    private String brokerList;
    @SerializedName("tpc")
    private String topic;

    // optional, user want to load partitions.
    @SerializedName("ckp")
    private List<Integer> customKafkaPartitions = Lists.newArrayList();
    // current kafka partitions is the actually partition which will be fetched
    @SerializedName("ctkp")
    private List<Integer> currentKafkaPartitions = Lists.newArrayList();
    // optional, user want to set default offset when new partition add or offset not set.
    private Long kafkaDefaultOffSet = null;
    // kafka properties, property prefix will be mapped to kafka custom parameters, which can be extended in the future
    @SerializedName("cpr")
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();
    @SerializedName("csru")
    private String confluentSchemaRegistryUrl = null;
    @SerializedName("ckpo")
    private List<Pair<Integer, Long>> customeKafkaPartitionOffsets = null;
    boolean useDefaultGroupId = true;

    private Map<Integer, Long> latestPartitionOffsets = Maps.newHashMap();

    public KafkaRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.KAFKA);
        this.progress = new KafkaProgress();
        this.timestampProgress = new KafkaProgress();
    }

    public KafkaRoutineLoadJob(Long id, String name,
                               long dbId, long tableId, String brokerList, String topic) {
        super(id, name, dbId, tableId, LoadDataSourceType.KAFKA);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
        this.timestampProgress = new KafkaProgress();
    }

    public String getConfluentSchemaRegistryUrl() {
        return confluentSchemaRegistryUrl;
    }

    public void setConfluentSchemaRegistryUrl(String confluentSchemaRegistryUrl) {
        this.confluentSchemaRegistryUrl = confluentSchemaRegistryUrl;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    @Override
    protected String getSourceProgressString() {
        // To be compatible with progress format, we convert the Map<Integer, Long> to Map<String, String>
        Map<String, String> partitionOffsets = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : latestPartitionOffsets.entrySet()) {
            partitionOffsets.put(entry.getKey().toString(), entry.getValue().toString());
        }

        Gson gson = new Gson();
        return gson.toJson(partitionOffsets);
    }


    @Override
    protected String getSourceLagString(String progressJsonStr) {
        Gson gson = new Gson();
        Map<String, String> progress = gson.fromJson(progressJsonStr, Map.class);

        if (progress == null || progress.isEmpty()) {
            return gson.toJson(progress);
        }

        if (latestPartitionOffsets == null || latestPartitionOffsets.isEmpty()) {
            return gson.toJson(latestPartitionOffsets);
        }

        Map<String, String> partitionLag = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : latestPartitionOffsets.entrySet()) {
            // progress and latest all have same id
            String mapKey = entry.getKey().toString();
            if (progress.containsKey(mapKey)) {
                String progressVal = progress.get(mapKey);
                //check progressVal
                if (!checkProgressVal(progressVal)) {
                    continue;
                }
                Long lag = entry.getValue() - Long.valueOf(progress.get(mapKey));
                lag = lag < 0 ? 0 : lag;
                partitionLag.put(mapKey, lag.toString());
            }
        }
        return gson.toJson(partitionLag);
    }

    private boolean checkProgressVal(String progressVal) {
        if (progressVal == null) {
            return false;
        }
        if (progressVal.equals(KafkaProgress.OFFSET_ZERO) || progressVal.equals(KafkaProgress.OFFSET_END) ||
                progressVal.equals(KafkaProgress.OFFSET_BEGINNING)) {
            return false;
        }
        if (!StringUtils.isNumeric(progressVal)) {
            return false;
        }
        return true;
    }

    public void setPartitionOffset(int partition, long offset) {
        latestPartitionOffsets.put(Integer.valueOf(partition), Long.valueOf(offset));
    }

    public Long getPartitionOffset(int partition) {
        return latestPartitionOffsets.get(Integer.valueOf(partition));
    }

    @Override
    public void prepare() throws StarRocksException {
        super.prepare();
        checkCustomPartition(customKafkaPartitions);
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);

        ((KafkaProgress) progress).convertOffset(brokerList, topic, convertedCustomProperties, computeResource);
    }

    public synchronized void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, KAFKA_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }
        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)) {
            try {
                kafkaDefaultOffSet = CreateRoutineLoadStmt.getKafkaOffset(
                        convertedCustomProperties.remove(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws StarRocksException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                // divide kafkaPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<Integer, Long> taskKafkaProgress = Maps.newHashMap();
                    for (int j = 0; j < currentKafkaPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            int kafkaPartition = currentKafkaPartitions.get(j);
                            taskKafkaProgress.put(kafkaPartition,
                                    ((KafkaProgress) progress).getOffsetByPartition(kafkaPartition));
                        }
                    }
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUIDUtil.genUUID(), this,
                            taskSchedIntervalS * 1000,
                            timeToExecuteMs, taskKafkaProgress, taskTimeoutSecond * 1000);
                    kafkaTaskInfo.setComputeResource(computeResource);
                    routineLoadTaskInfoList.add(kafkaTaskInfo);
                    result.add(kafkaTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null);
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
            // save task into queue of needScheduleTasks
            GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        // TODO: need to refactor after be split into cn + dn
        int aliveNodeNum = systemInfoService.getAliveBackendNumber();
        if (RunMode.isSharedDataMode()) {
            aliveNodeNum = 0;
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            final List<Long> computeIds = warehouseManager.getAllComputeNodeIds(computeResource);
            for (long nodeId : computeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAlive()) {
                    ++aliveNodeNum;
                }
            }
        }
        int partitionNum = currentKafkaPartitions.size();
        if (partitionNum == 0) {
            // In non-stop states (NEED_SCHEDULE/RUNNING), having `partitionNum` as 0 is equivalent
            // to `currentKafkaPartitions` being uninitialized. When `currentKafkaPartitions` is
            // uninitialized, it indicates that the job has just been created and hasn't been scheduled yet.
            // At this point, the user-specified number of partitions is used.
            partitionNum = customKafkaPartitions.size();
            if (partitionNum == 0) {
                // If the user hasn't specified partition information, then we no longer take the `partition`
                // variable into account when calculating concurrency.
                partitionNum = Integer.MAX_VALUE;
            }
        }

        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(partition num: {}, desire task concurrent num: {}, alive be num: {}, config: {})",
                partitionNum, desireTaskConcurrentNum, aliveNodeNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveNodeNum)),
                Config.max_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
                                      TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // For committed txn, update the progress.
            return true;
        }

        // For compatible reason, the default behavior of empty load is still returning
        // "No rows were imported from upstream" and abort transaction.
        // In this situation, we also need update commit info.
        if (txnStatusChangeReason != null &&
                txnStatusChangeReason == TxnStatusChangeReason.NO_ROWS_IMPORTED) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "No rows were imported from upstream" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the offset to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the offset.
        LOG.debug("no need to update the progress of kafka routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws StarRocksException {
        super.updateProgress(attachment);
        this.progress.update(attachment.getProgress());
        this.timestampProgress.update(attachment.getTimestampProgress());
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        KafkaTaskInfo oldKafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
        // add new task
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(timeToExecuteMs, oldKafkaTaskInfo,
                ((KafkaProgress) progress).getPartitionIdToOffset(oldKafkaTaskInfo.getPartitions()),
                ((KafkaTaskInfo) routineLoadTaskInfo).getLatestOffset());
        // cngroup
        kafkaTaskInfo.setComputeResource(routineLoadTaskInfo.getComputeResource());
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(kafkaTaskInfo);
        return kafkaTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() {
        updateNewPartitionProgress();
    }

    // Refresh `currentKafkaPartitions` against the broker. The slow step is a FE -> BE brpc
    // (KafkaUtil.getAllKafkaPartitions), so we must not hold the per-job writeLock across it -
    // otherwise readers (admin RPCs, SHOW ROUTINE LOAD, processTimeoutTasks) stall for the
    // full RPC plus retries. Three phases:
    //   1. Snapshot brokerList/topic/state/configVersion under readLock.
    //   2. Run convertCustomProperties (self-synchronized) and the brpc with no per-job lock.
    //   3. Apply under writeLock, discarding the result if configVersion changed mid-flight.
    @Override
    protected void refreshPartitionsIfNeeded() throws StarRocksException {
        FetchSnapshot snapshot;
        readLock();
        try {
            snapshot = takeFetchSnapshot();
        } finally {
            readUnlock();
        }
        if (snapshot == null) {
            return;
        }
        switch (snapshot.kind) {
            case PAUSED_AUTO_SCHEDULE:
                applyPausedAutoSchedule();
                return;
            case CUSTOM_ONLY:
                applyCustomPartitions(snapshot);
                return;
            case FETCH:
                break;
            default:
                return;
        }

        List<Integer> newPartitions = null;
        Exception fetchError = null;
        try {
            newPartitions = KafkaUtil.getAllKafkaPartitions(snapshot.brokerList, snapshot.topic,
                    snapshotConvertedCustomProperties(), snapshot.computeResource);
        } catch (Exception e) {
            fetchError = e;
        }

        applyFetchResult(snapshot, newPartitions, fetchError);
    }

    // Phase 1 helper. Must be called with at least readLock held so the read of state,
    // customKafkaPartitions, brokerList, topic, dataSourceConfigVersion is consistent.
    // Returns null when the job is in a final state (STOPPED/CANCELLED) or any state that
    // does not require partition refresh; callers must short-circuit the rest of the cycle
    // in that case.
    private FetchSnapshot takeFetchSnapshot() {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customKafkaPartitions != null && !customKafkaPartitions.isEmpty()) {
                // User pinned the partition list at CREATE time; no broker RPC needed.
                return FetchSnapshot.customOnly(dataSourceConfigVersion);
            }
            return FetchSnapshot.fetch(brokerList, topic, computeResource, dataSourceConfigVersion);
        }
        if (this.state == JobState.PAUSED) {
            // PAUSED jobs do not need partition info, but may be eligible for auto-resume.
            return FetchSnapshot.pausedAutoSchedule();
        }
        return null;
    }

    // Phase 3 (fast path). For jobs created with PROPERTIES("kafka_partitions"=...) there is no
    // broker RPC; we just copy the user-pinned list into currentKafkaPartitions. We still take
    // writeLock and re-check configVersion + state because phase 1 ran under readLock and an
    // ALTER ROUTINE LOAD could have landed in between - in that case we drop the assignment and
    // let the next scheduler tick re-snapshot with the new config.
    private void applyCustomPartitions(FetchSnapshot snapshot) {
        writeLock();
        try {
            if (dataSourceConfigVersion != snapshot.configVersion) {
                return;
            }
            if (this.state != JobState.RUNNING && this.state != JobState.NEED_SCHEDULE) {
                return;
            }
            if (customKafkaPartitions != null && !customKafkaPartitions.isEmpty()) {
                currentKafkaPartitions = customKafkaPartitions;
            }
        } finally {
            writeUnlock();
        }
    }

    // Phase 3 (PAUSED auto-resume). If a PAUSED job's pauseReason is recoverable (currently:
    // REPLICA_FEW_ERR within ScheduleRule's retry/window budget), promote it back to
    // NEED_SCHEDULE so the next RoutineLoadScheduler tick re-divides it into tasks. Pure FE
    // state-machine work; no external calls, so the writeLock window is tiny.
    private void applyPausedAutoSchedule() throws StarRocksException {
        writeLock();
        try {
            if (this.state != JobState.PAUSED) {
                return;
            }
            if (!ScheduleRule.isNeedAutoSchedule(this)) {
                return;
            }
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("name", name)
                    .add("current_state", this.state)
                    .add("msg", "Job need to be rescheduled")
                    .build());
            unprotectUpdateProgress();
            unprotectUpdateState(JobState.NEED_SCHEDULE, null);
        } finally {
            writeUnlock();
        }
    }

    private void applyFetchResult(FetchSnapshot snapshot, List<Integer> newPartitions, Exception fetchError)
            throws StarRocksException {
        writeLock();
        try {
            if (dataSourceConfigVersion != snapshot.configVersion) {
                // ALTER ROUTINE LOAD landed during the brpc; drop the stale fetch and let the
                // next scheduler tick refetch with the new config.
                return;
            }
            if (this.state != JobState.RUNNING && this.state != JobState.NEED_SCHEDULE) {
                return;
            }
            if (fetchError != null) {
                String msg = "Job failed to fetch all current partition with error [" + fetchError.getMessage() + "]";
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("error_msg", msg)
                        .build(), fetchError);
                // Only PAUSE jobs that were waiting to be (re)scheduled; jobs already RUNNING
                // keep their current tasks so a transient broker hiccup does not nuke an
                // otherwise-healthy pipeline. The next scheduler tick will retry.
                if (this.state == JobState.NEED_SCHEDULE) {
                    unprotectUpdateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.PARTITIONS_ERR, msg));
                }
                return;
            }
            // Diff currentKafkaPartitions vs newPartitions. Three cases:
            //   1) current is a strict superset of new  -> partitions were removed
            //      -> swap to the smaller list and reschedule
            //   2) current equals new (same set, same size) -> no change
            //   3) current is missing at least one of new -> partitions were added (or set was
            //      replaced) -> swap to the new list and reschedule
            boolean changed;
            if (currentKafkaPartitions.containsAll(newPartitions)) {
                if (currentKafkaPartitions.size() > newPartitions.size()) {
                    currentKafkaPartitions = newPartitions;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                .add("msg", "current kafka partitions has been change")
                                .build());
                    }
                    changed = true;
                } else {
                    changed = false;
                }
            } else {
                currentKafkaPartitions = newPartitions;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                            .add("msg", "current kafka partitions has been change")
                            .build());
                }
                changed = true;
            }
            if (changed) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("msg", "Job need to be rescheduled")
                        .build());
                unprotectUpdateProgress();
                unprotectUpdateState(JobState.NEED_SCHEDULE, null);
            }
        } finally {
            writeUnlock();
        }
    }

    // Discriminator for the apply phase of refreshPartitionsIfNeeded.
    //   FETCH                - state RUNNING/NEED_SCHEDULE, no custom partitions; needs broker RPC
    //   CUSTOM_ONLY          - state RUNNING/NEED_SCHEDULE, user-pinned partitions; no RPC needed
    //   PAUSED_AUTO_SCHEDULE - state PAUSED; evaluate ScheduleRule for auto-resume
    private enum FetchSnapshotKind { FETCH, CUSTOM_ONLY, PAUSED_AUTO_SCHEDULE }

    // Immutable snapshot of the inputs captured in phase 1 (readLock) and consumed unchanged
    // through phase 2 (unlocked broker RPC) and phase 3 (writeLock apply). Carrying the values
    // on a snapshot rather than re-reading the mutable fields keeps the RPC inputs stable, and
    // pairing them with configVersion lets phase 3 detect a concurrent ALTER and discard the
    // stale fetch result.
    private static final class FetchSnapshot {
        final FetchSnapshotKind kind;
        final String brokerList;
        final String topic;
        final ComputeResource computeResource;
        final long configVersion;

        private FetchSnapshot(FetchSnapshotKind kind, String brokerList, String topic,
                              ComputeResource computeResource, long configVersion) {
            this.kind = kind;
            this.brokerList = brokerList;
            this.topic = topic;
            this.computeResource = computeResource;
            this.configVersion = configVersion;
        }

        static FetchSnapshot fetch(String brokerList, String topic, ComputeResource cr, long ver) {
            return new FetchSnapshot(FetchSnapshotKind.FETCH, brokerList, topic, cr, ver);
        }

        static FetchSnapshot customOnly(long ver) {
            return new FetchSnapshot(FetchSnapshotKind.CUSTOM_ONLY, null, null, null, ver);
        }

        static FetchSnapshot pausedAutoSchedule() {
            return new FetchSnapshot(FetchSnapshotKind.PAUSED_AUTO_SCHEDULE, null, null, null, 0L);
        }
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - errorRows - unselectedRows));
        summary.put("errorRows", Long.valueOf(errorRows));
        summary.put("unselectedRows", Long.valueOf(unselectedRows));
        summary.put("receivedBytes", Long.valueOf(receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(receivedBytes * 1000 / totalTaskExcutionTimeMs));
        summary.put("loadRowsRate",
                Long.valueOf((totalRows - errorRows - unselectedRows) * 1000 / totalTaskExcutionTimeMs));
        summary.put("committedTaskNum", Long.valueOf(committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(abortedTaskNum));
        summary.put("partitionLagTime", new HashMap<>(getRoutineLoadLagTime()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    private List<Integer> getAllKafkaPartitions() throws StarRocksException {
        return KafkaUtil.getAllKafkaPartitions(brokerList, topic,
                snapshotConvertedCustomProperties(), computeResource);
    }

    // Snapshot `convertedCustomProperties` for use in an unlocked RPC call. Holds the intrinsic
    // monitor across convertCustomProperties(false) + ImmutableMap.copyOf so a concurrent ALTER
    // ROUTINE LOAD running modifyDataSourceProperties (which calls customProperties.putAll +
    // convertCustomProperties(true) under the same monitor) cannot rebuild the backing HashMap
    // mid-iteration. All RPC paths that need the converted-properties map must go through this
    // helper - direct ImmutableMap.copyOf(convertedCustomProperties) is racy outside the monitor.
    private ImmutableMap<String, String> snapshotConvertedCustomProperties() throws DdlException {
        synchronized (this) {
            convertCustomProperties(false);
            return ImmutableMap.copyOf(convertedCustomProperties);
        }
    }

    public static KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws StarRocksException {
        // check db and table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), stmt.getTableName());
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, stmt.getTableName());
        }

        long tableId = table.getId();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            Load.checkMergeCondition(stmt.getMergeConditionStr(), (OlapTable) table, table.getFullSchema(), false);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        }

        if (CreateRoutineLoadStmt.ENVELOPE_DEBEZIUM.equalsIgnoreCase(stmt.getEnvelope())
                && table instanceof OlapTable
                && ((OlapTable) table).getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new StarRocksException("envelope=debezium is only supported on PRIMARY KEY tables");
        }

        // init kafka routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
        kafkaRoutineLoadJob.setOptional(stmt);
        kafkaRoutineLoadJob.checkCustomProperties();

        return kafkaRoutineLoadJob;
    }

    private void checkCustomPartition(List<Integer> customKafkaPartitions) throws StarRocksException {
        if (customKafkaPartitions.isEmpty()) {
            return;
        }
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();
        for (Integer customPartition : customKafkaPartitions) {
            if (!allKafkaPartitions.contains(customPartition)) {
                throw new LoadException("there is an invalid custom partition: " + customPartition + " for topic: " + topic);
            }
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, KAFKA_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with catalog: " + KAFKA_FILE_CATALOG);
                }
            }
        }
    }

    private void updateNewPartitionProgress() {
        // update the progress of new partitions
        for (Integer kafkaPartition : currentKafkaPartitions) {
            if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                // if offset is not assigned, start from OFFSET_END
                long beginOffSet = kafkaDefaultOffSet == null ? KafkaProgress.OFFSET_END_VAL : kafkaDefaultOffSet;
                ((KafkaProgress) progress).addPartitionOffset(Pair.create(kafkaPartition, beginOffSet));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("kafka_partition_id", kafkaPartition)
                            .add("begin_offset", beginOffSet)
                            .add("msg", "The new partition has been added in job"));
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws StarRocksException {
        super.setOptional(stmt);

        if (!stmt.getKafkaPartitionOffsets().isEmpty()) {
            setCustomKafkaPartitions(stmt.getKafkaPartitionOffsets());
        }
        if (!stmt.getCustomKafkaProperties().isEmpty()) {
            setCustomKafkaProperties(stmt.getCustomKafkaProperties());
        }

        if (stmt.getConfluentSchemaRegistryUrl() != null) {
            setConfluentSchemaRegistryUrl(stmt.getConfluentSchemaRegistryUrl());
        }

        setDefaultKafkaGroupID();
    }

    // this is a unprotected method which is called in the initialization function
    private void setCustomKafkaPartitions(List<Pair<Integer, Long>> kafkaPartitionOffsets) throws LoadException {
        if (kafkaPartitionOffsets != null) {
            customeKafkaPartitionOffsets = kafkaPartitionOffsets;
        }
        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            this.customKafkaPartitions.add(partitionOffset.first);
            ((KafkaProgress) progress).addPartitionOffset(partitionOffset);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customProperties = kafkaProperties;
    }

    private void setDefaultKafkaGroupID() {
        if (this.customProperties.containsKey(PROPERTY_KAFKA_GROUP_ID)) {
            useDefaultGroupId = false;
            return;
        }
        this.customProperties.put(PROPERTY_KAFKA_GROUP_ID, name + "_" + UUID.randomUUID());
    }

    @Override
    public String dataSourcePropertiesToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(\n");
        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY).append("\"=\"");
        sb.append(brokerList).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY).append("\"=\"");
        sb.append(topic).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY).append("\"=\"");
        if (customeKafkaPartitionOffsets == null) {
            List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
            Collections.sort(sortedPartitions);
            sb.append(Joiner.on(",").join(sortedPartitions)).append("\",\n");
        } else {
            class PairComparator implements Comparator<Pair<Integer, Long>> {
                @Override
                public int compare(Pair<Integer, Long> p1, Pair<Integer, Long> p2) {
                    return Integer.compare(p1.first, p2.first);
                }
            }
            Collections.sort(customeKafkaPartitionOffsets, new PairComparator());
            List<Integer> customeKafkaPartitions = new ArrayList<>();
            List<Long> customeKakfaOffsets = new ArrayList<>();
            for (Pair<Integer, Long> partitionOffset : customeKafkaPartitionOffsets) {
                customeKafkaPartitions.add(partitionOffset.first);
                customeKakfaOffsets.add(partitionOffset.second);
            }
            sb.append(Joiner.on(",").join(customeKafkaPartitions)).append("\",\n");

            sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY).append("\"=\"");
            for (int i = 0; i < customeKakfaOffsets.size(); i++) {
                if (customeKakfaOffsets.get(i) == KafkaProgress.OFFSET_BEGINNING_VAL) {
                    sb.append("OFFSET_BEGINNING");
                } else if (customeKakfaOffsets.get(i) == KafkaProgress.OFFSET_END_VAL) {
                    sb.append("OFFSET_END");
                } else {
                    sb.append(Long.toString(customeKakfaOffsets.get(i)));
                }
                if (i != customeKakfaOffsets.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("\",\n");
        }
        if (confluentSchemaRegistryUrl != null) {
            sb.append("\"").append(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL).append("\"=\"");
            sb.append(getPrintableConfluentSchemaRegistryUrl()).append("\",\n");
        }

        Map<String, String> maskedProperties = getMaskedCustomProperties();
        if (useDefaultGroupId) {
            maskedProperties.remove(PROPERTY_KAFKA_GROUP_ID);
        }
        Iterator<Map.Entry<String, String>> iterator = maskedProperties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            sb.append("\"").append("property.").append(key).append("\"=\"");
            sb.append(value).append("\",\n");
        }
        if (sb.length() >= 2) {
            sb.delete(sb.length() - 2, sb.length());
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    private String getPrintableConfluentSchemaRegistryUrl() {
        String confluentSchemaRegistryUrl = getConfluentSchemaRegistryUrl();
        if (confluentSchemaRegistryUrl != null) {
            // confluentSchemaRegistryUrl have three patterns:
            // 1. https or http://key:password@addr
            // 2. https://key:password@addr
            // 3. http://key:password@addr
            // https://IAP4CQUET7L243C7:QEiI9CwV1szDViwIaBXiow2zXicQ1MY5/PLrmaaJE/FolDCCFmf2KPUkvv+UJozo@psrc-0kywq.us-east-2.aws.confluent.cloud
            String[] fragments = confluentSchemaRegistryUrl.split("@");
            if (fragments.length != 2) {
                // case 1
                return confluentSchemaRegistryUrl;
            } else {
                if (fragments[0].length() < 5) {
                    return confluentSchemaRegistryUrl;
                } else {
                    // case 2
                    String addr;
                    if ("https".equals(fragments[0].substring(0, 5))) {
                        addr = "https://" + fragments[1];
                    } else {
                        addr = "http://" + fragments[1];
                    }
                    return addr;
                }
            }
        } else {
            return null;
        }
    }

    @VisibleForTesting
    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("brokerList", brokerList);
        dataSourceProperties.put("topic", topic);
        String confluentSchemaRegistryUrl = getConfluentSchemaRegistryUrl();
        if (confluentSchemaRegistryUrl != null) {
            dataSourceProperties.put("confluent.schema.registry.url", getPrintableConfluentSchemaRegistryUrl());
        }
        List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentKafkaPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    protected Map<String, String> getMaskedCustomProperties() {
        Map<String, String> maskedProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getKey().contains("password") || entry.getKey().contains("secret")) {
                maskedProperties.put(entry.getKey(), "******");
            } else {
                maskedProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return maskedProperties;
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        Map<String, String> maskedProperties = getMaskedCustomProperties();
        return gson.toJson(maskedProperties);
    }

    @Override
    protected void checkDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        if (!dataSourceProperties.hasAnalyzedProperties()) {
            return;
        }

        // check kafka partition offsets
        // If customKafkaPartition is specified, only the specific partitions can be modified
        // otherwise, will check if partition is validated by actually reading kafka meta from kafka proxy
        List<Pair<Integer, Long>> kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
        if (customKafkaPartitions != null && !customKafkaPartitions.isEmpty()) {
            for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
                if (!customKafkaPartitions.contains(pair.first)) {
                    throw new DdlException("The specified partition " + pair.first + " is not in the custom partitions");
                }
            }
        } else {
            // check if partition is validate
            try {
                checkCustomPartition(kafkaPartitionOffsets.stream().map(k -> k.first).collect(Collectors.toList()));
            } catch (StarRocksException e) {
                throw new DdlException("The specified partition is not in the consumed partitions ", e);
            }
        }

        Map<String, String> changedProperties = dataSourceProperties.getCustomKafkaProperties();
        // check kafka default offsets
        if (changedProperties.containsKey(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)) {
            try {
                CreateRoutineLoadStmt.getKafkaOffset(
                        changedProperties.get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // check file existence
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : changedProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, KAFKA_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with catalog: " + KAFKA_FILE_CATALOG);
                }
            }
        }
    }

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
        Map<String, String> customKafkaProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
            customKafkaProperties = dataSourceProperties.getCustomKafkaProperties();
        }

        // modify partition offset first
        if (!kafkaPartitionOffsets.isEmpty()) {
            // we can only modify the partition that is being consumed
            ((KafkaProgress) progress).modifyOffset(kafkaPartitionOffsets);
        }

        if (!customKafkaProperties.isEmpty()) {
            // Hold the intrinsic monitor across putAll + convertCustomProperties(true): the
            // scheduler's lock-free refresh path reads customProperties via the synchronized
            // convertCustomProperties(false); putAll outside the monitor would race that read.
            synchronized (this) {
                this.customProperties.putAll(customKafkaProperties);
                convertCustomProperties(true);
            }
        }

        if (dataSourceProperties.getConfluentSchemaRegistryUrl() != null) {
            confluentSchemaRegistryUrl = dataSourceProperties.getConfluentSchemaRegistryUrl();
        }

        // modify broker list
        if (dataSourceProperties.getKafkaBrokerList() != null) {
            this.brokerList = dataSourceProperties.getKafkaBrokerList();
        }

        LOG.info("modify the data source properties of kafka routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }

    // update substate according to the lag.
    @Override
    public void updateSubstate() throws StarRocksException {
        KafkaProgress progress = (KafkaProgress) getTimestampProgress();
        Map<Integer, Long> partitionTimestamps = progress.getPartitionIdToOffset();
        long now = System.currentTimeMillis();

        for (Map.Entry<Integer, Long> entry : partitionTimestamps.entrySet()) {
            int partition = entry.getKey();
            long lag = (now - entry.getValue().longValue()) / 1000;
            if (lag > Config.routine_load_unstable_threshold_second) {
                updateSubstate(JobSubstate.UNSTABLE, new ErrorReason(InternalErrorCode.SLOW_RUNNING_ERR,
                        String.format("The lag [%d] of partition [%d] exceeds " +
                                        "Config.routine_load_unstable_threshold_second [%d]",
                                lag, partition, Config.routine_load_unstable_threshold_second)));
                return;
            }
        }
        updateSubstate(JobSubstate.STABLE, null);
    }

    @Override
    public void afterVisible(TransactionState txnState) {
        super.afterVisible(txnState);
        // Update lag time metrics when Kafka transaction becomes visible
        if (Config.enable_routine_load_lag_time_metrics) {
            updateLagTimeMetricsFromProgress();
        }
    }

    /**
     * Update lag time metrics using current Kafka progress
     */
    private void updateLagTimeMetricsFromProgress() {
        try {
            KafkaProgress progress = (KafkaProgress) getTimestampProgress();
            if (progress == null) {
                LOG.warn("Progres is null for Kafka job {}:{}", id, name);
                return;
            }
            Map<Integer, Long> partitionTimestamps = progress.getPartitionIdToOffset();
            Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
            
            long now = System.currentTimeMillis();
            for (Map.Entry<Integer, Long> entry : partitionTimestamps.entrySet()) {
                int partition = entry.getKey();
                Long timestampValue = entry.getValue();
                long lag = 0L;
                //   Check for clock drift (future timestamps)
                if (timestampValue > now) {
                    long clockDrift = timestampValue - now;
                    LOG.warn("Clock drift detected for job {} ({}) partition {}: " +
                            "timestamp {}ms is {}ms ahead of current time {}ms. ",
                            id, name, partition, timestampValue, clockDrift, now);
                } else {
                    lag = (now - timestampValue) / 1000; // convert to seconds
                } 
                partitionLagTimes.put(partition, lag);
            }
            
            if (!partitionLagTimes.isEmpty()) {
                RoutineLoadLagTimeMetricMgr.getInstance()
                        .updateRoutineLoadLagTimeMetric(this.getDbId(), this.getName(), partitionLagTimes);
            }
        } catch (Exception e) {
            LOG.warn("Failed to update lag time metrics for Kafka job {} ({}): {}", id, name, e.getMessage(), e);
        }
    }

    private Map<Integer, Long> getRoutineLoadLagTime() {
        try {
            RoutineLoadLagTimeMetricMgr metricMgr = RoutineLoadLagTimeMetricMgr.getInstance();
            Map<Integer, Long> lagTimes = metricMgr.getPartitionLagTimes(this.getDbId(), this.getName());
            return lagTimes != null ? lagTimes : Maps.newHashMap();
        } catch (Exception e) {
            LOG.warn("Failed to get routine load lag time for job {} ({}): {}", id, name, e.getMessage(), e);
            // Return empty map as fallback
            return Maps.newHashMap();
        }
    }

    protected Long getKafkaDefaultOffSet() {
        return kafkaDefaultOffSet;
    }
}
