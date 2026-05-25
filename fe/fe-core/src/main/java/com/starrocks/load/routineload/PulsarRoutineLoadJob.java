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


package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
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
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.Load;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * PulsarRoutineLoadJob is a kind of RoutineLoadJob which fetch data from pulsar.
 * The progress which is super class property is seems like "{"partition1": backlognum1, "partition2": backlognum2}"
 */
public class PulsarRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(PulsarRoutineLoadJob.class);

    public static final String PULSAR_FILE_CATALOG = "pulsar";

    @SerializedName("svu")
    private String serviceUrl;
    @SerializedName("tpc")
    private String topic;
    @SerializedName("sbs")
    private String subscription;
    // optional, user want to load partitions.
    @SerializedName("cpp")
    private List<String> customPulsarPartitions = Lists.newArrayList();
    // current pulsar partitions is the actually partition which will be fetched
    private List<String> currentPulsarPartitions = Lists.newArrayList();
    // pulsar properties, property prefix will be mapped to pulsar custom parameters, which can be extended in the future
    @SerializedName("cpt")
    private Map<String, String> customProperties = Maps.newHashMap();
    private final Map<String, String> convertedCustomProperties = Maps.newHashMap();

    public static final String POSITION_EARLIEST = "POSITION_EARLIEST"; // 1
    public static final String POSITION_LATEST = "POSITION_LATEST"; // 0
    public static final long POSITION_LATEST_VAL = 0;
    public static final long POSITION_EARLIEST_VAL = 1;

    private Long defaultInitialPosition = null;

    public PulsarRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.PULSAR);
        this.progress = new PulsarProgress();
        this.timestampProgress = new PulsarProgress();
    }

    public PulsarRoutineLoadJob(Long id, String name, long dbId, long tableId,
                                String serviceUrl, String topic, String subscription) {
        super(id, name, dbId, tableId, LoadDataSourceType.PULSAR);
        this.serviceUrl = serviceUrl;
        this.topic = topic;
        this.subscription = subscription;
        this.progress = new PulsarProgress();
        this.timestampProgress = new PulsarProgress();
    }

    public String getTopic() {
        return topic;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getSubscription() {
        return subscription;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    @Override
    protected String getSourceProgressString() {
        // empty implement.
        return "";
    }

    @Override
    protected String getSourceLagString(String progressJsonStr) {
        // empty implement.
        return "";
    }

    @Override
    public void prepare() throws StarRocksException {
        super.prepare();
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);
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
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, PULSAR_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }

        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION)) {
            try {
                this.defaultInitialPosition = CreateRoutineLoadStmt.getPulsarPosition(
                        convertedCustomProperties.remove(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION));
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
                // divide pulsarPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    List<String> partitions = Lists.newArrayList();
                    Map<String, Long> initialPositions = Maps.newHashMap();
                    for (int j = 0; j < currentPulsarPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            String partition = currentPulsarPartitions.get(j);
                            partitions.add(partition);
                            // initial position was set, need to be added into PulsarTaskInfo
                            Long initialPosition = ((PulsarProgress) progress).getInitialPosition(partition);
                            if (initialPosition != -1L) {
                                initialPositions.put(partition, initialPosition);
                            }
                        }
                    }
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(UUIDUtil.genUUID(), this,
                            taskSchedIntervalS * 1000, timeToExecuteMs, partitions,
                            initialPositions, getTaskTimeoutSecond() * 1000);
                    pulsarTaskInfo.setComputeResource(computeResource);
                    LOG.debug("pulsar routine load task created: " + pulsarTaskInfo);
                    routineLoadTaskInfoList.add(pulsarTaskInfo);
                    result.add(pulsarTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
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
            final List<Long> computeNodeIds = warehouseManager.getAllComputeNodeIds(computeResource);
            for (long nodeId : computeNodeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAlive()) {
                    ++aliveNodeNum;
                }
            }
        }
        int partitionNum = currentPulsarPartitions.size();
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
        if (txnStatusChangeReason == TxnStatusChangeReason.NO_ROWS_IMPORTED) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "No rows were imported from upstream" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the position to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the position.
        LOG.debug("no need to update the progress of pulsar routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    public String dataSourcePropertiesToSql() {
        return "";
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
        PulsarTaskInfo oldPulsarTaskInfo = (PulsarTaskInfo) routineLoadTaskInfo;
        // add new task
        PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(timeToExecuteMs, oldPulsarTaskInfo,
                ((PulsarProgress) progress).getPartitionToInitialPosition(oldPulsarTaskInfo.getPartitions()));
        pulsarTaskInfo.setComputeResource(routineLoadTaskInfo.getComputeResource());
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(pulsarTaskInfo);
        LOG.debug("pulsar routine load task renewed: " + pulsarTaskInfo);
        return pulsarTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() {
        ((PulsarProgress) progress).unprotectUpdate(currentPulsarPartitions, defaultInitialPosition);
    }

    // Refresh `currentPulsarPartitions` against the broker. The slow step is a FE -> BE brpc
    // (PulsarUtil.getAllPulsarPartitions), so we must not hold the per-job writeLock across it -
    // otherwise readers (admin RPCs, SHOW ROUTINE LOAD, processTimeoutTasks) stall for the
    // full RPC plus retries. Three phases: snapshot under readLock, fetch with no lock, apply
    // under writeLock with a configVersion guard to discard a mid-flight ALTER.
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

        List<String> newPartitions = null;
        Exception fetchError = null;
        try {
            newPartitions = PulsarUtil.getAllPulsarPartitions(snapshot.serviceUrl, snapshot.topic,
                    snapshot.subscription, snapshotConvertedCustomProperties(), snapshot.computeResource);
        } catch (Exception e) {
            fetchError = e;
        }

        applyFetchResult(snapshot, newPartitions, fetchError);
    }

    // Phase 1 helper. Must be called with at least readLock held so the read of state,
    // customPulsarPartitions, serviceUrl, topic, subscription, dataSourceConfigVersion is
    // consistent. Returns null when the job is in a final state (STOPPED/CANCELLED) or any
    // state that does not require partition refresh; callers must short-circuit the rest of
    // the cycle in that case.
    private FetchSnapshot takeFetchSnapshot() {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customPulsarPartitions != null && !customPulsarPartitions.isEmpty()) {
                // User pinned the partition list at CREATE time; no broker RPC needed.
                return FetchSnapshot.customOnly(dataSourceConfigVersion);
            }
            return FetchSnapshot.fetch(serviceUrl, topic, subscription, computeResource, dataSourceConfigVersion);
        }
        if (this.state == JobState.PAUSED) {
            // PAUSED jobs do not need partition info, but may be eligible for auto-resume.
            return FetchSnapshot.pausedAutoSchedule();
        }
        return null;
    }

    // Phase 3 (fast path). For jobs created with PROPERTIES("pulsar_partitions"=...) there is no
    // broker RPC; we just copy the user-pinned list into currentPulsarPartitions. We still take
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
            if (customPulsarPartitions != null && !customPulsarPartitions.isEmpty()) {
                currentPulsarPartitions = customPulsarPartitions;
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
            unprotectUpdateState(JobState.NEED_SCHEDULE, null, false /* not replay */);
        } finally {
            writeUnlock();
        }
    }

    private void applyFetchResult(FetchSnapshot snapshot, List<String> newPartitions, Exception fetchError)
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
                            new ErrorReason(InternalErrorCode.PARTITIONS_ERR, msg),
                            false /* not replay */);
                }
                return;
            }
            // Diff currentPulsarPartitions vs newPartitions. Three cases:
            //   1) current is a strict superset of new  -> partitions were removed
            //      -> swap to the smaller list and reschedule
            //   2) current equals new (same set, same size) -> no change
            //   3) current is missing at least one of new -> partitions were added (or set was
            //      replaced) -> swap to the new list and reschedule
            boolean changed;
            if (currentPulsarPartitions.containsAll(newPartitions)) {
                if (currentPulsarPartitions.size() > newPartitions.size()) {
                    unprotectUpdateCurrentPartitions(newPartitions);
                    changed = true;
                } else {
                    changed = false;
                }
            } else {
                unprotectUpdateCurrentPartitions(newPartitions);
                changed = true;
            }
            if (changed) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("msg", "Job need to be rescheduled")
                        .build());
                unprotectUpdateProgress();
                unprotectUpdateState(JobState.NEED_SCHEDULE, null, false /* not replay */);
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
        final String serviceUrl;
        final String topic;
        final String subscription;
        final ComputeResource computeResource;
        final long configVersion;

        private FetchSnapshot(FetchSnapshotKind kind, String serviceUrl, String topic, String subscription,
                              ComputeResource computeResource, long configVersion) {
            this.kind = kind;
            this.serviceUrl = serviceUrl;
            this.topic = topic;
            this.subscription = subscription;
            this.computeResource = computeResource;
            this.configVersion = configVersion;
        }

        static FetchSnapshot fetch(String serviceUrl, String topic, String subscription,
                                   ComputeResource cr, long ver) {
            return new FetchSnapshot(FetchSnapshotKind.FETCH, serviceUrl, topic, subscription, cr, ver);
        }

        static FetchSnapshot customOnly(long ver) {
            return new FetchSnapshot(FetchSnapshotKind.CUSTOM_ONLY, null, null, null, null, ver);
        }

        static FetchSnapshot pausedAutoSchedule() {
            return new FetchSnapshot(FetchSnapshotKind.PAUSED_AUTO_SCHEDULE, null, null, null, null, 0L);
        }
    }

    protected void unprotectUpdateCurrentPartitions(List<String> newCurrentPartitions) {
        currentPulsarPartitions = newCurrentPartitions;
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("current_pulsar_partitions", Joiner.on(",").join(currentPulsarPartitions))
                    .add("msg", "current pulsar partitions has been change")
                    .build());
        }
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", totalRows);
        summary.put("loadedRows", totalRows - errorRows - unselectedRows);
        summary.put("errorRows", errorRows);
        summary.put("unselectedRows", unselectedRows);
        summary.put("receivedBytes", receivedBytes);
        summary.put("taskExecuteTimeMs", totalTaskExcutionTimeMs);
        summary.put("receivedBytesRate", receivedBytes * 1000 / totalTaskExcutionTimeMs);
        summary.put("loadRowsRate",
                (totalRows - errorRows - unselectedRows) * 1000 / totalTaskExcutionTimeMs);
        summary.put("committedTaskNum", committedTaskNum);
        summary.put("abortedTaskNum", abortedTaskNum);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    public List<String> getAllPulsarPartitions() throws StarRocksException {
        // Get custom properties like tokens.
        return PulsarUtil.getAllPulsarPartitions(serviceUrl, topic,
                subscription, snapshotConvertedCustomProperties(), computeResource);
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

    public static PulsarRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws StarRocksException {
        // check db and table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        long tableId = -1L;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), stmt.getTableName());
            Load.checkMergeCondition(stmt.getMergeConditionStr(), (OlapTable) table, table.getFullSchema(), false);
            tableId = table.getId();
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // init pulsar routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        PulsarRoutineLoadJob pulsarRoutineLoadJob = new PulsarRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, stmt.getPulsarServiceUrl(), stmt.getPulsarTopic(),
                stmt.getPulsarSubscription());
        pulsarRoutineLoadJob.setOptional(stmt);
        pulsarRoutineLoadJob.checkCustomProperties();
        pulsarRoutineLoadJob.checkCustomPartition();

        return pulsarRoutineLoadJob;
    }

    private void checkCustomPartition() throws StarRocksException {
        if (customPulsarPartitions.isEmpty()) {
            return;
        }
        List<String> allPulsarPartitions = getAllPulsarPartitions();
        for (String customPartition : customPulsarPartitions) {
            if (!allPulsarPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom pulsar partition " + customPartition
                        + " which is invalid for topic " + topic);
            }
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, PULSAR_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with globalStateMgr: " + PULSAR_FILE_CATALOG);
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws StarRocksException {
        super.setOptional(stmt);

        if (!stmt.getPulsarPartitions().isEmpty()) {
            setCustomPulsarPartitions(stmt.getPulsarPartitions());
        }

        if (!stmt.getPulsarPartitionInitialPositions().isEmpty()) {
            setPulsarPatitionInitialPositions(stmt.getPulsarPartitionInitialPositions());
        }

        if (!stmt.getCustomPulsarProperties().isEmpty()) {
            setCustomPulsarProperties(stmt.getCustomPulsarProperties());
        }
    }

    // this is an unprotected method which is called in the initialization function
    private void setCustomPulsarPartitions(List<String> pulsarPartitions) {
        this.customPulsarPartitions = pulsarPartitions;
    }

    private void setPulsarPatitionInitialPositions(List<Pair<String, Long>> patitionToInitialPositions) {
        for (Pair<String, Long> entry : patitionToInitialPositions) {
            ((PulsarProgress) progress).addPartitionToInitialPosition(entry);
        }
    }

    private void setCustomPulsarProperties(Map<String, String> pulsarProperties) {
        this.customProperties = pulsarProperties;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("serviceUrl", serviceUrl);
        dataSourceProperties.put("topic", topic);
        dataSourceProperties.put("subscription", subscription);
        List<String> sortedPartitions = Lists.newArrayList(currentPulsarPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentPulsarPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }






    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        List<Pair<String, Long>> partitionInitialPositions = Lists.newArrayList();
        Map<String, String> customPulsarProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            partitionInitialPositions = dataSourceProperties.getPulsarPartitionInitialPositions();
            customPulsarProperties = dataSourceProperties.getCustomPulsarProperties();
        }

        if (!customPulsarProperties.isEmpty()) {
            // Hold the intrinsic monitor across putAll + convertCustomProperties(true): the
            // scheduler's lock-free refresh path reads customProperties via the synchronized
            // convertCustomProperties(false); putAll outside the monitor would race that read.
            synchronized (this) {
                this.customProperties.putAll(customPulsarProperties);
                convertCustomProperties(true);
            }

            if (customPulsarProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION)) {
                // defaultInitialPosition should be updated by convertCustomProperties()
                List<Pair<String, Long>> initialPositions = new ArrayList<>();
                // defaultInitialPosition can only update currentPulsarPartitions
                currentPulsarPartitions.forEach(
                        entry -> initialPositions.add(Pair.create(entry, this.defaultInitialPosition)));
                ((PulsarProgress) progress).modifyInitialPositions(initialPositions);
            }
        }

        // modify partition positions
        if (!partitionInitialPositions.isEmpty()) {
            // we can only modify the partition if it's specified in the create statement
            for (Pair<String, Long> pair : partitionInitialPositions) {
                if (!customPulsarPartitions.contains(pair.first)) {
                    throw new DdlException("The partition " +
                            pair.first + " is not specified in the create statement");
                }
            }

            ((PulsarProgress) progress).modifyInitialPositions(partitionInitialPositions);
        }

        LOG.info("modify the data source properties of pulsar routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }
}
