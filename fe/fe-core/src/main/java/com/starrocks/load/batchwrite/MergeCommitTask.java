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

package com.starrocks.load.batchwrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.LoadErrorUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task responsible for executing a load.
 */
public class MergeCommitTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitTask.class);

    // Initialized in constructor ==================================
    private final TableId tableId;
    private final String label;
    private final TUniqueId loadId;
    private final StreamLoadInfo streamLoadInfo;
    private final StreamLoadKvParams loadParameters;
    private final Set<Long> coordinatorBackendIds;
    private final int batchWriteIntervalMs;
    private final Coordinator.Factory coordinatorFactory;
    private final MergeCommitTaskCallback mergeCommitTaskCallback;
    private final TimeTrace timeTrace;
    private final AtomicReference<Throwable> failure;

    // Initialized in beginTxn() ==================================
    private long txnId = -1;

    // Initialized in executeLoad() ==================================
    ConnectContext context;
    LoadPlanner loadPlanner;
    private Coordinator coordinator;
    private List<TabletCommitInfo> tabletCommitInfo;
    private List<TabletFailInfo> tabletFailInfo;
    private LoadJobFinalOperation loadJobFinalOperation;
    private boolean collectProfileSuccess = false;

    public MergeCommitTask(
            TableId tableId,
            String label,
            TUniqueId loadId,
            StreamLoadInfo streamLoadInfo,
            int batchWriteIntervalMs,
            StreamLoadKvParams loadParameters,
            Set<Long> coordinatorBackendIds,
            Coordinator.Factory coordinatorFactory,
            MergeCommitTaskCallback mergeCommitTaskCallback) {
        this.tableId = tableId;
        this.label = label;
        this.loadId = loadId;
        this.streamLoadInfo = streamLoadInfo;
        this.batchWriteIntervalMs = batchWriteIntervalMs;
        this.loadParameters = loadParameters;
        this.coordinatorBackendIds = coordinatorBackendIds;
        this.coordinatorFactory = coordinatorFactory;
        this.mergeCommitTaskCallback = mergeCommitTaskCallback;
        this.timeTrace = new TimeTrace();
        this.failure = new AtomicReference<>();
    }

    @Override
    public void run() {
        try {
            beginTxn();
            executeLoad();
            commitAndPublishTxn();
        } catch (Exception e) {
            failure.set(e);
            abortTxn(e);
            LOG.error("Failed to execute load, label: {}, load id: {}, txn id: {}",
                    label, DebugUtil.printId(loadId), txnId, e);
        } finally {
            mergeCommitTaskCallback.finish(this);
            timeTrace.finishTimeMs = System.currentTimeMillis();
            MergeCommitMetricRegistry.getInstance().updateLoadLatency(timeTrace.totalCostMs());
            reportProfile();
            LOG.debug("Finish load, label: {}, load id: {}, txn_id: {}, {}",
                    label, DebugUtil.printId(loadId), txnId, timeTrace.summary());
        }
    }

    public String getLabel() {
        return label;
    }

    public long getTxnId() {
        return txnId;
    }

    public Set<Long> getBackendIds() {
        return Collections.unmodifiableSet(coordinatorBackendIds);
    }

    /**
     * Checks if the given backend id is contained in the coordinator backend IDs.
     */
    public boolean containCoordinatorBackend(long backendId) {
        return coordinatorBackendIds.contains(backendId);
    }

    /**
     * Checks if this batch is active and can accept new load requests.
     */
    public boolean isActive() {
        if (failure.get() != null) {
            return false;
        }
        long joinPlanTimeMs = timeTrace.joinPlanTimeMs.get();
        return joinPlanTimeMs <= 0 || (System.currentTimeMillis() - joinPlanTimeMs < batchWriteIntervalMs);
    }

    public Throwable getFailure() {
        return failure.get();
    }

    private void beginTxn() throws Exception {
        timeTrace.beginTxnTimeMs = System.currentTimeMillis();
        Pair<Database, OlapTable> pair = getDbAndTable();
        txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                pair.first.getId(), Lists.newArrayList(pair.second.getId()), label,
                TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING,
                streamLoadInfo.getTimeout(), streamLoadInfo.getComputeResource());
    }

    private void commitAndPublishTxn() throws Exception {
        timeTrace.commitTxnTimeMs = System.currentTimeMillis();
        Database database = getDb();
        long publishTimeoutMs =
                streamLoadInfo.getTimeout() * 1000L - (timeTrace.commitTxnTimeMs - timeTrace.beginTxnTimeMs);
        boolean publishSuccess = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                database, txnId, tabletCommitInfo, tabletFailInfo, publishTimeoutMs, null);
        if (!publishSuccess) {
            LOG.warn("Publish timeout, txn_id: {}, label: {}, total timeout: {} ms, publish timeout: {} ms",
                        txnId, label, streamLoadInfo.getTimeout() * 1000, publishTimeoutMs);
        }
    }

    private void abortTxn(Throwable reason) {
        if (txnId == -1) {
            return;
        }
        try {
            Database database = getDb();
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    database.getId(), txnId, reason == null ? "" : reason.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to abort transaction {}", txnId, e);
        }
    }

    private void executeLoad() throws Exception {
        try {
            timeTrace.executeLoadTimeMs = System.currentTimeMillis();
            context = new ConnectContext();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            context.setCurrentComputeResource(streamLoadInfo.getComputeResource());
            context.setThreadLocalInfo();

            Pair<Database, OlapTable> pair = getDbAndTable();
            // although merge commit uses pipeline engine, use table property to control the profile same as stream load
            if (pair.second.enableLoadProfile()) {
                long sampleIntervalMs = Config.load_profile_collect_interval_second * 1000;
                if (sampleIntervalMs > 0 &&
                        System.currentTimeMillis() - pair.second.getLastCollectProfileTime() > sampleIntervalMs) {
                    context.getSessionVariable().setEnableProfile(true);
                    pair.second.updateLastCollectProfileTime();
                }
                context.getSessionVariable().setBigQueryProfileThreshold(
                        Config.stream_load_profile_collect_threshold_second + "s");
                // do not enable runtime profile report currently
                context.getSessionVariable().setRuntimeProfileReportInterval(-1);
            }

            loadPlanner = new LoadPlanner(-1, loadId, txnId, pair.first.getId(),
                    tableId.getDbName(), pair.second, streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(),
                    streamLoadInfo.isPartialUpdate(), context, null,
                    streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                    streamLoadInfo.getNegative(), coordinatorBackendIds.size(), streamLoadInfo.getColumnExprDescs(),
                    streamLoadInfo, label, streamLoadInfo.getTimeout());
            loadPlanner.setBatchWrite(batchWriteIntervalMs,
                    ImmutableMap.<String, String>builder()
                            .putAll(loadParameters.toMap()).build(), coordinatorBackendIds);
            loadPlanner.setMergeConditionStr(streamLoadInfo.getMergeConditionStr());
            loadPlanner.plan();
            timeTrace.deployPlanTimeMs = System.currentTimeMillis();
            coordinator = coordinatorFactory.createStreamLoadScheduler(loadPlanner);
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coordinator);
            coordinator.exec();
            int waitSecond = streamLoadInfo.getTimeout() -
                    (int) (System.currentTimeMillis() - timeTrace.createTimeMs) / 1000;
            timeTrace.joinPlanTimeMs.set(System.currentTimeMillis());
            if (coordinator.join(waitSecond)) {
                Status status = coordinator.getExecStatus();
                if (!status.ok()) {
                    throw new LoadException(
                            String.format("Failed to execute load, status code: %s, error message: %s",
                                    status.getErrorCodeString(), status.getErrorMsg()));
                }
                tabletCommitInfo = TabletCommitInfo.fromThrift(coordinator.getCommitInfos());
                tabletFailInfo = TabletFailInfo.fromThrift(coordinator.getFailInfos());

                // TODO add more information such as progress, unfinished backends
                loadJobFinalOperation = new LoadJobFinalOperation();
                EtlStatus etlStatus = loadJobFinalOperation.getLoadingStatus();
                etlStatus.setState(TEtlState.FINISHED);
                etlStatus.setCounters(coordinator.getLoadCounters());
                if (coordinator.getTrackingUrl() != null) {
                    etlStatus.setTrackingUrl(coordinator.getTrackingUrl());
                }
                if (!coordinator.getRejectedRecordPaths().isEmpty()) {
                    etlStatus.setRejectedRecordPaths(coordinator.getRejectedRecordPaths());
                }
                long loadedRows = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_NORMAL_ALL, "0"));
                long loadBytes = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadJob.LOADED_BYTES, "0"));
                MergeCommitMetricRegistry.getInstance().incLoadData(loadedRows, loadBytes);
                long filteredRows = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_ABNORMAL_ALL, "0"));
                double maxFilterRatio = loadParameters.getMaxFilterRatio().orElse(0.0);
                if (isProfileEnabled()) {
                    try {
                        coordinator.collectProfileSync();
                        collectProfileSuccess = true;
                    } catch (Exception e) {
                        LOG.error("Failed to collect profile, label: {}, txn id: {}, load id: {}",
                                label, DebugUtil.printId(loadId), txnId, e);
                    }
                }
                if (filteredRows > (filteredRows + loadedRows) * maxFilterRatio) {
                    throw new LoadException(String.format("There is data quality issue, please check the " +
                                    "tracking url for details. Max filter ratio: %s. The tracking url: %s",
                                    maxFilterRatio, coordinator.getTrackingUrl()));
                }
            } else {
                throw new LoadException(
                        String.format("Timeout to execute load after waiting for %s seconds", waitSecond));
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
            ConnectContext.remove();
        }
    }

    private Database getDb() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new LoadException(String.format("Database %s does not exist", tableId.getDbName()));
        }

        return db;
    }

    private Pair<Database, OlapTable> getDbAndTable() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new LoadException(String.format("Database %s does not exist", tableId.getDbName()));
        }

        Table table = db.getTable(tableId.getTableName());
        if (table == null) {
            throw new LoadException(String.format(
                    "Table [%s.%s] does not exist", tableId.getDbName(), tableId.getTableName()));
        }
        return Pair.create(db, (OlapTable) table);
    }

    private boolean isProfileEnabled() {
        return (context != null && context.isProfileEnabled()) || LoadErrorUtils.enableProfileAfterError(coordinator);
    }

    private void reportProfile() {
        if (!isProfileEnabled()) {
            return;
        }
        RuntimeProfile profile = new RuntimeProfile("Load");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(timeTrace.createTimeMs));
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(timeTrace.finishTimeMs));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(timeTrace.totalCostMs()));
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString(ProfileManager.LOAD_TYPE, "MERGE_COMMIT");
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString("Default Db", tableId.getDbName());
        summaryProfile.addInfoString("Sql Statement",
                String.format("merge commit, table: %s, label: %s, %s",
                        tableId.getTableName(), label, loadParameters.toString()));
        summaryProfile.addInfoString(ProfileManager.VARIABLES, "{}");
        summaryProfile.addInfoString("NonDefaultSessionVariables", "{}");
        summaryProfile.addInfoString("TxnId", txnId == -1 ? "N/A" : String.valueOf(txnId));
        summaryProfile.addInfoString("Backends", coordinatorBackendIds.toString());
        summaryProfile.addInfoString("Time Trace", timeTrace.summary());
        if (failure.get() != null) {
            summaryProfile.addInfoString("Exception", failure.get().getMessage());
        }
        if (loadJobFinalOperation != null) {
            EtlStatus etlStatus = loadJobFinalOperation.getLoadingStatus();
            summaryProfile.addInfoString("LoadResult", String.format("loadRows: %s, filterRows: %s, loadBytes: %s",
                    etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_NORMAL_ALL, "0"),
                    etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_ABNORMAL_ALL, "0"),
                    etlStatus.getCounters().getOrDefault(LoadJob.LOADED_BYTES, "0")));
        }
        profile.addChild(summaryProfile);
        if (collectProfileSuccess) {
            profile.addChild(coordinator.buildQueryProfile(true));
        }
        ProfileManager.getInstance().pushProfile(
                loadPlanner == null ? null : loadPlanner.getExecPlan().getProfilingPlan(), profile);
    }

    @VisibleForTesting
    Set<Long> getCoordinatorBackendIds() {
        return coordinatorBackendIds;
    }

    @VisibleForTesting
    Coordinator getCoordinator() {
        return coordinator;
    }

    @VisibleForTesting
    TimeTrace getTimeTrace() {
        return timeTrace;
    }

    // Trace the timing of various stages of the load operation.
    static class TimeTrace {
        long createTimeMs;
        long beginTxnTimeMs = -1;
        long executeLoadTimeMs = -1;
        long deployPlanTimeMs = -1;
        AtomicLong joinPlanTimeMs = new AtomicLong(-1);
        long commitTxnTimeMs = -1;
        long finishTimeMs = -1;

        public TimeTrace() {
            this.createTimeMs = System.currentTimeMillis();
        }

        public long totalCostMs() {
            return finishTimeMs > 0 ? finishTimeMs - createTimeMs : System.currentTimeMillis() - createTimeMs;
        }

        String summary() {
            StringBuilder sb = new StringBuilder();
            sb.append("total: ").append(totalCostMs()).append(" ms");
            appendTraceItem(sb, ", pending: ", createTimeMs, beginTxnTimeMs);
            appendTraceItem(sb, ", begin txn: ", beginTxnTimeMs, executeLoadTimeMs);
            appendTraceItem(sb, ", plan: ", executeLoadTimeMs, deployPlanTimeMs);
            appendTraceItem(sb, ", deploy: ", deployPlanTimeMs, joinPlanTimeMs.get());
            appendTraceItem(sb, ", load: ", joinPlanTimeMs.get(), commitTxnTimeMs);
            appendTraceItem(sb, ", commit/publish txn: ", commitTxnTimeMs, finishTimeMs);
            return sb.toString();
        }

        private void appendTraceItem(StringBuilder sb, String msg, long startTimeMs, long endTimeMs) {
            if (startTimeMs > 0) {
                sb.append(msg).append(endTimeMs - startTimeMs).append(" ms");
            }
        }
    }
}
