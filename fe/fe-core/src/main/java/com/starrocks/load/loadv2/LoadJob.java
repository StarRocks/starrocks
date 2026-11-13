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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadJob.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.load.FailMsg.CancelType;
import com.starrocks.load.Load;
import com.starrocks.load.LoadConstants;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTask;
import com.starrocks.task.PriorityLeaderTaskExecutor;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.LoadJobWithWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class LoadJob extends AbstractTxnStateChangeCallback
        implements LoadTaskCallback, Writable, LoadJobWithWarehouse, LoadsSystemTable.Job {

    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    public static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    public static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";
    public static final String UNSELECTED_ROWS = "unselected.rows";
    public static final String LOADED_BYTES = "loaded.bytes";

    private static final int TASK_SUBMIT_RETRY_NUM = 2;
    // Length of the varchar columns of _statistics_.loads_history; values built here must fit.
    public static final int LOADS_VARCHAR_COLUMN_LENGTH = 2048;

    @SerializedName("id")
    protected long id;
    // input params
    @SerializedName("d")
    protected long dbId;
    @SerializedName("l")
    protected String label;
    // Read on the daemon thread (LoadsHistorySyncer.collectShowingLoadJobs,
    // SHOW LOAD result builders) and on cancel paths without holding the
    // job's writeLock — declare volatile so transitions made under the
    // writeLock are visible to lock-free readers.
    @SerializedName("s")
    protected volatile JobState state = JobState.PENDING;
    @SerializedName("j")
    protected EtlJobType jobType;

    // optional properties
    // timeout second need to be reset in constructor of subclass
    @SerializedName("t")
    protected long timeoutSecond = Config.broker_load_default_timeout_second;
    @SerializedName("lm")
    protected long loadMemLimit = 0;      // default no limit for load memory
    @SerializedName("m")
    protected double maxFilterRatio = 0;
    @SerializedName("st")
    protected boolean strictMode = false; // default is false
    @SerializedName("tz")
    protected String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    @SerializedName("p")
    protected boolean partialUpdate = false;
    @SerializedName("pum")
    protected String partialUpdateMode = "row";
    @SerializedName("pr")
    protected int priority = LoadPriority.NORMAL_VALUE;
    @SerializedName("ln")
    protected long logRejectedRecordNum = 0;

    @SerializedName("c")
    protected long createTimestamp = -1;
    @SerializedName("ls")
    protected long loadStartTimestamp = -1;
    @SerializedName("loadCommittedTimestamp")
    protected long loadCommittedTimestamp = -1;
    @SerializedName("f")
    protected long finishTimestamp = -1;
    // When the transaction became VISIBLE; -1 until then and for cancelled jobs.
    @SerializedName("vt")
    protected long visibleTimestamp = -1;

    @SerializedName("tx")
    protected long transactionId;
    @SerializedName("fm")
    protected FailMsg failMsg;
    protected Map<Long, LoadTask> idToTasks = Maps.newConcurrentMap();
    // Persisted so that LOAD_ID survives a restart; cleared when the job is retried.
    @SerializedName("lids")
    protected List<String> loadIds = Lists.newArrayList();
    // Captured when the transaction commits: the transaction only keeps the value in memory and
    // is purged long before the job is. -1 when ingestion slowdown never evaluated the commit.
    @SerializedName("ctm")
    protected long commitThrottleMs = -1;
    protected Set<Long> finishedTaskIds = Sets.newHashSet();
    @SerializedName("lt")
    protected EtlStatus loadingStatus = new EtlStatus();
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all of tasks have been finished
    // 100: txn status is visible and load has been finished
    @SerializedName("pg")
    protected int progress;

    @SerializedName("warehouseId")
    protected long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
    @SerializedName("computeResource")
    protected ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;

    @SerializedName("mc")
    protected String mergeCondition;
    @SerializedName("jo")
    protected JSONOptions jsonOptions = new JSONOptions();

    @SerializedName("user")
    protected String user = "";

    public int getProgress() {
        return this.progress;
    }

    // non-persistence
    // This param is set true during txn is committing.
    // During committing, the load job could not be cancelled.
    protected boolean isCommitting = false;

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    // Whether the history row is to be recorded when the write lock is released. Guarded by the write lock.
    private boolean statePending = false;
    // The creation time before the first retry replaced it, -1 if the job was never retried. A retry
    // is not journaled, so after a restart the job is back at the time it was first created.
    private volatile long firstCreateTimestamp = -1;
    private final Object rowOfferLock = new Object();

    // this request id is only used for checking if a load begin request is a duplicate request.
    protected TUniqueId requestId;

    protected int retryTime = 2;

    // only for persistence param. see readFields() for usage
    private boolean isJobTypeRead = false;

    protected boolean startLoad = false;

    // only for log replay
    public LoadJob() {
    }

    public LoadJob(long dbId, String label) {
        this.id = GlobalStateMgr.getCurrentState().getNextId();
        this.dbId = dbId;
        this.label = label;
        if (ConnectContext.get() != null) {
            this.createTimestamp = ConnectContext.get().getStartTime();
        } else {
            // only for test used
            this.createTimestamp = System.currentTimeMillis();
        }
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        // The row is taken once the lock is released: toThrift() looks the transaction up, and
        // the transaction manager must not be entered with the job lock held.
        boolean record = statePending && lock.getWriteHoldCount() == 1;
        if (record) {
            statePending = false;
        }
        lock.writeLock().unlock();
        if (record) {
            offerCurrentRow();
        }
    }

    /**
     * Hand the job's current row to the load_jobs writer and, once the job is in a final
     * state, to the loads_history writer as well. With the job's write lock held the row is
     * taken when the lock is released; the conversion runs at most once, and only when a writer
     * is active on this node.
     */
    protected void recordStateChange() {
        if (lock.isWriteLockedByCurrentThread()) {
            statePending = true;
        } else {
            offerCurrentRow();
        }
    }

    // Rows of one job are taken and queued one at a time, so that a row taken earlier cannot be
    // queued after a later one and replace it.
    private void offerCurrentRow() {
        synchronized (rowOfferLock) {
            Supplier<TLoadInfo> row = Suppliers.memoize(this::toThrift);
            LoadJobsTable.offer(() -> keyedByFirstCreateTime(row.get()));
            if (isCompleted()) {
                LoadsHistoryWriter.offer(row);
            }
        }
    }

    /**
     * The row of this job in load_jobs. It differs from {@link #toThrift()} in the creation
     * time of a job that was retried: load_jobs is keyed by the creation time, so the row keeps
     * the time the job was first created, where the loads view shows the start of the attempt.
     */
    public TLoadInfo toLoadJobsRow() {
        return keyedByFirstCreateTime(toThrift());
    }

    private TLoadInfo keyedByFirstCreateTime(TLoadInfo row) {
        long first = firstCreateTimestamp;
        if (first <= 0 || !row.isSetCreate_time_ms() || row.getCreate_time_ms() == first) {
            return row;
        }
        TLoadInfo keyed = new TLoadInfo(row);
        keyed.setCreate_time_ms(first);
        return keyed;
    }

    /** Run {@code offer} as the only row offer of this job at a time, see {@link #offerCurrentRow()}. */
    protected void offerRows(Runnable offer) {
        synchronized (rowOfferLock) {
            offer.run();
        }
    }

    @Override
    public long getCurrentWarehouseId() {
        return warehouseId;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    @Override
    public boolean isFinal() {
        return isCompleted();
    }

    @Override
    public long getFinishTimestampMs() {
        return getFinishTimestamp();
    }

    public long getId() {
        return id;
    }

    // unit test
    public void setId(long id) {
        this.id = id;
    }

    protected void setLabel(String label) {
        this.label = label;
    }

    public Database getDb() throws MetaNotFoundException {
        // get db
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Database " + dbId + " already has been deleted");
        }
        return db;
    }

    @Override
    public long getDbId() {
        return dbId;
    }

    public String getLabel() {
        return label;
    }

    public JobState getState() {
        return state;
    }

    protected void setState(JobState state) {
        this.state = state;
    }

    public JobState getAcutalState() {
        if (state == JobState.LOADING) {
            if (startLoad) {
                return JobState.LOADING;
            } else {
                return JobState.QUEUEING;
            }
        } else {
            return state;
        }
    }

    public EtlJobType getJobType() {
        return jobType;
    }

    protected long getDeadlineMs() {
        return createTimestamp + timeoutSecond * 1000;
    }

    protected void reset() {
        if (firstCreateTimestamp <= 0) {
            firstCreateTimestamp = createTimestamp;
        }
        createTimestamp = System.currentTimeMillis();
        idToTasks.clear();
        finishedTaskIds.clear();
        // the next attempt runs under load ids of its own
        loadIds.clear();
        loadingStatus.reset();
    }

    protected void captureCommitThrottle(TransactionState txnState) {
        if (txnState != null && txnState.getCommitThrottleMs() >= 0) {
            commitThrottleMs = txnState.getCommitThrottleMs();
        }
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > getDeadlineMs();
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Whether {@link #beginTxn()} has run and registered a transaction with the
     * Global Transaction Manager. Subclasses that defer {@code beginTxn} (e.g.
     * {@link com.starrocks.load.loadv2.BrokerLoadJob}, where the pre-split hook
     * fires first) can fail before {@code beginTxn} runs; abort paths must skip
     * the GTM call in that window because there is no transaction to abort.
     */
    public boolean hasBegunTransaction() {
        return transactionId != 0L;
    }

    public long getLoadStartTimestamp() {
        return loadStartTimestamp;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getStateName() {
        if (state == JobState.COMMITTED) {
            return "PREPARED";
        }
        if (state == JobState.LOADING) {
            return null;
        }
        return state.name();
    }

    @Override
    public boolean matchTableName(String tableName) {
        try {
            return getTableNames(true).contains(tableName);
        } catch (MetaNotFoundException e) {
            return false;
        }
    }

    @Override
    public Long getCreateTimeMs() {
        return createTimestamp;
    }

    @Override
    public Long getLoadStartTimeMs() {
        return loadStartTimestamp;
    }

    @Override
    public Long getLoadFinishTimeMs() {
        return finishTimestamp;
    }

    public void initLoadProgress(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
        loadingStatus.getLoadStatistic().initLoad(loadId, fragmentIds, relatedBackendIds);
        boolean started = !startLoad;
        startLoad = true;
        loadStartTimestamp = System.currentTimeMillis();
        if (started) {
            // The shown state turns from QUEUEING to LOADING here.
            recordStateChange();
        }
    }

    public void updateProgress(TReportExecStatusParams params) {
        loadingStatus.getLoadStatistic().updateLoadProgress(params);
    }

    public void setLoadFileInfo(int fileNum, long fileSize) {
        loadingStatus.setLoadFileInfo(fileNum, fileSize);
    }

    public void updateScanRangeNum(long scanRangeNum) {
        loadingStatus.updateScanRangeNum(scanRangeNum);
    }

    public TUniqueId getRequestId() {
        return requestId;
    }

    /**
     * Show table names for frontend
     * If table name could not be found by id, the table id will be used instead.
     *
     * @return
     */
    abstract Set<String> getTableNamesForShow();

    /**
     * Return the real table names by table ids.
     * The method is invoked by 'checkAuth' when authorization info is null in job.
     * It is also invoked by 'gatherAuthInfo' which saves the auth info in the constructor of job.
     * Throw MetaNofFoundException when table name could not be found.
     *
     * @return
     */
    public abstract Set<String> getTableNames(boolean noThrow) throws MetaNotFoundException;

    /**
     * Ids of the tables this job loads into, whether or not they still exist.
     */
    public abstract Set<Long> getTableIds();

    // Which DML statement produced the job (INSERT jobs only), otherwise null.
    protected String getStatementKind() {
        return null;
    }

    // Address of the client that submitted the job, otherwise null.
    protected String getClientIp() {
        return null;
    }

    protected abstract List<TabletCommitInfo> getTabletCommitInfos();

    protected abstract List<TabletFailInfo> getTabletFailInfos();

    // return true if the corresponding transaction is done(COMMITTED, FINISHED, CANCELLED)
    public boolean isTxnDone() {
        return state == JobState.COMMITTED || state == JobState.FINISHED || state == JobState.CANCELLED;
    }

    // return true if job is done(FINISHED/CANCELLED/UNKNOWN)
    public boolean isCompleted() {
        return state == JobState.FINISHED || state == JobState.CANCELLED || state == JobState.UNKNOWN;
    }

    public void setJobProperties(Map<String, String> properties) throws DdlException {
        // resource info
        if (ConnectContext.get() != null) {
            loadMemLimit = ConnectContext.get().getSessionVariable().getLoadMemLimit();
        }

        if (ConnectContext.get() != null) {
            user = ConnectContext.get().getQualifiedUser();
        }

        // job properties
        if (properties != null) {
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    timeoutSecond = Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Timeout is not INT", e);
                }
            }

            if (properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
                try {
                    maxFilterRatio = Double.parseDouble(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Max filter ratio is not DOUBLE", e);
                }
            }

            if (properties.containsKey(LoadStmt.LOAD_DELETE_FLAG_PROPERTY)) {
                throw new DdlException("delete flag is not supported");
            }
            if (properties.containsKey(LoadStmt.PARTIAL_UPDATE)) {
                partialUpdate = Boolean.valueOf(properties.get(LoadStmt.PARTIAL_UPDATE));
            }
            if (properties.containsKey(LoadStmt.PARTIAL_UPDATE_MODE)) {
                partialUpdateMode = properties.get(LoadStmt.PARTIAL_UPDATE_MODE);
            }
            if (properties.containsKey(LoadStmt.MERGE_CONDITION)) {
                mergeCondition = properties.get(LoadStmt.MERGE_CONDITION);
            }

            if (properties.containsKey(LoadStmt.LOAD_MEM_LIMIT)) {
                try {
                    loadMemLimit = Long.parseLong(properties.get(LoadStmt.LOAD_MEM_LIMIT));
                } catch (NumberFormatException e) {
                    throw new DdlException("Execute memory limit is not Long", e);
                }
            }

            if (properties.containsKey(LoadStmt.STRICT_MODE)) {
                strictMode = Boolean.valueOf(properties.get(LoadStmt.STRICT_MODE));
            }

            if (properties.containsKey(LoadStmt.TIMEZONE)) {
                timezone = properties.get(LoadStmt.TIMEZONE);
            } else if (ConnectContext.get() != null) {
                // get timezone for session variable
                timezone = ConnectContext.get().getSessionVariable().getTimeZone();
            }

            if (properties.containsKey(LoadStmt.PRIORITY)) {
                priority = LoadPriority.priorityByName(properties.get(LoadStmt.PRIORITY));
            }

            if (properties.containsKey(LoadStmt.LOG_REJECTED_RECORD_NUM)) {
                logRejectedRecordNum = Long.parseLong(properties.get(LoadStmt.LOG_REJECTED_RECORD_NUM));
            }

            if (RunMode.isSharedDataMode()) {
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
                    String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
                    Warehouse warehouse =
                            GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
                    warehouseId = warehouse.getId();
                } else {
                    warehouseId = ConnectContext.get().getCurrentWarehouseId();
                }
            }

            if (properties.containsKey(LoadStmt.STRIP_OUTER_ARRAY)) {
                jsonOptions.stripOuterArray = Boolean.parseBoolean(properties.get(LoadStmt.STRIP_OUTER_ARRAY));
            }

            if (properties.containsKey(LoadStmt.JSONPATHS)) {
                jsonOptions.jsonPaths = properties.get(LoadStmt.JSONPATHS);
            }

            if (properties.containsKey(LoadStmt.JSONROOT)) {
                jsonOptions.jsonRoot = properties.get(LoadStmt.JSONROOT);
            }

            if (properties.containsKey(LoadStmt.ENVELOPE)) {
                String envelope = properties.get(LoadStmt.ENVELOPE);
                if (!envelope.equalsIgnoreCase(LoadStmt.ENVELOPE_DEBEZIUM)) {
                    throw new DdlException("Unknown envelope type: " + envelope);
                }
                if (!Strings.isNullOrEmpty(jsonOptions.jsonRoot)) {
                    throw new DdlException(LoadStmt.JSONROOT + " cannot be specified when envelope is set");
                }
                if (jsonOptions.stripOuterArray) {
                    throw new DdlException(LoadStmt.STRIP_OUTER_ARRAY + " cannot be specified when envelope is set");
                }
                jsonOptions.envelope = envelope;
            }
        } else {
            if (RunMode.isSharedDataMode()) {
                // if no properties set, we should still set warehouse here
                warehouseId = ConnectContext.get().getCurrentWarehouseId();
            }
        }
    }

    public void isJobTypeRead(boolean jobTypeRead) {
        isJobTypeRead = jobTypeRead;
    }

    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException,
            RunningTxnExceedException, AnalysisException, DuplicatedRequestException {
    }

    /**
     * create pending task for load job and add pending task into pool
     * if job has been cancelled, this step will be ignored
     *
     * @throws LabelAlreadyUsedException  the job is duplicated
     * @throws RunningTxnExceedException  the limit of load job is exceeded
     * @throws AnalysisException          there are error params in job
     * @throws DuplicatedRequestException
     */
    public void execute() throws LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException,
            DuplicatedRequestException, LoadException {
        writeLock();
        try {
            unprotectedExecute();
        } finally {
            writeUnlock();
        }
    }

    public void unprotectedExecute() throws LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException,
            DuplicatedRequestException, LoadException {
        // check if job state is pending
        if (state != JobState.PENDING) {
            return;
        }
        try {
            // the limit of job will be restrict when begin txn
            beginTxn();
            unprotectedExecuteJob();
            // update spark load job state from PENDING to ETL when pending task is finished
            if (jobType != EtlJobType.SPARK) {
                unprotectedUpdateState(JobState.LOADING);
            }
        } catch (BeginTransactionException e) {
            throw new LoadException(e.getMessage());
        }
    }

    protected void submitTask(LeaderTaskExecutor executor, LoadTask task) throws LoadException {
        int retryNum = 0;
        while (!executor.submit(task)) {
            LOG.warn("submit load task failed. try to resubmit. job id: {}, task id: {}, retry: {}",
                    id, task.getSignature(), retryNum);
            if (++retryNum > TASK_SUBMIT_RETRY_NUM) {
                throw new LoadException("submit load task failed");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn("Failed to execute submitTask", e);
            }
        }
    }

    protected void submitTask(PriorityLeaderTaskExecutor executor, LoadTask task) throws LoadException {
        int retryNum = 0;
        while (!executor.submit(task)) {
            LOG.warn("submit load task failed. try to resubmit. job id: {}, task id: {}, retry: {}",
                    id, task.getSignature(), retryNum);
            if (++retryNum > TASK_SUBMIT_RETRY_NUM) {
                throw new LoadException("submit load task failed");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn("Failed to execute sleep", e);
            }
        }
    }

    public void processTimeout() {
        // this is only for jobs which transaction is not started.
        // if transaction is started, global transaction manager will handle the timeout.
        writeLock();
        try {
            if (state != JobState.PENDING) {
                return;
            }

            if (!isTimeout()) {
                return;
            }

            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.TIMEOUT, "loading timeout to cancel"),
                    false, true);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectedExecuteJob() throws LoadException {
    }

    /**
     * This method only support update state to finished and loading.
     * It will not be persisted when desired state is finished because txn visible will edit the log.
     * If you want to update state to cancelled, please use the cancelJob function.
     *
     * @param jobState
     */
    public void updateState(JobState jobState) {
        writeLock();
        try {
            unprotectedUpdateState(jobState);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectedUpdateState(JobState jobState) {
        switch (jobState) {
            case UNKNOWN:
                executeUnknown();
                break;
            case LOADING:
                executeLoad();
                break;
            case COMMITTED:
                executeCommitted();
                break;
            case FINISHED:
                executeFinish();
                break;
            default:
                break;
        }
        recordStateChange();
    }

    private void executeUnknown() {
        // set finished timestamp to create timestamp, so that this unknown job
        // can be remove due to label expiration so soon as possible
        finishTimestamp = createTimestamp;
        state = JobState.UNKNOWN;
    }

    private void executeLoad() {
        state = JobState.LOADING;
    }

    private void executeCommitted() {
        state = JobState.COMMITTED;
    }

    // if needLog is false, no need to write edit log.
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn) {
        writeLock();
        try {
            unprotectedExecuteCancel(failMsg, abortTxn, true);
        } finally {
            writeUnlock();
        }
    }

    public void cancelJob(FailMsg failMsg) throws DdlException {
        writeLock();
        try {
            // mini load can not be cancelled by frontend
            if (jobType == EtlJobType.MINI) {
                throw new DdlException("Job could not be cancelled in type " + jobType.name());
            }
            if (isCommitting) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("error_msg", "The txn which belongs to job is committing. "
                                + "The job could not be cancelled in this step").build());
                throw new DdlException("Job could not be cancelled while txn is committing");
            }
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "Job could not be cancelled when job is " + state)
                        .build());
                throw new DdlException("Job could not be cancelled when job is finished or cancelled");
            }

            unprotectedExecuteCancel(failMsg, true, true);
        } finally {
            writeUnlock();
        }
    }

    public void alterJob(AlterLoadStmt stmt) throws DdlException {
    }

    public void replayAlterJob(AlterLoadJobOperationLog log) {
    }

    /**
     * This method will cancel job without edit log and lock
     *
     * @param failMsg
     * @param abortTxn true: abort txn when cancel job, false: only change the state of job and ignore abort txn
     */
    protected void unprotectedExecuteCancel(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id).add("transaction_id", transactionId)
                .add("error_msg", "Failed to execute load with error: " + failMsg.getMsg()).build());

        // clean the loadingStatus
        loadingStatus.setState(TEtlState.CANCELLED);

        // get load ids of all loading tasks, we will cancel their coordinator process later
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (PriorityLeaderTask loadTask : idToTasks.values()) {
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }
        idToTasks.clear();

        // set failMsg and state
        this.failMsg = failMsg;
        if (failMsg.getCancelType() == CancelType.TXN_UNKNOWN) {
            // for bug fix, see LoadManager's fixLoadJobMetaBugs() method
            finishTimestamp = createTimestamp;
        } else {
            finishTimestamp = System.currentTimeMillis();
        }

        // remove callback before abortTransaction(), so that the afterAborted() callback will not be called again
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);

        if (abortTxn && hasBegunTransaction()) {
            // abort txn. !hasBegunTransaction() means the job was cancelled before
            // beginTxn ran (e.g. BrokerLoadJob defers beginTxn until after the
            // pre-split hook returns); there is no GTM record to abort.
            try {
                LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("transaction_id", transactionId)
                        .add("msg", "begin to abort txn")
                        .build());
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .abortTransaction(dbId, transactionId, failMsg.getMsg(),
                                getTabletCommitInfos(), getTabletFailInfos(), null);
            } catch (StarRocksException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("transaction_id", transactionId)
                        .add("error_msg", "failed to abort txn when job is cancelled. " + e.getMessage())
                        .build());
            }
        }

        // cancel all running coordinators, so that the scheduler's worker thread will be released
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel(failMsg.getMsg());
            }
        }

        // change state
        if (needLog) {
            LoadJobFinalOperation operation = new LoadJobFinalOperation(id, loadingStatus,
                    progress, loadStartTimestamp, finishTimestamp, JobState.CANCELLED, failMsg);
            GlobalStateMgr.getCurrentState().getEditLog().logEndLoadJob(
                    operation, wal -> this.state = JobState.CANCELLED);
        } else {
            state = JobState.CANCELLED;
        }
        recordStateChange();
    }

    private void executeFinish() {
        progress = 100;
        finishTimestamp = System.currentTimeMillis();
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        state = JobState.FINISHED;
        failMsg = null;

        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        }
        // when load job finished, there is no need to hold the tasks which are the biggest memory consumers.
        idToTasks.clear();
    }

    public boolean checkDataQuality() {
        Map<String, String> counters = loadingStatus.getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }

        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * maxFilterRatio) {
            return false;
        }

        return true;
    }

    public void unprotectReadEndOperation(LoadJobFinalOperation loadJobFinalOperation, boolean isReplay) {
        loadingStatus = loadJobFinalOperation.getLoadingStatus();
        progress = loadJobFinalOperation.getProgress();
        loadStartTimestamp = loadJobFinalOperation.getLoadStartTimestamp();
        finishTimestamp = loadJobFinalOperation.getFinishTimestamp();
        if (isReplay) {
            state = loadJobFinalOperation.getJobState();
        }
        failMsg = loadJobFinalOperation.getFailMsg();
    }

    public void unprotectUpdateLoadingStatus(TransactionState txnState) {
        if (txnState.getTxnCommitAttachment() == null) {
            // The txn attachment maybe null when broker load has been cancelled without attachment.
            // The end log of broker load has been record but the callback id of txnState hasn't been removed
            // So the callback of txn is executed when log of txn aborted is replayed.
            return;
        }
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment(), false);
    }

    /**
     * This method will update job failMsg without edit log and lock
     *
     * @param failMsg
     */
    public void unprotectUpdateFailMsg(FailMsg failMsg) {
        this.failMsg = failMsg;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }

    public List<Comparable> getShowInfo() throws DdlException {
        readLock();
        try {
            List<Comparable> jobInfo = Lists.newArrayList();
            // jobId
            jobInfo.add(id);
            // label
            jobInfo.add(label);
            // state
            if (state == JobState.COMMITTED) {
                jobInfo.add("PREPARED");
            } else if (state == JobState.LOADING && !startLoad) {
                jobInfo.add("QUEUEING");
            } else {
                jobInfo.add(state.name());
            }
            // progress
            switch (state) {
                case PENDING:
                    jobInfo.add("ETL:0%; LOAD:0%");
                    break;
                case CANCELLED:
                    jobInfo.add("ETL:N/A; LOAD:N/A");
                    break;
                case ETL:
                    jobInfo.add("ETL:" + progress + "%; LOAD:0%");
                    break;
                default:
                    jobInfo.add("ETL:100%; LOAD:" + progress + "%");
                    break;
            }

            // type
            jobInfo.add(jobType);
            // priority
            jobInfo.add(LoadPriority.priorityToName(priority));

            jobInfo.add(loadingStatus.getLoadStatistic().totalSourceLoadRows());
            jobInfo.add(loadingStatus.getLoadStatistic().totalFilteredRows());
            jobInfo.add(loadingStatus.getLoadStatistic().totalUnselectedRows());
            jobInfo.add(loadingStatus.getLoadStatistic().totalSinkLoadRows());

            // etl info
            if (jobType != EtlJobType.SPARK || loadingStatus.getCounters().size() == 0) {
                jobInfo.add(FeConstants.NULL_STRING);
            } else {
                jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
            }

            // task info
            jobInfo.add("resource:" + getResourceName() + "; timeout(s):" + timeoutSecond
                    + "; max_filter_ratio:" + maxFilterRatio);

            // error msg
            if (failMsg == null) {
                jobInfo.add(FeConstants.NULL_STRING);
            } else {
                jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
            }

            // create time
            jobInfo.add(TimeUtils.longToTimeString(createTimestamp));
            // etl start time
            jobInfo.add(TimeUtils.longToTimeString(getEtlStartTimestamp()));
            // etl end time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load start time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load end time
            jobInfo.add(TimeUtils.longToTimeString(finishTimestamp));
            // tracking sql
            if (!loadingStatus.getTrackingUrl().equals(EtlStatus.DEFAULT_TRACKING_URL)) {
                jobInfo.add("select tracking_log from information_schema.load_tracking_logs where job_id=" + id);
            } else {
                jobInfo.add("");
            }
            jobInfo.add(loadingStatus.getLoadStatistic().toShowInfoStr());
            // warehouse
            if (RunMode.isSharedDataMode()) {
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
                if (warehouse != null) {
                    jobInfo.add(warehouse.getName());
                } else {
                    jobInfo.add(String.format("Warehouse id: %d not exist.", warehouseId));
                }
            } else {
                jobInfo.add("");
            }
            return jobInfo;
        } finally {
            readUnlock();
        }
    }

    public String toRuntimeDetails() {
        TreeMap<String, Object> runtimeDetails = Maps.newTreeMap();
        if (jobType == EtlJobType.SPARK) {
            if (loadingStatus.getCounters().size() != 0) {
                runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_ETL_INFO,
                        Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
            }
            if (getEtlStartTimestamp() != -1) {
                runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_ETL_START_TIME,
                        TimeUtils.longToTimeString(getEtlStartTimestamp()));
            }
            if (loadStartTimestamp != -1) {
                // etl end time
                runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_ETL_FINISH_TIME,
                        TimeUtils.longToTimeString(loadStartTimestamp));
            }
        }
        runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_LOAD_ID, Joiner.on(", ").join(loadIds));
        runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_TXN_ID, transactionId);
        TransactionState txnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getTransactionState(dbId, transactionId);
        if (txnState != null) {
            // NOTE: Do NOT acquire txnState write lock here to avoid lock inversion with
            // DatabaseTransactionMgr.finishTransaction() where txnState write lock is held
            // and LoadJob lock is requested in callbacks. Reading errMsg without locking is acceptable
            // for diagnostics, even if slightly stale.
            runtimeDetails.put(LoadConstants.RUNTIME_DETAILS_TXN_ERROR_MSG, txnState.getErrMsg());
        }
        runtimeDetails.putAll(loadingStatus.getLoadStatistic().toRuntimeDetails());
        Gson gson = new Gson();
        return gson.toJson(runtimeDetails);
    }

    public String toProperties() {
        TreeMap<String, Object> properties = Maps.newTreeMap();
        properties.put(LoadConstants.PROPERTIES_TIMEOUT, timeoutSecond);
        properties.put(LoadConstants.PROPERTIES_MAX_FILTER_RATIO, maxFilterRatio);
        Gson gson = new Gson();
        return gson.toJson(properties);
    }

    /**
     * Join values for a varchar(2048) column of information_schema.loads and its archive,
     * keeping only as many leading values as fit so that no value is cut in the middle.
     */
    public static String joinForLoadsColumn(String separator, Iterable<?> values) {
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            String text = String.valueOf(value);
            int length = sb.length() + (sb.length() == 0 ? 0 : separator.length()) + text.length();
            if (length > LOADS_VARCHAR_COLUMN_LENGTH) {
                break;
            }
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(text);
        }
        return sb.toString();
    }

    public TLoadInfo toThrift() {
        // Looked up before taking the job lock. On a follower, replaying a transaction holds the
        // transaction manager lock while it takes the job lock, so the columns added here do not
        // enter the transaction manager with the job lock held.
        long lookedUpTxnId = transactionId;
        TransactionState txnState = GlobalStateMgr.getCurrentState()
                .getGlobalTransactionMgr().getTransactionState(dbId, lookedUpTxnId);
        readLock();
        try {
            if (lookedUpTxnId != transactionId) {
                // the job moved on to another transaction in between
                txnState = null;
            }
            TLoadInfo info = new TLoadInfo();
            info.setJob_id(id);
            info.setLabel(label);
            try {
                info.setDb(getDb().getFullName());
            } catch (MetaNotFoundException e) {
                info.setDb("");
            }
            if (transactionId > 0) {
                info.setTxn_id(transactionId);
            }
            if (!loadIds.isEmpty()) {
                info.setLoad_id(joinForLoadsColumn(", ", loadIds));

                List<String> profileIds = Lists.newArrayList();
                for (String loadId : loadIds) {
                    if (ProfileManager.getInstance().hasProfile(loadId)) {
                        profileIds.add(loadId);
                    }
                }
                if (!profileIds.isEmpty()) {
                    info.setProfile_id(joinForLoadsColumn(", ", loadIds));
                }
            }
            try {
                info.setTable(getTableNames(true).stream().collect(Collectors.joining(",")));
            } catch (MetaNotFoundException e) {
                info.setTable("");
            }
            info.setUser(user);

            if (state == JobState.COMMITTED) {
                info.setState("PREPARED");
            } else if (state == JobState.LOADING && !startLoad) {
                info.setState("QUEUEING");
            } else {
                info.setState(state.name());
            }
            // progress
            switch (state) {
                case PENDING:
                    info.setProgress("ETL:0%; LOAD:0%");
                    break;
                case CANCELLED:
                    info.setProgress("ETL:N/A; LOAD:N/A");
                    break;
                case ETL:
                    info.setProgress("ETL:" + progress + "%; LOAD:0%");
                    break;
                default:
                    info.setProgress("ETL:100%; LOAD:" + progress + "%");
                    break;
            }

            // type
            info.setType(jobType.name());
            // priority
            info.setPriority(LoadPriority.priorityToName(priority));

            // etl info
            if (jobType != EtlJobType.SPARK || loadingStatus.getCounters().size() == 0) {
                info.setEtl_info("");
            } else {
                info.setEtl_info(Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
            }

            // task info
            info.setRuntime_details(toRuntimeDetails());

            // error msg
            if (failMsg != null) {
                info.setError_msg("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
            }

            // create time. Set both the legacy wall-clock string and the new
            // UTC epoch ms. New BE materializes the DATETIME column from the
            // ms field (via from_unixtime() in the session zone), bypassing
            // the from_date_str() round-trip that loses timezone information.
            if (createTimestamp != -1) {
                info.setCreate_time(TimeUtils.longToTimeString(createTimestamp));
                info.setCreate_time_ms(createTimestamp);
            }
            // etl start time
            if (getEtlStartTimestamp() != -1) {
                info.setEtl_start_time(TimeUtils.longToTimeString(getEtlStartTimestamp()));
            }
            if (loadStartTimestamp != -1) {
                // etl end time
                info.setEtl_finish_time(TimeUtils.longToTimeString(loadStartTimestamp));
                // load start time
                info.setLoad_start_time(TimeUtils.longToTimeString(loadStartTimestamp));
                info.setLoad_start_time_ms(loadStartTimestamp);
            }
            if (loadCommittedTimestamp != -1) {
                info.setLoad_commit_time(TimeUtils.longToTimeString(loadCommittedTimestamp));
                info.setLoad_commit_time_ms(loadCommittedTimestamp);
            }
            // load end time
            if (finishTimestamp != -1) {
                info.setLoad_finish_time(TimeUtils.longToTimeString(finishTimestamp));
                info.setLoad_finish_time_ms(finishTimestamp);
            }
            // tracking url
            if (!loadingStatus.getTrackingUrl().equals(EtlStatus.DEFAULT_TRACKING_URL)) {
                info.setUrl(loadingStatus.getTrackingUrl());
                info.setTracking_sql("select tracking_log from information_schema.load_tracking_logs where job_id=" + id);
            }
            if (!loadingStatus.getRejectedRecordPaths().isEmpty()) {
                info.setRejected_record_path(Joiner.on(", ").join(loadingStatus.getRejectedRecordPaths()));
            }
            info.setProperties(toProperties());
            info.setNum_filtered_rows(loadingStatus.getLoadStatistic().totalFilteredRows());
            info.setNum_unselected_rows(loadingStatus.getLoadStatistic().totalUnselectedRows());
            info.setNum_scan_rows(loadingStatus.getLoadStatistic().totalSourceLoadRows());
            info.setNum_sink_rows(loadingStatus.getLoadStatistic().totalSinkLoadRows());
            info.setNum_scan_bytes(loadingStatus.getLoadStatistic().sourceScanBytes());
            // warehouse
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
                if (warehouse != null) {
                    info.setWarehouse(warehouse.getName());
                } else {
                    info.setWarehouse(String.format("Warehouse id: %d not exist.", warehouseId));
                }
            } else {
                info.setWarehouse("");
            }

            Set<Long> tableIds = getTableIds();
            if (tableIds.size() == 1) {
                info.setTable_id(tableIds.iterator().next());
            }
            String statementKind = getStatementKind();
            if (!Strings.isNullOrEmpty(statementKind)) {
                info.setStatement(statementKind);
            }
            String clientIp = getClientIp();
            if (!Strings.isNullOrEmpty(clientIp)) {
                info.setClient_ip(clientIp);
            }
            if (visibleTimestamp > 0) {
                info.setVisible_time_ms(visibleTimestamp);
            } else if (state == JobState.FINISHED && txnState != null
                    && txnState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                // Jobs that finished before visibleTimestamp existed: trust the transaction, not the
                // job, because an INSERT is marked FINISHED even when its publish wait timed out.
                long visibleMs = txnState.getFinishTime() > 0 ? txnState.getFinishTime() : finishTimestamp;
                if (visibleMs > 0) {
                    info.setVisible_time_ms(visibleMs);
                }
            }
            info.setSink_bytes(loadingStatus.getLoadStatistic().totalSinkLoadBytes());
            Set<Long> backendIds = loadingStatus.getLoadStatistic().distinctBackendIds();
            if (!backendIds.isEmpty()) {
                info.setBe_ids(joinForLoadsColumn(",", backendIds));
            }
            if (commitThrottleMs >= 0) {
                info.setCommit_throttle_ms(commitThrottleMs);
            } else if (txnState != null && txnState.getCommitThrottleMs() >= 0) {
                info.setCommit_throttle_ms(txnState.getCommitThrottleMs());
            }
            return info;
        } finally {
            readUnlock();
        }
    }

    public String getResourceName() {
        return "N/A";
    }

    protected long getEtlStartTimestamp() {
        return loadStartTimestamp;
    }

    public void getJobInfo(Load.JobInfo jobInfo) {
        jobInfo.tblNames.addAll(getTableNamesForShow());
        jobInfo.state = com.starrocks.load.loadv2.JobState.valueOf(state.name());
        if (failMsg != null) {
            jobInfo.failMsg = failMsg.getMsg();
        } else {
            jobInfo.failMsg = "";
        }
        jobInfo.trackingUrl = loadingStatus.getTrackingUrl();
    }

    @Override
    public long getCallbackId() {
        return id;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            if (isTxnDone()) {
                throw new TransactionException("txn could not be committed because job is: " + state);
            }
            isCommitting = true;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState) throws StarRocksException {
        writeLock();
        try {
            unprotectUpdateLoadingStatus(txnState);
            captureCommitThrottle(txnState);
            isCommitting = false;
            state = JobState.COMMITTED;
            loadCommittedTimestamp = System.currentTimeMillis();
            recordStateChange();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            progress = 99;
            transactionId = txnState.getTransactionId();
            state = JobState.COMMITTED;
            failMsg = null;
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will cancel job without edit log.
     * The job will be cancelled by replayOnAborted when journal replay
     *
     * @param txnState
     * @param txnStatusChangeReason
     */
    @Override
    public void afterAborted(TransactionState txnState, String txnStatusChangeReason) {
        writeLock();
        try {
            if (isTxnDone()) {
                return;
            }
            // record attachment in load job
            unprotectUpdateLoadingStatus(txnState);
            // cancel load job
            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnStatusChangeReason),
                    false, false);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method is used to replay the cancelled state of load job
     *
     * @param txnState
     */
    @Override
    public void replayOnAborted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnState.getReason());
            finishTimestamp = txnState.getFinishTime();
            state = JobState.CANCELLED;
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will finish the load job without edit log.
     * The job will be finished by replayOnVisible when txn journal replay
     *
     * @param txnState
     */
    @Override
    public void afterVisible(TransactionState txnState) {
        GlobalStateMgr.getCurrentState().getOperationListenerBus().onLoadJobTransactionFinish(txnState);
        unprotectUpdateLoadingStatus(txnState);
        writeLock();
        try {
            visibleTimestamp = txnState.getFinishTime() > 0 ? txnState.getFinishTime() : System.currentTimeMillis();
            captureCommitThrottle(txnState);
        } finally {
            writeUnlock();
        }
        updateState(JobState.FINISHED);
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            progress = 100;
            finishTimestamp = txnState.getFinishTime();
            visibleTimestamp = txnState.getFinishTime();
            state = JobState.FINISHED;
            failMsg = null;
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    protected void replayTxnAttachment(TransactionState txnState) {
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg, TaskAttachment attachment) {
    }

    // This analyze will be invoked after the replay is finished.
    // The edit log of LoadJob saves the origin param which is not analyzed.
    // So, the re-analyze must be invoked between the replay is finished and LoadJobScheduler is started.
    // Only, the PENDING load job need to be analyzed.
    public void analyze() {
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LoadJob other = (LoadJob) obj;

        return this.id == other.id
                && this.dbId == other.dbId
                && this.label.equals(other.label)
                && this.state.equals(other.state)
                && this.jobType.equals(other.jobType);
    }

    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        state = info.getState();
        transactionId = info.getTransactionId();
        loadStartTimestamp = info.getLoadStartTimestamp();
    }

    public boolean hasTxn() {
        return true;
    }

    public static class LoadJobStateUpdateInfo implements Writable {
        @SerializedName(value = "jobId")
        private long jobId;
        @SerializedName(value = "state")
        private JobState state;
        @SerializedName(value = "transactionId")
        private long transactionId;
        @SerializedName(value = "loadStartTimestamp")
        private long loadStartTimestamp;

        public LoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, long loadStartTimestamp) {
            this.jobId = jobId;
            this.state = state;
            this.transactionId = transactionId;
            this.loadStartTimestamp = loadStartTimestamp;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public long getLoadStartTimestamp() {
            return loadStartTimestamp;
        }

        @Override
        public String toString() {
            return GsonUtils.GSON.toJson(this);
        }




    }

    public static class JSONOptions {
        @SerializedName("s")
        public boolean stripOuterArray;

        @SerializedName("jp")
        public String jsonPaths;

        @SerializedName("jr")
        public String jsonRoot;

        @SerializedName("env")
        public String envelope;
    }
}
