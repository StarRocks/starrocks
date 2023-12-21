// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadJob.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RowDelimiter;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Routine load job is a function which stream load data from streaming medium to starrocks.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA and Pulsar
 */
public abstract class RoutineLoadJob extends AbstractTxnStateChangeCallback implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    public static final long DEFAULT_MAX_ERROR_NUM = 0;
    public static final long DEFAULT_MAX_BATCH_ROWS = 200000;

    public static final long DEFAULT_TASK_SCHED_INTERVAL_SECOND = 10;
    public static final boolean DEFAULT_STRICT_MODE = false; // default is false

    protected static final String STAR_STRING = "*";

    /*
                     +-----------------+
    fe schedule job  |  NEED_SCHEDULE  |  user resume job
         +-----------+                 | <---------+
         |           |                 |           |
         v           +-----------------+           ^
         |                                         |
    +------------+   user(system)pause job +-------+----+
    |  RUNNING   |                         |  PAUSED    |
    |            +-----------------------> |            |
    +----+-------+                         +-------+----+
    |    |                                         |
    |    |           +---------------+             |
    |    |           | STOPPED       |             |
    |    +---------> |               | <-----------+
    |   user stop job+---------------+    user stop job
    |
    |
    |               +---------------+
    |               | CANCELLED     |
    +-------------> |               |
    system error    +---------------+
    */
    public enum JobState {
        NEED_SCHEDULE,
        RUNNING,
        PAUSED,
        STOPPED,
        CANCELLED;

        public boolean isFinalState() {
            return this == STOPPED || this == CANCELLED;
        }
    }

    public enum JobSubstate {
        STABLE,
        UNSTABLE
    }

    protected long id;
    protected String name;
    protected long dbId;
    protected long tableId;
    // this code is used to verify be task request
    protected long authCode;
    //    protected RoutineLoadDesc routineLoadDesc; // optional
    protected PartitionNames partitions; // optional
    protected List<ImportColumnDesc> columnDescs; // optional
    protected Expr whereExpr; // optional
    protected ColumnSeparator columnSeparator; // optional
    protected RowDelimiter rowDelimiter; // optional
    protected int desireTaskConcurrentNum; // optional
    protected JobState state = JobState.NEED_SCHEDULE;
    protected JobSubstate substate = JobSubstate.STABLE;
    protected LoadDataSourceType dataSourceType;
    protected double maxFilterRatio = 1;
    // max number of error data in max batch rows * 10
    // maxErrorNum / (maxBatchRows * 10) = max error rate of routine load job
    // if current error rate is more than max error rate, the job will be paused
    protected long maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
    // include strict mode
    protected Map<String, String> jobProperties = Maps.newHashMap();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    /**
     * task submitted to be every taskSchedIntervalS,
     * and consume data for Config.routine_load_consume_second or consumed data size exceeds Config.max_routine_load_batch_size
     * maxBatchRows only used to judge error data and dose not limit the batch data
     */
    protected long taskSchedIntervalS = DEFAULT_TASK_SCHED_INTERVAL_SECOND;
    protected long maxBatchRows = DEFAULT_MAX_BATCH_ROWS;

    // deprecated
    // set to Config.max_routine_load_batch_size, so that if fe rolls back to old version, routine load can still work
    protected long maxBatchSizeBytes = Config.max_routine_load_batch_size;

    /**
     * RoutineLoad support json data.
     * Require Params:
     * 1) format = "json"
     * 2) jsonPath = "$.XXX.xxx"
     */
    private static final String PROPS_FORMAT = "format";
    private static final String PROPS_STRIP_OUTER_ARRAY = "strip_outer_array";
    private static final String PROPS_JSONPATHS = "jsonpaths";
    private static final String PROPS_JSONROOT = "json_root";

    protected int currentTaskConcurrentNum;
    protected RoutineLoadProgress progress;

    protected RoutineLoadProgress timestampProgress;

    protected long firstResumeTimestamp; // the first resume time
    protected long autoResumeCount;
    protected boolean autoResumeLock = false; //it can't auto resume iff true
    // some other msg which need to show to user;
    protected String otherMsg = "";
    protected ErrorReason pauseReason;
    protected ErrorReason cancelReason;

    protected ErrorReason stateChangedReason;

    protected long createTimestamp = System.currentTimeMillis();
    protected long pauseTimestamp = -1;
    protected long endTimestamp = -1;

    /*
     * The following variables are for statistics
     * currentErrorRows/currentTotalRows: the row statistics of current sampling period
     * errorRows/totalRows/receivedBytes: cumulative measurement
     * totalTaskExcutorTimeMs: cumulative execution time of tasks
     */
    /*
     * Rows will be updated after txn state changed when txn state has been successfully changed.
     */
    protected long currentErrorRows = 0;
    protected long currentTotalRows = 0;
    protected long errorRows = 0;
    protected long totalRows = 0;
    protected long unselectedRows = 0;
    protected long receivedBytes = 0;
    protected long totalTaskExcutionTimeMs = 1; // init as 1 to avoid division by zero
    protected long committedTaskNum = 0;
    protected long abortedTaskNum = 0;

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();

    // this is the origin stmt of CreateRoutineLoadStmt, we use it to persist the RoutineLoadJob,
    // because we can not serialize the Expressions contained in job.
    protected OriginStatement origStmt;

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    // TODO(ml): error sample

    // save the latest 3 error log urls
    private Queue<String> errorLogUrls = EvictingQueue.create(3);

    protected boolean isTypeRead = false;

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public RoutineLoadJob(long id, LoadDataSourceType type) {
        this.id = id;
        this.dataSourceType = type;
    }

    public RoutineLoadJob(Long id, String name,
                          long dbId, long tableId, LoadDataSourceType dataSourceType) {
        this(id, dataSourceType);
        this.name = name;
        this.dbId = dbId;
        this.tableId = tableId;
        this.authCode = 0;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
            sessionVariables.put(SessionVariable.EXEC_MEM_LIMIT, Long.toString(var.getMaxExecMemByte()));
            sessionVariables.put(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE, var.getloadTransmissionCompressionType());
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
            sessionVariables.put(SessionVariable.EXEC_MEM_LIMIT, Long.toString(SessionVariable.DEFAULT_EXEC_MEM_LIMIT));
        }
    }

    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        if (stmt.getRoutineLoadDesc() != null) {
            setRoutineLoadDesc(stmt.getRoutineLoadDesc());
        }
        if (stmt.getDesiredConcurrentNum() != -1) {
            this.desireTaskConcurrentNum = stmt.getDesiredConcurrentNum();
        }
        if (stmt.getMaxErrorNum() != -1) {
            this.maxErrorNum = stmt.getMaxErrorNum();
        }
        this.maxFilterRatio = stmt.getMaxFilterRatio();
        if (stmt.getMaxBatchIntervalS() != -1) {
            this.taskSchedIntervalS = stmt.getMaxBatchIntervalS();
        }
        if (stmt.getMaxBatchRows() != -1) {
            this.maxBatchRows = stmt.getMaxBatchRows();
        }
        jobProperties.put(LoadStmt.PARTIAL_UPDATE, String.valueOf(stmt.isPartialUpdate()));
        jobProperties.put(LoadStmt.TIMEZONE, stmt.getTimezone());
        jobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(stmt.isStrictMode()));
        if (stmt.getMergeConditionStr() != null) {
            jobProperties.put(LoadStmt.MERGE_CONDITION, stmt.getMergeConditionStr());
        }
        if (Strings.isNullOrEmpty(stmt.getFormat()) || stmt.getFormat().equals("csv")) {
            jobProperties.put(PROPS_FORMAT, "csv");
            jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "false");
            jobProperties.put(PROPS_JSONPATHS, "");
            jobProperties.put(PROPS_JSONROOT, "");
        } else if (stmt.getFormat().equals("json")) {
            jobProperties.put(PROPS_FORMAT, "json");
            if (!Strings.isNullOrEmpty(stmt.getJsonPaths())) {
                jobProperties.put(PROPS_JSONPATHS, stmt.getJsonPaths());
            } else {
                jobProperties.put(PROPS_JSONPATHS, "");
            }
            if (!Strings.isNullOrEmpty(stmt.getJsonRoot())) {
                jobProperties.put(PROPS_JSONROOT, stmt.getJsonRoot());
            } else {
                jobProperties.put(PROPS_JSONROOT, "");
            }
            if (stmt.isStripOuterArray()) {
                jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "true");
            } else {
                jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "false");
            }
        } else {
            throw new UserException("Invalid format type.");
        }
    }

    private void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        if (routineLoadDesc == null) {
            return;
        }
        if (routineLoadDesc.getColumnsInfo() != null) {
            ImportColumnsStmt columnsStmt = routineLoadDesc.getColumnsInfo();
            if (columnsStmt.getColumns() != null || columnsStmt.getColumns().size() != 0) {
                columnDescs = Lists.newArrayList();
                columnDescs.addAll(columnsStmt.getColumns());
            }
        }
        if (routineLoadDesc.getWherePredicate() != null) {
            whereExpr = routineLoadDesc.getWherePredicate().getExpr();
        }
        if (routineLoadDesc.getColumnSeparator() != null) {
            columnSeparator = routineLoadDesc.getColumnSeparator();
        }
        if (routineLoadDesc.getRowDelimiter() != null) {
            rowDelimiter = routineLoadDesc.getRowDelimiter();
        }
        if (routineLoadDesc.getPartitionNames() != null) {
            partitions = routineLoadDesc.getPartitionNames();
        }
    }

    @Override
    public long getId() {
        return id;
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
        lock.writeLock().unlock();
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public void setOtherMsg(String otherMsg) {
        this.otherMsg = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                + ": " + Strings.nullToEmpty(otherMsg);
    }

    public String getDbFullName() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
<<<<<<< HEAD
        database.readLock();
        try {
            return database.getFullName();
        } finally {
            database.readUnlock();
        }
=======
        return db.getFullName();
>>>>>>> 7178177f83 ([Enhancement] remove lock for routineLoadJob getDbFullName (#37474))
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        Table table = database.getTable(tableId);
        if (table == null) {
            throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
        }
        return table.getName();
    }

    public JobState getState() {
        return state;
    }

    public long getAuthCode() {
        return authCode;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public long getPauseTimestamp() {
        return pauseTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public String getDataSourceTypeName() {
        return dataSourceType.name();
    }

    public String getPauseReason() {
        return pauseReason == null ? "" : pauseReason.toString();
    }

    public String getOtherMsg() {
        return otherMsg;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public List<ImportColumnDesc> getColumnDescs() {
        return columnDescs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public boolean isStrictMode() {
        String value = jobProperties.get(LoadStmt.STRICT_MODE);
        if (value == null) {
            return DEFAULT_STRICT_MODE;
        }
        return Boolean.valueOf(value);
    }

    public String getTimezone() {
        String value = jobProperties.get(LoadStmt.TIMEZONE);
        if (value == null) {
            return TimeUtils.DEFAULT_TIME_ZONE;
        }
        return value;
    }

    public boolean isPartialUpdate() {
        String value = jobProperties.get(LoadStmt.PARTIAL_UPDATE);
        if (value == null) {
            return false;
        }
        return Boolean.valueOf(value);
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public RoutineLoadProgress getTimestampProgress() {
        return timestampProgress;
    }

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getTaskSchedIntervalS() {
        return taskSchedIntervalS;
    }

    public String getFormat() {
        String value = jobProperties.get(PROPS_FORMAT);
        if (value == null) {
            return "csv";
        }
        return value;
    }

    public String getMergeCondition() {
        return jobProperties.get(LoadStmt.MERGE_CONDITION);
    }

    public boolean isStripOuterArray() {
        return Boolean.valueOf(jobProperties.get(PROPS_STRIP_OUTER_ARRAY));
    }

    public String getJsonPaths() {
        String value = jobProperties.get(PROPS_JSONPATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    public String getJsonRoot() {
        String value = jobProperties.get(PROPS_JSONROOT);
        if (value == null) {
            return "";
        }
        return value;
    }

    public int getSizeOfRoutineLoadTaskInfoList() {
        readLock();
        try {
            return routineLoadTaskInfoList.size();
        } finally {
            readUnlock();
        }
    }

    // RoutineLoadScheduler will run this method at fixed interval, and renew the timeout tasks
    public void processTimeoutTasks() {
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if (routineLoadTaskInfo.isRunningTimeout()) {
                    // here we simply discard the timeout task and create a new one.
                    // the corresponding txn will be aborted by txn manager.
                    // and after renew, the previous task is removed from routineLoadTaskInfoList,
                    // so task can no longer be committed successfully.
                    // the already committed task will not be handled here.
                    RoutineLoadTaskInfo newTask =
                            unprotectRenewTask(System.currentTimeMillis() + taskSchedIntervalS * 1000,
                                    routineLoadTaskInfo);
                    GlobalStateMgr.getCurrentState().getRoutineLoadManager()
                            .releaseBeTaskSlot(routineLoadTaskInfo.getBeId());
                    GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTaskInQueue(newTask);
                    LOG.warn(
                            "routine load task [job name {}, task id {}] timeout, remove old task and generate new one",
                            name, routineLoadTaskInfo.getId().toString());
                }
            }
        } finally {
            writeUnlock();
        }
    }

    abstract void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException;

    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        return 0;
    }

    public boolean containsTask(UUID taskId) {
        readLock();
        try {
            return routineLoadTaskInfoList.stream()
                    .anyMatch(entity -> entity.getId().equals(taskId));
        } finally {
            readUnlock();
        }
    }

    // All of private method could not be call without lock
    private void checkStateTransform(RoutineLoadJob.JobState desireState) throws UserException {
        switch (state) {
            case PAUSED:
                if (desireState == JobState.PAUSED) {
                    throw new DdlException("Could not transform " + state + " to " + desireState);
                }
                break;
            case STOPPED:
            case CANCELLED:
                throw new DdlException("Could not transform " + state + " to " + desireState);
            default:
                break;
        }
    }

    // if rate of error data is more than max_filter_ratio, pause job
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(),
                false /* not replay */);
    }

    private void updateNumOfData(long numOfTotalRows, long numOfErrorRows, long unselectedRows, long receivedBytes,
                                 long taskExecutionTime, boolean isReplay) throws UserException {
        this.totalRows += numOfTotalRows;
        this.errorRows += numOfErrorRows;
        this.unselectedRows += unselectedRows;
        this.receivedBytes += receivedBytes;
        this.totalTaskExcutionTimeMs += taskExecutionTime;

        if (MetricRepo.hasInit && !isReplay) {
            MetricRepo.COUNTER_ROUTINE_LOAD_ROWS.increase(numOfTotalRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_ERROR_ROWS.increase(numOfErrorRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_RECEIVED_BYTES.increase(receivedBytes);
        }

        // check error rate
        currentErrorRows += numOfErrorRows;
        currentTotalRows += numOfTotalRows;
        if (currentTotalRows > maxBatchRows * 10) {
            if (currentErrorRows > maxErrorNum) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("current_total_rows", currentTotalRows)
                        .add("current_error_rows", currentErrorRows)
                        .add("max_error_num", maxErrorNum)
                        .add("msg", "current error rows is more than max error num, begin to pause job")
                        .build());
                // if this is a replay thread, the update state should already be replayed by OP_CHANGE_ROUTINE_LOAD_JOB
                if (!isReplay) {
                    // remove all of task in jobs and change job state to paused
                    updateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                                    "current error rows of job is more than max error num"),
                            isReplay);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("current_total_rows", currentTotalRows)
                        .add("current_error_rows", currentErrorRows)
                        .add("max_error_num", maxErrorNum)
                        .add("msg", "reset current total rows and current error rows "
                                + "when current total rows is more than base")
                        .build());
            }
            // reset currentTotalNum and currentErrorNum
            currentErrorRows = 0;
            currentTotalRows = 0;
        } else if (currentErrorRows > maxErrorNum) {
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("current_total_rows", currentTotalRows)
                    .add("current_error_rows", currentErrorRows)
                    .add("max_error_num", maxErrorNum)
                    .add("msg", "current error rows is more than max error rows, begin to pause job")
                    .build());
            if (!isReplay) {
                // remove all of task in jobs and change job state to paused
                updateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                                "current error rows is more than max error num"),
                        isReplay);
            }
            // reset currentTotalNum and currentErrorNum
            currentErrorRows = 0;
            currentTotalRows = 0;
        }
    }

    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        try {
            updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                    attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(), true /* is replay */);
        } catch (UserException e) {
            LOG.error("should not happen", e);
        }
    }

    abstract RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo);

    // call before first scheduling
    // derived class can override this.
    public void prepare() throws UserException {
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        db.readLock();
        try {
            Table table = db.getTable(this.tableId);
            if (table == null) {
                throw new MetaNotFoundException("table " + this.tableId + " does not exist");
            }
            StreamLoadPlanner planner =
                    new StreamLoadPlanner(db, (OlapTable) table, StreamLoadInfo.fromRoutineLoadJob(this));
            TExecPlanFragmentParams planParams = planner.plan(loadId);
            planParams.query_options.setLoad_job_type(TLoadJobType.ROUTINE_LOAD);
            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());

            return planParams;
        } finally {
            db.readUnlock();
        }
    }

    // if task not exists, before aborted will reset the txn attachment to null, task will not be updated
    // if task pass the checker, task will be updated by attachment
    // *** Please do not call before individually. It must be combined use with after ***
    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                    .add("txn_state", txnState)
                    .add("msg", "task before aborted")
                    .build());
        }
        executeBeforeCheck(txnState, TransactionStatus.ABORTED);
    }

    // if task not exists, before committed will throw exception, commit txn will failed
    // if task pass the checker, lock job will be locked
    // *** Please do not call before individually. It must be combined use with after ***
    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                    .add("txn_state", txnState)
                    .add("msg", "task before committed")
                    .build());
        }
        executeBeforeCheck(txnState, TransactionStatus.COMMITTED);
    }

    /*
     * try lock the write lock.
     * Make sure lock is released if any exception being thrown
     */
    private void executeBeforeCheck(TransactionState txnState, TransactionStatus transactionStatus)
            throws TransactionException {
        // this lock will be released in afterCommitted/afterAborted function if no exception thrown
        writeLock();

        // task already pass the checker
        boolean passCheck = false;
        try {
            // check if task has been aborted
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.stream()
                            .filter(entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
            if (!routineLoadTaskInfoOptional.isPresent()) {
                switch (transactionStatus) {
                    case COMMITTED:
                        throw new TransactionException("txn " + txnState.getTransactionId()
                                + " could not be " + transactionStatus
                                + " while task " + txnState.getLabel() + " has been aborted.");
                    default:
                        break;
                }
            }
            passCheck = true;
        } finally {
            if (!passCheck) {
                writeUnlock();
                LOG.debug("unlock write lock of routine load job before check: {}", id);
            }
        }
    }

    // txn already committed before calling afterCommitted
    // the task will be committed
    // check currentErrorRows > maxErrorRows
    // paused job or renew task
    // *** Please do not call after individually. It must be combined use with before ***
    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        long taskBeId = -1L;
        try {
            if (txnOperated) {
                // find task in job
                Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                        entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
                if (routineLoadTaskInfoOptional.isPresent()) {
                    RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
                    taskBeId = routineLoadTaskInfo.getBeId();
                    executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.COMMITTED, null);
                }
                ++committedTaskNum;
                LOG.debug("routine load task committed. task id: {}, job id: {}", txnState.getLabel(), id);
            }
        } catch (Throwable e) {
            LOG.warn("after committed failed", e);
            String errmsg = "be " + taskBeId + " commit task failed " + txnState.getLabel()
                    + " with error " + e.getMessage()
                    + " while transaction " + txnState.getTransactionId() + " has been committed";
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.INTERNAL_ERR, errmsg),
                    false /* not replay */);
        } finally {
            // this lock is locked in executeBeforeCheck function
            writeUnlock();
            LOG.debug("unlock write lock of routine load job after committed: {}", id);
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        this.committedTaskNum++;
        LOG.debug("replay on committed: {}", txnState);
    }

    /*
     * the corresponding txn is visible, create a new task
     */
    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            String msg = String.format(
                    "should not happen, we find that txnOperated if false when handling afterVisble. job id: %d, txn_id: %d",
                    id, txnState.getTransactionId());
            LOG.warn(msg);
            // print a log and return.
            // if this really happen, the job will be blocked, and this task can be seen by
            // "show routine load task" stmt, which is in COMMITTED state for a long time.
            // so we can find this error and step in.
            return;
        }

        writeLock();
        try {
            if (state != JobState.RUNNING) {
                // job is not running, nothing need to be done
                return;
            }

            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                    entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
            if (!routineLoadTaskInfoOptional.isPresent()) {
                // not find task in routineLoadTaskInfoList. this may happen in following case:
                //      After the txn of the task is COMMITTED, but before it becomes VISIBLE,
                //      the routine load job has been paused and then start again.
                //      The routineLoadTaskInfoList will be cleared when job being paused.
                //      So the task can not be found here.
                // This is a normal case, we just print a log here to observe.
                LOG.info("Can not find task with transaction {} after visible, job: {}", txnState.getTransactionId(),
                        id);
                return;
            }
            RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
            if (routineLoadTaskInfo.getTxnStatus() != TransactionStatus.COMMITTED) {
                // TODO(cmy): Normally, this should not happen. But for safe reason, just pause the job
                String msg = String.format(
                        "should not happen, we find that task %s is not COMMITTED when handling afterVisble. " +
                                "job id: %d, txn_id: %d, txn status: %s",
                        DebugUtil.printId(routineLoadTaskInfo.getId()), id, txnState.getTransactionId(),
                        routineLoadTaskInfo.getTxnStatus().name());
                LOG.warn(msg);
                try {
                    updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.IMPOSSIBLE_ERROR_ERR, msg),
                            false /* not replay */);
                } catch (UserException e) {
                    // should not happen
                    LOG.warn("failed to pause the job {}. this should not happen", id, e);
                }
                return;
            }

            // create new task
            long timeToExecuteMs;
            RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment =
                    (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
            // isProgressKeepUp returns false means there is too much data in kafka/pulsar stream,
            // we set timeToExecuteMs to now, so that data not accumulated in kafka/pulsar
            if (!routineLoadTaskInfo.isProgressKeepUp(rlTaskTxnCommitAttachment.getProgress())) {
                timeToExecuteMs = System.currentTimeMillis();
            } else {
                timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
            }
            RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(timeToExecuteMs, routineLoadTaskInfo);
            GlobalStateMgr.getCurrentState().getRoutineLoadManager().releaseBeTaskSlot(routineLoadTaskInfo.getBeId());
            GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
        } finally {
            writeUnlock();
        }
    }

    // the task is aborted when the correct number of rows is more than 0
    // be will abort txn when all of kafka data is wrong or total consume data is 0
    // txn will be aborted but progress will be update
    // progress will be update otherwise the progress will be hung
    // *** Please do not call after individually. It must be combined use with before ***
    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReasonString)
            throws UserException {
        long taskBeId = -1L;
        try {
            if (txnOperated) {
                // step0: find task in job
                Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                        entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
                if (!routineLoadTaskInfoOptional.isPresent()) {
                    // task will not be update when task has been aborted by fe
                    return;
                }
                RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
                taskBeId = routineLoadTaskInfo.getBeId();
                // step1: job state will be changed depending on txnStatusChangeReasonString
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                            .add("txn_id", txnState.getTransactionId())
                            .add("msg", "txn abort with reason " + txnStatusChangeReasonString)
                            .build());
                }
                ++abortedTaskNum;
                setOtherMsg(txnStatusChangeReasonString);
                TransactionState.TxnStatusChangeReason txnStatusChangeReason = null;
                if (txnStatusChangeReasonString != null) {
                    txnStatusChangeReason =
                            TransactionState.TxnStatusChangeReason.fromString(txnStatusChangeReasonString);
                    if (txnStatusChangeReason != null) {
                        switch (txnStatusChangeReason) {
                            case OFFSET_OUT_OF_RANGE:
                            case PAUSE:
                                String msg = "be " + taskBeId + " abort task "
                                        + "with reason: " + txnStatusChangeReasonString;
                                updateState(JobState.PAUSED,
                                        new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg),
                                        false /* not replay */);
                                return;
                            default:
                                break;
                        }
                    }
                    // TODO(ml): use previous be id depend on change reason
                }
                // step2: commit task , update progress, maybe create a new task
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.ABORTED,
                        txnStatusChangeReasonString);
            }
        } catch (Exception e) {
            String msg =
                    "be " + taskBeId + " abort task " + txnState.getLabel() + " failed with error " + e.getMessage();
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg),
                    false /* not replay */);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("task_id", txnState.getLabel())
                    .add("error_msg",
                            "change job state to paused when task has been aborted with error " + e.getMessage())
                    .build(), e);
        } finally {
            // this lock is locked in executeBeforeCheck function
            writeUnlock();
            LOG.debug("unlock write lock of routine load job after aborted: {}", id);
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        // Attachment may be null if this task is aborted by FE.
        // For the aborted txn, we should check the cause of the error,
        // the detailed information is in the checkCommitInfo function.
        if (txnState.getTxnCommitAttachment() != null &&
                checkCommitInfo((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment(),
                        txnState,
                        TransactionState.TxnStatusChangeReason.fromString(txnState.getReason()))) {
            replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        }
        this.abortedTaskNum++;
        LOG.debug("replay on aborted: {}, has attachment: {}", txnState, txnState.getTxnCommitAttachment() == null);
    }

    // check task exists or not before call method
    private void executeTaskOnTxnStatusChanged(RoutineLoadTaskInfo routineLoadTaskInfo, TransactionState txnState,
                                               TransactionStatus txnStatus, String txnStatusChangeReasonStr)
            throws UserException {
        // step0: get progress from transaction state
        RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment =
                (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        if (rlTaskTxnCommitAttachment == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                        .add("job_id", routineLoadTaskInfo.getJobId())
                        .add("txn_id", routineLoadTaskInfo.getTxnId())
                        .add("msg", "commit task will be ignore when attachment txn of task is null,"
                                + " maybe task was aborted by master when timeout")
                        .build());
            }
        } else if (checkCommitInfo(rlTaskTxnCommitAttachment, txnState,
                TransactionState.TxnStatusChangeReason.fromString(txnStatusChangeReasonStr))) {
            // step2: update job progress
            updateProgress(rlTaskTxnCommitAttachment);
        }

        if (rlTaskTxnCommitAttachment != null && !Strings.isNullOrEmpty(rlTaskTxnCommitAttachment.getErrorLogUrl())) {
            errorLogUrls.add(rlTaskTxnCommitAttachment.getErrorLogUrl());
        }

        routineLoadTaskInfo.setTxnStatus(txnStatus);

        if (TransactionState.TxnStatusChangeReason.fromString(txnStatusChangeReasonStr) == 
                                            TransactionState.TxnStatusChangeReason.FILTERED_ROWS) {
            updateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, txnStatusChangeReasonStr),
                        false /* not replay */);
            LOG.warn(
                    "routine load task [job name {}, task id {}] aborted because of {}, change state to PAUSED",
                     name, routineLoadTaskInfo.getId().toString(), txnStatusChangeReasonStr);
            return;
        }

        if (state == JobState.RUNNING) {
            if (txnStatus == TransactionStatus.ABORTED) {
                RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(
                        System.currentTimeMillis() + taskSchedIntervalS * 1000, routineLoadTaskInfo);
                newRoutineLoadTaskInfo.setMsg("previous task aborted because of " + txnStatusChangeReasonStr);
                GlobalStateMgr.getCurrentState().getRoutineLoadManager()
                        .releaseBeTaskSlot(routineLoadTaskInfo.getBeId());
                GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
                LOG.warn(
                        "routine load task [job name {}, task id {}] aborted because of {}, remove old task and generate new one",
                        name, routineLoadTaskInfo.getId().toString(), txnStatusChangeReasonStr);
            } else if (txnStatus == TransactionStatus.COMMITTED) {
                // this txn is just COMMITTED, create new task when the this txn is VISIBLE
                // or if publish version task has some error, there will be lots of COMMITTED txns in GlobalTransactionMgr
            }
        }
    }

    protected static void unprotectedCheckMeta(Database db, String tblName, RoutineLoadDesc routineLoadDesc)
            throws UserException {
        Table table = db.getTable(tblName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }

        if (table instanceof MaterializedView) {
            throw new AnalysisException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tblName, tblName));
        }

        if (!table.isOlapOrLakeTable()) {
            throw new AnalysisException("Only olap/lake table support routine load");
        }

        if (routineLoadDesc == null) {
            return;
        }

        PartitionNames partitionNames = routineLoadDesc.getPartitionNames();
        if (partitionNames == null) {
            return;
        }

        // check partitions
        OlapTable olapTable = (OlapTable) table;
        for (String partName : partitionNames.getPartitionNames()) {
            if (olapTable.getPartition(partName, partitionNames.isTemp()) == null) {
                throw new DdlException("Partition " + partName + " does not exist");
            }
        }

        // columns will be checked when planing
    }

    public void updateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
        writeLock();
        try {
            unprotectUpdateState(jobState, reason, isReplay);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_state", getState())
                .add("desire_job_state", jobState)
                .add("msg", reason)
                .build());

        checkStateTransform(jobState);
        switch (jobState) {
            case RUNNING:
                executeRunning();
                break;
            case PAUSED:
                executePause(reason);
                LOG.warn("routine load job {}-{} changed to PAUSED with reason: {}", id, name, reason);
                break;
            case NEED_SCHEDULE:
                executeNeedSchedule();
                break;
            case STOPPED:
                executeStop();
                LOG.warn("routine load job {}-{} changed to STOPPED with reason: {}", id, name, reason);
                break;
            case CANCELLED:
                executeCancel(reason);
                LOG.warn("routine load job {}-{} changed to CANCELLED with reason: {}", id, name, reason);
                break;
            default:
                break;
        }

        if (state.isFinalState()) {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        }

        if (!isReplay && jobState != JobState.RUNNING) {
            GlobalStateMgr.getCurrentState().getEditLog().logOpRoutineLoadJob(new RoutineLoadOperation(id, jobState));
        }

        if (!isReplay && MetricRepo.hasInit && JobState.PAUSED == jobState) {
            MetricRepo.COUNTER_ROUTINE_LOAD_PAUSED.increase(1L);
        }
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_state", getState())
                .add("msg", "job state has been changed")
                .add("is replay", String.valueOf(isReplay))
                .build());
    }

    private void executeRunning() {
        state = JobState.RUNNING;
    }

    private void executePause(ErrorReason reason) {
        // remove all of task in jobs and change job state to paused
        pauseReason = reason;
        state = JobState.PAUSED;
        pauseTimestamp = System.currentTimeMillis();
        clearTasks();
    }

    private void executeNeedSchedule() {
        state = JobState.NEED_SCHEDULE;
        pauseTimestamp = -1;
        clearTasks();
    }

    private void executeStop() {
        state = JobState.STOPPED;
        clearTasks();
        endTimestamp = System.currentTimeMillis();
    }

    private void executeCancel(ErrorReason reason) {
        cancelReason = reason;
        state = JobState.CANCELLED;
        clearTasks();
        endTimestamp = System.currentTimeMillis();
    }

    private void clearTasks() {
        for (RoutineLoadTaskInfo task : routineLoadTaskInfoList) {
            if (task.getBeId() != RoutineLoadTaskInfo.INVALID_BE_ID) {
                GlobalStateMgr.getCurrentState().getRoutineLoadManager().releaseBeTaskSlot(task.getBeId());
            }
        }
        routineLoadTaskInfoList.clear();
    }

    public void update() throws UserException {
        // check if db and table exist
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("db_id", dbId)
                    .add("msg", "The database has been deleted. Change job state to cancelled").build());
            writeLock();
            try {
                if (!state.isFinalState()) {
                    unprotectUpdateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.DB_ERR, "db " + dbId + "not exist"),
                            false /* not replay */);
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        // check table belong to database
        database.readLock();
        Table table;
        try {
            table = database.getTable(tableId);
        } finally {
            database.readUnlock();
        }
        if (table == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id).add("db_id", dbId)
                    .add("table_id", tableId)
                    .add("msg", "The table has been deleted change job state to cancelled").build());
            writeLock();
            try {
                if (!state.isFinalState()) {
                    unprotectUpdateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.TABLE_ERR, "table not exist"), false /* not replay */);
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        // check if partition has been changed
        writeLock();
        try {
            if (unprotectNeedReschedule()) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("msg", "Job need to be rescheduled")
                        .build());
                unprotectUpdateProgress();
                unprotectUpdateState(JobState.NEED_SCHEDULE, null, false);
            }
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectUpdateProgress() {
    }

    protected boolean unprotectNeedReschedule() throws UserException {
        return false;
    }

    public void setOrigStmt(OriginStatement origStmt) {
        this.origStmt = origStmt;
    }

    public OriginStatement getOrigStmt() {
        return origStmt;
    }

    // check the correctness of commit info
    protected abstract boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                               TransactionState txnState,
                                               TransactionState.TxnStatusChangeReason txnStatusChangeReason);

    protected abstract String getStatistic();

    public List<String> getShowInfo() {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Table tbl = null;
        if (db != null) {
            db.readLock();
            try {
                tbl = db.getTable(tableId);
            } finally {
                db.readUnlock();
            }
        }

        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(id));
            row.add(name);
            row.add(TimeUtils.longToTimeString(createTimestamp));
            row.add(TimeUtils.longToTimeString(pauseTimestamp));
            row.add(TimeUtils.longToTimeString(endTimestamp));
            row.add(db == null ? String.valueOf(dbId) : db.getFullName());
            row.add(tbl == null ? String.valueOf(tableId) : tbl.getName());
            if (state == JobState.RUNNING) {
                row.add(substate == JobSubstate.STABLE ? state.name() : substate.name());
            } else {
                row.add(state.name());
            }
            row.add(dataSourceType.name());
            row.add(String.valueOf(getSizeOfRoutineLoadTaskInfoList()));
            row.add(jobPropertiesToJsonString());
            row.add(dataSourcePropertiesJsonToString());
            row.add(customPropertiesJsonToString());
            row.add(getStatistic());
            row.add(getProgress().toJsonString());
            row.add(getTimestampProgress().toJsonString());
            switch (state) {
                case PAUSED:
                    row.add(pauseReason == null ? "" : pauseReason.toString());
                    break;
                case CANCELLED:
                    row.add(cancelReason == null ? "" : cancelReason.toString());
                    break;
                case RUNNING:
                    if (substate == JobSubstate.UNSTABLE) {
                        row.add(stateChangedReason == null ? "" : stateChangedReason.toString());
                    } else {
                        row.add("");
                    }
                    break;
                default:
                    row.add("");
            }
            row.add(Joiner.on(", ").join(errorLogUrls));
            row.add(otherMsg);
            return row;
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getTasksShowInfo() {
        readLock();
        try {
            List<List<String>> rows = Lists.newArrayList();
            routineLoadTaskInfoList.stream().forEach(entity -> rows.add(entity.getTaskShowInfo()));
            return rows;
        } finally {
            readUnlock();
        }
    }

    public List<String> getShowStatistic() {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(name);
            row.add(String.valueOf(id));
            row.add(db == null ? String.valueOf(dbId) : db.getFullName());
            row.add(getStatistic());
            row.add(getTaskStatistic());
            return row;
        } finally {
            readUnlock();
        }
    }

    private String getTaskStatistic() {
        Map<String, String> result = Maps.newHashMap();
        result.put("running_task",
                String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> entity.isRunning()).count()));
        result.put("waiting_task",
                String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> !entity.isRunning()).count()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(result);
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    private String jobPropertiesToJsonString() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put("partitions",
                partitions == null ? STAR_STRING : Joiner.on(",").join(partitions.getPartitionNames()));
        jobProperties.put("columnToColumnExpr", columnDescs == null ? STAR_STRING : Joiner.on(",").join(columnDescs));
        jobProperties.put("whereExpr", whereExpr == null ? STAR_STRING : whereExpr.toSql());
        if (getFormat().equalsIgnoreCase("json")) {
            jobProperties.put("dataFormat", "json");
        } else {
            jobProperties.put("columnSeparator", columnSeparator == null ? "\t" : columnSeparator.toString());
            jobProperties.put("rowDelimiter", rowDelimiter == null ? "\t" : rowDelimiter.toString());
        }
        jobProperties.put("maxErrorNum", String.valueOf(maxErrorNum));
        jobProperties.put("maxFilterRatio", String.valueOf(maxFilterRatio));
        jobProperties.put("maxBatchIntervalS", String.valueOf(taskSchedIntervalS));
        jobProperties.put("maxBatchRows", String.valueOf(maxBatchRows));
        jobProperties.put("currentTaskConcurrentNum", String.valueOf(currentTaskConcurrentNum));
        jobProperties.put("desireTaskConcurrentNum", String.valueOf(desireTaskConcurrentNum));
        jobProperties.putAll(this.jobProperties);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jobProperties);
    }

    abstract String dataSourcePropertiesJsonToString();

    abstract String customPropertiesJsonToString();

    public boolean needRemove() {
        if (!isFinal()) {
            return false;
        }
        Preconditions.checkState(endTimestamp != -1, endTimestamp);
        if ((System.currentTimeMillis() - endTimestamp) > Config.label_keep_max_second * 1000) {
            return true;
        }
        return false;
    }

    public boolean isFinal() {
        return state.isFinalState();
    }

    public static RoutineLoadJob read(DataInput in) throws IOException {
        RoutineLoadJob job = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(Text.readString(in));
        if (type == LoadDataSourceType.KAFKA) {
            job = new KafkaRoutineLoadJob();
        } else if (type == LoadDataSourceType.PULSAR) {
            job = new PulsarRoutineLoadJob();
        } else {
            throw new IOException("Unknown load data source type: " + type.name());
        }

        job.setTypeRead(true);
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, dataSourceType.name());

        out.writeLong(id);
        Text.writeString(out, name);
        Text.writeString(out, SystemInfoService.DEFAULT_CLUSTER);
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeInt(desireTaskConcurrentNum);
        Text.writeString(out, state.name());
        out.writeLong(maxErrorNum);
        out.writeLong(taskSchedIntervalS);
        out.writeLong(maxBatchRows);
        out.writeLong(maxBatchSizeBytes);
        progress.write(out);

        out.writeLong(createTimestamp);
        out.writeLong(pauseTimestamp);
        out.writeLong(endTimestamp);

        out.writeLong(currentErrorRows);
        out.writeLong(currentTotalRows);
        out.writeLong(errorRows);
        out.writeLong(totalRows);
        out.writeLong(unselectedRows);
        out.writeLong(receivedBytes);
        out.writeLong(totalTaskExcutionTimeMs);
        out.writeLong(committedTaskNum);
        out.writeLong(abortedTaskNum);

        origStmt.write(out);
        out.writeInt(jobProperties.size());
        for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }

        out.writeInt(sessionVariables.size());
        for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            dataSourceType = LoadDataSourceType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        id = in.readLong();
        name = Text.readString(in);

        // ignore the clusterName param
        Text.readString(in);

        dbId = in.readLong();
        tableId = in.readLong();
        desireTaskConcurrentNum = in.readInt();
        state = JobState.valueOf(Text.readString(in));
        maxErrorNum = in.readLong();
        taskSchedIntervalS = in.readLong();
        maxBatchRows = in.readLong();
        maxBatchSizeBytes = in.readLong();

        switch (dataSourceType) {
            case KAFKA: {
                progress = new KafkaProgress();
                timestampProgress = new KafkaProgress();
                progress.readFields(in);
                break;
            }
            case PULSAR: {
                progress = new PulsarProgress();
                progress.readFields(in);
                break;
            }
            default:
                throw new IOException("unknown data source type: " + dataSourceType);
        }

        createTimestamp = in.readLong();
        pauseTimestamp = in.readLong();
        endTimestamp = in.readLong();

        currentErrorRows = in.readLong();
        currentTotalRows = in.readLong();
        errorRows = in.readLong();
        totalRows = in.readLong();
        unselectedRows = in.readLong();
        receivedBytes = in.readLong();
        totalTaskExcutionTimeMs = in.readLong();
        committedTaskNum = in.readLong();
        abortedTaskNum = in.readLong();

        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_76) {
            String stmt = Text.readString(in);
            origStmt = new OriginStatement(stmt, 0);
        } else {
            origStmt = OriginStatement.read(in);
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_59) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                jobProperties.put(key, value);
            }
        } else {
            // The behaviors of old broker load could not be changed
            jobProperties.put(LoadStmt.STRICT_MODE, Boolean.toString(false));
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_66) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                sessionVariables.put(key, value);
            }
        } else {
            // old version of load does not have sqlmode, set it to default
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }

        setRoutineLoadDesc(CreateRoutineLoadStmt.getLoadDesc(origStmt, sessionVariables));
    }

    public void modifyJob(RoutineLoadDesc routineLoadDesc,
                          Map<String, String> jobProperties,
                          RoutineLoadDataSourceProperties dataSourceProperties,
                          OriginStatement originStatement,
                          boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (routineLoadDesc != null) {
                setRoutineLoadDesc(routineLoadDesc);
                mergeLoadDescToOriginStatement(routineLoadDesc);
            }
            if (jobProperties != null) {
                modifyCommonJobProperties(jobProperties);
            }
            if (dataSourceProperties != null) {
                modifyDataSourceProperties(dataSourceProperties);
            }
            if (!isReplay) {
                AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(id,
                        jobProperties, dataSourceProperties, originStatement);
                GlobalStateMgr.getCurrentState().getEditLog().logAlterRoutineLoadJob(log);
            }
        } finally {
            writeUnlock();
        }
    }

    public void mergeLoadDescToOriginStatement(RoutineLoadDesc routineLoadDesc) {
        if (origStmt == null) {
            return;
        }

        RoutineLoadDesc originLoadDesc = CreateRoutineLoadStmt.getLoadDesc(origStmt, sessionVariables);
        if (originLoadDesc == null) {
            originLoadDesc = new RoutineLoadDesc();
        }
        if (routineLoadDesc.getColumnSeparator() != null) {
            originLoadDesc.setColumnSeparator(routineLoadDesc.getColumnSeparator());
        }
        if (routineLoadDesc.getRowDelimiter() != null) {
            originLoadDesc.setRowDelimiter(routineLoadDesc.getRowDelimiter());
        }
        if (routineLoadDesc.getColumnsInfo() != null) {
            originLoadDesc.setColumnsInfo(routineLoadDesc.getColumnsInfo());
        }
        if (routineLoadDesc.getWherePredicate() != null) {
            originLoadDesc.setWherePredicate(routineLoadDesc.getWherePredicate());
        }
        if (routineLoadDesc.getPartitionNames() != null) {
            originLoadDesc.setPartitionNames(routineLoadDesc.getPartitionNames());
        }

        String tableName = null;
        try {
            tableName = getTableName();
        } catch (Exception e) {
            LOG.warn("get table name failed", e);
            tableName = "unknown";
        }

        // we use sql to persist the load properties, so we just put the load properties to sql.
        String sql = String.format("CREATE ROUTINE LOAD %s ON %s %s" +
                        " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                        " FROM KAFKA (\"kafka_topic\" = \"my_topic\")",
                name, tableName, originLoadDesc.toSql());
        LOG.debug("merge result: {}", sql);
        origStmt = new OriginStatement(sql, 0);
    }

    protected abstract void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties)
            throws DdlException;

    // for ALTER ROUTINE LOAD
    private void modifyCommonJobProperties(Map<String, String> jobProperties) {
        // Some properties will be remove from the map, so we copy the jobProperties to copiedJobProperties
        Map<String, String> copiedJobProperties = new HashMap<>(jobProperties);
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            this.desireTaskConcurrentNum = Integer.parseInt(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        }
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)) {
            this.maxErrorNum = Long.parseLong(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
        }
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            this.maxFilterRatio = Double.parseDouble(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY));
        }
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            this.taskSchedIntervalS = Long.parseLong(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        }
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)) {
            this.maxBatchRows = Long.parseLong(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
        }
        this.jobProperties.putAll(copiedJobProperties);
    }
    protected void updateSubstate(JobSubstate substate, ErrorReason reason) throws UserException {
        writeLock();
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_substate", this.substate)
                .add("desire_job_substate", substate)
                .add("msg", reason)
                .build());
        try {
            this.substate = substate;
            this.stateChangedReason = reason;
        } finally {
            writeUnlock();
        }
    }

    public void updateSubstateStable() throws UserException {
        updateSubstate(JobSubstate.STABLE, null);
    }

    public void updateSubstate() throws UserException {
    }
}
