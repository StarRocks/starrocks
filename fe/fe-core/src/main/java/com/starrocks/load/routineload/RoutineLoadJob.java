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
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
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
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.LoadJobWithWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseIdleChecker;
import org.apache.commons.text.StringEscapeUtils;
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

import static com.starrocks.common.ErrorCode.ERR_LOAD_DATA_PARSE_ERROR;
import static com.starrocks.common.ErrorCode.ERR_TOO_MANY_ERROR_ROWS;

/**
 * Routine load job is a function which stream load data from streaming medium to starrocks.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA and Pulsar
 */
public abstract class RoutineLoadJob extends AbstractTxnStateChangeCallback
        implements Writable, GsonPostProcessable, LoadJobWithWarehouse {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    public static final long DEFAULT_MAX_ERROR_NUM = 0;
    public static final long DEFAULT_MAX_BATCH_ROWS = 200000;

    public static final long DEFAULT_TASK_SCHED_INTERVAL_SECOND = 10;
    public static final boolean DEFAULT_STRICT_MODE = false; // default is false
    public static final boolean DEFAULT_PAUSE_ON_FATAL_PARSE_ERROR = false;

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

    @SerializedName("i")
    protected long id;
    @SerializedName("n")
    protected String name;
    @SerializedName("d")
    protected long dbId;
    @SerializedName("t")
    protected long tableId;
    // this code is used to verify be task request
    protected long authCode;
    //    protected RoutineLoadDesc routineLoadDesc; // optional
    protected PartitionNames partitions; // optional
    protected List<ImportColumnDesc> columnDescs; // optional
    protected Expr whereExpr; // optional
    protected ColumnSeparator columnSeparator; // optional
    protected RowDelimiter rowDelimiter; // optional
    @SerializedName("de")
    protected int desireTaskConcurrentNum; // optional
    @SerializedName("s")
    protected JobState state = JobState.NEED_SCHEDULE;
    protected JobSubstate substate = JobSubstate.STABLE;
    @SerializedName("da")
    protected LoadDataSourceType dataSourceType;
    protected double maxFilterRatio = 1;
    // max number of error data in max batch rows * 10
    // maxErrorNum / (maxBatchRows * 10) = max error rate of routine load job
    // if current error rate is more than max error rate, the job will be paused
    @SerializedName("me")
    protected long maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
    // include strict mode
    @SerializedName("jp")
    protected Map<String, String> jobProperties = Maps.newHashMap();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    @SerializedName("sv")
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    /**
     * task submitted to be every taskSchedIntervalS,
     * and consume data for Config.routine_load_consume_second or consumed data size exceeds Config.max_routine_load_batch_size
     * maxBatchRows only used to judge error data and dose not limit the batch data
     */
    @SerializedName("ta")
    protected long taskSchedIntervalS = DEFAULT_TASK_SCHED_INTERVAL_SECOND;
    @SerializedName("mr")
    protected long maxBatchRows = DEFAULT_MAX_BATCH_ROWS;

    // deprecated
    // set to Config.max_routine_load_batch_size, so that if fe rolls back to old version, routine load can still work
    @SerializedName("mb")
    protected long maxBatchSizeBytes = Config.max_routine_load_batch_size;

    private String confluentSchemaRegistryUrl;

    protected int currentTaskConcurrentNum;
    @SerializedName("p")
    protected RoutineLoadProgress progress;

    @SerializedName("tp")
    protected RoutineLoadProgress timestampProgress;

    protected long firstResumeTimestamp; // the first resume time
    protected long autoResumeCount;
    protected boolean autoResumeLock = false; //it can't auto resume iff true
    // some other msg which need to show to user;
    protected String otherMsg = "";
    protected ErrorReason pauseReason;
    protected ErrorReason cancelReason;

    protected ErrorReason stateChangedReason;

    @SerializedName("c")
    protected long createTimestamp = System.currentTimeMillis();
    @SerializedName("pa")
    protected long pauseTimestamp = -1;
    @SerializedName("e")
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
    @SerializedName("ce")
    protected long currentErrorRows = 0;
    @SerializedName("ct")
    protected long currentTotalRows = 0;
    @SerializedName("er")
    protected long errorRows = 0;
    @SerializedName("tr")
    protected long totalRows = 0;
    @SerializedName("u")
    protected long unselectedRows = 0;
    @SerializedName("rb")
    protected long receivedBytes = 0;
    @SerializedName("tt")
    protected long totalTaskExcutionTimeMs = 1; // init as 1 to avoid division by zero
    @SerializedName("cn")
    protected long committedTaskNum = 0;
    @SerializedName("at")
    protected long abortedTaskNum = 0;
    @SerializedName("tcs")
    protected long taskConsumeSecond = Config.routine_load_task_consume_second;
    @SerializedName("tts")
    protected long taskTimeoutSecond = Config.routine_load_task_timeout_second;

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();

    // this is the origin stmt of CreateRoutineLoadStmt, we use it to persist the RoutineLoadJob,
    // because we can not serialize the Expressions contained in job.
    @SerializedName("os")
    protected OriginStatement origStmt;

    @SerializedName("warehouseId")
    protected long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

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

    @Override
    public long getCurrentWarehouseId() {
        return warehouseId;
    }

    @Override
    public boolean isFinal() {
        return state.isFinalState();
    }

    @Override
    public long getFinishTimestampMs() {
        return getEndTimestamp();
    }

    protected void setOptional(CreateRoutineLoadStmt stmt) throws StarRocksException {
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
        jobProperties.put(LoadStmt.LOG_REJECTED_RECORD_NUM, String.valueOf(stmt.getLogRejectedRecordNum()));
        jobProperties.put(LoadStmt.PARTIAL_UPDATE, String.valueOf(stmt.isPartialUpdate()));
        jobProperties.put(LoadStmt.PARTIAL_UPDATE_MODE, String.valueOf(stmt.getPartialUpdateMode()));
        jobProperties.put(LoadStmt.TIMEZONE, stmt.getTimezone());
        jobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(stmt.isStrictMode()));
        jobProperties.put(CreateRoutineLoadStmt.PAUSE_ON_FATAL_PARSE_ERROR, String.valueOf(stmt.isPauseOnFatalParseError()));
        if (stmt.getMergeConditionStr() != null) {
            jobProperties.put(LoadStmt.MERGE_CONDITION, stmt.getMergeConditionStr());
        }
        if (Strings.isNullOrEmpty(stmt.getFormat()) || stmt.getFormat().equals("csv")) {
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            jobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, "false");
            jobProperties.put(CreateRoutineLoadStmt.JSONPATHS, "");
            jobProperties.put(CreateRoutineLoadStmt.JSONROOT, "");
            jobProperties.put(CreateRoutineLoadStmt.TRIMSPACE, stmt.isTrimspace() ? "true" : "false");
            jobProperties.put(CreateRoutineLoadStmt.ENCLOSE, Byte.toString(stmt.getEnclose()));
            jobProperties.put(CreateRoutineLoadStmt.ESCAPE, Byte.toString(stmt.getEscape()));
        } else if (stmt.getFormat().equals("json")) {
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "json");
            if (!Strings.isNullOrEmpty(stmt.getJsonPaths())) {
                jobProperties.put(CreateRoutineLoadStmt.JSONPATHS, stmt.getJsonPaths());
            } else {
                jobProperties.put(CreateRoutineLoadStmt.JSONPATHS, "");
            }
            if (!Strings.isNullOrEmpty(stmt.getJsonRoot())) {
                jobProperties.put(CreateRoutineLoadStmt.JSONROOT, stmt.getJsonRoot());
            } else {
                jobProperties.put(CreateRoutineLoadStmt.JSONROOT, "");
            }
            if (stmt.isStripOuterArray()) {
                jobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, "true");
            } else {
                jobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, "false");
            }
        } else if (stmt.getFormat().equals("avro")) {
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "avro");
            if (!Strings.isNullOrEmpty(stmt.getJsonPaths())) {
                jobProperties.put(CreateRoutineLoadStmt.JSONPATHS, stmt.getJsonPaths());
            } else {
                jobProperties.put(CreateRoutineLoadStmt.JSONPATHS, "");
            }
            this.confluentSchemaRegistryUrl = stmt.getConfluentSchemaRegistryUrl();
        } else {
            throw new StarRocksException("Invalid format type.");
        }
        taskConsumeSecond = stmt.getTaskConsumeSecond();
        taskTimeoutSecond = stmt.getTaskTimeoutSecond();

        // set warehouse
        Map<String, String> prop = stmt.getJobProperties();
        if (prop.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = prop.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            jobProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, warehouseName);

            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
            if (warehouse == null) {
                throw new StarRocksException("Warehouse " + warehouseName + " not exists.");
            }

            setWarehouseId(warehouse.getId());
        } else {
            setWarehouseId(ConnectContext.get().getCurrentWarehouseId());
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

    public String getConfluentSchemaRegistryUrl() {
        return confluentSchemaRegistryUrl;
    }

    public void setConfluentSchemaRegistryUrl(String confluentSchemaRegistryUrl) {
        this.confluentSchemaRegistryUrl = confluentSchemaRegistryUrl;
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

    public long getTaskConsumeSecond() {
        return taskConsumeSecond;
    }

    public long getTaskTimeoutSecond() {
        return taskTimeoutSecond;
    }

    public boolean isTrimspace() {
        return Boolean.parseBoolean(jobProperties.getOrDefault(CreateRoutineLoadStmt.TRIMSPACE, "false"));
    }

    public byte getEnclose() {
        return Byte.parseByte(jobProperties.getOrDefault(CreateRoutineLoadStmt.ENCLOSE, "0"));
    }

    public byte getEscape() {
        return Byte.parseByte(jobProperties.getOrDefault(CreateRoutineLoadStmt.ESCAPE, "0"));
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public void setOtherMsg(String otherMsg) {
        this.otherMsg = String.format("[%s] %s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                Strings.nullToEmpty(otherMsg));
    }

    public void clearOtherMsg() {
        this.otherMsg = "";
    }

    public String getDbFullName() throws MetaNotFoundException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return db.getFullName();
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (table == null) {
            throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
        }
        return table.getName();
    }

    public JobState getState() {
        return state;
    }

    public boolean isUnstable() {
        return state == JobState.RUNNING && substate == JobSubstate.UNSTABLE;
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

    public boolean isPauseOnFatalParseError() {
        String value = jobProperties.get(CreateRoutineLoadStmt.PAUSE_ON_FATAL_PARSE_ERROR);
        if (value == null) {
            return DEFAULT_PAUSE_ON_FATAL_PARSE_ERROR;
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

    public String getPartialUpdateMode() {
        // RoutineLoad job only support row mode.
        return "row";
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public RoutineLoadProgress getTimestampProgress() {
        return timestampProgress;
    }

    protected abstract String getSourceProgressString();

    protected abstract String getSourceLagString(String progressJsonStr);

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getLogRejectedRecordNum() {
        String v = jobProperties.get(LoadStmt.LOG_REJECTED_RECORD_NUM);
        if (v == null) {
            return 0;
        } else {
            return Long.valueOf(v);
        }
    }

    public long getTaskSchedIntervalS() {
        return taskSchedIntervalS;
    }

    public String getFormat() {
        String value = jobProperties.get(CreateRoutineLoadStmt.FORMAT);
        if (value == null) {
            return "csv";
        }
        return value;
    }

    public String getMergeCondition() {
        return jobProperties.get(LoadStmt.MERGE_CONDITION);
    }

    public boolean isStripOuterArray() {
        return Boolean.parseBoolean(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY));
    }

    public String getJsonPaths() {
        String value = jobProperties.get(CreateRoutineLoadStmt.JSONPATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    public String getJsonRoot() {
        String value = jobProperties.get(CreateRoutineLoadStmt.JSONROOT);
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

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public long getWarehouseId() {
        return warehouseId;
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
                    GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                            .releaseBeTaskSlot(routineLoadTaskInfo.getWarehouseId(),
                                    routineLoadTaskInfo.getJobId(), routineLoadTaskInfo.getBeId());
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

    abstract void divideRoutineLoadJob(int currentConcurrentTaskNum) throws StarRocksException;

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
    private void checkStateTransform(RoutineLoadJob.JobState desireState) throws StarRocksException {
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
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws StarRocksException {
        updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(),
                false /* not replay */);
    }

    private void updateNumOfData(long numOfTotalRows, long numOfErrorRows, long unselectedRows, long receivedBytes,
                                 long taskExecutionTime, boolean isReplay) throws StarRocksException {
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
                    String errMsg =
                            String.format("Current error rows: %d is more than max error num: %d", currentErrorRows, maxErrorNum);
                    updateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                                    ERR_TOO_MANY_ERROR_ROWS.formatErrorMsg(errMsg, "max_error_number")),
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
                    .add("msg", "current error rows is more than max error num, begin to pause job")
                    .build());
            if (!isReplay) {
                // remove all of task in jobs and change job state to paused
                String errMsg =
                        String.format("Current error rows: %d is more than max error num: %d", currentErrorRows, maxErrorNum);
                updateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                                ERR_TOO_MANY_ERROR_ROWS.formatErrorMsg(errMsg, "max_error_number")),
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
        } catch (StarRocksException e) {
            LOG.error("should not happen", e);
        }
    }

    abstract RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo);

    // call before first scheduling
    // derived class can override this.
    public void prepare() throws StarRocksException {
    }

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId, String label) throws StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), this.tableId);
        if (table == null) {
            throw new MetaNotFoundException("table " + this.tableId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            StreamLoadInfo info = StreamLoadInfo.fromRoutineLoadJob(this);
            info.setTxnId(txnId);
            StreamLoadPlanner planner =
                    new StreamLoadPlanner(db, (OlapTable) table, info);
            TExecPlanFragmentParams planParams = planner.plan(loadId);
            planParams.query_options.setLoad_job_type(TLoadJobType.ROUTINE_LOAD);
            StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();

            StreamLoadTask streamLoadTask = streamLoadManager.createLoadTaskWithoutLock(db, table, label, "", "",
                    taskTimeoutSecond * 1000, true, warehouseId);
            streamLoadTask.setTxnId(txnId);
            streamLoadTask.setLabel(label);
            streamLoadTask.setTUniqueId(loadId);
            streamLoadManager.addLoadTask(streamLoadTask);

            Coordinator coord =
                    getCoordinatorFactory().createSyncStreamLoadScheduler(planner, planParams.getCoord());
            streamLoadTask.setCoordinator(coord);

            QeProcessorImpl.INSTANCE.registerQuery(loadId, coord);

            LOG.info(new LogBuilder("routine load task create stream load task success").
                    add("transactionId", txnId).
                    add("label", label).
                    add("streamLoadTaskId", streamLoadTask.getId()));

            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new StarRocksException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());

            planParams.setImport_label(txnState.getLabel());
            planParams.setDb_name(db.getFullName());
            planParams.setLoad_job_id(txnId);

            return planParams;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
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
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException {
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
                    routineLoadTaskInfo.afterCommitted(txnState, txnOperated);
                }
                ++committedTaskNum;
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tableId);
                entity.counterRoutineLoadCommittedTasksTotal.increase(1L);
                LOG.debug("routine load task committed. task id: {}, job id: {}", txnState.getLabel(), id);

                StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                        getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
                if (streamLoadTask != null) {
                    streamLoadTask.afterCommitted(txnState, txnOperated);
                }
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
        StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
        if (streamLoadTask != null) {
            streamLoadTask.replayOnCommitted(txnState);
        }
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

            StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                    getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
            if (streamLoadTask != null) {
                streamLoadTask.afterVisible(txnState, txnOperated);
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
                } catch (StarRocksException e) {
                    // should not happen
                    LOG.warn("failed to pause the job {}. this should not happen", id, e);
                }
                return;
            }

            try {
                routineLoadTaskInfo.afterVisible(txnState, txnOperated);
            } catch (StarRocksException e) {
                LOG.warn("failed to execute 'routineLoadTaskInfo.afterVisible', txnId {}, label {}. " +
                        "this should not happen", txnState.getTransactionId(), routineLoadTaskInfo.getLabel());
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
            GlobalStateMgr.getCurrentState().getRoutineLoadMgr().
                    releaseBeTaskSlot(routineLoadTaskInfo.getWarehouseId(),
                            routineLoadTaskInfo.getJobId(), routineLoadTaskInfo.getBeId());
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
            throws StarRocksException {
        long taskBeId = -1L;
        try {
            if (txnOperated) {
                StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                        getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
                if (streamLoadTask != null) {
                    streamLoadTask.afterAborted(txnState, txnOperated, txnStatusChangeReasonString);
                }

                // step0: find task in job
                Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                        entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tableId);
                if (!routineLoadTaskInfoOptional.isPresent()) {
                    //  The task of the timed-out transaction will be detected by the transaction checker thread
                    //  and subsequently aborted. Here, we need to update the abortedTaskNum.
                    ++abortedTaskNum;
                    entity.counterRoutineLoadAbortedTasksTotal.increase(1L);
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
                routineLoadTaskInfo.afterAborted(txnState, txnOperated, txnStatusChangeReasonString);
                ++abortedTaskNum;
                entity.counterRoutineLoadAbortedTasksTotal.increase(1L);
                setOtherMsg(txnStatusChangeReasonString);
                TxnStatusChangeReason txnStatusChangeReason = null;
                if (txnStatusChangeReasonString != null) {
                    txnStatusChangeReason =
                            TxnStatusChangeReason.fromString(txnStatusChangeReasonString);
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
                        TxnStatusChangeReason.fromString(txnState.getReason()))) {
            replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        }
        this.abortedTaskNum++;
        StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
        if (streamLoadTask != null) {
            streamLoadTask.replayOnAborted(txnState);
        }
        LOG.debug("replay on aborted: {}, has attachment: {}", txnState, txnState.getTxnCommitAttachment() == null);
    }

    private Optional<ErrorReason> shouldPauseOnTxnStateChange(String txnStatusChangeReasonStr) {
        TxnStatusChangeReason reason =
                TxnStatusChangeReason.fromString(txnStatusChangeReasonStr);
        if (reason == null) {
            return Optional.empty();
        }
        switch (reason) {
            case FILTERED_ROWS:
                return Optional.of(new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                        ERR_TOO_MANY_ERROR_ROWS.formatErrorMsg(txnStatusChangeReasonStr, "max_filter_ratio")));
            case PARSE_ERROR:
                return !isPauseOnFatalParseError() ? Optional.empty() : Optional.of(
                        new ErrorReason(ERR_LOAD_DATA_PARSE_ERROR.getCode(),
                                ERR_LOAD_DATA_PARSE_ERROR.formatErrorMsg(txnStatusChangeReasonStr)));
            default:
                return Optional.empty();
        }
    }

    // check task exists or not before call method
    private void executeTaskOnTxnStatusChanged(RoutineLoadTaskInfo routineLoadTaskInfo, TransactionState txnState,
                                               TransactionStatus txnStatus, String txnStatusChangeReasonStr)
            throws StarRocksException {
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
                TxnStatusChangeReason.fromString(txnStatusChangeReasonStr))) {
            // step2: update job progress
            updateProgress(rlTaskTxnCommitAttachment);
        }

        if (rlTaskTxnCommitAttachment != null && !Strings.isNullOrEmpty(rlTaskTxnCommitAttachment.getErrorLogUrl())) {
            errorLogUrls.add(rlTaskTxnCommitAttachment.getErrorLogUrl());
        }

        routineLoadTaskInfo.setTxnStatus(txnStatus);

        Optional<ErrorReason> pauseOptional = shouldPauseOnTxnStateChange(txnStatusChangeReasonStr);
        if (pauseOptional.isPresent()) {
            updateState(JobState.PAUSED, pauseOptional.get(), false /* not replay */);
            LOG.warn(
                    "routine load task [job name {}, task id {}] aborted because of {}, change state to PAUSED",
                    name, routineLoadTaskInfo.getId().toString(), txnStatusChangeReasonStr);
            return;
        }

        if (state == JobState.RUNNING) {
            if (txnStatus == TransactionStatus.ABORTED) {
                RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(
                        System.currentTimeMillis() + taskSchedIntervalS * 1000, routineLoadTaskInfo);
                newRoutineLoadTaskInfo.setMsg("previous task aborted because of " + txnStatusChangeReasonStr, true);
                GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                        .releaseBeTaskSlot(routineLoadTaskInfo.getWarehouseId(),
                                routineLoadTaskInfo.getJobId(), routineLoadTaskInfo.getBeId());
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
            throws StarRocksException {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);

        if (table instanceof MaterializedView) {
            throw new AnalysisException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tblName, tblName));
        }

        if (!table.isOlapOrCloudNativeTable()) {
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

    public void updateState(JobState jobState, ErrorReason reason, boolean isReplay) throws StarRocksException {
        writeLock();
        try {
            unprotectUpdateState(jobState, reason, isReplay);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) throws
            StarRocksException {
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
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
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
        WarehouseIdleChecker.updateJobLastFinishTime(warehouseId);
    }

    private void executeCancel(ErrorReason reason) {
        cancelReason = reason;
        state = JobState.CANCELLED;
        clearTasks();
        endTimestamp = System.currentTimeMillis();
        WarehouseIdleChecker.updateJobLastFinishTime(warehouseId);
    }

    private void clearTasks() {
        for (RoutineLoadTaskInfo task : routineLoadTaskInfoList) {
            if (task.getBeId() != RoutineLoadTaskInfo.INVALID_BE_ID) {
                GlobalStateMgr.getCurrentState().getRoutineLoadMgr().
                        releaseBeTaskSlot(task.getWarehouseId(), task.getJobId(), task.getBeId());
            }
        }
        routineLoadTaskInfoList.clear();
    }

    public void update() throws StarRocksException {
        // check if db and table exist
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
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
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
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

    protected boolean unprotectNeedReschedule() throws StarRocksException {
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
                                               TxnStatusChangeReason txnStatusChangeReason);

    protected abstract String getStatistic();

    public List<String> getShowInfo() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Table tbl = null;
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
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
            String progressJsonStr = getProgress().toJsonString();
            row.add(progressJsonStr);
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
            // tracking url
            if (!errorLogUrls.isEmpty()) {
                row.add(Joiner.on(", ").join(errorLogUrls));
                row.add("select tracking_log from information_schema.load_tracking_logs where job_id=" + id);
            } else {
                row.add("");
                row.add("");
            }
            row.add(otherMsg);

            if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
                try {
                    Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                    row.add(warehouse.getName());
                } catch (Exception e) {
                    row.add(e.getMessage());
                }
            }
            row.add(getSourceProgressString());
            row.add(getSourceLagString(progressJsonStr));
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
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
            jobProperties.put("columnSeparator", columnSeparator == null ? "\t" : columnSeparator.getOriSeparator());
            jobProperties.put("rowDelimiter", rowDelimiter == null ? "\n" : rowDelimiter.getOriDelimiter());
        }
        jobProperties.put("maxErrorNum", String.valueOf(maxErrorNum));
        jobProperties.put("maxFilterRatio", String.valueOf(maxFilterRatio));
        jobProperties.put("maxBatchIntervalS", String.valueOf(taskSchedIntervalS));
        jobProperties.put("maxBatchRows", String.valueOf(maxBatchRows));
        jobProperties.put("currentTaskConcurrentNum", String.valueOf(currentTaskConcurrentNum));
        jobProperties.put("desireTaskConcurrentNum", String.valueOf(desireTaskConcurrentNum));
        jobProperties.put("taskConsumeSecond", String.valueOf(taskConsumeSecond));
        jobProperties.put("taskTimeoutSecond", String.valueOf(taskTimeoutSecond));
        jobProperties.putAll(this.jobProperties);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jobProperties);
    }

    public String jobPropertiesToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(\n");
        sb.append("\"").append(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY).append("\"=\"");
        sb.append(desireTaskConcurrentNum).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY).append("\"=\"");
        sb.append(maxErrorNum).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY).append("\"=\"");
        sb.append(maxFilterRatio).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY).append("\"=\"");
        sb.append(taskSchedIntervalS).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY).append("\"=\"");
        sb.append(maxBatchRows).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.TASK_CONSUME_SECOND).append("\"=\"");
        sb.append(taskConsumeSecond).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND).append("\"=\"");
        sb.append(taskTimeoutSecond).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.FORMAT).append("\"=\"");
        sb.append(getFormat()).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.JSONPATHS).append("\"=\"");
        sb.append(getJsonPaths()).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY).append("\"=\"");
        sb.append(isStripOuterArray()).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.JSONROOT).append("\"=\"");
        sb.append(getJsonRoot()).append("\",\n");

        sb.append("\"").append(LoadStmt.STRICT_MODE).append("\"=\"");
        sb.append(isStrictMode()).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.PAUSE_ON_FATAL_PARSE_ERROR).append("\"=\"");
        sb.append(isPauseOnFatalParseError()).append("\",\n");

        sb.append("\"").append(LoadStmt.TIMEZONE).append("\"=\"");
        sb.append(getTimezone()).append("\",\n");

        sb.append("\"").append(LoadStmt.PARTIAL_UPDATE).append("\"=\"");
        sb.append(isPartialUpdate()).append("\",\n");

        if (getMergeCondition() != null) {
            sb.append("\"").append(LoadStmt.MERGE_CONDITION).append("\"=\"");
            sb.append(getMergeCondition()).append("\",\n");
        }

        sb.append("\"").append(CreateRoutineLoadStmt.TRIMSPACE).append("\"=\"");
        sb.append(isTrimspace()).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.ENCLOSE).append("\"=\"");
        sb.append(StringEscapeUtils.escapeJava(String.valueOf(getEnclose()))).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.ESCAPE).append("\"=\"");
        sb.append(StringEscapeUtils.escapeJava(String.valueOf(getEscape()))).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.LOG_REJECTED_RECORD_NUM_PROPERTY).append("\"=\"");
        sb.append(getLogRejectedRecordNum());

        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            sb.append("\",\n");
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_WAREHOUSE).append("\"=\"");
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
            if (warehouse != null) {
                sb.append(warehouse.getName()).append("\"\n");
            } else {
                sb.append("NULL").append("\"\n");
            }
        } else {
            sb.append("\"\n");
        }

        sb.append(")\n");
        return sb.toString();
    }

    public abstract String dataSourcePropertiesToSql();

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

        origStmt = OriginStatement.read(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            jobProperties.put(key, value);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            sessionVariables.put(key, value);
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
    private void modifyCommonJobProperties(Map<String, String> jobProperties) throws DdlException {
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
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.TASK_CONSUME_SECOND)) {
            this.taskConsumeSecond = Long.parseLong(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
        }
        if (copiedJobProperties.containsKey(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND)) {
            this.taskTimeoutSecond = Long.parseLong(
                    copiedJobProperties.remove(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
        }

        if (copiedJobProperties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = copiedJobProperties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
            if (warehouse != null) {
                this.warehouseId = warehouse.getId();
            } else {
                // because default_warehouse not persist, so when replay, warehouse will be null, cause npe
                if (warehouseName.equalsIgnoreCase(WarehouseManager.DEFAULT_WAREHOUSE_NAME)) {
                    this.warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
                } else {
                    throw new DdlException("Warehouse " + warehouseName + " not exist");
                }
            }
        }

        this.jobProperties.putAll(copiedJobProperties);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        setRoutineLoadDesc(CreateRoutineLoadStmt.getLoadDesc(origStmt, sessionVariables));
    }

    public TRoutineLoadJobInfo toThrift() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Table tbl = null;
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
        readLock();
        try {
            TRoutineLoadJobInfo info = new TRoutineLoadJobInfo();
            info.setId(id);
            info.setName(name);
            info.setCreate_time(TimeUtils.longToTimeString(createTimestamp));
            info.setPause_time(TimeUtils.longToTimeString(pauseTimestamp));
            info.setEnd_time(TimeUtils.longToTimeString(endTimestamp));
            info.setDb_name(db == null ? String.valueOf(dbId) : db.getFullName());
            info.setTable_name(tbl == null ? String.valueOf(tableId) : tbl.getName());
            info.setState(getState().name());
            info.setCurrent_task_num(getSizeOfRoutineLoadTaskInfoList());
            info.setJob_properties(jobPropertiesToJsonString());
            info.setData_source_properties(dataSourcePropertiesJsonToString());
            info.setCustom_properties(customPropertiesJsonToString());
            info.setData_source_type(dataSourceType.name());
            info.setStatistic(getStatistic());
            String progressJsonStr = getProgress().toJsonString();
            info.setProgress(progressJsonStr);
            switch (state) {
                case PAUSED:
                    info.setReasons_of_state_changed(pauseReason == null ? "" : pauseReason.toString());
                    break;
                case CANCELLED:
                    info.setReasons_of_state_changed(cancelReason == null ? "" : cancelReason.toString());
                    break;
                default:
                    info.setReasons_of_state_changed("");
            }

            // tracking url
            if (!errorLogUrls.isEmpty()) {
                info.setError_log_urls(Joiner.on(",").join(errorLogUrls));
                info.setTracking_sql("select tracking_log from information_schema.load_tracking_logs where job_id=" + id);
            }
            info.setOther_msg(otherMsg);
            info.setLatest_source_position(getSourceProgressString());
            info.setOffset_lag(getSourceLagString(progressJsonStr));
            return info;
        } finally {
            readUnlock();
        }
    }

    protected void updateSubstate(JobSubstate substate, ErrorReason reason) throws StarRocksException {
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

    public void updateSubstateStable() throws StarRocksException {
        updateSubstate(JobSubstate.STABLE, null);
    }

    public void updateSubstate() throws StarRocksException {
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(txnState.getTransactionId());
        if (streamLoadTask != null) {
            streamLoadTask.replayOnVisible(txnState);
        }
    }
}
