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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BulkLoadJob.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.AuthorizationInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.FailMsg;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

/**
 * parent class of BrokerLoadJob and SparkLoadJob from load stmt
 */
public abstract class BulkLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(BulkLoadJob.class);

    // input params
    @SerializedName("bds")
    protected BrokerDesc brokerDesc;
    // this param is used to persist the expr of columns
    // the origin stmt is persisted instead of columns expr
    // the expr of columns will be reanalyze when the log is replayed
    @SerializedName("ost")
    protected OriginStatement originStmt;

    // include broker desc and data desc
    protected BrokerFileGroupAggInfo fileGroupAggInfo = new BrokerFileGroupAggInfo();
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();
    protected List<TabletFailInfo> failInfos = Lists.newArrayList();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    @SerializedName("svs")
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    protected static final String PRIORITY_SESSION_VARIABLE_KEY = "priority.session.variable.key";
    public static final String LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY = "log.rejected.record.num.session.variable.key";
    public static final String CURRENT_USER_IDENT_KEY = "current.user.ident.key";
    public static final String CURRENT_QUALIFIED_USER_KEY = "current.qualified.user.key";

    // only for log replay
    public BulkLoadJob() {
        super();
    }

    public BulkLoadJob(long dbId, String label, OriginStatement originStmt) throws MetaNotFoundException {
        super(dbId, label);
        this.originStmt = originStmt;
        this.authorizationInfo = gatherAuthInfo();

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
            sessionVariables.put(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE, var.getloadTransmissionCompressionType());
            sessionVariables.put(CURRENT_QUALIFIED_USER_KEY, ConnectContext.get().getQualifiedUser());
            sessionVariables.put(CURRENT_USER_IDENT_KEY, ConnectContext.get().getCurrentUserIdentity().toString());
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    public static BulkLoadJob fromLoadStmt(LoadStmt stmt, ConnectContext context) throws DdlException {
        // get db id
        String dbName = stmt.getLabel().getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        // create job
        BulkLoadJob bulkLoadJob = null;
        try {
            switch (stmt.getEtlJobType()) {
                case BROKER:
                    bulkLoadJob = new BrokerLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                            stmt.getBrokerDesc(), stmt.getOrigStmt(), context);
                    break;
                case SPARK:
                    bulkLoadJob = new SparkLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                            stmt.getResourceDesc(), stmt.getOrigStmt());
                    break;
                case MINI:
                case DELETE:
                case HADOOP:
                case INSERT:
                    throw new DdlException("LoadManager only support create broker and spark load job from stmt.");
                default:
                    throw new DdlException("Unknown load job type.");
            }
            bulkLoadJob.setJobProperties(stmt.getProperties());
            if (bulkLoadJob.priority != 0) {
                bulkLoadJob.sessionVariables.put(BulkLoadJob.PRIORITY_SESSION_VARIABLE_KEY,
                        Integer.toString(bulkLoadJob.priority));
            }
            if (bulkLoadJob.logRejectedRecordNum != 0) {
                bulkLoadJob.sessionVariables.put(BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY,
                        Long.toString(bulkLoadJob.logRejectedRecordNum));
            }
            bulkLoadJob.checkAndSetDataSourceInfo(db, stmt.getDataDescriptions());
            return bulkLoadJob;
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void checkAndSetDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        // check data source info
        db.readLock();
        try {
            for (DataDescription dataDescription : dataDescriptions) {
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
                fileGroup.parse(db, dataDescription);
                fileGroupAggInfo.addFileGroup(fileGroup);
            }
        } finally {
            db.readUnlock();
        }
    }

    private AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames(false));
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Set<String> result = Sets.newHashSet();
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            for (long tableId : fileGroupAggInfo.getAllTableIds()) {
                result.add(String.valueOf(tableId));
            }
            return result;
        }
        for (long tableId : fileGroupAggInfo.getAllTableIds()) {
            Table table = database.getTable(tableId);
            if (table == null) {
                result.add(String.valueOf(tableId));
            } else {
                result.add(table.getName());
            }
        }
        return result;
    }

    @Override
    public Set<String> getTableNames(boolean noThrow) throws MetaNotFoundException {
        Set<String> result = Sets.newHashSet();
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            if (noThrow) {
                return result;
            } else {
                throw new MetaNotFoundException("Database " + dbId + "has been deleted");
            }
        }
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        for (long tableId : fileGroupAggInfo.getAllTableIds()) {
            Table table = database.getTable(tableId);
            if (table == null) {
                if (!noThrow) {
                    throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
                }
            } else {
                result.add(table.getName());
            }
        }
        return result;
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
        writeLock();
        try {
            // check if job has been completed
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            LoadTask loadTask = idToTasks.get(taskId);
            if (loadTask == null) {
                return;
            }
            if (loadTask.getRetryTime() <= 0) {
                unprotectedExecuteCancel(failMsg, true);
                logFinalOperation();
                return;
            } else {
                failMsg.setMsg("Still retrying, error: " + failMsg.getMsg());
                unprotectUpdateFailMsg(failMsg);

                // retry task
                idToTasks.remove(loadTask.getSignature());
                if (loadTask instanceof LoadLoadingTask) {
                    loadingStatus.getLoadStatistic().removeLoad(((LoadLoadingTask) loadTask).getLoadId());
                }
                loadTask.updateRetryInfo();
                idToTasks.put(loadTask.getSignature(), loadTask);
                // load id will be added to loadStatistic when executing this task
                try {
                    if (loadTask.getTaskType() == LoadTask.TaskType.PENDING) {
                        submitTask(GlobalStateMgr.getCurrentState().getPendingLoadTaskScheduler(), loadTask);
                    } else if (loadTask.getTaskType() == LoadTask.TaskType.LOADING) {
                        submitTask(GlobalStateMgr.getCurrentState().getLoadingLoadTaskScheduler(), loadTask);
                    } else {
                        throw new LoadException(String.format("Unknown load task type: %s. task id: %d, job id, %d",
                                loadTask.getTaskType(), loadTask.getSignature(), id));
                    }
                } catch (RejectedExecutionException | LoadException e) {
                    unprotectedExecuteCancel(failMsg, true);
                    logFinalOperation();
                    return;
                }
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * If the db or table could not be found, the Broker load job will be cancelled.
     */
    @Override
    public void analyze() {
        if (originStmt == null || Strings.isNullOrEmpty(originStmt.originStmt)) {
            return;
        }
        // Reset dataSourceInfo, it will be re-created in analyze
        fileGroupAggInfo = new BrokerFileGroupAggInfo();
        LoadStmt stmt = null;
        try {
            stmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parseFirstStatement(originStmt.originStmt,
                    Long.parseLong(sessionVariables.get(SessionVariable.SQL_MODE)));
            for (DataDescription dataDescription : stmt.getDataDescriptions()) {
                dataDescription.analyzeWithoutCheckPriv();
            }
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                throw new DdlException("Database[" + dbId + "] does not exist");
            }
            checkAndSetDataSourceInfo(db, stmt.getDataDescriptions());
        } catch (Exception e) {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("origin_stmt", originStmt)
                    .add("msg", "The failure happens in analyze, the load job will be cancelled with error:"
                            + e.getMessage())
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), false, true);
        }
    }

    @Override
    protected void replayTxnAttachment(TransactionState txnState) {
        if (txnState.getTxnCommitAttachment() == null) {
            // The txn attachment maybe null when broker load has been cancelled without attachment.
            // The end log of broker load has been record but the callback id of txnState hasn't been removed
            // So the callback of txn is executed when log of txn aborted is replayed.
            return;
        }
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment(), true);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        brokerDesc.write(out);
        originStmt.write(out);

        out.writeInt(sessionVariables.size());
        for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        brokerDesc = BrokerDesc.read(in);

        originStmt = OriginStatement.read(in);
        // The origin stmt does not be analyzed in here.
        // The reason is that it will thrown MetaNotFoundException when the tableId could not be found by tableName.
        // The origin stmt will be analyzed after the replay is completed.

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            sessionVariables.put(key, value);
        }
        if (sessionVariables.containsKey(PRIORITY_SESSION_VARIABLE_KEY)) {
            priority = Integer.parseInt(sessionVariables.get(PRIORITY_SESSION_VARIABLE_KEY));
        }
        if (sessionVariables.containsKey(LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY)) {
            logRejectedRecordNum = Long.parseLong(sessionVariables.get(LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY));
        }
    }
}
