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

package com.starrocks.alter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.PartitionUtils;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class OnlineOptimizeJobV2 extends AlterJobV2 implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(OnlineOptimizeJobV2.class);

    // The optimize job will wait all transactions before this txn id finished, then send the optimize tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    private String postfix;

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    private Future<Constants.TaskRunState> future = null;

    @SerializedName(value = "tmpPartitionIds")
    private List<Long> tmpPartitionIds = Lists.newArrayList();

    private OptimizeClause optimizeClause;

    private String dbName = "";
    private Map<String, String> properties = Maps.newHashMap();

    @SerializedName(value = "rewriteTasks")
    private List<OptimizeTask> rewriteTasks = Lists.newArrayList();
    private int progress = 0;

    @SerializedName(value = "sourcePartitionNames")
    private List<String> sourcePartitionNames = Lists.newArrayList();

    @SerializedName(value = "tmpPartitionNames")
    private List<String> tmpPartitionNames = Lists.newArrayList();

    @SerializedName(value = "allPartitionOptimized")
    private Boolean allPartitionOptimized = false;

    @SerializedName(value = "distributionInfo")
    private DistributionInfo distributionInfo;

    @SerializedName(value = "optimizeOperation")
    private String optimizeOperation = "";

    // for deserialization
    public OnlineOptimizeJobV2() {
        super(JobType.OPTIMIZE);
    }

    public OnlineOptimizeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                               OptimizeClause optimizeClause) {
        this(jobId, dbId, tableId, tableName, timeoutMs);

        this.optimizeClause = optimizeClause;
    }

    public OnlineOptimizeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.OPTIMIZE, dbId, tableId, tableName, timeoutMs);

        this.postfix = "_" + jobId;
    }

    public List<Long> getTmpPartitionIds() {
        return tmpPartitionIds;
    }

    public void setTmpPartitionIds(List<Long> tmpPartitionIds) {
        this.tmpPartitionIds = tmpPartitionIds;
    }

    public String getName() {
        return "online-optimize-" + this.postfix;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<OptimizeTask> getOptimizeTasks() {
        return rewriteTasks;
    }

    private OlapTable checkAndGetTable(Database db, long tableId) throws AlterCancelException {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new AlterCancelException("table: " + tableId + " does not exist in database: " + db.getFullName());
        }
        Preconditions.checkState(table instanceof OlapTable);
        return (OlapTable) table;
    }

    /**
     * runPendingJob():
     * 1. Create all temp partitions and wait them finished.
     * 2. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        LOG.info("begin to send create temp partitions. job: {}", jobId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        if (optimizeClause == null) {
            throw new AlterCancelException("optimize clause is null since FE restart, job: " + jobId);
        }
        if (optimizeClause.isTableOptimize()) {
            allPartitionOptimized = true;
        }

        // 1. create temp partitions
        for (int i = 0; i < optimizeClause.getSourcePartitionIds().size(); ++i) {
            tmpPartitionIds.add(GlobalStateMgr.getCurrentState().getNextId());
        }

        long createPartitionStartTimestamp = System.currentTimeMillis();
        OlapTable targetTable = checkAndGetTable(db, tableId);
        try {
            PartitionUtils.createAndAddTempPartitionsForTable(db, targetTable, postfix,
                        optimizeClause.getSourcePartitionIds(), getTmpPartitionIds(), optimizeClause.getDistributionDesc(),
                        warehouseId);
            LOG.debug("create temp partitions {} success. job: {}", getTmpPartitionIds(), jobId);
        } catch (Exception e) {
            LOG.warn("create temp partitions failed", e);
            throw new AlterCancelException("create temp partitions failed " + e);
        }
        long createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;

        // wait previous transactions finished
        this.jobState = JobState.WAITING_TXN;
        this.optimizeOperation = optimizeClause.toString();
        span.setAttribute("createPartitionElapse", createPartitionElapse);
        span.setAttribute("watershedTxnId", this.watershedTxnId);
        span.addEvent("setWaitingTxn");

        // write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("transfer optimize job {} state to {}, watershed txn_id: {}", jobId, this.jobState, watershedTxnId);
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, start insert into data to temp partitions.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        if (optimizeClause == null) {
            throw new AlterCancelException("optimize clause is null since FE restart, job: " + jobId);
        }

        List<String> tmpPartitionNames;
        List<String> partitionNames = Lists.newArrayList();
        List<String> tableColumnNames = Lists.newArrayList();

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            dbName = db.getFullName();
            OlapTable tbl = checkAndGetTable(db, tableId);
            if (getTmpPartitionIds().stream().anyMatch(id -> tbl.getPartition(id) == null)) {
                throw new AlterCancelException("partitions changed during insert");
            }
            tmpPartitionNames = getTmpPartitionIds().stream()
                    .map(partitionId -> tbl.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            optimizeClause.getSourcePartitionIds().stream()
                    .map(partitionId -> tbl.getPartition(partitionId)).forEach(
                        partition -> {
                            partitionNames.add(partition.getName());
                        }
            );
            tableColumnNames = tbl.getBaseSchema().stream().filter(column -> !column.isGeneratedColumn())
                        .map(col -> ParseUtil.backquote(col.getName())).collect(Collectors.toList());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // start insert job
        for (int i = 0; i < tmpPartitionNames.size(); ++i) {
            String tmpPartitionName = tmpPartitionNames.get(i);
            String partitionName = partitionNames.get(i);
            String rewriteSql = "insert into " + ParseUtil.backquote(dbName) + "."
                        + ParseUtil.backquote(tableName) + " TEMPORARY PARTITION ("
                        + ParseUtil.backquote(tmpPartitionName) + ") select " + Joiner.on(", ").join(tableColumnNames)
                        + " from " + ParseUtil.backquote(dbName) + "." + ParseUtil.backquote(tableName)
                        + " partition (" + ParseUtil.backquote(partitionName) + ")";
            String taskName = getName() + "_" + tmpPartitionName;
            OptimizeTask rewriteTask = TaskBuilder.buildOptimizeTask(taskName, properties, rewriteSql, dbName, warehouseId);
            rewriteTask.setPartitionName(partitionName);
            rewriteTask.setTempPartitionName(tmpPartitionName);
            rewriteTasks.add(rewriteTask);
        }

        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer optimize job {} state to {}", jobId, this.jobState);
    }

    private void enableDoubleWritePartition(Database db, OlapTable tbl, String sourcePartitionName, String tempPartitionName) {
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.OPTIMIZE);
            Partition temp = tbl.getPartition(tempPartitionName, true);
            if (temp != null) {
                Preconditions.checkState(temp.getSubPartitions().size() == 1);
                Partition p = tbl.getPartition(sourcePartitionName);
                if (p != null) {
                    Preconditions.checkState(p.getSubPartitions().size() == 1);
                    tbl.addDoubleWritePartition(p.getId(), temp.getId());

                    LOG.info("job {} add double write partition: {}:{} -> {}:{}", jobId, sourcePartitionName,
                                p.getId(), tempPartitionName, temp.getId());
                } else {
                    LOG.warn("job {} add double partition {} does not exist", jobId, sourcePartitionName);
                }
            } else {
                LOG.warn("job {} add double partition {} does not exist", jobId, tempPartitionName);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    private void disableDoubleWritePartition(Database db, OlapTable tbl) {
        try (AutoCloseableLock ignored =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
            tbl.clearDoubleWritePartition();
            LOG.info("job {} clear double write partitions", jobId);
        }
    }

    /**
     * runRunningJob()
     * 1. Wait insert into tasks to be finished.
     * 2. Replace partitions with temp partitions.
     * 3. Set job'state as FINISHED.
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        OlapTable tbl = checkAndGetTable(db, tableId);

        int progress = 0;

        for (OptimizeTask rewriteTask : rewriteTasks) {
            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED
                        || rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.SUCCESS) {
                progress += 100 / rewriteTasks.size();
                if (this.progress < progress) {
                    this.progress = progress;
                }
                continue;
            }

            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.PENDING) {
                enableDoubleWritePartition(db, tbl, rewriteTask.getPartitionName(), rewriteTask.getTempPartitionName());
                this.watershedTxnId = GlobalStateMgr.getCurrentState()
                            .getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
                rewriteTask.setOptimizeTaskState(Constants.TaskRunState.RUNNING);
            }

            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.RUNNING) {
                try {
                    if (!isPreviousLoadFinished()) {
                        LOG.info("wait transactions before {} to be finished, optimize job: {}", watershedTxnId, jobId);
                        return;
                    }
                } catch (AnalysisException e) {
                    throw new AlterCancelException(e.getMessage());
                }

                if (this.future != null) {
                    if (this.future.isDone()) {
                        try {
                            rewriteTask.setOptimizeTaskState(future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.warn("get rewrite task result failed", e);
                            rewriteTask.setOptimizeTaskState(Constants.TaskRunState.FAILED);
                        }

                        if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED) {
                            this.allPartitionOptimized = false;
                        }
                        this.future = null;
                    } else {
                        LOG.info("wait rewrite task {} to be finished, optimize job: {}", rewriteTask.getName(), jobId);
                        return;
                    }
                } else {
                    LOG.info("previous transactions are all finished, begin to optimize task {}. job: {}",
                                rewriteTask.toString(), jobId);
                    Callable<Constants.TaskRunState> task = () -> {
                        try {
                            executeSql(rewriteTask.getDefinition());
                            LOG.info("finish rewrite task: {}", rewriteTask.getName());
                            return Constants.TaskRunState.SUCCESS;
                        } catch (Exception e) {
                            LOG.warn("create rewrite task failed", e);
                            return Constants.TaskRunState.FAILED;
                        }
                    };
                    this.future = EXECUTOR.submit(task);
                    return;
                }
            }

            try (AutoCloseableLock ignore =
                        new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
                onTaskFinished(db, tbl, rewriteTask);
            }
        }

        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
            onFinished(db, tbl);
        }

        LOG.debug("all insert overwrite tasks finished, optimize job: {}", jobId);

        this.progress = 100;
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("optimize job finished: {}", jobId);
        this.span.end();
    }

    @Override
    protected void runFinishedRewritingJob() {
        // nothing to do
    }

    private void onFinished(Database db, OlapTable targetTable) throws AlterCancelException {
        try {
            targetTable.setState(OlapTableState.UPDATING_META);
            if (allPartitionOptimized && optimizeClause.getDistributionDesc() != null) {
                this.distributionInfo = optimizeClause.getDistributionDesc().toDistributionInfo(targetTable.getColumns());
                targetTable.setDefaultDistributionInfo(distributionInfo);
            }
            targetTable.setState(OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("optimize table failed dbId:{}, tableId:{} exception: {}", dbId, tableId, e);
            throw new AlterCancelException("optimize table failed " + e.getMessage());
        }
    }

    private void onTaskFinished(Database db, OlapTable targetTable, OptimizeTask rewriteTask) throws AlterCancelException {
        try {
            String sourcePartitionName = rewriteTask.getPartitionName();
            String tmpPartitionName = rewriteTask.getTempPartitionName();
            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED) {
                LOG.info("optimize job {} rewrite task {} state {} failed on partition {}",
                            jobId, rewriteTask.getName(), rewriteTask.getOptimizeTaskState(), sourcePartitionName);
                targetTable.dropTempPartition(rewriteTask.getTempPartitionName(), true);

                throw new AlterCancelException(sourcePartitionName + " rewrite task execute failed");
            }

            Set<Tablet> sourceTablets = Sets.newHashSet();
            Partition partition = targetTable.getPartition(sourcePartitionName);
            for (MaterializedIndex index
                    : partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                sourceTablets.addAll(index.getTablets());
            }

            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                targetTable.replaceTempPartitions(
                            Arrays.asList(sourcePartitionName), Arrays.asList(tmpPartitionName), true, false);
            } else if (partitionInfo instanceof SinglePartitionInfo) {
                targetTable.replacePartition(sourcePartitionName, tmpPartitionName);
            } else {
                throw new AlterCancelException("partition type " + partitionInfo.getType() + " is not supported");
            }

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), targetTable.getId(),
                        Arrays.asList(sourcePartitionName), Arrays.asList(tmpPartitionName),
                        true, false, partitionInfo instanceof SinglePartitionInfo);
            GlobalStateMgr.getCurrentState().getEditLog().logReplaceTempPartition(info);

            // mark all source tablet ids force delete to drop it directly on BE,
            // not to move it to trash
            sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

            try {
                GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(targetTable,
                            true /* isJoin */, null /* expectGroupId */);
            } catch (DdlException e) {
                // log an error if update colocation info failed, insert overwrite already succeeded
                LOG.error("table {} update colocation info failed after insert overwrite, {}.", tableId, e.getMessage());
            }
            targetTable.lastSchemaUpdateTime.set(System.nanoTime());

            LOG.info("optimize job {} finish replace partitions dbId:{}, tableId:{},"
                                    + "source partition:{}, tmp partition:{}",
                        jobId, dbId, tableId, sourcePartitionName, tmpPartitionName);
        } catch (Exception e) {
            allPartitionOptimized = false;
            LOG.warn("optimize table failed dbId:{}, tableId:{} exception: {}", dbId, tableId, DebugUtil.getStackTrace(e));
            throw new AlterCancelException("optimize table failed " + e.getMessage());
        } finally {
            disableDoubleWritePartition(db, targetTable);
        }
    }

    /**
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }
        cancelInternal();

        jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        span.setStatus(StatusCode.ERROR, errMsg);
        span.end();
        return true;
    }

    private void cancelInternal() {
        // remove temp partitions, and set state to NORMAL
        Database db = null;
        Locker locker = new Locker();
        try {
            db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                throw new AlterCancelException("database id:" + dbId + " does not exist");
            }

            if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
                throw new AlterCancelException("insert overwrite commit failed because locking db:" + dbId + " failed");
            }

        } catch (Exception e) {
            LOG.warn("get and write lock database failed when cancel job: {}", jobId, e);
            return;
        }

        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table == null) {
                throw new AlterCancelException("table:" + tableId + " does not exist in database:" + db.getFullName());
            }
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable targetTable = (OlapTable) table;

            disableDoubleWritePartition(db, targetTable);

            Set<Tablet> tmpTablets = Sets.newHashSet();
            if (getTmpPartitionIds() != null) {
                for (long pid : getTmpPartitionIds()) {
                    LOG.info("optimize job {} drop temp partition:{}", jobId, pid);

                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                            // hash set is able to deduplicate the elements
                            tmpTablets.addAll(index.getTablets());
                        }
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }
            // mark all tmp tablet ids force delete to drop it directly on BE,
            // not to move it to trash
            tmpTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);
            targetTable.setState(OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("exception when cancel optimize job.", e);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     */
    private void replayPending(OnlineOptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            // set table state
            tbl.setState(OlapTableState.OPTIMIZE);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;
        this.optimizeOperation = replayedJob.optimizeOperation;

        LOG.info("replay pending optimize job: {}", jobId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(OnlineOptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        OlapTable tbl = null;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        for (long id : replayedJob.getTmpPartitionIds()) {
            tmpPartitionIds.add(id);
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;
        this.optimizeOperation = replayedJob.optimizeOperation;

        LOG.info("replay waiting txn optimize job: {}", jobId);
    }

    private void onReplayFinished(OnlineOptimizeJobV2 replayedJob, OlapTable targetTable) {
        this.sourcePartitionNames = replayedJob.sourcePartitionNames;
        this.tmpPartitionNames = replayedJob.tmpPartitionNames;
        this.allPartitionOptimized = replayedJob.allPartitionOptimized;
        this.optimizeOperation = replayedJob.optimizeOperation;

        targetTable.setState(OlapTableState.UPDATING_META);
        Set<Tablet> sourceTablets = Sets.newHashSet();
        for (long id : replayedJob.getTmpPartitionIds()) {
            Partition partition = targetTable.getPartition(id);
            if (partition != null) {
                for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    sourceTablets.addAll(index.getTablets());
                }
                targetTable.dropTempPartition(partition.getName(), true);
            }
        }
        sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

        if (allPartitionOptimized) {
            this.distributionInfo = replayedJob.distributionInfo;
            LOG.debug("set distribution info to table: {}", distributionInfo);
            targetTable.setDefaultDistributionInfo(distributionInfo);
        }
        targetTable.setState(OlapTableState.NORMAL);

        LOG.info("finish replay optimize job {} dbId:{}, tableId:{},"
                                + "source partitions:{}, tmp partitions:{}, allOptimized:{}",
                    jobId, dbId, tableId, sourcePartitionNames, tmpPartitionNames, allPartitionOptimized);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(OnlineOptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                if (tbl != null) {
                    onReplayFinished(replayedJob, tbl);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;

        LOG.info("replay finished optimize job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(OnlineOptimizeJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled optimize job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        OnlineOptimizeJobV2 replayedOptimizeJob = (OnlineOptimizeJobV2) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
                replayPending(replayedOptimizeJob);
                break;
            case WAITING_TXN:
                replayWaitingTxn(replayedOptimizeJob);
                break;
            case FINISHED:
                replayFinished(replayedOptimizeJob);
                break;
            case CANCELLED:
                replayCancelled(replayedOptimizeJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(optimizeOperation != null ? optimizeOperation : "");
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        // progress
        info.add(progress);
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, OnlineOptimizeJobV2.class);
        Text.writeString(out, json);
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }

    protected void executeSql(String sql) throws Exception {
        LOG.info("execute sql : {}", sql);
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            context = ConnectContext.buildInner();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            context.setThreadLocalInfo();
        }
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
        if (parsedStmt instanceof InsertStmt) {
            ((InsertStmt) parsedStmt).setIsVersionOverwrite(true);
        }
        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);

        // set default session variables for stats context
        SessionVariable sessionVariable = context.getSessionVariable();
        sessionVariable.setUsePageCache(false);
        sessionVariable.setEnableMaterializedViewRewrite(false);
        sessionVariable.setInsertTimeoutS((int) timeoutMs / 2000);

        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        context.setStartTime();
        executor.execute();

        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("Execute sql fail | Error Message [{}] | {} | SQL [{}]",
                        context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
            throw new AlterCancelException(context.getState().getErrorMessage());
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.postfix = "_" + jobId;
    }
}
