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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.PartitionUtils;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.OptimizeClause;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OptimizeJobV2 extends AlterJobV2 implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2.class);

    // The optimize job will wait all transactions before this txn id finished, then send the optimize tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    private final String postfix;

    @SerializedName(value = "tmpPartitionIds")
    private List<Long> tmpPartitionIds = Lists.newArrayList();

    @SerializedName(value = "optimizeClause")
    private OptimizeClause optimizeClause;

    private String dbName = "";
    private Map<String, String> properties = Maps.newHashMap();

    @SerializedName(value = "rewriteTasks")
    private List<OptimizeTask> rewriteTasks = Lists.newArrayList();
    private int progress = 0;

    public OptimizeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                         OptimizeClause optimizeClause) {
        this(jobId, dbId, tableId, tableName, timeoutMs);

        this.optimizeClause = optimizeClause;
    }

    public OptimizeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
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
        return "optimize-" + this.postfix;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<OptimizeTask> getOptimizeTasks() {
        return rewriteTasks;
    }

    private Database getAndReadLockDatabase(long dbId) throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database id: " + dbId + " does not exist");
        }
        if (!db.readLockAndCheckExist()) {
            throw new AlterCancelException("insert overwrite commit failed because locking db: " + dbId + " failed");
        }
        return db;
    }

    private OlapTable checkAndGetTable(Database db, long tableId) throws AlterCancelException {
        Table table = db.getTable(tableId);
        if (table == null) {
            throw new AlterCancelException("table: " + tableId + " does not exist in database: " + db.getFullName());
        }
        Preconditions.checkState(table instanceof OlapTable);
        return (OlapTable) table;
    }

    private Database getAndWriteLockDatabase(long dbId) throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database id:" + dbId + " does not exist");
        }
        if (!db.writeLockAndCheckExist()) {
            throw new AlterCancelException("insert overwrite commit failed because locking db:" + dbId + " failed");
        }
        return db;
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create temp partitions
        for (int i = 0; i < optimizeClause.getSourcePartitionIds().size(); ++i) {
            tmpPartitionIds.add(GlobalStateMgr.getCurrentState().getNextId());
        }

        long createPartitionStartTimestamp = System.currentTimeMillis();
        OlapTable targetTable;
        db.readLock();
        try {
            targetTable = checkAndGetTable(db, tableId);
        } finally {
            db.readUnlock();
        }
        try {
            PartitionUtils.createAndAddTempPartitionsForTable(db, targetTable, postfix,
                    optimizeClause.getSourcePartitionIds(), getTmpPartitionIds(), optimizeClause.getDistributionDesc());
            LOG.debug("create temp partitions {} success. job: {}", getTmpPartitionIds(), jobId);
        } catch (Exception e) {
            LOG.warn("create temp partitions failed", e);
            throw new AlterCancelException("create temp partitions failed " + e);
        }
        long createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;

        // wait previous transactions finished
        this.watershedTxnId =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;
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

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, optimize job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to optimize table. job: {}", jobId);

        List<String> tmpPartitionNames;
        List<String> partitionNames = Lists.newArrayList();
        List<Long> partitionLastVersion = Lists.newArrayList();
        Database db = getAndReadLockDatabase(dbId);
        try {
            dbName = db.getFullName();
            OlapTable targetTable = checkAndGetTable(db, tableId);
            if (getTmpPartitionIds().stream().anyMatch(id -> targetTable.getPartition(id) == null)) {
                throw new AlterCancelException("partitions changed during insert");
            }
            tmpPartitionNames = getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            optimizeClause.getSourcePartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId)).forEach(
                        partition -> {
                            partitionNames.add(partition.getName());
                            partitionLastVersion.add(partition.getSubPartitions().stream()
                                    .mapToLong(PhysicalPartition::getVisibleVersion).sum());
                        }
            );
        } finally {
            db.readUnlock();
        }

        // start insert job
        for (int i = 0; i < tmpPartitionNames.size(); ++i) {
            String tmpPartitionName = tmpPartitionNames.get(i);
            String partitionName = partitionNames.get(i);
            String rewriteSql = "insert into " + tableName + " TEMPORARY PARTITION ("
                    + tmpPartitionName + ") select * from " + tableName + " partition (" + partitionName + ")";
            String taskName = getName() + "_" + tmpPartitionName;
            OptimizeTask rewriteTask = TaskBuilder.buildOptimizeTask(taskName, properties, rewriteSql, dbName);
            rewriteTask.setPartitionName(partitionName);
            rewriteTask.setTempPartitionName(tmpPartitionName);
            rewriteTask.setLastVersion(partitionLastVersion.get(i));
            rewriteTasks.add(rewriteTask);
        }

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        for (OptimizeTask rewriteTask : rewriteTasks) {
            try {
                taskManager.createTask(rewriteTask, false);
                taskManager.executeTask(rewriteTask.getName());
                LOG.debug("create rewrite task {}", rewriteTask.toString());
            } catch (DdlException e) {
                rewriteTask.setOptimizeTaskState(Constants.TaskRunState.FAILED);
                LOG.warn("create rewrite task failed", e);
            }
        }
        
        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer optimize job {} state to {}", jobId, this.jobState);
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

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        OlapTable tbl = null;
        db.readLock();
        try {
            tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
        } finally {
            db.readUnlock();
        }

        // wait insert tasks finished
        boolean allFinished = true;
        int progress = 0;
        for (OptimizeTask rewriteTask : rewriteTasks) {
            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED
                    || rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.SUCCESS) {
                progress += 100 / rewriteTasks.size();
                continue;
            }
            TaskRun taskRun = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                    .getRunnableTaskRun(rewriteTask.getId());
            if (taskRun != null) {
                if (taskRun.getStatus() != null) {
                    progress += taskRun.getStatus().getProgress() / rewriteTasks.size();
                }
                allFinished = false;
                continue;
            }
            TaskRunStatus status = GlobalStateMgr.getCurrentState().getTaskManager()
                    .getTaskRunManager().getTaskRunHistory().getTaskByName(rewriteTask.getName());
            if (status == null) {
                allFinished = false;
                continue;
            }

            if (status.getState() == Constants.TaskRunState.FAILED) {
                LOG.warn("optimize task {} failed", rewriteTask.getName());
                rewriteTask.setOptimizeTaskState(Constants.TaskRunState.FAILED);
            }
            progress += 100 / rewriteTasks.size();
        }

        if (!allFinished) {
            LOG.debug("wait insert tasks to be finished, optimize job: {}", jobId);
            this.progress = progress;
            return;
        }

        this.progress = 99;

        LOG.debug("all insert overwrite tasks finished, optimize job: {}", jobId);

        // replace partition
        db.writeLock();
        try {
            onFinished(tbl, false);
        } finally {
            db.writeUnlock();
        }

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

    private void onFinished(OlapTable targetTable, boolean isReplay) throws AlterCancelException {
        try {
            List<String> tmpPartitionNames = getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());

            List<String> sourcePartitionNames = Lists.newArrayList();
            Map<String, Long> partitionLastVersion = Maps.newHashMap();
            optimizeClause.getSourcePartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId)).forEach(
                        partition -> {
                            sourcePartitionNames.add(partition.getName());
                            partitionLastVersion.put(partition.getName(), partition.getSubPartitions().stream()
                                    .mapToLong(PhysicalPartition::getVisibleVersion).sum());
                        }
            );

            boolean hasFailedTask = false;
            for (OptimizeTask rewriteTask : rewriteTasks) {
                if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED
                        || partitionLastVersion.get(rewriteTask.getPartitionName()) != rewriteTask.getLastVersion()) {
                    LOG.info("rewrite task {} state {} failed or partition {} version {} change to {}",
                            rewriteTask.getName(), rewriteTask.getOptimizeTaskState(), rewriteTask.getPartitionName(),
                            rewriteTask.getLastVersion(), partitionLastVersion.get(rewriteTask.getPartitionName()));
                    sourcePartitionNames.remove(rewriteTask.getPartitionName());
                    tmpPartitionNames.remove(rewriteTask.getTempPartitionName());
                    targetTable.dropTempPartition(rewriteTask.getTempPartitionName(), true);
                    hasFailedTask = true;
                }
            }

            if (sourcePartitionNames.isEmpty()) {
                throw new AlterCancelException("all partitions rewrite failed");
            }

            if (hasFailedTask && (optimizeClause.getKeysDesc() != null || !optimizeClause.getSortKeys().isEmpty())) {
                rewriteTasks.forEach(
                        rewriteTask -> targetTable.dropTempPartition(rewriteTask.getTempPartitionName(), true));
                throw new AlterCancelException("optimize keysType or sort keys failed since some partitions rewrite failed");
            }

            Set<Tablet> sourceTablets = Sets.newHashSet();
            sourcePartitionNames.forEach(name -> {
                Partition partition = targetTable.getPartition(name);
                for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    sourceTablets.addAll(index.getTablets());
                }
            });

            boolean allPartitionOptimized = false;
            if (!hasFailedTask && optimizeClause.getDistributionDesc() != null) {
                Set<String> targetPartitionNames = targetTable.getPartitionNames();
                long targetPartitionNum = targetPartitionNames.size();
                targetPartitionNames.retainAll(sourcePartitionNames);

                if (targetPartitionNames.size() == targetPartitionNum && targetPartitionNum == sourcePartitionNames.size()) {
                    // all partitions of target table are optimized
                    // so that we can change default distribution info of target table
                    allPartitionOptimized = true;
                } else if (optimizeClause.getDistributionDesc().getType() != targetTable.getDefaultDistributionInfo().getType()) {
                    // partial partitions of target table are optimized
                    throw new AlterCancelException("can not change distribution type of target table" +
                            "since partial partitions are not optimized");
                }
            }

            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                targetTable.replaceTempPartitions(sourcePartitionNames, tmpPartitionNames, true, false);
            } else if (partitionInfo instanceof SinglePartitionInfo) {
                targetTable.replacePartition(sourcePartitionNames.get(0), tmpPartitionNames.get(0));
            } else {
                throw new AlterCancelException("partition type " + partitionInfo.getType() + " is not supported");
            }
            if (!isReplay) {
                // mark all source tablet ids force delete to drop it directly on BE,
                // not to move it to trash
                sourceTablets.forEach(GlobalStateMgr.getCurrentInvertedIndex()::markTabletForceDelete);

                try {
                    GlobalStateMgr.getCurrentColocateIndex().updateLakeTableColocationInfo(targetTable,
                            true /* isJoin */, null /* expectGroupId */);
                } catch (DdlException e) {
                    // log an error if update colocation info failed, insert overwrite already succeeded
                    LOG.error("table {} update colocation info failed after insert overwrite, {}.", tableId, e.getMessage());
                }

                targetTable.lastSchemaUpdateTime.set(System.currentTimeMillis());
            }
            if (allPartitionOptimized) {
                targetTable.setDefaultDistributionInfo(
                        optimizeClause.getDistributionDesc().toDistributionInfo(targetTable.getColumns()));
            }
            targetTable.setState(OlapTableState.NORMAL);

            LOG.info("finish replace partitions dbId:{}, tableId:{}, source partitions:{}, tmp partitions:{}",
                    dbId, tableId, sourcePartitionNames, tmpPartitionNames);
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into dbId:{}, tableId:{}",
                    dbId, tableId, e);
            throw new AlterCancelException("replace partitions failed " + e);
        }
    }

    /**
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected boolean cancelImpl(String errMsg) {
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
        try {
            db = getAndWriteLockDatabase(dbId);
        } catch (Exception e) {
            LOG.warn("get and write lock database failed when cancel job: {}", jobId, e);
            return;
        }
        try {
            Table table = db.getTable(tableId);
            if (table == null) {
                throw new AlterCancelException("table:" + tableId + " does not exist in database:" + db.getFullName());
            }
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable targetTable = (OlapTable) table;
            Set<Tablet> sourceTablets = Sets.newHashSet();
            if (getTmpPartitionIds() != null) {
                for (long pid : getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);

                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                            // hash set is able to deduplicate the elements
                            sourceTablets.addAll(index.getTablets());
                        }
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }
            // mark all source tablet ids force delete to drop it directly on BE,
            // not to move it to trash
            sourceTablets.forEach(GlobalStateMgr.getCurrentInvertedIndex()::markTabletForceDelete);
            targetTable.setState(OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("exception when cancel optimize job.", e);
        } finally {
            db.writeUnlock();
        }
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     */
    private void replayPending(OptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            // set table state
            tbl.setState(OlapTableState.SCHEMA_CHANGE);
        } finally {
            db.writeUnlock();
        }

        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay pending optimize job: {}", jobId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(OptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        OlapTable tbl = null;
        db.writeLock();
        try {
            tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
        } finally {
            db.writeUnlock();
        }

        for (long id : replayedJob.getTmpPartitionIds()) {
            tmpPartitionIds.add(id);
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay waiting txn optimize job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(OptimizeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    onFinished(tbl, true);
                }
            } catch (Exception e) {
                LOG.warn("failed to replay finished job: {}", jobId, e);
            } finally {
                db.writeUnlock();
            }
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;

        LOG.info("replay finished optimize job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(OptimizeJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled optimize job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        OptimizeJobV2 replayedOptimizeJob = (OptimizeJobV2) replayedJob;
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
        info.add(optimizeClause.toString());
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
        String json = GsonUtils.GSON.toJson(this, OptimizeJobV2.class);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (jobState != JobState.PENDING) {
            return;
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
