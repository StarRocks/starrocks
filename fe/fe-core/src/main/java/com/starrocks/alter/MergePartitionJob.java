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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.OptimizeRange;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MergePartitionJob extends AlterJobV2 implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MergePartitionJob.class);

    // The merge partition job will wait all transactions before this txn id finished, then send the merge partition tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    private String postfix;

    @SerializedName(value = "tmpPartitionIdToSourcePartitionIds")
    Multimap<Long, Long> tempPartitionIdToSourcePartitionIds = ArrayListMultimap.create();

    @SerializedName(value = "tempPartitionNameToSourcePartitionNames")
    Multimap<String, String> tempPartitionNameToSourcePartitionNames = ArrayListMultimap.create();

    private OptimizeClause optimizeClause;

    private String dbName = "";
    private Map<String, String> properties = Maps.newHashMap();

    @SerializedName(value = "rewriteTasks")
    private List<OptimizeTask> rewriteTasks = Lists.newArrayList();
    private int progress = 0;

    @SerializedName(value = "distributionInfo")
    private DistributionInfo distributionInfo;

    @SerializedName(value = "optimizeOperation")
    private String optimizeOperation = "";

    // for deserialization
    public MergePartitionJob() {
        super(JobType.OPTIMIZE);
    }

    public MergePartitionJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                         OptimizeClause optimizeClause) {
        this(jobId, dbId, tableId, tableName, timeoutMs);

        this.optimizeClause = optimizeClause;
    }

    public MergePartitionJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.OPTIMIZE, dbId, tableId, tableName, timeoutMs);

        this.postfix = "job" + jobId;
    }

    public List<Long> getTmpPartitionIds() {
        return tempPartitionIdToSourcePartitionIds.keySet().stream().collect(Collectors.toList());
    }

    public Multimap<Long, Long> getTempPartitionIdToSourcePartitionIds() {
        return tempPartitionIdToSourcePartitionIds;
    }

    public String getName() {
        return "merge-partition-" + this.postfix;
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

    public void filterPartitionIds(OlapTable olapTable) throws AlterCancelException {
        ArrayList<Long> filterdPartitionIds = Lists.newArrayList();
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        ExpressionRangePartitionInfo sourcePartitionInfo;
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            sourcePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        } else {
            throw new AlterCancelException("partition type " + partitionInfo.getType() + " is not supported");
        }

        if (optimizeClause.getRange() != null) {
            OptimizeRange range = optimizeClause.getRange();
            String start = range.getStart().getStringValue();
            String end = range.getEnd().getStringValue();
            DateTimeFormatter startDateTimeFormat;
            DateTimeFormatter endDateTimeFormat;
            try {
                startDateTimeFormat = DateUtils.probeFormat(start);
                startTime = DateUtils.parseStringWithDefaultHSM(start, startDateTimeFormat);
                endDateTimeFormat = DateUtils.probeFormat(end);
                endTime = DateUtils.parseStringWithDefaultHSM(end, startDateTimeFormat);
                // if end time is date, add one day
                if (endDateTimeFormat == DateUtils.DATEKEY_FORMATTER || endDateTimeFormat == DateUtils.DATE_FORMATTER_UNIX) {
                    endTime = endTime.plusDays(1);
                }
            } catch (DateTimeParseException e) {
                LOG.warn(e);
                throw new AlterCancelException("parse start or end time failed: " + e.getMessage());
            }
        }
        for (long partitionId : optimizeClause.getSourcePartitionIds()) {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new AlterCancelException("partition " + partitionId + " does not exist in table " + tableName);
            }
            if (optimizeClause.getRange() == null) {
                filterdPartitionIds.add(partitionId);
                continue;
            }
            Range<PartitionKey> range = sourcePartitionInfo.getRange(partitionId);
            try {
                String rangeStart = range.lowerEndpoint().getKeys().get(0).getStringValue();
                String rangeEnd = range.upperEndpoint().getKeys().get(0).getStringValue();
                LocalDateTime rangeStartTime = DateUtils.parseStringWithDefaultHSM(
                        rangeStart, DateUtils.probeFormat(rangeStart));
                LocalDateTime rangeEndTime = DateUtils.parseStringWithDefaultHSM(
                        rangeEnd, DateUtils.probeFormat(rangeEnd));
                if (!rangeStartTime.isBefore(startTime) && !rangeEndTime.isAfter(endTime)) {
                    // Partition range is a subset of filter range
                    filterdPartitionIds.add(partitionId);
                } else if (rangeStartTime.isBefore(startTime) && rangeEndTime.isAfter(startTime)) {
                    // Partition range intersects with filter range but is not a subset
                    throw new AlterCancelException("Partition range intersects with filter range but is not a subset: "
                            + "partition " + partitionId + " with range [" + rangeStart + ", " + rangeEnd 
                            + "] intersects with filter range [" + startTime + ", " + endTime + "]");
                } else if (rangeStartTime.isBefore(endTime) && rangeEndTime.isAfter(endTime)) {
                    // Partition range intersects with filter range but is not a subset
                    throw new AlterCancelException("Partition range intersects with filter range but is not a subset: "
                            + "partition " + partitionId + " with range [" + rangeStart + ", " + rangeEnd 
                            + "] intersects with filter range [" + startTime + ", " + endTime + "]");
                } else {
                    LOG.info("Partition range is not in filter range: partition {} [{}, {}] is not in filter range [{}, {}]",
                            partitionId, rangeStart, rangeEnd, startTime, endTime);
                }
            } catch (DateTimeParseException e) {
                LOG.warn(e);
                throw new AlterCancelException("parse partition range start or end time failed: " + e.getMessage());
            }
        }

        optimizeClause.setSourcePartitionIds(filterdPartitionIds);
    }

    public Multimap<Long, Long> createMergedTempPartitionsFromPartitions(
            Database db, OlapTable olapTable, String namePostfix, List<Long> sourcePartitionIds,
            DistributionDesc distributionDesc, long warehouseId) throws DdlException {
        Multimap<Long, Long> tempPartitionIdToSourcePartitionIds = ArrayListMultimap.create();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ExpressionRangePartitionInfo);  
        ExpressionRangePartitionInfo sourcePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo; 
        for (int i = 0; i < sourcePartitionIds.size(); ++i) {
            long sourcePartitionId = sourcePartitionIds.get(i);

            Range<PartitionKey> range = sourcePartitionInfo.getRange(sourcePartitionId);

            List<String> partitionValues = Lists.newArrayList();
            partitionValues.add(range.lowerEndpoint().getKeys().get(0).getStringValue());
            LOG.info("create temp partition with partition values: {} {}",
                    partitionValues, olapTable.getPartition(sourcePartitionId).getName());
            
            AddPartitionClause addPartitionClause;
            try (AutoCloseableLock ignore = new AutoCloseableLock(
                    new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ)) {
                addPartitionClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                        olapTable, optimizeClause.getPartitionDesc(), List.of(partitionValues), true, namePostfix);
            } catch (AnalysisException ex) {
                LOG.warn(ex);
                throw new DdlException(ex.getMessage());
            }

            PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
            List<String> partitionNames;
            if (partitionDesc instanceof RangePartitionDesc) {
                partitionNames = ((RangePartitionDesc) partitionDesc).getPartitionColNames();
            } else {
                throw new DdlException("Unsupported partitionDesc");
            }

            boolean isPartitionExist = false;
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName, true);
                if (partition != null) {
                    tempPartitionIdToSourcePartitionIds.put(partition.getId(), sourcePartitionId);
                    isPartitionExist = true;
                }
            }

            if (isPartitionExist) {
                continue;
            }
            
            ConnectContext context = Util.getOrCreateInnerContext();
            context.setCurrentWarehouseId(warehouseId);
            try {
                try (AutoCloseableLock ignore = new AutoCloseableLock(
                        new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ)) {
                    AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                    analyzer.analyze(context, addPartitionClause);
                }
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        context, db, olapTable.getName(), addPartitionClause);
            } catch (Exception ex) {
                LOG.warn(ex);
                throw new DdlException(ex.getMessage());
            }
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName, true);
                if (partition == null) {
                    throw new DdlException("Partition " + partitionName
                            + " does not exist in table " + olapTable.getName());
                }
                tempPartitionIdToSourcePartitionIds.put(partition.getId(), sourcePartitionId);
            }
        }
        return tempPartitionIdToSourcePartitionIds;
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

        OlapTable targetTable = checkAndGetTable(db, tableId);
        // 1. filter merge partitions
        try {
            filterPartitionIds(targetTable);
        } catch (Exception e) {
            LOG.warn("filter merge partitions failed", e);
            throw new AlterCancelException("filter merge partitions failed " + e.getMessage());
        }

        // 1 temporary(merged) partition -> n source partitions
        // 2. create temp partitions
        long createPartitionStartTimestamp = System.currentTimeMillis();
        try {
            tempPartitionIdToSourcePartitionIds = createMergedTempPartitionsFromPartitions(db, targetTable, null,
                        optimizeClause.getSourcePartitionIds(), optimizeClause.getDistributionDesc(), warehouseId);
            LOG.info("create temp partitions {} success. job: {}", tempPartitionIdToSourcePartitionIds, jobId);
        } catch (Exception e) {
            LOG.warn("create temp partitions failed", e);
            throw new AlterCancelException("create temp partitions failed " + e);
        }
        long createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;

        // wait previous transactions finished
        this.watershedTxnId =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;
        this.optimizeOperation = optimizeClause.toString();
        span.setAttribute("createPartitionElapse", createPartitionElapse);
        span.setAttribute("watershedTxnId", this.watershedTxnId);
        span.addEvent("setWaitingTxn");

        // write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("transfer merge partition job {} state to {}, watershed txn_id: {}", jobId, this.jobState, watershedTxnId);
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

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, merge partition job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to merge partition table. job: {}", jobId);

        // Changed from List to Map
        Map<String, Long> partitionLastVersion = Maps.newHashMap();
        List<String> tableColumnNames = Lists.newArrayList();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database id: " + dbId + " does not exist");
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.READ)) {
            throw new AlterCancelException("insert overwrite commit failed because locking db: " + dbId + " failed");
        }

        try {
            dbName = db.getFullName();
            OlapTable targetTable = checkAndGetTable(db, tableId);
            if (getTmpPartitionIds().stream().anyMatch(id -> targetTable.getPartition(id) == null)) {
                throw new AlterCancelException("partitions changed during insert");
            }
            for (Map.Entry<Long, Collection<Long>> entry : tempPartitionIdToSourcePartitionIds.asMap().entrySet()) {
                Long targetPartitionId = entry.getKey();
                Collection<Long> sourcePartitionIds = entry.getValue();

                // Get the target partition
                Partition targetPartition = targetTable.getPartition(targetPartitionId);
                if (targetPartition == null) {
                    continue; // Target partition does not exist, skip it
                }

                String targetPartitionName = targetPartition.getName();
                long versionSum = 0; // Calculate the version sum of sourcePartitions

                // Iterate through all source partition IDs
                for (Long sourcePartitionId : sourcePartitionIds) {
                    Partition sourcePartition = targetTable.getPartition(sourcePartitionId);
                    if (sourcePartition != null) {
                        String sourcePartitionName = sourcePartition.getName();
                        tempPartitionNameToSourcePartitionNames.put(targetPartitionName, sourcePartitionName);

                        // Calculate the sum of VisibleVersion for all subpartitions of sourcePartition
                        versionSum += sourcePartition.getSubPartitions().stream()
                                .mapToLong(PhysicalPartition::getVisibleVersion)
                                .sum();
                    }
                }

                // Use put instead of add
                partitionLastVersion.put(targetPartitionName, versionSum);
            }
            tableColumnNames = targetTable.getBaseSchema().stream().filter(column -> !column.isGeneratedColumn())
                        .map(col -> ParseUtil.backquote(col.getName())).collect(Collectors.toList());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        final List<String> finalTableColumnNames = tableColumnNames;
        // start insert job
        tempPartitionNameToSourcePartitionNames.keySet().forEach(tmpPartitionName -> {
            Collection<String> sourcePartitionNames = tempPartitionNameToSourcePartitionNames.get(tmpPartitionName);
            final String finalPartitionName = tmpPartitionName;
            String rewriteSql = "insert into " + ParseUtil.backquote(tableName) + " TEMPORARY PARTITION ("
                        + ParseUtil.backquote(finalPartitionName) + ") select " + Joiner.on(", ").join(finalTableColumnNames)
                        + " from " + ParseUtil.backquote(tableName) + " partition (" 
                        + Joiner.on(", ").join(sourcePartitionNames.stream()
                            .map(name -> ParseUtil.backquote(name))
                            .collect(Collectors.toList())) + ")";
            String taskName = getName() + "_" + tmpPartitionName;
            OptimizeTask rewriteTask = TaskBuilder.buildOptimizeTask(taskName, properties, rewriteSql, dbName, warehouseId);
            rewriteTask.setTempPartitionName(tmpPartitionName);
            // Use map key access instead of list index
            rewriteTask.setLastVersion(partitionLastVersion.get(tmpPartitionName));
            // use half of the alter timeout as rewrite task timeout
            rewriteTask.getProperties().put(SessionVariable.INSERT_TIMEOUT, String.valueOf(timeoutMs / 2000));
            rewriteTasks.add(rewriteTask);
        });

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        for (OptimizeTask rewriteTask : rewriteTasks) {
            try {
                taskManager.createTask(rewriteTask, false);
                taskManager.executeTask(rewriteTask.getName());
                LOG.info("create rewrite task {}", rewriteTask.toString());
            } catch (DdlException e) {
                rewriteTask.setOptimizeTaskState(Constants.TaskRunState.FAILED);
                LOG.warn("create rewrite task failed", e);
            }
        }

        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer merge partition job {} state to {}", jobId, this.jobState);
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        // wait insert tasks finished
        boolean allFinished = true;
        int progress = 0;
        TaskRunManager taskRunManager = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();

        // prepare for the history task info
        Set<String> taskNames = Sets.newHashSet();
        for (OptimizeTask rewriteTask : rewriteTasks) {
            taskNames.add(rewriteTask.getName());
        }
        List<TaskRunStatus> resStatus = GlobalStateMgr.getCurrentState().getTaskManager()
                    .getTaskRunManager().getTaskRunHistory().lookupHistoryByTaskNames(dbName, taskNames);

        for (OptimizeTask rewriteTask : rewriteTasks) {
            if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED
                        || rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.SUCCESS) {
                progress += 100 / rewriteTasks.size();
                continue;
            }

            TaskRun taskRun = taskRunScheduler.getRunnableTaskRun(rewriteTask.getId());
            if (taskRun != null) {
                if (taskRun.getStatus() != null) {
                    progress += taskRun.getStatus().getProgress() / rewriteTasks.size();
                }
                allFinished = false;
                continue;
            }

            if (resStatus == null || resStatus.isEmpty()) {
                allFinished = false;
                continue;
            }
            List<TaskRunStatus> filteredTask = resStatus.stream()
                        .filter(x -> rewriteTask.getName().equals(x.getTaskName())).collect(Collectors.toList());
            if (filteredTask.isEmpty()) {
                allFinished = false;
                continue;
            }
            Preconditions.checkState(filteredTask.size() == 1);
            TaskRunStatus status = filteredTask.get(0);

            if (status.getState() == Constants.TaskRunState.FAILED) {
                LOG.warn("optimize task {} failed", rewriteTask.getName());
                rewriteTask.setOptimizeTaskState(Constants.TaskRunState.FAILED);
            }
            progress += 100 / rewriteTasks.size();
        }

        if (!allFinished) {
            LOG.info("wait insert tasks to be finished, merge partition job: {}", jobId);
            this.progress = progress;
            return;
        }

        this.progress = 99;

        LOG.info("all insert overwrite tasks finished, merge partition job: {}", jobId);

        // replace partition
        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
            onFinished(db, tbl);
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

    private void onFinished(Database db, OlapTable targetTable) throws AlterCancelException {
        try {
            Map<String, Long> partitionLastVersion = Maps.newHashMap();
            tempPartitionNameToSourcePartitionNames.asMap().forEach((key, sourcePartitionIds) -> 
                    partitionLastVersion.put(
                        key, 
                        sourcePartitionIds.stream()
                            .mapToLong(sourcePartitionId -> 
                                (targetTable.getPartition(sourcePartitionId) != null) 
                                    ? targetTable.getPartition(sourcePartitionId)
                                        .getSubPartitions()
                                        .stream()
                                        .mapToLong(PhysicalPartition::getVisibleVersion)
                                        .sum() 
                                    : 0
                            )
                            .sum()
                    )
            );

            boolean hasFailedTask = false;
            String errMsg = "";
            for (OptimizeTask rewriteTask : rewriteTasks) {
                if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED
                            || partitionLastVersion.get(rewriteTask.getTempPartitionName()) != rewriteTask.getLastVersion()) {
                    LOG.info("merge partitions job {} rewrite task {} state {} failed or partition {} version {} change to {}",
                                jobId, rewriteTask.getName(), rewriteTask.getOptimizeTaskState(),
                                rewriteTask.getTempPartitionName(), rewriteTask.getLastVersion(),
                                partitionLastVersion.get(rewriteTask.getTempPartitionName()));
                    tempPartitionNameToSourcePartitionNames.removeAll(rewriteTask.getTempPartitionName());
                    targetTable.dropTempPartition(rewriteTask.getTempPartitionName(), true);
                    hasFailedTask = true;
                    if (rewriteTask.getOptimizeTaskState() == Constants.TaskRunState.FAILED) {
                        errMsg += rewriteTask.getTempPartitionName() + " rewrite task execute failed, ";
                    } else {
                        errMsg += rewriteTask.getTempPartitionName() + " has ingestion during optimize, ";
                    }
                }
            }

            if (tempPartitionNameToSourcePartitionNames.isEmpty()) {
                throw new AlterCancelException("all partitions rewrite failed [" + errMsg + "]");
            }

            Set<Tablet> sourceTablets = Sets.newHashSet();
            tempPartitionNameToSourcePartitionNames.values().forEach(sourcePartitionName -> {
                Partition partition = targetTable.getPartition(sourcePartitionName);
                if (partition != null) {
                    for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        sourceTablets.addAll(index.getTablets());
                    }
                }
            });

            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            if (partitionInfo.isRangePartition()) {
                for (Map.Entry<String, Collection<String>> entry : tempPartitionNameToSourcePartitionNames.asMap().entrySet()) {
                    String tmpPartitionName = entry.getKey();
                    Collection<String> sourcePartitionNames = entry.getValue();
                    
                    LOG.info("merge partitions job {} replace partition dbId:{}, tableId:{},"
                            + "source partitions:{}, target partition:{}",
                            jobId, dbId, tableId, sourcePartitionNames, tmpPartitionName);
                    targetTable.replaceTempPartitions(
                            new ArrayList<>(sourcePartitionNames), 
                            Collections.singletonList(tmpPartitionName), 
                            false, true);
                    
                    // write log
                    ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(
                            db.getId(), 
                            targetTable.getId(),
                            new ArrayList<>(sourcePartitionNames), 
                            Collections.singletonList(tmpPartitionName),
                            false, 
                            true, 
                            partitionInfo instanceof SinglePartitionInfo);
                    
                    GlobalStateMgr.getCurrentState().getEditLog().logReplaceTempPartition(info);
                }
            } else {
                throw new AlterCancelException("partition type " + partitionInfo.getType() + " is not supported");
            }
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

            targetTable.setState(OlapTableState.NORMAL);

            LOG.info("merge partitions job {} finish replace partitions dbId:{}, tableId:{},"
                                    + "partitions:{}",
                        jobId, dbId, tableId, tempPartitionNameToSourcePartitionNames);
        } catch (Exception e) {
            LOG.warn("merge partitions failed dbId:{}, tableId:{} exception: {}", dbId, tableId, e);
            throw new AlterCancelException("merge partitions failed " + e.getMessage());
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
            Set<Tablet> sourceTablets = Sets.newHashSet();
            if (getTmpPartitionIds() != null) {
                for (long pid : getTmpPartitionIds()) {
                    LOG.info("merge partitions job {} drop temp partition:{}", jobId, pid);

                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
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
            sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);
            targetTable.setState(OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("exception when cancel merge partition job.", e);
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
    private void replayPending(MergePartitionJob replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }
        try (AutoCloseableLock ignore =
                    new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
            // set table state
            tbl.setState(OlapTableState.OPTIMIZE);
        }

        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;
        this.optimizeOperation = replayedJob.optimizeOperation;

        LOG.info("replay pending merge partition job: {}", jobId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(MergePartitionJob replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }

        for (Entry<Long, Collection<Long>> entry : replayedJob.getTempPartitionIdToSourcePartitionIds().asMap().entrySet()) {
            long tempPartitionId = entry.getKey();
            Collection<Long> sourcePartitionIds = entry.getValue();
            tempPartitionIdToSourcePartitionIds.putAll(tempPartitionId, sourcePartitionIds);
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;
        this.optimizeOperation = replayedJob.optimizeOperation;

        LOG.info("replay waiting txn merge partition job: {}", jobId);
    }

    private void onReplayFinished(MergePartitionJob replayedJob, OlapTable targetTable) {
        this.tempPartitionNameToSourcePartitionNames = replayedJob.tempPartitionNameToSourcePartitionNames;
        this.optimizeOperation = replayedJob.optimizeOperation;

        Set<Tablet> sourceTablets = Sets.newHashSet();
        for (long id : replayedJob.getTmpPartitionIds()) {
            Partition partition = targetTable.getPartition(id);
            if (partition != null) {
                for (MaterializedIndex index
                        : partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    sourceTablets.addAll(index.getTablets());
                }
                targetTable.dropTempPartition(partition.getName(), true);
            }
        }
        sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

        targetTable.setState(OlapTableState.NORMAL);

        LOG.info("finish replay merge partition job {} dbId:{}, tableId:{}, partitions:{}",
                    jobId, dbId, tableId, tempPartitionNameToSourcePartitionNames);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(MergePartitionJob replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (tbl != null) {
                try (AutoCloseableLock ignore =
                            new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE)) {
                    onReplayFinished(replayedJob, tbl);
                }
            }
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;

        LOG.info("replay finished merge partition job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(MergePartitionJob replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled merge partition job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        MergePartitionJob replayedOptimizeJob = (MergePartitionJob) replayedJob;
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
        String json = GsonUtils.GSON.toJson(this, MergePartitionJob.class);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.postfix = "job" + jobId;
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
