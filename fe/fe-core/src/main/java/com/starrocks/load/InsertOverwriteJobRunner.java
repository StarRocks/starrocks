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

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.load.InsertOverwriteJobState.OVERWRITE_FAILED;

// InsertOverwriteJobRunner will execute the insert overwrite.
// The main idea is that:
//     1. create temporary partitions as target partitions
//     2. insert selected data into temporary partitions. This is done by modifying the
//          insert target partition names of InsertStmt and replan.
//     3. if insert successfully, swap the temporary partitions with source partitions
//     4. if insert failed, remove the temporary partitions created
//     5. if FE restart, the insert overwrite job will fail.
public class InsertOverwriteJobRunner {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobRunner.class);

    private final InsertOverwriteJob job;

    private InsertStmt insertStmt;
    private StmtExecutor stmtExecutor;
    private ConnectContext context;
    private final long dbId;
    private final long tableId;
    private final String postfix;

    // execution stat
    private long createPartitionElapse;
    private long insertElapse;
    private TransactionState transactionState;

    public InsertOverwriteJobRunner(InsertOverwriteJob job, ConnectContext context, StmtExecutor stmtExecutor) {
        this.job = job;
        this.context = context;
        this.stmtExecutor = stmtExecutor;
        this.insertStmt = job.getInsertStmt();
        this.dbId = job.getTargetDbId();
        this.tableId = job.getTargetTableId();
        this.postfix = "_" + job.getJobId();
        this.createPartitionElapse = 0;
        this.insertElapse = 0;
    }

    // for replay
    public InsertOverwriteJobRunner(InsertOverwriteJob job) {
        this.job = job;
        this.dbId = job.getTargetDbId();
        this.tableId = job.getTargetTableId();
        this.postfix = "_" + job.getJobId();
        this.createPartitionElapse = 0;
        this.insertElapse = 0;
    }

    public boolean isFinished() {
        return job.isFinished();
    }

    // only called from log replay
    // there is no concurrent problem here
    public void cancel() {
        if (isFinished()) {
            LOG.warn("cancel failed. insert overwrite job:{} already finished. state:{}", job.getJobId(),
                    job.getJobState());
            return;
        }
        try {
            transferTo(OVERWRITE_FAILED);
        } catch (Exception e) {
            LOG.warn("cancel insert overwrite job:{} failed", job.getJobId(), e);
        }
    }

    public void run() throws Exception {
        try {
            handle();
        } catch (Exception e) {
            if (job.getJobState() != OVERWRITE_FAILED) {
                transferTo(OVERWRITE_FAILED);
            }
            throw e;
        }
    }

    public void handle() throws Exception {
        switch (job.getJobState()) {
            case OVERWRITE_PENDING:
                prepare();
                break;
            case OVERWRITE_RUNNING:
                doLoad();
                break;
            case OVERWRITE_FAILED:
                gc(false);
                LOG.warn("insert overwrite job:{} failed. createPartitionElapse:{} ms, insertElapse:{} ms",
                        job.getJobId(), createPartitionElapse, insertElapse);
                break;
            case OVERWRITE_SUCCESS:
                LOG.info("insert overwrite job:{} succeed. createPartitionElapse:{} ms, insertElapse:{} ms",
                        job.getJobId(), createPartitionElapse, insertElapse);
                break;
            default:
                throw new RuntimeException("invalid jobState:" + job.getJobState());
        }
    }

    private void doLoad() throws Exception {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        createTempPartitions();
        prepareInsert();
        executeInsert();
        doCommit(false);
        transferTo(InsertOverwriteJobState.OVERWRITE_SUCCESS);
    }

    public void replayStateChange(InsertOverwriteStateChangeInfo info) {
        LOG.info("replay state change:{}", info);
        // If the final status is failure, then GC must be done
        if (info.getToState() == OVERWRITE_FAILED) {
            job.setTmpPartitionIds(info.getTmpPartitionIds());
            job.setJobState(OVERWRITE_FAILED);
            LOG.info("replay insert overwrite job:{} to FAILED", job.getJobId());
            gc(true);
            return;
        } else if (job.getJobState() != info.getFromState()) {
            LOG.warn("invalid job info. current state:{}, from state:{}", job.getJobState(), info.getFromState());
            return;
        }
        // state can not be PENDING here
        switch (info.getToState()) {
            case OVERWRITE_RUNNING:
                job.setSourcePartitionIds(info.getSourcePartitionIds());
                job.setTmpPartitionIds(info.getTmpPartitionIds());
                job.setSourcePartitionNames(info.getSourcePartitionNames());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
                break;
            case OVERWRITE_SUCCESS:
                job.setTmpPartitionIds(info.getTmpPartitionIds());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_SUCCESS);
                doCommit(true);
                LOG.info("replay insert overwrite job:{} to SUCCESS", job.getJobId());
                break;
            default:
                LOG.warn("invalid to state:{} for insert overwrite job:{}", info.getToState(), job.getJobId());
                break;
        }
    }

    private void transferTo(InsertOverwriteJobState state) throws Exception {
        if (state == InsertOverwriteJobState.OVERWRITE_SUCCESS) {
            Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        }
        job.setJobState(state);
        handle();
    }

    private void prepare() throws Exception {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_PENDING);
        // support automatic partition
        createPartitionByValue(insertStmt);
        // get tmpPartitionIds first because they are used to drop created partitions when restarted.
        List<Long> tmpPartitionIds = Lists.newArrayList();
        for (int i = 0; i < job.getSourcePartitionIds().size(); ++i) {
            tmpPartitionIds.add(GlobalStateMgr.getCurrentState().getNextId());
        }
        job.setTmpPartitionIds(tmpPartitionIds);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }

        try {
            OlapTable targetTable;
            targetTable = checkAndGetTable(db, tableId);
            List<String> sourcePartitionNames = Lists.newArrayList();
            for (Long partitionId : job.getSourcePartitionIds()) {
                Partition partition = targetTable.getPartition(partitionId);
                if (partition == null) {
                    throw new DmlException("partition id:%s does not exist in table id:%s", partitionId, tableId);
                }
                sourcePartitionNames.add(partition.getName());
            }
            job.setSourcePartitionNames(sourcePartitionNames);

            InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                    InsertOverwriteJobState.OVERWRITE_RUNNING, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                    job.getTmpPartitionIds());
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        transferTo(InsertOverwriteJobState.OVERWRITE_RUNNING);
    }

    private void createPartitionByValue(InsertStmt insertStmt) {
        AddPartitionClause addPartitionClause;
        if (insertStmt.getTargetPartitionNames() == null) {
            return;
        }
        if (insertStmt.isDynamicOverwrite()) {
            return;
        }
        OlapTable olapTable = (OlapTable) insertStmt.getTargetTable();
        List<List<String>> partitionValues = Lists.newArrayList();
        if (!olapTable.getPartitionInfo().isAutomaticPartition()) {
            return;
        }

        if (insertStmt.isSpecifyPartitionNames()) {
            List<String> partitionNames = insertStmt.getTargetPartitionNames().getPartitionNames();
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new RuntimeException("Partition '" + partitionName
                            + "' does not exist in table '" + olapTable.getName() + "'.");
                }
                if (partitionInfo instanceof ListPartitionInfo) {
                    ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
                    List<List<LiteralExpr>> lists = listPartitionInfo.getMultiLiteralExprValues().get(partition.getId());
                    for (List<LiteralExpr> list : lists) {
                        List<String> values = Lists.newArrayList();
                        for (LiteralExpr literalExpr : list) {
                            values.add(literalExpr.getStringValue());
                        }
                        partitionValues.add(values);
                    }
                } else {
                    throw new RuntimeException("Specify the partition name, and automatically create partition names. " +
                            "Currently, only List partitions are supported.");
                }
            }
        } else {
            List<Expr> partitionColValues = insertStmt.getTargetPartitionNames().getPartitionColValues();
            // Currently we only support overwriting one partition at a time
            List<String> firstValues = Lists.newArrayList();
            partitionValues.add(firstValues);
            for (Expr expr : partitionColValues) {
                if (expr instanceof LiteralExpr) {
                    firstValues.add(((LiteralExpr) expr).getStringValue());
                } else {
                    throw new SemanticException("Only support literal value for partition column.");
                }
            }
        }

        try {
            addPartitionClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(olapTable, partitionValues, false, null);
        } catch (AnalysisException ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        String targetDb = insertStmt.getTableName().getDb();
        Database db = state.getLocalMetastore().getDb(targetDb);
        List<Long> sourcePartitionIds = job.getSourcePartitionIds();
        try {
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
            analyzer.analyze(context, addPartitionClause);
            state.getLocalMetastore().addPartitions(context, db, olapTable.getName(), addPartitionClause);
        } catch (Exception ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        List<String> partitionColNames;
        if (partitionDesc instanceof RangePartitionDesc) {
            partitionColNames = ((RangePartitionDesc) partitionDesc).getPartitionColNames();
        } else if (partitionDesc instanceof ListPartitionDesc) {
            partitionColNames = ((ListPartitionDesc) partitionDesc).getPartitionColNames();
        } else {
            throw new RuntimeException("Unsupported partitionDesc");
        }
        for (String partitionColName : partitionColNames) {
            Partition partition = olapTable.getPartition(partitionColName);
            if (!sourcePartitionIds.contains(partition.getId())) {
                sourcePartitionIds.add(partition.getId());
            }
        }
    }

    private void executeInsert() throws Exception {
        long insertStartTimestamp = System.currentTimeMillis();
        // should replan here because prepareInsert has changed the targetPartitionNames of insertStmt
        try (var guard = context.bindScope()) {
            ExecPlan newPlan = StatementPlanner.plan(insertStmt, context);
            // Use `handleDMLStmt` instead of `handleDMLStmtWithProfile` because cannot call `writeProfile` in
            // InsertOverwriteJobRunner.
            // InsertOverWriteJob is executed as below:
            // - StmtExecutor#handleDMLStmtWithProfile
            //    - StmtExecutor#executeInsert
            //  - StmtExecutor#handleInsertOverwrite#InsertOverwriteJobMgr#run
            //  - InsertOverwriteJobRunner#executeInsert
            //  - StmtExecutor#handleDMLStmt <- no call `handleDMLStmt` again.
            // `writeProfile` is called in `handleDMLStmt`, and no need call it again later.
            stmtExecutor.handleDMLStmt(newPlan, insertStmt);
        }
        insertElapse = System.currentTimeMillis() - insertStartTimestamp;
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("insert overwrite failed. error message:{}", context.getState().getErrorMessage());
            throw new DmlException(context.getState().getErrorMessage());
        }
    }

    private void createTempPartitions() throws DdlException {
        if (job.isDynamicOverwrite()) {
            return;
        }
        long createPartitionStartTimestamp = System.currentTimeMillis();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, tableId, LockType.READ)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        OlapTable targetTable;
        try {
            targetTable = checkAndGetTable(db, tableId);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }
        PartitionUtils.createAndAddTempPartitionsForTable(db, targetTable, postfix,
                job.getSourcePartitionIds(), job.getTmpPartitionIds(), null, job.getWarehouseId());
        createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;
    }

    private void gc(boolean isReplay) {
        LOG.info("insert overwrite job {} start to garbage collect", job.getJobId());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }

        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table == null) {
                throw new DmlException("table:%d does not exist in database:%s", tableId, db.getFullName());
            }
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable targetTable = (OlapTable) table;
            Set<Tablet> sourceTablets = Sets.newHashSet();
            if (job.getTmpPartitionIds() != null) {
                for (long pid : job.getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);

                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                            for (MaterializedIndex index : subPartition.getMaterializedIndices(
                                    MaterializedIndex.IndexExtState.ALL)) {
                                sourceTablets.addAll(index.getTablets());
                            }
                        }
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }
            // if dynamic overwrite, drop all runtime created partitions
            if (job.isDynamicOverwrite()) {
                List<String> tmpPartitionNames = Lists.newArrayList();
                if (!isReplay) {
                    int waitTimes = 10;
                    // wait for transaction state to be finished
                    TransactionState txnState = null;
                    do {
                        txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                .getTransactionState(dbId, insertStmt.getTxnId());
                        if (txnState == null) {
                            throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, insertStmt.getTxnId());
                        }
                        // wait a little bit even if txnState is finished
                        Thread.sleep(200);
                    } while (txnState.isRunning() && --waitTimes > 0);
                    tmpPartitionNames = txnState.getCreatedPartitionNames();
                    job.setTmpPartitionIds(tmpPartitionNames.stream()
                            .map(name -> targetTable.getPartition(name, true).getId())
                            .collect(Collectors.toList()));
                    LOG.info("dynamic overwrite job {} drop tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
                }
                for (String partitionName : tmpPartitionNames) {
                    Partition partition = targetTable.getPartition(partitionName, true);
                    if (partition != null) {
                        for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                            sourceTablets.addAll(index.getTablets());
                        }
                        targetTable.dropTempPartition(partitionName, true);
                    }
                }
            }
            if (!isReplay) {
                // mark all source tablet ids force delete to drop it directly on BE,
                // not to move it to trash
                sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

                InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                        OVERWRITE_FAILED, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                        job.getTmpPartitionIds());
                GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
            }
        } catch (Exception e) {
            LOG.warn("exception when gc insert overwrite job.", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }
    }

    private void doCommit(boolean isReplay) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        OlapTable tmpTargetTable = null;
        InsertOverwriteJobStats stats = new InsertOverwriteJobStats();
        stats.setSourcePartitionIds(job.getSourcePartitionIds());
        stats.setTargetPartitionIds(job.getTmpPartitionIds());
        try {
            // try exception to release write lock finally
            final OlapTable targetTable = checkAndGetTable(db, tableId);
            tmpTargetTable = targetTable;
            List<String> sourcePartitionNames = job.getSourcePartitionNames();
            if (sourcePartitionNames == null || sourcePartitionNames.isEmpty()) {
                sourcePartitionNames = new ArrayList<>();
                for (Long partitionId : job.getSourcePartitionIds()) {
                    Partition partition = targetTable.getPartition(partitionId);
                    if (partition == null) {
                        throw new DmlException("Partition id:%s does not exist in table id:%s", partitionId, tableId);
                    } else {
                        sourcePartitionNames.add(partition.getName());
                    }
                }
            }
            List<String> tmpPartitionNames = job.getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            Set<Tablet> sourceTablets = Sets.newHashSet();
            sourcePartitionNames.forEach(name -> {
                Partition partition = targetTable.getPartition(name);
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : subPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        sourceTablets.addAll(index.getTablets());
                    }
                }
            });
            long sumSourceRows = job.getSourcePartitionIds().stream()
                    .mapToLong(p -> targetTable.mayGetPartition(p).stream().mapToLong(Partition::getRowCount).sum())
                    .sum();
            stats.setSourceRows(sumSourceRows);

            LOG.info("overwrite job {} replace source partitions:{} to tmp partitions:{}", job.getJobId(),
                    sourcePartitionNames, tmpPartitionNames);
            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                if (job.isDynamicOverwrite()) {
                    if (!isReplay) {
                        TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                .getTransactionState(dbId, insertStmt.getTxnId());
                        if (txnState == null) {
                            throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, insertStmt.getTxnId());
                        }
                        tmpPartitionNames = txnState.getCreatedPartitionNames();
                        job.setTmpPartitionIds(tmpPartitionNames.stream()
                                .map(name -> targetTable.getPartition(name, true).getId())
                                .collect(Collectors.toList()));
                    }
                    LOG.info("dynamic overwrite job {} replace tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
                    targetTable.replaceMatchPartitions(tmpPartitionNames);
                } else {
                    targetTable.replaceTempPartitions(sourcePartitionNames, tmpPartitionNames, true, false);
                }
            } else if (partitionInfo instanceof SinglePartitionInfo) {
                targetTable.replacePartition(sourcePartitionNames.get(0), tmpPartitionNames.get(0));
            } else {
                throw new DdlException("partition type " + partitionInfo.getType() + " is not supported");
            }

            long sumTargetRows = job.getTmpPartitionIds().stream()
                    .mapToLong(p -> targetTable.mayGetPartition(p).stream().mapToLong(Partition::getRowCount).sum())
                    .sum();
            stats.setTargetRows(sumTargetRows);
            if (!isReplay) {
                // mark all source tablet ids force delete to drop it directly on BE,
                // not to move it to trash
                sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

                InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                        InsertOverwriteJobState.OVERWRITE_SUCCESS, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                        job.getTmpPartitionIds());
                GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);

                try {
                    GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(targetTable,
                            true /* isJoin */, null /* expectGroupId */);
                } catch (DdlException e) {
                    // log an error if update colocation info failed, insert overwrite already succeeded
                    LOG.error("table {} update colocation info failed after insert overwrite, {}.", tableId, e.getMessage());
                }

                targetTable.lastSchemaUpdateTime.set(System.nanoTime());
            }
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into dbId:{}, tableId:{}",
                    job.getTargetDbId(), job.getTargetTableId(), e);
            throw new DmlException("replace partitions failed", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        if (!isReplay) {
            // trigger listeners after insert overwrite committed, trigger listeners after
            // write unlock to avoid holding lock too long
            GlobalStateMgr.getCurrentState().getOperationListenerBus()
                    .onInsertOverwriteJobCommitFinish(db, tmpTargetTable, stats);
        }
    }

    private void prepareInsert() {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        Preconditions.checkState(insertStmt != null);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, tableId, LockType.READ)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        try {
            OlapTable targetTable = checkAndGetTable(db, tableId);
            if (job.getTmpPartitionIds().stream().anyMatch(id -> targetTable.getPartition(id) == null)) {
                throw new DmlException("partitions changed during insert");
            }
            List<String> tmpPartitionNames = job.getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            PartitionNames partitionNames = new PartitionNames(true, tmpPartitionNames);
            // change the TargetPartitionNames from source partitions to new tmp partitions
            // should replan when load data
            if (insertStmt.getTargetPartitionNames() == null) {
                insertStmt.setPartitionNotSpecifiedInOverwrite(true);
            }
            if (!job.isDynamicOverwrite()) {
                insertStmt.setTargetPartitionNames(partitionNames);
                insertStmt.setTargetPartitionIds(job.getTmpPartitionIds());
            }
            insertStmt.setOverwrite(false);
            insertStmt.setIsFromOverwrite(true);
            insertStmt.setSystem(true);

        } catch (Exception e) {
            throw new DmlException("prepareInsert exception", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }
    }

    private OlapTable checkAndGetTable(Database db, long tableId) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new DmlException("table:% does not exist in database:%s", tableId, db.getFullName());
        }
        Preconditions.checkState(table instanceof OlapTable);
        return (OlapTable) table;
    }

    protected void testDoCommit(boolean isReplay) {
        doCommit(isReplay);
    }
}
