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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
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
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                gc();
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
        doCommit();
        transferTo(InsertOverwriteJobState.OVERWRITE_SUCCESS);
    }

    public void replayStateChange(InsertOverwriteStateChangeInfo info) {
        LOG.info("replay state change:{}", info);
        // If the final status is failure, then GC must be done
        if (info.getToState() == OVERWRITE_FAILED) {
            job.setTmpPartitionIds(info.getTmpPartitionIds());
            job.setTxnId(info.getTxnId());
            job.setJobState(OVERWRITE_FAILED);
            LOG.info("replay insert overwrite job:{} to FAILED", job.getJobId());
            replayGC();
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
                job.setTxnId(info.getTxnId());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
                break;
            case OVERWRITE_SUCCESS:
                job.setTmpPartitionIds(info.getTmpPartitionIds());
                job.setTxnId(info.getTxnId());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_SUCCESS);
                replayCommit();
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
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
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

            // For dynamic overwrite, begin transaction here to get txnId early.
            // This allows us to persist txnId in the log, so that after FE restart,
            // we can identify temp partitions belonging to this job (prefix: "txn{txnId}_").
            if (job.isDynamicOverwrite()) {
                long txnId = beginTransactionForDynamicOverwrite(db, targetTable);
                job.setTxnId(txnId);
                LOG.info("dynamic overwrite job {} begin transaction, txnId: {}", job.getJobId(), txnId);
            }

            InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                    InsertOverwriteJobState.OVERWRITE_RUNNING, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                    job.getTmpPartitionIds(), job.getTxnId());
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info, wal -> {});
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        transferTo(InsertOverwriteJobState.OVERWRITE_RUNNING);
    }

    private long beginTransactionForDynamicOverwrite(Database db, OlapTable targetTable) throws Exception {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        String label = MetaUtils.genInsertLabel(context.getExecutionId());
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;

        return transactionMgr.beginTransaction(
                db.getId(),
                Lists.newArrayList(targetTable.getId()),
                label,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                        FrontendOptions.getLocalHostAddress()),
                sourceType,
                context.getExecTimeout(),
                context.getCurrentComputeResource());
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

        if (insertStmt.isSpecifyPartitionNames())  {
            // The specified partition must already exist; it does not need to be created.
            return;
        } else {
            // This is for insert overwrite t partition(k=v) values(...)
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

        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        String targetDb = insertStmt.getDbName();
        Database db = state.getLocalMetastore().getDb(targetDb);
        try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()),
                LockType.READ)) {
            addPartitionClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(olapTable, partitionValues, false, null);
        } catch (AnalysisException ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        List<Long> sourcePartitionIds = job.getSourcePartitionIds();
        try {
            try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()),
                    LockType.READ)) {
                AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                analyzer.analyze(context, addPartitionClause);
            }
            state.getLocalMetastore().addPartitions(context, db, olapTable.getName(), addPartitionClause);
        } catch (Exception ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        List<String> partitionNames;
        if (partitionDesc instanceof RangePartitionDesc) {
            partitionNames = ((RangePartitionDesc) partitionDesc).getPartitionNames();
        } else if (partitionDesc instanceof ListPartitionDesc) {
            partitionNames = ((ListPartitionDesc) partitionDesc).getPartitionNames();
        } else {
            throw new RuntimeException("Unsupported partitionDesc");
        }
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            if (!sourcePartitionIds.contains(partition.getId())) {
                sourcePartitionIds.add(partition.getId());
            }
        }
    }

    private void executeInsert() throws Exception {
        long insertStartTimestamp = System.currentTimeMillis();
        // should replan here because prepareInsert has changed the targetPartitionNames of insertStmt
        try (var guard = context.bindScope()) {
            // For dynamic overwrite, set the txnId to insertStmt so that StatementPlanner.beginTransaction()
            // will skip creating a new transaction and reuse the one we created in prepare().
            if (job.isDynamicOverwrite() && job.getTxnId() > 0) {
                insertStmt.setTxnId(job.getTxnId());
            }
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
        if (db == null || !db.isExist()) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        if (!db.isExist()) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        OlapTable targetTable = checkAndGetTable(db, tableId);
        // acquire compute resource
        ComputeResource computeResource = context.getCurrentComputeResource();
        if (computeResource == null) {
            throw new DmlException("insert overwrite commit failed because no available resource");
        }
        PartitionUtils.createAndAddTempPartitionsForTable(db, targetTable, postfix,
                job.getSourcePartitionIds(), job.getTmpPartitionIds(), null,
                computeResource);
        createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;
    }

    protected void gc() {
        LOG.info("insert overwrite job {} start to garbage collect", job.getJobId());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null || !db.isExist()) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new DmlException("table:%d does not exist in database:%s", tableId, db.getFullName());
        }
        Preconditions.checkState(table instanceof OlapTable);
        OlapTable targetTable = (OlapTable) table;

        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        try {
            Set<Tablet> sourceTablets = Sets.newHashSet();
            // Drop temp partitions by partition IDs (for non-dynamic overwrite)
            List<String> partitionNamesToDrop = new ArrayList<>();
            if (job.getTmpPartitionIds() != null) {
                for (long pid : job.getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);
                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        collectTabletsFromPartition(partition, sourceTablets);
                        partitionNamesToDrop.add(partition.getName());
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }

            // Drop temp partitions for dynamic overwrite
            if (job.isDynamicOverwrite()) {
                List<String> tmpPartitions = getDynamicOverwriteTempPartitions(targetTable);
                List<Long> tmpPartitionIds = new ArrayList<>();
                for (String partitionName : tmpPartitions) {
                    Partition partition = targetTable.getPartition(partitionName, true);
                    if (partition != null) {
                        collectTabletsFromPartition(partition, sourceTablets);
                        partitionNamesToDrop.add(partitionName);
                        tmpPartitionIds.add(partition.getId());
                    }
                }
                job.setTmpPartitionIds(tmpPartitionIds);
            }

            // Mark all source tablet ids force delete to drop it directly on BE
            sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

            // Abort the transaction if it was created in prepare()
            if (job.isDynamicOverwrite() && job.getTxnId() > 0) {
                try {
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                            dbId, job.getTxnId(), "insert overwrite job failed");
                    LOG.info("dynamic overwrite job {} aborted transaction {}", job.getJobId(), job.getTxnId());
                } catch (Exception e) {
                    LOG.warn("failed to abort transaction {} for dynamic overwrite job {}: {}",
                            job.getTxnId(), job.getJobId(), e.getMessage());
                }
            }

            InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                    OVERWRITE_FAILED, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                    job.getTmpPartitionIds(), job.getTxnId());
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info, wal -> {
                for (String pName : partitionNamesToDrop) {
                    targetTable.dropTempPartition(pName, true);
                }
            });
        } catch (Exception e) {
            LOG.warn("exception when gc insert overwrite job.", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }
    }

    /**
     * Get temp partitions for dynamic overwrite during GC.
     * Handles three scenarios:
     * 1. Normal execution: get temp partition names from TransactionState
     * 2. After FE restart: identify temp partitions by prefix "txn{txnId}_"
     * 3. Cancelled before prepare: no temp partitions to clean up
     */
    private List<String> getDynamicOverwriteTempPartitions(OlapTable targetTable)
            throws InterruptedException {
        List<String> tmpPartitionNames = Lists.newArrayList();
        if (insertStmt != null && job.getTxnId() > 0) {
            // Normal execution: get temp partition names from TransactionState
            tmpPartitionNames = getTempPartitionNamesFromTxnState();
        } else if (job.getTxnId() > 0) {
            // After FE restart: identify temp partitions by prefix "txn{txnId}_"
            String tempPartitionPrefix = "txn" + job.getTxnId() + "_";
            tmpPartitionNames = targetTable.getTempPartitions().stream()
                    .map(Partition::getName)
                    .filter(name -> name.startsWith(tempPartitionPrefix))
                    .collect(Collectors.toList());
            LOG.info("dynamic overwrite job {} (FE restarted) drop temp partitions with prefix '{}': {}",
                    job.getJobId(), tempPartitionPrefix, tmpPartitionNames);
        } else {
            // Cancelled before prepare: no temp partitions to clean up
            LOG.info("dynamic overwrite job {} cancelled before prepare phase, no temp partitions to clean up",
                    job.getJobId());
        }

        return tmpPartitionNames;
    }

    private List<String> getTempPartitionNamesFromTxnState() throws InterruptedException {
        int waitTimes = 10;
        TransactionState txnState = null;
        do {
            txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionState(dbId, job.getTxnId());
            if (txnState == null) {
                throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, job.getTxnId());
            }
            Thread.sleep(200);
        } while (txnState.isRunning() && --waitTimes > 0);

        List<String> tmpPartitionNames = txnState.getCreatedPartitionNames(tableId);
        LOG.info("dynamic overwrite job {} drop tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
        return tmpPartitionNames;
    }

    private void collectTabletsFromPartition(Partition partition, Set<Tablet> tablets) {
        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
            for (MaterializedIndex index : subPartition.getAllMaterializedIndices(IndexExtState.ALL)) {
                tablets.addAll(index.getTablets());
            }
        }
    }

    protected void replayGC() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            return;
        }
        OlapTable targetTable = (OlapTable) table;
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            return;
        }
        try {
            if (job.getTmpPartitionIds() != null) {
                for (long pid : job.getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);
                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }
    }

    protected void doCommit() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        OlapTable tmpTargetTable = null;
        InsertOverwriteJobStats stats = new InsertOverwriteJobStats();
        stats.setSourcePartitionIds(job.getSourcePartitionIds());
        stats.setTargetPartitionIds(job.getTmpPartitionIds());

        // Collect partition tablet row counts for statistics sampling
        com.google.common.collect.Table<Long, Long, Long> partitionTabletRowCounts = HashBasedTable.create();

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
            List<Long> tmpPartitionIds = job.getTmpPartitionIds();
            if (tmpPartitionIds == null) {
                throw new DmlException("tmp partitions are empty for job:%s", job.getJobId());
            }
            List<String> tmpPartitionNames = tmpPartitionIds.stream()
                    .map(partitionId -> {
                        Partition partition = targetTable.getPartition(partitionId);
                        if (partition == null) {
                            throw new DmlException("temp partition id:%s does not exist", partitionId);
                        }
                        return partition.getName();
                    })
                    .collect(Collectors.toList());
            Set<Tablet> sourceTablets = Sets.newHashSet();
            sourcePartitionNames.forEach(name -> {
                Partition partition = targetTable.getPartition(name);
                for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : subPartition.getAllMaterializedIndices(IndexExtState.ALL)) {
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
                    TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .getTransactionState(dbId, insertStmt.getTxnId());
                    if (txnState == null) {
                        throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, insertStmt.getTxnId());
                    }
                    tmpPartitionNames = txnState.getCreatedPartitionNames(tableId);

                    List<Long> dynamicSourcePartitionIds = new ArrayList<>();
                    for (String tempPartitionName : tmpPartitionNames) {
                        String oldPartitionName = tempPartitionName.substring(
                                tempPartitionName.indexOf(AnalyzerUtils.PARTITION_NAME_PREFIX_SPLIT) + 1);
                        Partition oldPartition = targetTable.getPartition(oldPartitionName, false);
                        if (oldPartition != null) {
                            dynamicSourcePartitionIds.add(oldPartition.getId());
                        }
                    }

                    // Collect target partition IDs for stats (the new temp partitions)
                    List<Long> dynamicTargetPartitionIds = tmpPartitionNames.stream()
                            .map(name -> {
                                Partition partition = targetTable.getPartition(name, true);
                                if (partition == null) {
                                    throw new DmlException("temp partition %s does not exist", name);
                                }
                                return partition.getId();
                            })
                            .collect(Collectors.toList());

                    job.setTmpPartitionIds(dynamicTargetPartitionIds);
                    tmpPartitionIds = dynamicTargetPartitionIds;

                    if (stats.getSourcePartitionIds().isEmpty()) {
                        stats.setSourcePartitionIds(dynamicSourcePartitionIds);
                    }

                    if (stats.getTargetPartitionIds().isEmpty()) {
                        stats.setTargetPartitionIds(dynamicTargetPartitionIds);
                    }

                    if (stats.getSourceRows() == 0) {
                        // Recalculate sumSourceRows for dynamic overwrite
                        sumSourceRows = dynamicSourcePartitionIds.stream()
                                .mapToLong(pid -> targetTable.mayGetPartition(pid).stream()
                                        .mapToLong(Partition::getRowCount).sum())
                                .sum();
                        stats.setSourceRows(sumSourceRows);
                    }
                    LOG.info("dynamic overwrite job {} replace tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
                    ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
                } else {
                    ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
                    targetTable.checkReplaceTempPartitions(sourcePartitionNames, tmpPartitionNames, true);
                }
            } else if (partitionInfo instanceof SinglePartitionInfo) {
                ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
            } else {
                throw new DdlException("partition type " + partitionInfo.getType() + " is not supported");
            }

            long sumTargetRows = 0;
            if (insertStmt != null) {
                TransactionState txnState = GlobalStateMgr.getCurrentState()
                        .getGlobalTransactionMgr()
                        .getTransactionState(dbId, insertStmt.getTxnId());
                if (txnState != null) {
                    // Get target rows from TxnCommitAttachment
                    if (txnState.getTxnCommitAttachment() != null) {
                        TxnCommitAttachment attachment = txnState.getTxnCommitAttachment();
                        if (attachment instanceof InsertTxnCommitAttachment) {
                            sumTargetRows = ((InsertTxnCommitAttachment) attachment).getLoadedRows();
                        }
                    }

                    // Collect tablet row counts from TableCommitInfo for statistics sampling
                    TableCommitInfo tableCommitInfo = txnState.getIdToTableCommitInfos().get(tableId);
                    if (tableCommitInfo != null) {
                        for (var entry : tableCommitInfo.getIdToPartitionCommitInfo().entrySet()) {
                            long physicalPartitionId = entry.getKey();
                            PartitionCommitInfo partitionCommitInfo = entry.getValue();
                            Map<Long, Long> tabletRows = partitionCommitInfo.getTabletIdToRowCountForPartitionFirstLoad();

                            PhysicalPartition physicalPartition = targetTable.getPhysicalPartition(physicalPartitionId);
                            if (physicalPartition != null) {
                                Partition partition = targetTable.getPartition(physicalPartition.getParentId());
                                if (partition != null && !tabletRows.isEmpty() &&
                                        stats.getTargetPartitionIds().contains(partition.getId())) {
                                    long partitionId = partition.getId();
                                    tabletRows.forEach((tabletId, rowCount) -> partitionTabletRowCounts.put(partitionId,
                                            tabletId, rowCount));
                                }
                            }
                        }
                    }
                }
            }

            if (sumTargetRows == 0) {
                LOG.warn("TxnCommitAttachment is null or invalid, fallback to partition.getRowCount() for " +
                        "table_id={}, partition_ids={}, txn_id={}",
                        tableId, job.getTmpPartitionIds(), insertStmt != null ? insertStmt.getTxnId() : "null");
                sumTargetRows = job.getTmpPartitionIds().stream()
                        .mapToLong(p -> targetTable.mayGetPartition(p).stream().mapToLong(Partition::getRowCount).sum())
                        .sum();
            }

            stats.setTargetRows(sumTargetRows);
            stats.setPartitionTabletRowCounts(partitionTabletRowCounts);
            // mark all source tablet ids force delete to drop it directly on BE,
            // not to move it to trash
            sourceTablets.forEach(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()::markTabletForceDelete);

            InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                    InsertOverwriteJobState.OVERWRITE_SUCCESS, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                    job.getTmpPartitionIds(), job.getTxnId());
            final List<String> finalSourcePartitionNames = sourcePartitionNames;
            final List<String> finalTmpPartitionNames = tmpPartitionNames;
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info, wal -> {
                replacePartition(targetTable, finalSourcePartitionNames, finalTmpPartitionNames);
            });

            try {
                GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(targetTable,
                        true /* isJoin */, null /* expectGroupId */);
            } catch (DdlException e) {
                // log an error if update colocation info failed, insert overwrite already succeeded
                LOG.error("table {} update colocation info failed after insert overwrite, {}.", tableId, e.getMessage());
            }

            targetTable.lastSchemaUpdateTime.set(System.nanoTime());
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into dbId:{}, tableId:{}",
                    job.getTargetDbId(), job.getTargetTableId(), e);
            throw new DmlException("replace partitions failed", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        // trigger listeners after insert overwrite committed, trigger listeners after
        // write unlock to avoid holding lock too long
        GlobalStateMgr.getCurrentState().getOperationListenerBus()
                .onInsertOverwriteJobCommitFinish(db, tmpTargetTable, stats);
    }

    private void replacePartition(OlapTable targetTable,
                                  List<String> sourcePartitionNames,
                                  List<String> tmpPartitionNames) {
        PartitionInfo partitionInfo = targetTable.getPartitionInfo();
        if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
            if (job.isDynamicOverwrite()) {
                targetTable.replaceMatchPartitions(dbId, tmpPartitionNames);
            } else {
                targetTable.replaceTempPartitionsWithoutCheck(dbId, sourcePartitionNames, tmpPartitionNames,  false);
            }
        } else {
            targetTable.replacePartition(dbId, sourcePartitionNames.get(0), tmpPartitionNames.get(0));
        }
    }

    protected void replayCommit() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return;
        }
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            return;
        }
        try {
            final OlapTable targetTable = checkAndGetTable(db, tableId);
            List<String> tmpPartitionNames = job.getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .toList();
            replacePartition(targetTable, job.getSourcePartitionNames(), tmpPartitionNames);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
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
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.READ)) {
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
            PartitionRef partitionNames = new PartitionRef(tmpPartitionNames, true, NodePosition.ZERO);
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

    protected void testDoCommit() {
        doCommit();
    }

    protected void ensureTempPartitionsVisible(OlapTable targetTable, List<Long> partitionIds) {
        if (partitionIds == null) {
            return;
        }
        for (Long partitionId : partitionIds) {
            if (partitionId == null) {
                continue;
            }
            Partition partition = targetTable.getPartition(partitionId);
            if (partition == null) {
                throw new DmlException("temp partition id:%s does not exist", partitionId);
            }
            if (hasCommittedNotVisible(partitionId)) {
                throw new DmlException("temp partition %s still has committed transactions not visible",
                        partition.getName());
            }
        }
    }

    protected boolean hasCommittedNotVisible(long partitionId) {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .existCommittedTxns(dbId, tableId, partitionId);
    }
}
