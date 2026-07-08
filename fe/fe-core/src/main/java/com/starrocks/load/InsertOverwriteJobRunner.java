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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Database;
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
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.MetaUtils;
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
//
// Concurrency: there is NO mutual exclusion between concurrent insert overwrite jobs
// on the same table or the same partitions. InsertOverwriteJobMgr keeps a list of
// running jobs per table and never rejects a new one; the table locks taken by the
// individual phases protect metadata consistency only, not job-level serialization.
// Overwrites of disjoint partition sets run in parallel without interference. For
// overlapping partitions the outcome depends on timing:
//   - if one job commits before another passes prepare()/createTempPartitions(), the
//     later job fails because its source partition ids no longer exist (the swap
//     replaces a partition with a new one that keeps the name but gets a new id);
//   - if both jobs pass prepare(), both succeed and the LAST commit wins: source
//     partitions are matched by NAME in doCommit(), so the later swap replaces the
//     earlier job's just-committed partitions, equivalent to running the two
//     statements serially in commit order.
// No interleaving corrupts metadata: the swap always runs under the table WRITE lock
// and re-resolves partitions at commit time.
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
            job.setTxnId(info.getTxnId());
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
                job.setTxnId(info.getTxnId());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
                break;
            case OVERWRITE_SUCCESS:
                job.setTmpPartitionIds(info.getTmpPartitionIds());
                job.setTxnId(info.getTxnId());
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

        // A READ lock is enough here: this section resolves partition ids to names,
        // begins the dynamic-overwrite load transaction, and logs the RUNNING state
        // change; none of it mutates table state. The READ lock keeps the id-to-name
        // mapping stable (rename/drop requires the table WRITE lock) until the log entry
        // is durable, and it provides NO exclusion between concurrent insert overwrite
        // jobs on the same table (see the class comment).
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.READ)) {
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

            // For dynamic overwrite, begin the transaction here to get txnId early. This
            // lets us persist txnId in the log so that after FE restart we can identify
            // temp partitions belonging to this job (prefix "txn{txnId}_"). It records the
            // table indexes, so it runs under the table lock; if prepare fails after this
            // point, gc() aborts the transaction.
            if (job.isDynamicOverwrite()) {
                long txnId = beginTransactionForDynamicOverwrite(db, targetTable);
                job.setTxnId(txnId);
                LOG.info("dynamic overwrite job {} begin transaction, txnId: {}", job.getJobId(), txnId);
            }

            InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                    InsertOverwriteJobState.OVERWRITE_RUNNING, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                    job.getTmpPartitionIds(), job.getTxnId());
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        transferTo(InsertOverwriteJobState.OVERWRITE_RUNNING);
    }

    private long beginTransactionForDynamicOverwrite(Database db, OlapTable targetTable) throws Exception {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        String label = MetaUtils.genInsertLabel(context.getExecutionId());
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;

        long txnId = transactionMgr.beginTransaction(
                db.getId(),
                Lists.newArrayList(targetTable.getId()),
                label,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                        FrontendOptions.getLocalHostAddress()),
                sourceType,
                context.getExecTimeout(),
                context.getCurrentComputeResource());

        // add table indexes to transaction state
        // If any operation fails after beginTransaction succeeds, we must abort the transaction
        // to avoid orphaned transactions that would only timeout eventually.
        try {
            TransactionState txnState = transactionMgr.getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new DmlException("transaction state is null after beginTransaction, dbId:%s, txnId:%s",
                        db.getId(), txnId);
            }
            txnState.addTableIndexes(targetTable);
        } catch (Exception e) {
            LOG.warn("failed to setup transaction state for dynamic overwrite, aborting txnId: {}", txnId, e);
            try {
                transactionMgr.abortTransaction(db.getId(), txnId,
                        "failed to setup transaction state: " + e.getMessage());
            } catch (Exception abortEx) {
                LOG.warn("failed to abort orphaned transaction {}: {}", txnId, abortEx.getMessage());
            }
            throw e;
        }

        return txnId;
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
        String targetDb = insertStmt.getTableName().getDb();
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
        // should replan here because prepareInsert has changed the targetPartitionNames of insertStmt.
        // The re-plan creates a fresh ColumnRefFactory; lambda-argument -> ColumnRefOperator caches
        // live on the factory (see ColumnRefFactory.computeLambdaArgRefIfAbsent), so stale ids from
        // the first plan cannot leak into the second.
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

    private void gc(boolean isReplay) {
        LOG.info("insert overwrite job {} start to garbage collect", job.getJobId());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null || !db.isExist()) {
            // the dynamic overwrite transaction needs only dbId and txnId to abort;
            // clean it up even when the database is gone, otherwise it lingers until
            // the transaction timeout checker reaps it. Skip on replay: the transaction
            // was already aborted when the job originally ran.
            if (!isReplay) {
                abortDynamicOverwriteTxnQuietly();
            }
            throw new DmlException("database id:%s does not exist", dbId);
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            if (!isReplay) {
                abortDynamicOverwriteTxnQuietly();
            }
            throw new DmlException("table:%d does not exist in database:%s", tableId, db.getFullName());
        }
        Preconditions.checkState(table instanceof OlapTable);
        OlapTable targetTable = (OlapTable) table;

        // For dynamic overwrite, the temp partition names come from the load transaction,
        // and the transaction may still be finalizing. The wait polls with sleeps, so run
        // it before taking the table lock; it only reads transaction state.
        if (job.isDynamicOverwrite() && insertStmt != null && job.getTxnId() > 0) {
            try {
                waitTransactionSettled();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("interrupted while waiting for insert overwrite transaction {} to settle", job.getTxnId(), e);
            } catch (Exception e) {
                LOG.warn("failed to wait for insert overwrite transaction {} to settle", job.getTxnId(), e);
            }
        }

        Set<Tablet> sourceTablets = Sets.newHashSet();
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
        try {
            // Drop temp partitions by partition IDs (for non-dynamic overwrite)
            if (job.getTmpPartitionIds() != null) {
                for (long pid : job.getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);
                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        collectTabletsFromPartition(partition, sourceTablets);
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }

            // Drop temp partitions for dynamic overwrite
            if (job.isDynamicOverwrite()) {
                gcDropDynamicOverwriteTempPartitions(targetTable, sourceTablets, isReplay);
            }

            if (!isReplay) {
                // Only the log entry needs the table WRITE lock; marking source tablets for
                // force delete and aborting the load transaction are moved out of the lock.
                InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                        OVERWRITE_FAILED, job.getSourcePartitionIds(), job.getSourcePartitionNames(),
                        job.getTmpPartitionIds(), job.getTxnId());
                GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
            }
        } catch (Exception e) {
            LOG.warn("exception when gc insert overwrite job.", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        if (!isReplay) {
            // Mark all source tablet ids force delete to drop it directly on BE. Best-effort
            // in-memory hints, no table lock needed.
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().markTabletsForceDelete(sourceTablets);

            // Abort the transaction created in prepare(), now that the table lock is released.
            abortDynamicOverwriteTxnQuietly();
        }
    }

    // Abort the transaction created in prepare() for a dynamic overwrite job, if any.
    // It needs only dbId and txnId and takes transaction manager locks, so it must be
    // called without the table lock held, and it works even when the target db/table
    // has been dropped concurrently.
    private void abortDynamicOverwriteTxnQuietly() {
        if (!job.isDynamicOverwrite() || job.getTxnId() <= 0) {
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    dbId, job.getTxnId(), "insert overwrite job failed");
            LOG.info("dynamic overwrite job {} aborted transaction {}", job.getJobId(), job.getTxnId());
        } catch (Exception e) {
            LOG.warn("failed to abort transaction {} for dynamic overwrite job {}: {}",
                    job.getTxnId(), job.getJobId(), e.getMessage());
        }
    }

    /**
     * Drop temp partitions for dynamic overwrite during GC.
     * Handles three scenarios:
     * 1. Normal execution: get temp partition names from TransactionState
     * 2. After FE restart: identify temp partitions by prefix "txn{txnId}_"
     * 3. Cancelled before prepare: no temp partitions to clean up
     */
    private void gcDropDynamicOverwriteTempPartitions(OlapTable targetTable, Set<Tablet> sourceTablets,
                                                       boolean isReplay) {
        List<String> tmpPartitionNames = Lists.newArrayList();
        if (!isReplay) {
            if (insertStmt != null && job.getTxnId() > 0) {
                // Normal execution: get temp partition names from TransactionState
                tmpPartitionNames = gcGetTempPartitionNamesFromTxnState(targetTable);
            } else if (job.getTxnId() > 0) {
                // After FE restart: identify temp partitions by prefix "txn{txnId}_"
                String tempPartitionPrefix = "txn" + job.getTxnId() + "_";
                tmpPartitionNames = targetTable.getTempPartitions().stream()
                        .map(Partition::getName)
                        .filter(name -> name.startsWith(tempPartitionPrefix))
                        .collect(Collectors.toList());
                gcUpdateTmpPartitionIds(targetTable, tmpPartitionNames);
                LOG.info("dynamic overwrite job {} (FE restarted) drop temp partitions with prefix '{}': {}",
                        job.getJobId(), tempPartitionPrefix, tmpPartitionNames);
            } else {
                // Cancelled before prepare: no temp partitions to clean up
                LOG.info("dynamic overwrite job {} cancelled before prepare phase, no temp partitions to clean up",
                        job.getJobId());
            }
        }

        for (String partitionName : tmpPartitionNames) {
            Partition partition = targetTable.getPartition(partitionName, true);
            if (partition != null) {
                collectTabletsFromPartition(partition, sourceTablets);
                targetTable.dropTempPartition(partitionName, true);
            }
        }
    }

    // Wait until the load transaction of a dynamic overwrite job is no longer running,
    // so that getCreatedPartitionNames() below returns the complete list. This polls
    // with sleeps and only reads transaction state, so it must be called before the
    // table lock is taken.
    private void waitTransactionSettled() throws InterruptedException {
        int waitTimes = 10;
        while (true) {
            TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionState(dbId, job.getTxnId());
            if (txnState == null) {
                throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, job.getTxnId());
            }
            if (!txnState.isRunning() || --waitTimes <= 0) {
                return;
            }
            Thread.sleep(200);
        }
    }

    private List<String> gcGetTempPartitionNamesFromTxnState(OlapTable targetTable) {
        TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(dbId, job.getTxnId());
        if (txnState == null) {
            throw new DmlException("transaction state is null dbId:%s, txnId:%s", dbId, job.getTxnId());
        }
        List<String> tmpPartitionNames = txnState.getCreatedPartitionNames(tableId);
        gcUpdateTmpPartitionIds(targetTable, tmpPartitionNames);
        LOG.info("dynamic overwrite job {} drop tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
        return tmpPartitionNames;
    }

    private void gcUpdateTmpPartitionIds(OlapTable targetTable, List<String> partitionNames) {
        job.setTmpPartitionIds(partitionNames.stream()
                .map(name -> {
                    Partition partition = targetTable.getPartition(name, true);
                    if (partition == null) {
                        LOG.warn("dynamic overwrite job {} temp partition {} does not exist during gc, skip",
                                job.getJobId(), name);
                        return null;
                    }
                    return partition.getId();
                })
                .filter(id -> id != null)
                .collect(Collectors.toList()));
    }

    private void collectTabletsFromPartition(Partition partition, Set<Tablet> tablets) {
        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
            for (MaterializedIndex index : subPartition.getMaterializedIndices(
                    MaterializedIndex.IndexExtState.ALL)) {
                tablets.addAll(index.getTablets());
            }
        }
    }

    private void doCommit(boolean isReplay) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new DmlException("database id:%s does not exist", dbId);
        }
        OlapTable tmpTargetTable = null;
        InsertOverwriteJobStats stats = new InsertOverwriteJobStats();
        stats.setSourcePartitionIds(job.getSourcePartitionIds());
        stats.setTargetPartitionIds(job.getTmpPartitionIds());
        Set<Tablet> sourceTablets = Sets.newHashSet();

        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            throw new DmlException("insert overwrite commit failed because locking db:%s failed", dbId);
        }
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
                    }
                    LOG.info("dynamic overwrite job {} replace tmpPartitionNames:{}", job.getJobId(), tmpPartitionNames);
                    ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
                    targetTable.replaceMatchPartitions(dbId, tmpPartitionNames);
                } else {
                    ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
                    targetTable.replaceTempPartitions(dbId, sourcePartitionNames, tmpPartitionNames, true, false);
                }
            } else if (partitionInfo instanceof SinglePartitionInfo) {
                ensureTempPartitionsVisible(targetTable, tmpPartitionIds);
                targetTable.replacePartition(dbId, sourcePartitionNames.get(0), tmpPartitionNames.get(0));
            } else {
                throw new DdlException("partition type " + partitionInfo.getType() + " is not supported");
            }

            if (!isReplay) {
                // Only the log entry and schema-update bump need the table WRITE lock. The
                // remaining post-commit work (force-delete marks, row-count stats, colocation
                // sync, listeners) is moved out of the lock into postCommit().
                InsertOverwriteStateChangeInfo info = new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(),
                        InsertOverwriteJobState.OVERWRITE_SUCCESS, job.getSourcePartitionIds(),
                        job.getSourcePartitionNames(), job.getTmpPartitionIds(), job.getTxnId());
                GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);

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
            postCommit(tmpTargetTable, sourceTablets, stats);

            // trigger listeners after insert overwrite committed, trigger listeners after
            // write unlock to avoid holding lock too long
            GlobalStateMgr.getCurrentState().getOperationListenerBus()
                    .onInsertOverwriteJobCommitFinish(db, tmpTargetTable, stats);
        }
    }

    // Post-commit follow-up. The partition swap is already durable in the journal, so
    // nothing here may fail the job: an exception escaping doCommit() at this point
    // would route the job to gc(), which would drop the partitions that just became
    // live. None of this work needs the table WRITE lock.
    private void postCommit(OlapTable targetTable, Set<Tablet> sourceTablets, InsertOverwriteJobStats stats) {
        // mark all source tablet ids force delete to drop it directly on BE, not to
        // move it to trash. The marks are best-effort in-memory hints consumed by
        // ReportHandler only after the recycle bin erases the swapped-out partitions,
        // which happens far later than this point.
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().markTabletsForceDelete(sourceTablets);

        // Collect partition tablet row counts for statistics sampling
        com.google.common.collect.Table<Long, Long, Long> partitionTabletRowCounts = HashBasedTable.create();
        try (AutoCloseableLock ignore =
                new AutoCloseableLock(new Locker(), dbId, Lists.newArrayList(tableId), LockType.READ)) {
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

            // ColocateTableIndex self-protects with its own lock, and for lake tables this
            // ends with a StarOS RPC, so it must not run under the table WRITE lock; the
            // table READ lock is enough for the shard group reads (same locking as
            // StarMgrMetaSyncer.syncTableColocationInfo).
            GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(targetTable,
                    true /* isJoin */, null /* expectGroupId */);
        } catch (Exception e) {
            // log an error if post-commit work failed, insert overwrite already succeeded
            LOG.error("insert overwrite post-commit work failed for dbId:{}, tableId:{}, the job still succeeds",
                    dbId, tableId, e);
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

    protected void testGc(boolean isReplay) {
        gc(isReplay);
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
