// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.DdlException;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

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

    private InsertOverwriteJob job;

    private InsertStmt insertStmt;
    private StmtExecutor stmtExecutor;
    private ConnectContext context;
    private Database db;
    private OlapTable targetTable;
    private String postfix;

    private long createPartitionElapse;
    private long insertElapse;

    public InsertOverwriteJobRunner(InsertOverwriteJob job, ConnectContext context, StmtExecutor stmtExecutor,
                                    Database db, OlapTable targetTable) {
        this.job = job;
        this.context = context;
        this.stmtExecutor = stmtExecutor;
        this.insertStmt = job.getInsertStmt();
        this.db = db;
        this.targetTable = targetTable;
        this.postfix = "_" + job.getJobId();
        this.createPartitionElapse = 0;
        this.insertElapse = 0;
    }

    // for replay
    public InsertOverwriteJobRunner(InsertOverwriteJob job) {
        this.job = job;
        this.db = GlobalStateMgr.getCurrentState().getDb(job.getTargetDbId());
        this.targetTable = (OlapTable) db.getTable(job.getTargetTableId());
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
            LOG.warn("cancel failed. insert overwrite job:{} already finished. state:{}", job.getJobState());
            return;
        }
        try {
            transferTo(InsertOverwriteJobState.OVERWRITE_FAILED);
        } catch (Exception e) {
            LOG.warn("cancel insert overwrite job:{} failed", job.getJobId(), e);
        }
    }

    public void run() throws Exception {
        try {
            handle();
        } catch (Exception e) {
            if (job.getJobState() != InsertOverwriteJobState.OVERWRITE_FAILED) {
                transferTo(InsertOverwriteJobState.OVERWRITE_FAILED);
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
        if (job.getJobState() != info.getFromState()) {
            LOG.warn("invalid job info. current state:{}, from state:{}", job.getJobState(), info.getFromState());
            return;
        }
        // state can not be PENDING here
        switch (info.getToState()) {
            case OVERWRITE_RUNNING:
                job.setSourcePartitionIds(info.getSourcePartitionIds());
                job.setTmpPartitionIds(info.getTmpPartitionIds());
                job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
                break;
            case OVERWRITE_FAILED:
                job.setJobState(InsertOverwriteJobState.OVERWRITE_FAILED);
                LOG.info("replay insert overwrite job:{} to FAILED", job.getJobId());
                gc();
                break;
            case OVERWRITE_SUCCESS:
                job.setJobState(InsertOverwriteJobState.OVERWRITE_SUCCESS);
                doCommit();
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
        if (!db.writeLockAndCheckExist()) {
            LOG.warn("database:{} do not exist.", db.getFullName());
            return;
        }
        try {
            InsertOverwriteStateChangeInfo info =
                    new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(), state,
                            job.getSourcePartitionIds(), job.getTmpPartitionIds());
            GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        } finally {
            db.writeUnlock();
        }
        job.setJobState(state);
        handle();
    }

    private void prepare() throws Exception {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_PENDING);
        // get tmpPartitionIds first because they used to drop created partitions when restart.
        List<Long> tmpPartitionIds = Lists.newArrayList();
        for (int i = 0; i < job.getSourcePartitionIds().size(); ++i) {
            tmpPartitionIds.add(GlobalStateMgr.getCurrentState().getNextId());
        }
        job.setTmpPartitionIds(tmpPartitionIds);
        transferTo(InsertOverwriteJobState.OVERWRITE_RUNNING);
    }

    private void executeInsert() throws Exception {
        long insertStartTimestamp = System.currentTimeMillis();
        // should replan here because prepareInsert has changed the targetPartitionNames of insertStmt
        ExecPlan newPlan = new StatementPlanner().plan(insertStmt, context);
        stmtExecutor.handleDMLStmt(newPlan, insertStmt);
        insertElapse = System.currentTimeMillis() - insertStartTimestamp;
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("insert overwrite failed. error message:{}", context.getState().getErrorMessage());
            throw new RuntimeException("execute insert failed");
        }
    }

    private void createTempPartitions() throws DdlException {
        try {
            long createPartitionStartTimestamp = System.currentTimeMillis();
            PartitionUtils.createAndAddTempPartitionsForTable(db, targetTable, postfix,
                    job.getSourcePartitionIds(), job.getTmpPartitionIds());
            createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;
        } catch (Throwable t) {
            LOG.warn("create temp partitions failed", t);
            throw t;
        }
    }

    private void gc() {
        LOG.info("start to garbage collect");
        db.writeLock();
        try {
            if (job.getTmpPartitionIds() != null) {
                for (Long pid : job.getTmpPartitionIds()) {
                    LOG.info("drop temp partition:{}", pid);

                    Partition partition = targetTable.getPartition(pid);
                    if (partition != null) {
                        targetTable.dropTempPartition(partition.getName(), true);
                    } else {
                        LOG.warn("partition {} is null", pid);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("exception when gc insert overwrite job.", e);
        } finally {
            db.writeUnlock();
        }
    }

    private void doCommit() {
        db.writeLock();
        try {
            List<String> sourcePartitionNames = job.getSourcePartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            List<String> tmpPartitionNames = job.getTmpPartitionIds().stream()
                    .map(partitionId -> targetTable.getPartition(partitionId).getName())
                    .collect(Collectors.toList());
            if (targetTable.getPartitionInfo().getType() == PartitionType.RANGE) {

                targetTable.replaceTempPartitions(sourcePartitionNames, tmpPartitionNames, true, false);
            } else {
                targetTable.replacePartition(sourcePartitionNames.get(0), tmpPartitionNames.get(0));
            }
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into dbId:{}, tableId:{}," +
                    " sourcePartitionNames:{}, newPartitionNames:{}", job.getTargetDbId(), job.getTargetTableId(), e);
            throw new RuntimeException("replace partitions failed", e);
        } finally {
            db.writeUnlock();
        }
    }

    private void prepareInsert() {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        Preconditions.checkState(insertStmt != null);
        try {
            db.readLock();
            try {
                List<String> tmpPartitionNames = job.getTmpPartitionIds().stream()
                        .map(partitionId -> targetTable.getPartition(partitionId).getName())
                        .collect(Collectors.toList());
                PartitionNames partitionNames = new PartitionNames(true, tmpPartitionNames);
                // change the TargetPartitionNames from source partitions to new tmp partitions
                // should replan when load data
                insertStmt.setTargetPartitionNames(partitionNames);
                insertStmt.setTargetPartitionIds(job.getTmpPartitionIds());
            } finally {
                db.readUnlock();
            }
        } catch (Exception e) {
            throw new RuntimeException("prepareInsert exception", e);
        }
    }
}
