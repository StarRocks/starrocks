// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.PartitionPersistInfo;
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

public class InsertOverwriteJobRunner {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobRunner.class);

    private InsertOverwriteJob job;

    private long watershedTxnId = -1;
    private InsertStmt insertStmt;
    private StmtExecutor stmtExecutor;
    private ConnectContext context;
    private Database db;
    private OlapTable targetTable;
    private String postfix;

    private long createPartitionElapse;
    private long waitInsertIntoElapse;
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
        this.waitInsertIntoElapse = 0;
        this.insertElapse = 0;
    }

    // for replay
    public InsertOverwriteJobRunner(InsertOverwriteJob job) {
        this.job = job;
        this.db = GlobalStateMgr.getCurrentState().getDb(job.getTargetDbId());
        this.targetTable = (OlapTable) db.getTable(job.getTargetTableId());
        this.postfix = "_" + job.getJobId();
        this.createPartitionElapse = 0;
        this.waitInsertIntoElapse = 0;
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
            LOG.warn("insert overwrite job:{} handle exception", job.getJobId(), e);
            if (job.getJobState() != InsertOverwriteJobState.OVERWRITE_FAILED) {
                transferTo(InsertOverwriteJobState.OVERWRITE_FAILED);
            }
            throw new RuntimeException("insert overwrite failed.", e);
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
                LOG.warn("insert overwrite job:{} failed. createPartitionElapse:{} ms," +
                                " waitInsertIntoElapse:{} ms, insertElapse:{} ms",
                        job.getJobId(), createPartitionElapse, waitInsertIntoElapse, insertElapse);
                break;
            case OVERWRITE_SUCCESS:
                LOG.info("insert overwrite job:{} succeed. createPartitionElapse:{} ms," +
                        " waitInsertIntoElapse:{} ms, insertElapse:{} ms",
                        job.getJobId(), createPartitionElapse, waitInsertIntoElapse, insertElapse);
                break;
            default:
                throw new RuntimeException("invalid jobState:" + job.getJobState());
        }
    }

    private void doLoad() {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        try {
            createTempPartitions();
            prepareInsert();
            executeInsert();
            doCommit();
            transferTo(InsertOverwriteJobState.OVERWRITE_SUCCESS);
        } catch (Exception e) {
            throw new RuntimeException("doLoad failed.", e);
        }
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
                job.setSourcePartitionNames(info.getSourcePartitionNames());
                job.setNewPartitionNames(info.getNewPartitionsName());
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
        InsertOverwriteStateChangeInfo info =
                new InsertOverwriteStateChangeInfo(job.getJobId(), job.getJobState(), state,
                        job.getSourcePartitionNames(), job.getNewPartitionNames());
        GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        job.setJobState(state);
        handle();
    }

    private void prepare() throws Exception {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_PENDING);
        this.watershedTxnId =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        List<Partition> sourcePartitions = job.getOriginalTargetPartitionIds().stream()
                .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
        List<String> sourcePartitionNames = sourcePartitions.stream().map(p -> p.getName()).collect(Collectors.toList());
        job.setSourcePartitionNames(sourcePartitionNames);
        job.setNewPartitionNames(sourcePartitionNames.stream().map(name -> name + postfix).collect(Collectors.toList()));
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

    private void createTempPartitions() {
        try {
            long createPartitionStartTimestamp = System.currentTimeMillis();
            List<Partition> newTempPartitions = GlobalStateMgr.getCurrentState().createTempPartitionsFromPartitions(
                    db, targetTable, postfix, job.getOriginalTargetPartitionIds());
            db.writeLock();
            try {
                List<Partition> sourcePartitions = job.getOriginalTargetPartitionIds().stream()
                        .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
                PartitionInfo partitionInfo = targetTable.getPartitionInfo();
                List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(newTempPartitions.size());
                for (int i = 0; i < newTempPartitions.size(); i++) {
                    targetTable.addTempPartition(newTempPartitions.get(i));
                    long sourcePartitionId = sourcePartitions.get(i).getId();
                    partitionInfo.addPartition(newTempPartitions.get(i).getId(),
                            partitionInfo.getDataProperty(sourcePartitionId),
                            partitionInfo.getReplicationNum(sourcePartitionId),
                            partitionInfo.getIsInMemory(sourcePartitionId));
                    Partition partition = newTempPartitions.get(i);
                    // range is null for UNPARTITIONED type
                    Range<PartitionKey> range = null;
                    if (partitionInfo.getType() == PartitionType.RANGE) {
                        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                        rangePartitionInfo.setRange(partition.getId(), true,
                                rangePartitionInfo.getRange(sourcePartitionId));
                        range = rangePartitionInfo.getRange(partition.getId());
                    }
                    PartitionPersistInfo info =
                            new PartitionPersistInfo(db.getId(), targetTable.getId(), partition,
                                    range,
                                    partitionInfo.getDataProperty(partition.getId()),
                                    partitionInfo.getReplicationNum(partition.getId()),
                                    partitionInfo.getIsInMemory(partition.getId()),
                                    true);
                    partitionInfoList.add(info);
                }
                AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
            } finally {
                createPartitionElapse = System.currentTimeMillis() - createPartitionStartTimestamp;
                db.writeUnlock();
            }
        } catch (Throwable t) {
            LOG.warn("create temp partitions failed", t);
            throw t;
        }
    }

    private void gc() {
        LOG.info("start to garbage collect");
        db.writeLock();
        try {
            if (job.getNewPartitionNames() != null) {
                for (String partitionName : job.getNewPartitionNames()) {
                    LOG.info("drop partition:{}", partitionName);

                    Partition partition = targetTable.getPartition(partitionName, true);
                    if (partition != null) {
                        targetTable.dropTempPartition(partitionName, true);
                    } else {
                        LOG.warn("partition is null for name:{}", partitionName);
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
            if (targetTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                targetTable.replaceTempPartitions(job.getSourcePartitionNames(), job.getNewPartitionNames(), true, false);
            } else {
                targetTable.replacePartition(job.getSourcePartitionNames().get(0), job.getNewPartitionNames().get(0));
            }
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into dbId:{}, tableId:{}," +
                            " sourcePartitionNames:{}, newPartitionNames:{}", job.getTargetDbId(), job.getTargetTableId(),
                    job.getSourcePartitionNames().stream().collect(Collectors.joining(",")),
                    job.getNewPartitionNames().stream().collect(Collectors.joining(",")), e);
            throw new RuntimeException("replace partitions failed", e);
        } finally {
            db.writeUnlock();
        }
    }

    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, job.getTargetDbId(), Lists.newArrayList(job.getTargetTableId()));
    }

    private void prepareInsert() {
        Preconditions.checkState(job.getJobState() == InsertOverwriteJobState.OVERWRITE_RUNNING);
        Preconditions.checkState(insertStmt != null);
        try {
            db.readLock();
            try {
                List<Long> newPartitionIds = job.getNewPartitionNames().stream()
                        .map(partitionName -> targetTable.getPartition(partitionName, true).getId())
                        .collect(Collectors.toList());
                PartitionNames partitionNames = new PartitionNames(true, job.getNewPartitionNames());
                insertStmt.setTargetPartitionNames(partitionNames);
                insertStmt.setTargetPartitionIds(newPartitionIds);
            } finally {
                db.readUnlock();
            }

            // wait the previous loads util finish
            long waitInsertStartTimestamp = System.currentTimeMillis();
            while (!isPreviousLoadFinished() && !context.isKilled()) {
                Thread.sleep(500);
            }
            waitInsertIntoElapse = System.currentTimeMillis() - waitInsertStartTimestamp;
            if (context.isKilled()) {
                throw new RuntimeException("insert overwrite context is killed");
            }
        } catch (Exception e) {
            throw new RuntimeException("prepareInsert exception", e);
        }
    }
}
