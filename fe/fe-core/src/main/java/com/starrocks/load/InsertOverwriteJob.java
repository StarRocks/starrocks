// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
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

public class InsertOverwriteJob {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJob.class);

    @SerializedName(value = "jobId")
    private long jobId;

    public enum OverwriteJobState {
        OVERWRITE_PENDING,
        OVERWRITE_PREPARED,
        OVERWRITE_SUCCESS,
        OVERWRITE_FAILED,
        OVERWRITE_CANCELLED
    }
    @SerializedName(value = "jobState")
    private OverwriteJobState jobState;

    @SerializedName(value = "sourcePartitionNames")
    private List<String> sourcePartitionNames;

    @SerializedName(value = "newPartitionNames")
    private List<String> newPartitionNames;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "targetTableId")
    private long targetTableId;

    @SerializedName(value = "targetTableName")
    private String targetTableName;

    @SerializedName(value = "targetPartitionIds")
    private List<Long> originalTargetPartitionIds;

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

    public InsertOverwriteJob(long jobId, ConnectContext context, StmtExecutor stmtExecutor,
                              InsertStmt insertStmt, Database db,
                              OlapTable targetTable) {
        this.jobId = jobId;
        this.context = context;
        this.stmtExecutor = stmtExecutor;
        this.insertStmt = insertStmt;
        this.db = db;
        this.targetTable = targetTable;
        this.originalTargetPartitionIds = insertStmt.getTargetPartitionIds();
        this.jobState = OverwriteJobState.OVERWRITE_PENDING;
        this.dbId = db.getId();
        this.targetTableId = targetTable.getId();
        this.targetTableName = targetTable.getName();
        this.postfix = "_" + jobId;
        this.createPartitionElapse = 0;
        this.waitInsertIntoElapse = 0;
        this.insertElapse = 0;
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long dbId, long targetTableId,
                              String targetTableName, List<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.jobState = OverwriteJobState.OVERWRITE_PENDING;
        this.dbId = dbId;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.originalTargetPartitionIds = targetPartitionIds;
        this.db = GlobalStateMgr.getCurrentState().getDb(dbId);
        this.targetTable = (OlapTable) db.getTable(targetTableId);
        this.postfix = "_" + jobId;
        this.createPartitionElapse = 0;
        this.waitInsertIntoElapse = 0;
        this.insertElapse = 0;
    }

    public long getTargetDbId() {
        return dbId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public List<Long> getOriginalTargetPartitionIds() {
        return originalTargetPartitionIds;
    }

    public OverwriteJobState getJobState() {
        return jobState;
    }

    public boolean isFinished() {
        return jobState == OverwriteJobState.OVERWRITE_SUCCESS
                || jobState == OverwriteJobState.OVERWRITE_FAILED
                || jobState == OverwriteJobState.OVERWRITE_CANCELLED;
    }

    // only called from log replay
    // there is no concurrent problem here
    public void cancel() {
        if (isFinished()) {
            LOG.warn("cancel failed. insert overwrite job:{} already finished. state:{}", jobState);
            return;
        }
        try {
            transferTo(OverwriteJobState.OVERWRITE_CANCELLED);
        } catch (Exception e) {
            LOG.warn("cancel insert overwrite job:{} failed", jobId, e);
        }
    }

    public void run() throws Exception {
        try {
            handle();
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} handle exception", jobId, e);
            if (jobState != OverwriteJobState.OVERWRITE_FAILED && jobState != OverwriteJobState.OVERWRITE_CANCELLED) {
                transferTo(OverwriteJobState.OVERWRITE_FAILED);
            }
            throw new RuntimeException("insert overwrite failed.", e);
        }
    }

    public void handle() throws Exception {
        switch (jobState) {
            case OVERWRITE_PENDING:
                prepare();
                break;
            case OVERWRITE_PREPARED:
                doLoad();
                break;
            case OVERWRITE_FAILED:
            case OVERWRITE_CANCELLED:
                gc();
                LOG.warn("insert overwrite job:{} failed. createPartitionElapse:{} ms," +
                                " waitInsertIntoElapse:{} ms, insertElapse:{} ms",
                        jobId, createPartitionElapse, waitInsertIntoElapse, insertElapse);
                break;
            case OVERWRITE_SUCCESS:
                LOG.info("insert overwrite job:{} succeed. createPartitionElapse:{} ms," +
                        " waitInsertIntoElapse:{} ms, insertElapse:{} ms",
                        jobId, createPartitionElapse, waitInsertIntoElapse, insertElapse);
                break;
            default:
                throw new RuntimeException("invalid jobState:" + jobState);
        }
    }

    private void doLoad() throws Exception {
        Preconditions.checkState(jobState == OverwriteJobState.OVERWRITE_PREPARED);
        try {
            createTempPartitions();
            prepareInsert();
            executeInsert();
            doCommit();
            transferTo(OverwriteJobState.OVERWRITE_SUCCESS);
        } catch (Exception e) {
            throw new RuntimeException("doLoad failed.", e);
        }
    }

    public void replayStateChange(InsertOverwriteStateChangeInfo info) {
        LOG.info("replay state change:{}", info);
        if (info.getFromState() != jobState) {
            LOG.warn("invalid job info. current state:{}, from state:{}", jobState, info.getFromState());
            return;
        }
        // state can not be PENDING here
        switch (info.getToState()) {
            case OVERWRITE_PREPARED:
                sourcePartitionNames = info.getSourcePartitionNames();
                newPartitionNames = info.getNewPartitionsName();
                jobState = OverwriteJobState.OVERWRITE_PREPARED;
                break;
            case OVERWRITE_FAILED:
                jobState = OverwriteJobState.OVERWRITE_FAILED;
                LOG.info("replay insert overwrite job:{} to FAILED", jobId);
                gc();
                break;
            case OVERWRITE_CANCELLED:
                jobState = OverwriteJobState.OVERWRITE_CANCELLED;
                LOG.info("replay insert overwrite job:{} to CANCELLED", jobId);
                gc();
                break;
            case OVERWRITE_SUCCESS:
                jobState = OverwriteJobState.OVERWRITE_SUCCESS;
                doCommit();
                LOG.info("replay insert overwrite job:{} to SUCCESS", jobId);
                break;
            default:
                LOG.warn("invalid to state:{} for insert overwrite job:{}", info.getToState(), jobId);
                break;
        }
    }

    private void transferTo(OverwriteJobState state) throws Exception {
        if (state == OverwriteJobState.OVERWRITE_SUCCESS) {
            Preconditions.checkState(jobState == OverwriteJobState.OVERWRITE_PREPARED);
        }
        InsertOverwriteStateChangeInfo info =
                new InsertOverwriteStateChangeInfo(jobId, jobState, state,
                        sourcePartitionNames, newPartitionNames);
        GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        jobState = state;
        handle();
    }

    private void prepare() throws Exception {
        Preconditions.checkState(jobState == OverwriteJobState.OVERWRITE_PENDING);
        this.watershedTxnId =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        List<Partition> sourcePartitions = originalTargetPartitionIds.stream()
                .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
        sourcePartitionNames = sourcePartitions.stream().map(p -> p.getName()).collect(Collectors.toList());
        newPartitionNames = sourcePartitionNames.stream().map(name -> name + postfix).collect(Collectors.toList());
        transferTo(OverwriteJobState.OVERWRITE_PREPARED);
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
                    db, targetTable, postfix, originalTargetPartitionIds);
            db.writeLock();
            try {
                List<Partition> sourcePartitions = originalTargetPartitionIds.stream()
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
            if (newPartitionNames != null) {
                for (String partitionName : newPartitionNames) {
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
                targetTable.replaceTempPartitions(sourcePartitionNames, newPartitionNames, true, false);
            } else {
                targetTable.replacePartition(sourcePartitionNames.get(0), newPartitionNames.get(0));
            }
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into tableId:{}, table:{}," +
                            " sourcePartitionNames:{}, newPartitionNames:{}",
                    targetTableId, targetTableName,
                    sourcePartitionNames.stream().collect(Collectors.joining(",")),
                    newPartitionNames.stream().collect(Collectors.joining(",")), e);
            throw new RuntimeException("replace partitions failed", e);
        } finally {
            db.writeUnlock();
        }
    }

    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(targetTableId));
    }

    private void prepareInsert() {
        Preconditions.checkState(jobState == OverwriteJobState.OVERWRITE_PREPARED);
        Preconditions.checkState(insertStmt != null);
        try {
            db.readLock();
            try {
                List<Long> newPartitionIds = newPartitionNames.stream()
                        .map(partitionName -> targetTable.getPartition(partitionName, true).getId())
                        .collect(Collectors.toList());
                PartitionNames partitionNames = new PartitionNames(true, newPartitionNames);
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
            waitInsertIntoElapse = waitInsertStartTimestamp - waitInsertStartTimestamp;
            if (context.isKilled()) {
                throw new RuntimeException("insert overwrite context is killed");
            }
        } catch (Exception e) {
            throw new RuntimeException("prepareInsert exception", e);
        }
    }
}
