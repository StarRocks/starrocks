// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/DeleteHandler.java

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

package com.starrocks.load;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.DeleteJob.DeleteState;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.qe.QueryStateException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PushTask;
import com.starrocks.thrift.TPriority;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DeleteHandler implements Writable {
    private static final Logger LOG = LogManager.getLogger(DeleteHandler.class);
    public static final int CHECK_INTERVAL = 1000;

    // TransactionId -> DeleteJob
    private final Map<Long, DeleteJob> idToDeleteJob;

    // Db -> DeleteInfo list
    @SerializedName(value = "dbToDeleteInfos")
    private final Map<Long, List<MultiDeleteInfo>> dbToDeleteInfos;

    // this lock is protect List<MultiDeleteInfo> add / remove dbToDeleteInfos is use newConcurrentMap
    // so it does not need to protect, although removeOldDeleteInfo only be called in one thread
    // but other thread may call deleteInfoList.add(deleteInfo) so deleteInfoList is not thread safe.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Set<Long> killJobSet;

    public DeleteHandler() {
        idToDeleteJob = Maps.newConcurrentMap();
        dbToDeleteInfos = Maps.newConcurrentMap();
        killJobSet = Sets.newConcurrentHashSet();
    }

    public void killJob(long jobId) {
        killJobSet.add(jobId);
    }

    private enum CancelType {
        METADATA_MISSING,
        TIMEOUT,
        COMMIT_FAIL,
        UNKNOWN,
        USER
    }

    public void process(DeleteStmt stmt) throws DdlException, QueryStateException {
        String dbName = stmt.getTableName().getDb();
        String tableName = stmt.getTableName().getTbl();
        List<String> partitionNames = stmt.getPartitionNames();
        List<Predicate> conditions = stmt.getDeleteConditions();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }

        DeleteJob deleteJob = null;
        try {
            MarkedCountDownLatch<Long, Long> countDownLatch;
            long transactionId = -1;
            db.readLock();
            try {
                Table table = db.getTable(tableName);
                if (table == null) {
                    throw new DdlException("Table does not exist. name: " + tableName);
                }

                if (table.getType() != Table.TableType.OLAP) {
                    throw new DdlException("Not olap type table. type: " + table.getType().name());
                }
                OlapTable olapTable = (OlapTable) table;

                if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                    throw new DdlException("Table's state is not normal: " + tableName);
                }

                boolean noPartitionSpecified = partitionNames.isEmpty();
                if (noPartitionSpecified) {
                    if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                        partitionNames = extractPartitionNamesByCondition(stmt, olapTable);
                        if (partitionNames.isEmpty()) {
                            LOG.info("The delete statement [{}] prunes all partitions",
                                    stmt.getOrigStmt().originStmt);
                            return;
                        }
                    } else if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                        // this is a unpartitioned table, use table name as partition name
                        partitionNames.add(olapTable.getName());
                    }
                }

                Map<Long, Short> partitionReplicaNum = Maps.newHashMap();
                List<Partition> partitions = Lists.newArrayListWithCapacity(partitionNames.size());
                for (String partitionName : partitionNames) {
                    Partition partition = olapTable.getPartition(partitionName);
                    if (partition == null) {
                        throw new DdlException("Partition does not exist. name: " + partitionName);
                    }
                    partitions.add(partition);
                    short replicationNum = ((OlapTable) table).getPartitionInfo().getReplicationNum(partition.getId());
                    partitionReplicaNum.put(partition.getId(), replicationNum);
                }

                List<String> deleteConditions = Lists.newArrayList();

                // pre check
                boolean hasValidCondition = checkDeleteV2(olapTable, partitions, conditions, deleteConditions);
                if (!hasValidCondition) {
                    return;
                }

                // generate label
                String label = "delete_" + UUID.randomUUID();
                long jobId = GlobalStateMgr.getCurrentState().getNextId();
                stmt.setJobId(jobId);
                // begin txn here and generate txn id
                transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                        Lists.newArrayList(table.getId()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        // For version compatibility, keep this set to FRONTEND,
                        // and set it to DELETE in the next release
                        TransactionState.LoadJobSourceType.FRONTEND, jobId, Config.stream_load_default_timeout_second);

                MultiDeleteInfo deleteInfo =
                        new MultiDeleteInfo(db.getId(), olapTable.getId(), tableName, deleteConditions);
                deleteInfo.setPartitions(noPartitionSpecified,
                        partitions.stream().map(Partition::getId).collect(Collectors.toList()), partitionNames);
                deleteJob = new DeleteJob(jobId, transactionId, label, partitionReplicaNum, deleteInfo);
                idToDeleteJob.put(deleteJob.getTransactionId(), deleteJob);

                GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(deleteJob);
                // task sent to be
                AgentBatchTask batchTask = new AgentBatchTask();
                // count total replica num
                int totalReplicaNum = 0;
                for (Partition partition : partitions) {
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            totalReplicaNum += ((LocalTablet) tablet).getReplicas().size();
                        }
                    }
                }

                countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);

                for (Partition partition : partitions) {
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();

                            // set push type
                            TPushType type = TPushType.DELETE;

                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                long replicaId = replica.getId();
                                long backendId = replica.getBackendId();
                                countDownLatch.addMark(backendId, tabletId);

                                // create push task for each replica
                                PushTask pushTask = new PushTask(null,
                                        replica.getBackendId(), db.getId(), olapTable.getId(),
                                        partition.getId(), indexId,
                                        tabletId, replicaId, schemaHash,
                                        -1, 0,
                                        -1, type, conditions,
                                        TPriority.NORMAL,
                                        TTaskType.REALTIME_PUSH,
                                        transactionId,
                                        GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator()
                                                .getNextTransactionId());
                                pushTask.setIsSchemaChanging(false);
                                pushTask.setCountDownLatch(countDownLatch);

                                if (AgentTaskQueue.addTask(pushTask)) {
                                    batchTask.addTask(pushTask);
                                    deleteJob.addPushTask(pushTask);
                                    deleteJob.addTablet(tabletId);
                                }
                            }
                        }
                    }
                }

                // submit push tasks
                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

            } catch (Throwable t) {
                LOG.warn("error occurred during delete process", t);
                // if transaction has been begun, need to abort it
                if (GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), transactionId) !=
                        null) {
                    cancelJob(deleteJob, CancelType.UNKNOWN, t.getMessage());
                }
                throw new DdlException(t.getMessage(), t);
            } finally {
                db.readUnlock();
            }

            long timeoutMs = deleteJob.getTimeoutMs();
            LOG.info("waiting delete Job finish, signature: {}, timeout: {}", transactionId, timeoutMs);
            boolean ok = false;
            try {
                long countDownTime = timeoutMs;
                while (countDownTime > 0) {
                    if (countDownTime > CHECK_INTERVAL) {
                        countDownTime -= CHECK_INTERVAL;
                        if (killJobSet.remove(deleteJob.getId())) {
                            cancelJob(deleteJob, CancelType.USER, "user cancelled");
                            throw new DdlException("Cancelled");
                        }
                        ok = countDownLatch.await(CHECK_INTERVAL, TimeUnit.MILLISECONDS);
                        if (ok) {
                            break;
                        }
                    } else {
                        ok = countDownLatch.await(countDownTime, TimeUnit.MILLISECONDS);
                        break;
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok) {
                String errMsg = "";
                List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                // only show at most 5 results
                List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 5));
                if (!subList.isEmpty()) {
                    errMsg = "unfinished replicas: " + Joiner.on(", ").join(subList);
                }
                LOG.warn(errMsg);

                try {
                    deleteJob.checkAndUpdateQuorum();
                } catch (MetaNotFoundException e) {
                    cancelJob(deleteJob, CancelType.METADATA_MISSING, e.getMessage());
                    throw new DdlException(e.getMessage(), e);
                }
                DeleteState state = deleteJob.getState();
                switch (state) {
                    case UN_QUORUM:
                        LOG.warn("delete job timeout: transactionId {}, timeout {}, {}", transactionId, timeoutMs,
                                errMsg);
                        cancelJob(deleteJob, CancelType.TIMEOUT, "delete job timeout");
                        throw new DdlException("failed to execute delete. transaction id " + transactionId +
                                ", timeout(ms) " + timeoutMs + ", " + errMsg);
                    case QUORUM_FINISHED:
                    case FINISHED:
                        try {
                            long nowQuorumTimeMs = System.currentTimeMillis();
                            long endQuorumTimeoutMs = nowQuorumTimeMs + timeoutMs / 2;
                            // if job's state is quorum_finished then wait for a period of time and commit it.
                            while (deleteJob.getState() == DeleteState.QUORUM_FINISHED &&
                                    endQuorumTimeoutMs > nowQuorumTimeMs) {
                                deleteJob.checkAndUpdateQuorum();
                                Thread.sleep(1000);
                                nowQuorumTimeMs = System.currentTimeMillis();
                                LOG.debug("wait for quorum finished delete job: {}, txn_id: {}", deleteJob.getId(),
                                        transactionId);
                            }
                        } catch (MetaNotFoundException e) {
                            cancelJob(deleteJob, CancelType.METADATA_MISSING, e.getMessage());
                            throw new DdlException(e.getMessage(), e);
                        } catch (InterruptedException e) {
                            cancelJob(deleteJob, CancelType.UNKNOWN, e.getMessage());
                            throw new DdlException(e.getMessage(), e);
                        }
                        commitJob(deleteJob, db, timeoutMs);
                        break;
                    default:
                        throw new IllegalStateException("wrong delete job state: " + state.name());
                }
            } else {
                commitJob(deleteJob, db, timeoutMs);
            }
        } finally {
            if (!FeConstants.runningUnitTest) {
                clearJob(deleteJob);
            }
        }
    }

    @VisibleForTesting
    public List<String> extractPartitionNamesByCondition(DeleteStmt stmt, OlapTable olapTable)
            throws DdlException, AnalysisException {
        List<String> partitionNames = Lists.newArrayList();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Map<String, PartitionColumnFilter> columnFilters = extractColumnFilter(stmt, olapTable,
                rangePartitionInfo.getPartitionColumns());
        Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
        if (columnFilters.isEmpty()) {
            partitionNames.addAll(olapTable.getPartitionNames());
        } else {
            RangePartitionPruner pruner = new RangePartitionPruner(keyRangeById,
                    rangePartitionInfo.getPartitionColumns(), columnFilters);
            Collection<Long> selectedPartitionIds = pruner.prune();

            if (selectedPartitionIds == null) {
                partitionNames.addAll(olapTable.getPartitionNames());
            } else {
                for (Long partitionId : selectedPartitionIds) {
                    Partition partition = olapTable.getPartition(partitionId);
                    partitionNames.add(partition.getName());
                }
            }
        }
        return partitionNames;
    }

    private Map<String, PartitionColumnFilter> extractColumnFilter(DeleteStmt stmt, Table table,
                                                                   List<Column> partitionColumns)
            throws DdlException, AnalysisException {
        Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
        List<Predicate> deleteConditions = stmt.getDeleteConditions();
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getBaseSchema()) {
            nameToColumn.put(column.getName(), column);
        }
        for (Predicate condition : deleteConditions) {
            SlotRef slotRef = (SlotRef) condition.getChild(0);
            String columnName = slotRef.getColumnName();

            // filter condition is not partition column;
            if (partitionColumns.stream().noneMatch(e -> e.getName().equals(columnName))) {
                continue;
            }

            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                LiteralExpr literalExpr = (LiteralExpr) binaryPredicate.getChild(1);
                Column column = nameToColumn.get(columnName);
                literalExpr = LiteralExpr.create(literalExpr.getStringValue(),
                        Objects.requireNonNull(Type.fromPrimitiveType(column.getPrimitiveType())));
                PartitionColumnFilter filter = columnFilters.getOrDefault(slotRef.getColumnName(),
                        new PartitionColumnFilter());
                switch (binaryPredicate.getOp()) {
                    case EQ:
                        filter.setLowerBound(literalExpr, true);
                        filter.setUpperBound(literalExpr, true);
                        break;
                    case LE:
                        filter.setUpperBound(literalExpr, true);
                        filter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        filter.setUpperBound(literalExpr, false);
                        filter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        filter.setLowerBound(literalExpr, true);
                        break;
                    case GT:
                        filter.setLowerBound(literalExpr, false);
                        break;
                    default:
                        break;
                }
                columnFilters.put(slotRef.getColumnName(), filter);
            } else if (condition instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) condition;
                if (inPredicate.isNotIn()) {
                    continue;
                }
                List<LiteralExpr> list = Lists.newArrayList();
                Column column = nameToColumn.get(columnName);
                for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                    LiteralExpr literalExpr = (LiteralExpr) inPredicate.getChild(i);
                    literalExpr = LiteralExpr.create(literalExpr.getStringValue(),
                            Objects.requireNonNull(Type.fromPrimitiveType(column.getPrimitiveType())));
                    list.add(literalExpr);
                }

                PartitionColumnFilter filter = columnFilters.getOrDefault(slotRef.getColumnName(),
                        new PartitionColumnFilter());
                filter.setInPredicateLiterals(list);
                columnFilters.put(slotRef.getColumnName(), filter);
            }

        }
        return columnFilters;
    }

    private void commitJob(DeleteJob job, Database db, long timeoutMs) throws DdlException, QueryStateException {
        TransactionStatus status = null;
        try {
            if (unprotectedCommitJob(job, db, timeoutMs)) {
                updateTableDeleteInfo(GlobalStateMgr.getCurrentState(), db.getId(), job.getDeleteInfo().getTableId());
            }
            status = GlobalStateMgr.getCurrentGlobalTransactionMgr().
                    getTransactionState(db.getId(), job.getTransactionId()).getTransactionStatus();
        } catch (UserException e) {
            if (cancelJob(job, CancelType.COMMIT_FAIL, e.getMessage())) {
                throw new DdlException(e.getMessage(), e);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(job.getLabel()).append("', 'status':'").append(status.name());
        sb.append("', 'txnId':'").append(job.getTransactionId()).append("'");

        switch (status) {
            case COMMITTED: {
                // Although publish is unfinished we should tell user that commit already success.
                String errMsg = "delete job is committed but may be taking effect later";
                sb.append(", 'err':'").append(errMsg).append("'");
                sb.append("}");
                throw new QueryStateException(MysqlStateType.OK, sb.toString());
            }
            case VISIBLE: {
                sb.append("}");
                throw new QueryStateException(MysqlStateType.OK, sb.toString());
            }
            default:
                throw new IllegalStateException("wrong transaction status: " + status.name());
        }
    }

    /**
     * unprotected commit delete job
     * return true when successfully commit and publish
     * return false when successfully commit but publish unfinished.
     * A UserException thrown if both commit and publish failed.
     *
     * @param job
     * @param db
     * @param timeoutMs
     * @return
     * @throws UserException
     */
    private boolean unprotectedCommitJob(DeleteJob job, Database db, long timeoutMs) throws UserException {
        long transactionId = job.getTransactionId();
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<TabletCommitInfo>();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (TabletDeleteInfo tDeleteInfo : job.getTabletDeleteInfo()) {
            for (Replica replica : tDeleteInfo.getFinishedReplicas()) {
                // the inverted index contains rolling up replica
                Long tabletId = invertedIndex.getTabletIdByReplica(replica.getId());
                if (tabletId == null) {
                    LOG.warn("could not find tablet id for replica {}, the tablet maybe dropped", replica);
                    continue;
                }
                tabletCommitInfos.add(new TabletCommitInfo(tabletId, replica.getBackendId()));
            }
        }
        return globalTransactionMgr.commitAndPublishTransaction(db, transactionId, tabletCommitInfos, timeoutMs);
    }

    /**
     * This method should always be called in the end of the delete process to clean the job.
     * Better put it in finally block.
     *
     * @param job
     */
    private void clearJob(DeleteJob job) {
        if (job != null) {
            long signature = job.getTransactionId();
            idToDeleteJob.remove(signature);
            for (PushTask pushTask : job.getPushTasks()) {
                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                        pushTask.getVersion(),
                        pushTask.getPushType(), pushTask.getTaskType());
            }

            // NOT remove callback from GlobalTransactionMgr's callback factory here.
            // the callback will be removed after transaction is aborted of visible.
        }
    }

    public void recordFinishedJob(DeleteJob job) {
        if (job != null) {
            long dbId = job.getDeleteInfo().getDbId();
            LOG.info("record finished deleteJob, transactionId {}, dbId {}",
                    job.getTransactionId(), dbId);
            dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
            List<MultiDeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
            lock.writeLock().lock();
            try {
                deleteInfoList.add(job.getDeleteInfo());
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * abort delete job
     * return true when successfully abort.
     * return true when some unknown error happened, just ignore it.
     * return false when the job is already committed
     *
     * @param job
     * @param cancelType
     * @param reason
     * @return
     */
    public boolean cancelJob(DeleteJob job, CancelType cancelType, String reason) {
        if (job == null) {
            LOG.warn("cancel a null job, cancelType: {}, reason: {}", cancelType.name(), reason);
            return true;
        }
        LOG.info("start to cancel delete job, transactionId: {}, cancelType: {}", job.getTransactionId(),
                cancelType.name());

        // create push task for each backends
        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
        for (Long backendId : backendIds) {
            PushTask cancelDeleteTask = new PushTask(backendId, TPushType.CANCEL_DELETE, TPriority.HIGH,
                    TTaskType.REALTIME_PUSH, job.getTransactionId(),
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId());
            AgentTaskQueue.removePushTaskByTransactionId(backendId, job.getTransactionId(),
                    TPushType.DELETE, TTaskType.REALTIME_PUSH);
            AgentTaskExecutor.submit(new AgentBatchTask(cancelDeleteTask));
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        try {
            globalTransactionMgr.abortTransaction(job.getDeleteInfo().getDbId(), job.getTransactionId(), reason);
        } catch (Exception e) {
            TransactionState state =
                    globalTransactionMgr.getTransactionState(job.getDeleteInfo().getDbId(), job.getTransactionId());
            if (state == null) {
                LOG.warn("cancel delete job failed because txn not found, transactionId: {}", job.getTransactionId());
            } else if (state.getTransactionStatus() == TransactionStatus.COMMITTED ||
                    state.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.warn("cancel delete job {} failed because it has been committed, transactionId: {}",
                        job.getId(), job.getTransactionId());
                return false;
            } else {
                LOG.warn("errors while abort transaction", e);
            }
        }
        return true;
    }

    public DeleteJob getDeleteJob(long transactionId) {
        return idToDeleteJob.get(transactionId);
    }

    private SlotRef getSlotRef(Predicate condition) {
        if (condition instanceof BinaryPredicate || condition instanceof IsNullPredicate ||
                condition instanceof InPredicate) {
            return (SlotRef) condition.getChild(0);
        }
        return null;
    }

    /**
     * @param table
     * @param partitions
     * @param conditions
     * @param deleteConditions
     * @return return false means no need to push delete condition to BE
     * return ture means it should go through the following procedure
     * @throws DdlException
     */
    private boolean checkDeleteV2(OlapTable table, List<Partition> partitions, List<Predicate> conditions,
                                  List<String> deleteConditions)
            throws DdlException {

        // check partition state
        for (Partition partition : partitions) {
            Partition.PartitionState state = partition.getState();
            if (state != Partition.PartitionState.NORMAL) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_BAD_PARTITION_STATE, partition.getName(), state.name());
                throw new DdlException("Partition[" + partition.getName() + "]' state is not NORMAL: " + state.name());
            }
        }

        // primary key table do not support delete sql statement yet
        if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
            throw new DdlException("primary key tablet do not support delete statement yet");
        }

        // check condition column is key column and condition value
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getBaseSchema()) {
            nameToColumn.put(column.getName(), column);
        }
        for (Predicate condition : conditions) {
            SlotRef slotRef = getSlotRef(condition);
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }

            Column column = nameToColumn.get(columnName);
            // Due to rounding errors, most floating-point numbers end up being slightly imprecise,
            // it also means that numbers expected to be equal often differ slightly, so we do not allow compare with
            // floating-point numbers, floating-point number not allowed in where clause
            if (!column.isKey() && table.getKeysType() != KeysType.DUP_KEYS
                    || column.getPrimitiveType().isFloatingPointType()) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_NOT_KEY_COLUMN, columnName);
                throw new DdlException("Column[" + columnName + "] is not key column or storage model " +
                        "is not duplicate or column type is float or double.");
            }

            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    if (binaryPredicate.getOp() == BinaryPredicate.Operator.EQ_FOR_NULL) {
                        throw new DdlException("Delete condition does not support null-safe equal currently.");
                    }
                    // delete a = null means delete nothing
                    if (binaryPredicate.getChild(1) instanceof NullLiteral) {
                        return false;
                    }
                    updatePredicate(binaryPredicate, column, 1);
                } catch (AnalysisException e) {
                    // ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, value);
                    throw new DdlException("Invalid value for column " + columnName + ": " + e.getMessage());
                }
            } else if (condition instanceof InPredicate) {
                String value = null;
                try {
                    InPredicate inPredicate = (InPredicate) condition;
                    // delete a in (null) means delete nothing
                    inPredicate.removeNullChild();
                    int inElementNum = inPredicate.getInElementNum();
                    if (inElementNum == 0) {
                        return false;
                    }
                    for (int i = 1; i <= inElementNum; i++) {
                        updatePredicate(inPredicate, column, i);
                    }
                } catch (AnalysisException e) {
                    throw new DdlException("Invalid column value[" + value + "] for column " + columnName);
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
        // check materialized index.
        // only need check the first partition because each partition has same materialized view
        Map<Long, List<Column>> indexIdToSchema = table.getIndexIdToSchema();
        Partition partition = partitions.get(0);
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            if (table.getBaseIndexId() == index.getId()) {
                continue;
            }
            // check table has condition column
            Map<String, Column> indexColNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : indexIdToSchema.get(index.getId())) {
                indexColNameToColumn.put(column.getName(), column);
            }
            String indexName = table.getIndexNameById(index.getId());
            for (Predicate condition : conditions) {
                String columnName = getSlotRef(condition).getColumnName();
                Column column = indexColNameToColumn.get(columnName);
                if (column == null) {
                    ErrorReport
                            .reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, "index[" + indexName + "]");
                }
                MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(index.getId());
                if (indexMeta.getKeysType() != KeysType.DUP_KEYS && !column.isKey()) {
                    throw new DdlException("Column[" + columnName + "] is not key column in index[" + indexName + "]");
                }
            }
        }

        if (deleteConditions == null) {
            return true;
        }

        // save delete conditions
        for (Predicate condition : conditions) {
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName).append(" ").append(binaryPredicate.getOp().name()).append(" \"")
                        .append(((LiteralExpr) binaryPredicate.getChild(1)).getStringValue()).append("\"");
                deleteConditions.add(sb.toString());
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName);
                if (isNullPredicate.isNotNull()) {
                    sb.append(" IS NOT NULL");
                } else {
                    sb.append(" IS NULL");
                }
                deleteConditions.add(sb.toString());
            } else if (condition instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) condition;
                SlotRef slotRef = (SlotRef) inPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder strBuilder = new StringBuilder();
                String notStr = inPredicate.isNotIn() ? "NOT " : "";
                strBuilder.append(columnName).append(" ").append(notStr).append("IN (");
                int inElementNum = inPredicate.getInElementNum();
                for (int i = 1; i <= inElementNum; ++i) {
                    strBuilder.append(inPredicate.getChild(i).toSql());
                    strBuilder.append((i != inPredicate.getInElementNum()) ? ", " : "");
                }
                strBuilder.append(")");
                deleteConditions.add(strBuilder.toString());
            }
        }
        return true;
    }

    // if a bool cond passed to be, be's zone_map cannot handle bool correctly,
    // change it to a tinyint type here;
    // for DateLiteral needs to be converted to the correct format uniformly
    // if datekey type pass will cause delete cond timeout.
    public void updatePredicate(Predicate predicate, Column column, int childNo) throws AnalysisException {
        String value;
        value = ((LiteralExpr) predicate.getChild(childNo)).getStringValue();
        if (column.getPrimitiveType() == PrimitiveType.BOOLEAN) {
            if (value.equalsIgnoreCase("true")) {
                predicate.setChild(childNo, LiteralExpr.create("1", Type.TINYINT));
            } else if (value.equalsIgnoreCase("false")) {
                predicate.setChild(childNo, LiteralExpr.create("0", Type.TINYINT));
            }
        }
<<<<<<< HEAD
        LiteralExpr result =
                LiteralExpr.create(value, Objects.requireNonNull(Type.fromPrimitiveType(column.getPrimitiveType())));
=======

        LiteralExpr result = LiteralExpr.create(value, Objects.requireNonNull(column.getType()));
>>>>>>> fa6c0f636 ([BugFix] fix delete predicate of array type (#10939))
        if (result instanceof DecimalLiteral) {
            ((DecimalLiteral) result).checkPrecisionAndScale(column.getPrecision(), column.getScale());
        } else if (result instanceof DateLiteral) {
            predicate.setChild(childNo, result);
        }
    }

    // show delete stmt
    public List<List<Comparable>> getDeleteInfosByDb(long dbId) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return infos;
        }

        String dbName = db.getFullName();
        List<MultiDeleteInfo> deleteInfos = dbToDeleteInfos.get(dbId);

        lock.readLock().lock();
        try {
            if (deleteInfos == null) {
                return infos;
            }

            for (MultiDeleteInfo deleteInfo : deleteInfos) {

                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                        deleteInfo.getTableName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                List<Comparable> info = Lists.newArrayList();
                info.add(deleteInfo.getTableName());
                if (deleteInfo.isNoPartitionSpecified()) {
                    info.add("*");
                } else {
                    if (deleteInfo.getPartitionNames() == null) {
                        info.add("");
                    } else {
                        info.add(Joiner.on(", ").join(deleteInfo.getPartitionNames()));
                    }
                }

                info.add(TimeUtils.longToTimeString(deleteInfo.getCreateTimeMs()));
                String conds = Joiner.on(", ").join(deleteInfo.getDeleteConditions());
                info.add(conds);

                info.add("FINISHED");
                infos.add(info);
            }
        } finally {
            lock.readLock().unlock();
        }
        // sort by createTimeMs
        ListComparator<List<Comparable>> comparator = new ListComparator<>(2);
        infos.sort(comparator);
        return infos;
    }

    public void replayDelete(DeleteInfo deleteInfo, GlobalStateMgr globalStateMgr) {
        // add to deleteInfos
        if (deleteInfo == null) {
            return;
        }
        long dbId = deleteInfo.getDbId();
        LOG.info("replay delete, dbId {}", dbId);
        updateTableDeleteInfo(globalStateMgr, dbId, deleteInfo.getTableId());

        if (isDeleteInfoExpired(deleteInfo, System.currentTimeMillis())) {
            LOG.info("discard expired delete info {}", deleteInfo);
            return;
        }

        dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
        List<MultiDeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
        lock.writeLock().lock();
        try {
            deleteInfoList.add(MultiDeleteInfo.upgrade(deleteInfo));
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void replayMultiDelete(MultiDeleteInfo deleteInfo, GlobalStateMgr globalStateMgr) {
        // add to deleteInfos
        if (deleteInfo == null) {
            return;
        }

        long dbId = deleteInfo.getDbId();
        LOG.info("replay delete, dbId {}", dbId);
        updateTableDeleteInfo(globalStateMgr, dbId, deleteInfo.getTableId());

        if (isDeleteInfoExpired(deleteInfo, System.currentTimeMillis())) {
            LOG.info("discard expired delete info {}", deleteInfo);
            return;
        }

        dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
        List<MultiDeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
        lock.writeLock().lock();
        try {
            deleteInfoList.add(deleteInfo);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateTableDeleteInfo(GlobalStateMgr globalStateMgr, long dbId, long tableId) {
        Database db = globalStateMgr.getDb(dbId);
        if (db == null) {
            return;
        }
        Table table = db.getTable(tableId);
        if (table == null) {
            return;
        }
        OlapTable olapTable = (OlapTable) table;
        olapTable.setHasDelete();
    }

    // for delete handler, we only persist those delete already finished.
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DeleteHandler read(DataInput in) throws IOException {
        String json;
        try {
            json = Text.readString(in);

            // In older versions of fe, the information in the deleteHandler is not cleaned up,
            // and if there are many delete statements, it will cause an int overflow
            // and report an IllegalArgumentException.
            //
            // dbToDeleteInfos is only used to record history delete info,
            // discarding it doesn't make much of a difference
        } catch (IllegalArgumentException e) {
            LOG.warn("read delete handler json string failed, ignore", e);
            return new DeleteHandler();
        }
        return GsonUtils.GSON.fromJson(json, DeleteHandler.class);
    }

    public long saveDeleteHandler(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    private boolean isDeleteInfoExpired(DeleteInfo deleteInfo, long currentTimeMs) {
        return (currentTimeMs - deleteInfo.getCreateTimeMs()) / 1000 > Config.label_keep_max_second;
    }

    private boolean isDeleteInfoExpired(MultiDeleteInfo deleteInfo, long currentTimeMs) {
        return (currentTimeMs - deleteInfo.getCreateTimeMs()) / 1000 > Config.label_keep_max_second;
    }

    public void removeOldDeleteInfo() {
        long currentTimeMs = System.currentTimeMillis();
        Iterator<Entry<Long, List<MultiDeleteInfo>>> logIterator = dbToDeleteInfos.entrySet().iterator();
        while (logIterator.hasNext()) {

            List<MultiDeleteInfo> deleteInfos = logIterator.next().getValue();
            lock.writeLock().lock();
            try {
                deleteInfos.sort((o1, o2) -> Long.signum(o1.getCreateTimeMs() - o2.getCreateTimeMs()));
                int numJobsToRemove = deleteInfos.size() - Config.label_keep_max_num;

                Iterator<MultiDeleteInfo> iterator = deleteInfos.iterator();
                while (iterator.hasNext()) {
                    MultiDeleteInfo deleteInfo = iterator.next();
                    if (isDeleteInfoExpired(deleteInfo, currentTimeMs) || numJobsToRemove > 0) {
                        iterator.remove();
                        --numJobsToRemove;
                    }
                }
                if (deleteInfos.isEmpty()) {
                    logIterator.remove();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
