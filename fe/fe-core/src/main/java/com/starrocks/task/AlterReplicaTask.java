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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/AlterReplicaTask.java

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

package com.starrocks.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.meta.TabletMetastore;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAlterJobType;
import com.starrocks.thrift.TAlterMaterializedViewParam;
import com.starrocks.thrift.TAlterTabletMaterializedColumnReq;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * This task is used for alter table process, such as rollup and schema change
 * The task will do data transformation from base replica to new replica.
 * The new replica should be created before.
 * The new replica can be a rollup replica, or a shadow replica of schema change.
 */
public class AlterReplicaTask extends AgentTask implements Runnable {

    private static final Logger LOG = LogManager.getLogger(AlterReplicaTask.class);

    private final long baseTabletId;
    private final long newReplicaId;
    private final int baseSchemaHash;
    private final int newSchemaHash;
    private final long version;
    private final long jobId;
    private final AlterJobV2.JobType jobType;
    private final TTabletType tabletType;
    private final long txnId;
    private final TAlterTabletMaterializedColumnReq generatedColumnReq;
    private List<Column> baseSchemaColumns;
    private RollupJobV2Params rollupJobV2Params;

    public static class RollupJobV2Params {
        private final Map<String, Expr> defineExprs;
        private final Expr whereExpr;
        private final DescriptorTable descTabl;
        private final List<ColumnId> baseTableColIds;

        public RollupJobV2Params(Map<String, Expr> defineExprs,
                                 Expr whereExpr,
                                 DescriptorTable descTabl,
                                 List<ColumnId> baseTableColIds) {
            this.defineExprs = defineExprs;
            this.whereExpr = whereExpr;
            this.descTabl = descTabl;
            this.baseTableColIds = baseTableColIds;
        }

        public Map<String, Expr> getDefineExprs() {
            return defineExprs;
        }

        public Expr getWhereExpr() {
            return whereExpr;
        }

        public DescriptorTable getDescTabl() {
            return descTabl;
        }

        public List<ColumnId> getBaseTableColIds() {
            return baseTableColIds;
        }
    }

    public static AlterReplicaTask alterLocalTablet(long backendId, long dbId, long tableId, long partitionId, long rollupIndexId,
                                                    long rollupTabletId, long baseTabletId, long newReplicaId, int newSchemaHash,
                                                    int baseSchemaHash, long version, long jobId,
                                                    TAlterTabletMaterializedColumnReq generatedColumnReq,
                                                    List<Column> baseSchemaColumns) {
        return new AlterReplicaTask(backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                baseTabletId, newReplicaId, newSchemaHash, baseSchemaHash, version, jobId, AlterJobV2.JobType.SCHEMA_CHANGE,
                TTabletType.TABLET_TYPE_DISK, 0, generatedColumnReq, baseSchemaColumns, null);
    }

    public static AlterReplicaTask alterLakeTablet(long backendId, long dbId, long tableId, long partitionId, long rollupIndexId,
                                                   long rollupTabletId, long baseTabletId, long version, long jobId, long txnId) {
        return new AlterReplicaTask(backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                baseTabletId, -1, -1, -1, version, jobId, AlterJobV2.JobType.SCHEMA_CHANGE,
                TTabletType.TABLET_TYPE_LAKE, txnId, null, Collections.emptyList(), null);
    }

    public static AlterReplicaTask rollupLocalTablet(long backendId, long dbId, long tableId, long partitionId,
                                                     long rollupIndexId, long rollupTabletId, long baseTabletId,
                                                     long newReplicaId, int newSchemaHash, int baseSchemaHash, long version,
                                                     long jobId, RollupJobV2Params rollupJobV2Params,
                                                     List<Column> baseSchemaColumns) {
        return new AlterReplicaTask(backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                baseTabletId, newReplicaId, newSchemaHash, baseSchemaHash, version, jobId, AlterJobV2.JobType.ROLLUP,
                TTabletType.TABLET_TYPE_DISK, 0, null,
                baseSchemaColumns, rollupJobV2Params);
    }

    private AlterReplicaTask(long backendId, long dbId, long tableId, long partitionId, long rollupIndexId, long rollupTabletId,
                             long baseTabletId, long newReplicaId, int newSchemaHash, int baseSchemaHash, long version,
                             long jobId, AlterJobV2.JobType jobType,
                             TTabletType tabletType, long txnId, TAlterTabletMaterializedColumnReq generatedColumnReq,
                             List<Column> baseSchemaColumns, RollupJobV2Params rollupJobV2Params) {
        super(null, backendId, TTaskType.ALTER, dbId, tableId, partitionId, rollupIndexId, rollupTabletId);

        this.baseTabletId = baseTabletId;
        this.newReplicaId = newReplicaId;

        this.newSchemaHash = newSchemaHash;
        this.baseSchemaHash = baseSchemaHash;

        this.version = version;
        this.jobId = jobId;

        this.jobType = jobType;
        this.tabletType = tabletType;
        this.txnId = txnId;

        this.generatedColumnReq = generatedColumnReq;
        this.baseSchemaColumns = baseSchemaColumns;

        this.rollupJobV2Params = rollupJobV2Params;
    }

    public long getBaseTabletId() {
        return baseTabletId;
    }

    public long getNewReplicaId() {
        return newReplicaId;
    }

    public int getNewSchemaHash() {
        return newSchemaHash;
    }

    public int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getJobId() {
        return jobId;
    }

    public AlterJobV2.JobType getJobType() {
        return jobType;
    }

    public TAlterTabletReqV2 toThrift() {
        TAlterTabletReqV2 req = new TAlterTabletReqV2(baseTabletId, signature, baseSchemaHash, newSchemaHash);
        req.setAlter_version(version);
        switch (jobType) {
            case ROLLUP:
                req.setAlter_job_type(TAlterJobType.ROLLUP);
                break;
            case SCHEMA_CHANGE:
                req.setAlter_job_type(TAlterJobType.SCHEMA_CHANGE);
                break;
            case DECOMMISSION_BACKEND:
                req.setAlter_job_type(TAlterJobType.DECOMMISSION_BACKEND);
                break;
            default:
                break;
        }
        if (rollupJobV2Params != null) {
            Map<String, Expr> defineExprs = rollupJobV2Params.getDefineExprs();
            Expr whereExpr = rollupJobV2Params.getWhereExpr();
            DescriptorTable descTable = rollupJobV2Params.getDescTabl();
            List<ColumnId> baseTableColIds = rollupJobV2Params.getBaseTableColIds();
            if (defineExprs != null) {
                for (Map.Entry<String, Expr> entry : defineExprs.entrySet()) {
                    List<SlotRef> slots = Lists.newArrayList();
                    entry.getValue().collect(SlotRef.class, slots);
                    TAlterMaterializedViewParam mvParam = new TAlterMaterializedViewParam(entry.getKey());
                    mvParam.setOrigin_column_name(slots.get(0).getColumnName());
                    mvParam.setMv_expr(entry.getValue().treeToThrift());
                    req.addToMaterialized_view_params(mvParam);
                }

                // we need this thing, otherwise some expr evalution will fail in BE
                TQueryGlobals queryGlobals = new TQueryGlobals();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                queryGlobals.setNow_string(dateFormat.format(new Date()));
                queryGlobals.setTimestamp_ms(System.currentTimeMillis());
                queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);
                TQueryOptions queryOptions = new TQueryOptions();
                req.setQuery_globals(queryGlobals);
                req.setQuery_options(queryOptions);
            }
            if (whereExpr != null) {
                req.setWhere_expr(whereExpr.treeToThrift());
            }
            if (descTable != null) {
                req.setDesc_tbl(descTable.toThrift());
            }
            req.setBase_table_column_names(baseTableColIds.stream().map(ColumnId::getId)
                    .collect(Collectors.toList()));
        }
        req.setMaterialized_column_req(generatedColumnReq);

        req.setTablet_type(tabletType);
        req.setTxn_id(txnId);
        req.setJob_id(jobId);

        if (baseSchemaColumns != null) {
            List<TColumn> columns = new ArrayList<TColumn>();
            for (Column column : baseSchemaColumns) {
                columns.add(column.toThrift());
            }
            req.setColumns(columns);
        }
        return req;
    }

    /*
     * Handle the finish report of alter task.
     * If task is success, which means the history data before specified version has been transformed successfully.
     * So here we should modify the replica's version.
     * We assume that the specified version is X.
     * Case 1:
     *      After alter table process starts, there is no new load job being submitted. So the new replica
     *      should be with version (1-0). So we just modify the replica's version to partition's visible version, which is X.
     * Case 2:
     *      After alter table process starts, there are some load job being processed.
     * Case 2.1:
     *      Only one new load job, and it failed on this replica. so the replica's last failed version should be X + 1
     *      and version is still 1. We should modify the replica's version to (last failed version - 1)
     * Case 2.2
     *      There are new load jobs after alter task, and at least one of them is succeed on this replica.
     *      So the replica's version should be larger than X. So we don't need to modify the replica version
     *      because its already looks like normal.
     * Case 3:
     *      There are new load jobs after alter task, and their version and LFV is smaller or equal to X.
     *      And because alter request report success, it means that we can increase replica's version to X.
     */
    public void handleFinishAlterTask() throws Exception {
        TabletMetastore tabletMetastore = GlobalStateMgr.getCurrentState().getTabletMetastore();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(getDbId());
        if (db == null) {
            throw new MetaNotFoundException("database " + getDbId() + " does not exist");
        }

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("tbl " + getTableId() + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            PhysicalPartition partition = tbl.getPhysicalPartition(getPartitionId());
            if (partition == null) {
                throw new MetaNotFoundException("partition " + getPartitionId() + " does not exist");
            }
            MaterializedIndex index = partition.getIndex(getIndexId());
            if (index == null) {
                throw new MetaNotFoundException("index " + getIndexId() + " does not exist");
            }
            Tablet tablet = tabletMetastore.getTablet(index, getTabletId());
            Preconditions.checkNotNull(tablet, getTabletId());
            if (!tbl.isCloudNativeTableOrMaterializedView()) {
                Replica replica = ((LocalTablet) tablet).getReplicaById(getNewReplicaId());
                if (replica == null) {
                    throw new MetaNotFoundException("replica " + getNewReplicaId() + " does not exist");
                }

                LOG.info("before handle alter task tablet {}, replica: {}, task version: {}", getSignature(), replica,
                        getVersion());
                boolean versionChanged = false;
                if (replica.getVersion() <= getVersion()) {
                    // Case 1, Case 2.1 or Case 3
                    replica.updateRowCount(getVersion(), replica.getDataSize(),
                            replica.getRowCount());
                    versionChanged = true;
                }

                if (versionChanged) {
                    ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(getDbId(), getTableId(),
                            getPartitionId(), getIndexId(), getTabletId(), getBackendId(),
                            replica.getId(), replica.getVersion(), -1,
                            replica.getDataSize(), replica.getRowCount(),
                            replica.getLastFailedVersion(),
                            replica.getLastSuccessVersion(), 0);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateReplica(info);
                }

                LOG.info("after handle alter task tablet: {}, replica: {}", getSignature(), replica);
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }
        setFinished(true);
    }

    @Override
    public void run() {
        try {
            handleFinishAlterTask();
        } catch (Exception e) {
            String errMsg = "failed to handle finish alter task: " + getSignature() + ", " + e.getMessage();
            LOG.warn(errMsg);
            setErrorMsg(errMsg);
            setFailed(true);
        }
    }

}
