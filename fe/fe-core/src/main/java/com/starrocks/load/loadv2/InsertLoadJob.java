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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/InsertLoadJob.java

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

package com.starrocks.load.loadv2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.AuthorizationInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.load.FailMsg.CancelType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TReportExecStatusParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * The class is performed to record the finished info of insert load job.
 * It is created after txn is visible which belongs to insert load job.
 * The state of insert load job is always finished, so it will never be scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    @SerializedName("tid")
    private long tableId;
    private long estimateScanRow;
    private TLoadJobType loadType;

    @SerializedName("wh")
    private String warehouse;

    @SerializedName("isj")
    private boolean isStatisticsJob;

    // only for log replay
    public InsertLoadJob() {
        super();
        this.jobType = EtlJobType.INSERT;
        this.warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        this.isStatisticsJob = false;
    }

    public InsertLoadJob(String label, long dbId, long tableId, long createTimestamp,
                         long estimateScanRow, TLoadJobType type, long timeout, String warehouse,
                         boolean isStatisticsJob) {
        super(dbId, label);
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.state = JobState.LOADING;
        this.jobType = EtlJobType.INSERT;
        this.estimateScanRow = estimateScanRow;
        this.loadType = type;
        this.timeoutSecond = timeout;
        this.warehouse = warehouse;
        this.isStatisticsJob = isStatisticsJob;
    }

    @VisibleForTesting
    InsertLoadJob(String label, long dbId, long tableId, long createTimestamp, String failMsg,
                  String trackingUrl) throws MetaNotFoundException {
        super(dbId, label);
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.finishTimestamp = System.currentTimeMillis();
        if (Strings.isNullOrEmpty(failMsg)) {
            this.state = JobState.FINISHED;
            this.progress = 100;
        } else {
            this.state = JobState.CANCELLED;
            this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
            this.progress = 0;
        }
        this.jobType = EtlJobType.INSERT;
        this.timeoutSecond = Config.insert_load_default_timeout_second;
        this.authorizationInfo = gatherAuthInfo();
        this.loadingStatus.setTrackingUrl(trackingUrl);
        this.loadType = TLoadJobType.INSERT_QUERY;
        this.warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        this.isStatisticsJob = false;
    }

    @Override
    public String getCurrentWarehouse() {
        return warehouse;
    }

    @Override
    public boolean isInternalJob() {
        return isStatisticsJob;
    }

    public void setLoadFinishOrCancel(String failMsg, String trackingUrl) throws UserException {
        writeLock();
        try {
            this.finishTimestamp = System.currentTimeMillis();
            if (Strings.isNullOrEmpty(failMsg)) {
                this.state = JobState.FINISHED;
                this.progress = 100;
            } else {
                this.state = JobState.CANCELLED;
                this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
                this.progress = 0;
            }
            this.authorizationInfo = gatherAuthInfo();
            this.loadingStatus.setTrackingUrl(trackingUrl);
        } finally {
            writeUnlock();
        }
        // persistent
        GlobalStateMgr.getCurrentState().getEditLog().logEndLoadJob(
                new LoadJobFinalOperation(this.id, this.loadingStatus, this.progress, 
                this.loadStartTimestamp, this.finishTimestamp, this.state, this.failMsg));
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames(false));
    }

    @Override
    public void updateProgress(TReportExecStatusParams params) {
        writeLock();
        try {
            super.updateProgress(params);
            if (!loadingStatus.getLoadStatistic().getLoadFinish()) {
                if (this.loadType == TLoadJobType.INSERT_QUERY) {
                    progress = (int) ((double) loadingStatus.getLoadStatistic().totalSourceLoadRows() 
                        / (estimateScanRow + 1) * 100);
                } else {
                    progress = (int) ((double) loadingStatus.getLoadStatistic().totalSinkLoadRows() 
                        / (estimateScanRow + 1) * 100);
                }
                
                if (progress >= 100) {
                    progress = 99;
                }
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        Table table = database.getTable(tableId);
        if (table == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public Set<String> getTableNames(boolean noThrow) throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        Table table = database.getTable(tableId);
        if (table == null) {
            if (noThrow) {
                return Sets.newHashSet();
            } else {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public boolean hasTxn() {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            return true;
        }

        Table table = database.getTable(tableId);
        if (table == null) {
            return true;
        }

        if (table instanceof SystemTable || table instanceof IcebergTable) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(tableId);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableId = in.readLong();
    }
}
