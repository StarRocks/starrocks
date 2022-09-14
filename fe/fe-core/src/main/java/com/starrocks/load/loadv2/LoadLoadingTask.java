// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadLoadingTask.java

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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.LoadException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.FailMsg;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    /*
     * load id is used for plan.
     * It should be changed each time we retry this load plan
     */
    private TUniqueId loadId;
    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final boolean strictMode;
    private final long txnId;
    private final String timezone;
    private final long createTimestamp;
    private final boolean partialUpdate;
    // timeout of load job, in seconds
    private final long timeoutS;
    private final Map<String, String> sessionVariables;
    private final TLoadJobType loadJobType;
    private boolean useLocalCache;

    private LoadingTaskPlanner planner;
    private ConnectContext context;

    public LoadLoadingTask(Database db, OlapTable table, BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, long createTimestamp, boolean partialUpdate, Map<String, String> sessionVariables, 
            ConnectContext context, TLoadJobType loadJobType, int priority, boolean useLocalCache) {
        super(callback, TaskType.LOADING, priority);
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 1; // load task retry does not satisfy transaction's atomic
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.createTimestamp = createTimestamp;
        this.partialUpdate = partialUpdate;
        this.sessionVariables = sessionVariables;
        this.context = context;
        this.loadJobType = loadJobType;
        this.useLocalCache = useLocalCache;
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList, int fileNum) throws UserException {
        this.loadId = loadId;
        planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                strictMode, timezone, timeoutS, createTimestamp, partialUpdate, useLocalCache);
        planner.plan(loadId, fileStatusList, fileNum);
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public OlapTable getTargetTable() {
        return table;
    }

    @Override
    protected void executeTask() throws Exception {
        LOG.info("begin to execute loading task. load id: {} job: {}. db: {}, tbl: {}. left retry: {}",
                DebugUtil.printId(loadId), callback.getCallbackId(), db.getOriginName(), table.getName(), retryTime);
        retryTime--;
        executeOnce();
    }

    private void executeOnce() throws Exception {
        // New one query id,
        Coordinator curCoordinator = new Coordinator(callback.getCallbackId(), loadId, planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(),
                planner.getTimezone(), planner.getStartTime(), sessionVariables, context);
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setLoadJobType(loadJobType);
        curCoordinator.setExecMemoryLimit(execMemLimit);
        /*
         * For broker load job, user only need to set mem limit by 'exec_mem_limit' property.
         * And the variable 'load_mem_limit' does not make any effect.
         * However, in order to ensure the consistency of semantics when executing on the BE side,
         * and to prevent subsequent modification from incorrectly setting the load_mem_limit,
         * here we use exec_mem_limit to directly override the load_mem_limit property.
         */
        curCoordinator.setLoadMemLimit(execMemLimit);
        curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, curCoordinator);
            actualExecute(curCoordinator);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private void actualExecute(Coordinator curCoordinator) throws Exception {
        int waitSecond = (int) (getLeftTimeMs() / 1000);
        if (waitSecond <= 0) {
            throw new LoadException("Load timeout. Increase the timeout and retry");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("task_id", signature)
                    .add("query_id", DebugUtil.printId(curCoordinator.getQueryId()))
                    .add("msg", "begin to execute plan")
                    .build());
        }
        curCoordinator.exec();
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok()) {
                attachment = new BrokerLoadingTaskAttachment(signature,
                        curCoordinator.getLoadCounters(),
                        curCoordinator.getTrackingUrl(),
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()));
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    private long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }

    @Override
    public void updateRetryInfo() {
        super.updateRetryInfo();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        planner.updateLoadInfo(this.loadId);
    }
}
