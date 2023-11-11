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
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.FailMsg;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
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
    private final String mergeConditionStr;
    private final TPartialUpdateMode partialUpdateMode;

    private LoadingTaskPlanner planner;
    private final ConnectContext context;

    private LoadPlanner loadPlanner;
    private final OriginStatement originStmt;
    private final List<List<TBrokerFileStatus>> fileStatusList;
    private final int fileNum;

    private LoadLoadingTask(Builder builder) {
        super(builder.callback, TaskType.LOADING, builder.priority);
        this.db = builder.db;
        this.table = builder.table;
        this.brokerDesc = builder.brokerDesc;
        this.fileGroups = builder.fileGroups;
        this.jobDeadlineMs = builder.jobDeadlineMs;
        this.execMemLimit = builder.execMemLimit;
        this.strictMode = builder.strictMode;
        this.txnId = builder.txnId;
        this.timezone = builder.timezone;
        this.timeoutS = builder.timeoutS;
        this.createTimestamp = builder.createTimestamp;
        this.partialUpdate = builder.partialUpdate;
        this.mergeConditionStr = builder.mergeConditionStr;
        this.sessionVariables = builder.sessionVariables;
        this.context = builder.context;
        this.loadJobType = builder.loadJobType;
        this.originStmt = builder.originStmt;
        this.partialUpdateMode = builder.partialUpdateMode;
        this.retryTime = 1; // load task retry does not satisfy transaction's atomic
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.loadId = builder.loadId;
        this.fileStatusList = builder.fileStatusList;
        this.fileNum = builder.fileNum;
    }

    public void prepare() throws UserException {
        if (!Config.enable_pipeline_load) {
            planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                    strictMode, timezone, timeoutS, createTimestamp, partialUpdate, sessionVariables, mergeConditionStr,
                    partialUpdateMode);
            planner.setConnectContext(context);
            planner.plan(loadId, fileStatusList, fileNum);
        } else {
            loadPlanner = new LoadPlanner(callback.getCallbackId(), loadId, txnId, db.getId(), table, strictMode,
                    timezone, timeoutS, createTimestamp, partialUpdate, context, sessionVariables, execMemLimit, execMemLimit,
                    brokerDesc, fileGroups, fileStatusList, fileNum);
            loadPlanner.setPartialUpdateMode(partialUpdateMode);
            loadPlanner.plan();
        }
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

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    private void executeOnce() throws Exception {
        // New one query id,
        Coordinator curCoordinator;
        if (!Config.enable_pipeline_load) {
            curCoordinator = getCoordinatorFactory().createNonPipelineBrokerLoadScheduler(
                    callback.getCallbackId(), loadId,
                    planner.getDescTable(),
                    planner.getFragments(), planner.getScanNodes(),
                    planner.getTimezone(), planner.getStartTime(), sessionVariables, context, execMemLimit);

            curCoordinator.setTimeoutSecond((int) (getLeftTimeMs() / 1000));
        } else {
            curCoordinator = getCoordinatorFactory().createBrokerLoadScheduler(loadPlanner);
        }
        curCoordinator.setLoadJobType(loadJobType);

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, curCoordinator);
            long beginTimeInNanoSecond = TimeUtils.getStartTime();
            actualExecute(curCoordinator);

            if (context.getSessionVariable().isEnableLoadProfile()) {
                RuntimeProfile profile = new RuntimeProfile("Load");
                RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
                summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(context.getExecutionId()));
                summaryProfile.addInfoString(ProfileManager.START_TIME,
                        TimeUtils.longToTimeString(createTimestamp));

                long currentTimestamp = System.currentTimeMillis();
                long totalTimeMs = currentTimestamp - createTimestamp;
                summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
                summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

                summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
                summaryProfile.addInfoString(ProfileManager.QUERY_STATE, context.getState().toString());
                summaryProfile.addInfoString("StarRocks Version",
                        String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
                summaryProfile.addInfoString(ProfileManager.USER, context.getQualifiedUser());
                summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, context.getDatabase());
                summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, originStmt.originStmt);

                SessionVariable variables = context.getSessionVariable();
                if (variables != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("load_parallel_instance_num=").append(Config.load_parallel_instance_num).append(",");
                    sb.append(SessionVariable.PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM).append("=")
                            .append(variables.getParallelExecInstanceNum()).append(",");
                    sb.append(SessionVariable.MAX_PARALLEL_SCAN_INSTANCE_NUM).append("=")
                            .append(variables.getMaxParallelScanInstanceNum()).append(",");
                    sb.append(SessionVariable.PIPELINE_DOP).append("=").append(variables.getPipelineDop()).append(",");
                    sb.append(SessionVariable.ENABLE_ADAPTIVE_SINK_DOP).append("=")
                            .append(variables.getEnableAdaptiveSinkDop())
                            .append(",");
                    if (context.getResourceGroup() != null) {
                        sb.append(SessionVariable.RESOURCE_GROUP).append("=")
                                .append(context.getResourceGroup().getName())
                                .append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    summaryProfile.addInfoString(ProfileManager.VARIABLES, sb.toString());

                    summaryProfile.addInfoString("NonDefaultSessionVariables", variables.getNonDefaultVariablesJson());
                }

                profile.addChild(summaryProfile);

                curCoordinator.getQueryProfile().getCounterTotalTime()
                        .setValue(TimeUtils.getEstimatedTime(beginTimeInNanoSecond));
                curCoordinator.endProfile();
                profile.addChild(curCoordinator.buildMergedQueryProfile());

                StringBuilder builder = new StringBuilder();
                profile.prettyPrint(builder, "");
                String profileContent = ProfileManager.getInstance().pushProfile(null, profile);
                if (context.getQueryDetail() != null) {
                    context.getQueryDetail().setProfile(profileContent);
                }
            }
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
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()),
                        TabletFailInfo.fromThrift(curCoordinator.getFailInfos()),
                        curCoordinator.getRejectedRecordPaths());
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

        if (!Config.enable_pipeline_load) {
            planner.updateLoadInfo(this.loadId);
        } else {
            loadPlanner.updateLoadInfo(this.loadId);
        }
    }

    public static class Builder {
        private TUniqueId loadId;
        private Database db;
        private OlapTable table;
        private BrokerDesc brokerDesc;
        private List<BrokerFileGroup> fileGroups;
        private long jobDeadlineMs;
        private long execMemLimit;
        private boolean strictMode;
        private long txnId;
        private String timezone;
        private long createTimestamp;
        private boolean partialUpdate;
        private long timeoutS;
        private Map<String, String> sessionVariables;
        private TLoadJobType loadJobType;
        private String mergeConditionStr;
        private TPartialUpdateMode partialUpdateMode;
        private ConnectContext context;
        private OriginStatement originStmt;
        private List<List<TBrokerFileStatus>> fileStatusList;
        private int fileNum = 0;
        private LoadTaskCallback callback;
        private int priority;

        public Builder setCallback(LoadTaskCallback callback) {
            this.callback = callback;
            return this;
        }

        public Builder setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder setLoadId(TUniqueId loadId) {
            this.loadId = loadId;
            return this;
        }

        public Builder setDb(Database db) {
            this.db = db;
            return this;
        }

        public Builder setTable(OlapTable table) {
            this.table = table;
            return this;
        }

        public Builder setBrokerDesc(BrokerDesc brokerDesc) {
            this.brokerDesc = brokerDesc;
            return this;
        }

        public Builder setFileGroups(List<BrokerFileGroup> fileGroups) {
            this.fileGroups = fileGroups;
            return this;
        }

        public Builder setJobDeadlineMs(long jobDeadlineMs) {
            this.jobDeadlineMs = jobDeadlineMs;
            return this;
        }

        public Builder setExecMemLimit(long execMemLimit) {
            this.execMemLimit = execMemLimit;
            return this;
        }

        public Builder setStrictMode(boolean strictMode) {
            this.strictMode = strictMode;
            return this;
        }

        public Builder setTxnId(long txnId) {
            this.txnId = txnId;
            return this;
        }

        public Builder setTimezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        public Builder setCreateTimestamp(long createTimestamp) {
            this.createTimestamp = createTimestamp;
            return this;
        }

        public Builder setPartialUpdate(boolean partialUpdate) {
            this.partialUpdate = partialUpdate;
            return this;
        }

        public Builder setTimeoutS(long timeoutS) {
            this.timeoutS = timeoutS;
            return this;
        }

        public Builder setSessionVariables(Map<String, String> sessionVariables) {
            this.sessionVariables = sessionVariables;
            return this;
        }

        public Builder setLoadJobType(TLoadJobType loadJobType) {
            this.loadJobType = loadJobType;
            return this;
        }

        public Builder setMergeConditionStr(String mergeConditionStr) {
            this.mergeConditionStr = mergeConditionStr;
            return this;
        }

        public Builder setPartialUpdateMode(TPartialUpdateMode partialUpdateMode) {
            this.partialUpdateMode = partialUpdateMode;
            return this;
        }

        public Builder setContext(ConnectContext context) {
            this.context = context;
            return this;
        }

        public Builder setOriginStmt(OriginStatement originStmt) {
            this.originStmt = originStmt;
            return this;
        }

        public Builder setFileStatusList(List<List<TBrokerFileStatus>> fileStatusList) {
            this.fileStatusList = fileStatusList;
            return this;
        }

        public Builder setFileNum(int fileNum) {
            this.fileNum = fileNum;
            return this;
        }

        public LoadLoadingTask build() {
            return new LoadLoadingTask(this);
        }
    }
}
