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
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TQueryType;
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
    private String mergeConditionStr;

    private LoadingTaskPlanner planner;
    private ConnectContext context;

    private LoadPlanner loadPlanner;
    private final OriginStatement originStmt;

    public LoadLoadingTask(Database db, OlapTable table, BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, long createTimestamp, boolean partialUpdate, String mergeConditionStr,
            Map<String, String> sessionVariables,
            ConnectContext context, TLoadJobType loadJobType, int priority, OriginStatement originStmt) {
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
        this.mergeConditionStr = mergeConditionStr;
        this.sessionVariables = sessionVariables;
        this.context = context;
        this.loadJobType = loadJobType;
        this.originStmt = originStmt;
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList, int fileNum) throws UserException {
        this.loadId = loadId;
        if (!Config.enable_pipeline_load) {
            planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                    strictMode, timezone, timeoutS, createTimestamp, partialUpdate, sessionVariables, mergeConditionStr);
            planner.setConnectContext(context);
            planner.plan(loadId, fileStatusList, fileNum);
        } else {
            loadPlanner = new LoadPlanner(callback.getCallbackId(), loadId, txnId, db.getId(), table, strictMode,
                timezone, timeoutS, createTimestamp, partialUpdate, context, sessionVariables, execMemLimit, execMemLimit, 
                brokerDesc, fileGroups, fileStatusList, fileNum);
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

    private void executeOnce() throws Exception {
        checkMeta();

        // New one query id,
        Coordinator curCoordinator;
        if (!Config.enable_pipeline_load) {
            curCoordinator = new Coordinator(callback.getCallbackId(), loadId, planner.getDescTable(),
                    planner.getFragments(), planner.getScanNodes(),
                    planner.getTimezone(), planner.getStartTime(), sessionVariables, context);
            /*
            * For broker load job, user only need to set mem limit by 'exec_mem_limit' property.
            * And the variable 'load_mem_limit' does not make any effect.
            * However, in order to ensure the consistency of semantics when executing on the BE side,
            * and to prevent subsequent modification from incorrectly setting the load_mem_limit,
            * here we use exec_mem_limit to directly override the load_mem_limit property.
            */
            curCoordinator.setQueryType(TQueryType.LOAD);
            curCoordinator.setExecMemoryLimit(execMemLimit);
            curCoordinator.setLoadMemLimit(execMemLimit);
            curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));
        } else {
            curCoordinator = new Coordinator(loadPlanner);
        }
        curCoordinator.setLoadJobType(loadJobType);

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, curCoordinator);
            long beginTimeInNanoSecond = TimeUtils.getStartTime();
            actualExecute(curCoordinator);

            if (context.isProfileEnabled()) {
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
                }

                profile.addChild(summaryProfile);

                curCoordinator.getQueryProfile().getCounterTotalTime()
                        .setValue(TimeUtils.getEstimatedTime(beginTimeInNanoSecond));
                curCoordinator.endProfile();
                curCoordinator.mergeIsomorphicProfiles();
                profile.addChild(curCoordinator.getQueryProfile());

                StringBuilder builder = new StringBuilder();
                profile.prettyPrint(builder, "");
                String profileContent = ProfileManager.getInstance().pushProfile(profile);
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
                        TabletFailInfo.fromThrift(curCoordinator.getFailInfos()));
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

    private void checkMeta() throws LoadException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db.getId());
        if (database == null) {
            throw new LoadException(String.format("db: %s-%d has been dropped", db.getFullName(), db.getId()));
        }

        if (database.getTable(table.getId()) == null) {
            throw new LoadException(String.format("table: %s-%d has been dropped from db: %s-%d",
                    table.getName(), table.getId(), db.getFullName(), db.getId()));
        }
    }
}
