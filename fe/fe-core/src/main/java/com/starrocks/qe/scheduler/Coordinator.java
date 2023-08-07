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

package com.starrocks.qe.scheduler;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.Status;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.RowBatch;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class Coordinator {
    public interface Factory {
        Coordinator createQueryScheduler(ConnectContext context,
                                         List<PlanFragment> fragments,
                                         List<ScanNode> scanNodes,
                                         TDescriptorTable descTable);

        Coordinator createInsertScheduler(ConnectContext context,
                                          List<PlanFragment> fragments,
                                          List<ScanNode> scanNodes,
                                          TDescriptorTable descTable);

        Coordinator createBrokerLoadScheduler(LoadPlanner loadPlanner);

        Coordinator createStreamLoadScheduler(LoadPlanner loadPlanner);

        Coordinator createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address);

        Coordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                         List<PlanFragment> fragments,
                                                         List<ScanNode> scanNodes, String timezone, long startTime,
                                                         Map<String, String> sessionVariables,
                                                         ConnectContext context, long execMemLimit);

        Coordinator createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                List<PlanFragment> fragments,
                                                List<ScanNode> scanNodes, String timezone, long startTime,
                                                Map<String, String> sessionVariables,
                                                long execMemLimit);
    }

    // ------------------------------------------------------------------------------------
    // Common methods for scheduling.
    // ------------------------------------------------------------------------------------

    public void exec() throws Exception {
        startScheduling();
    }

    public abstract void startScheduling() throws Exception;

    public abstract void updateFragmentExecStatus(TReportExecStatusParams params);

    public void cancel() {
        cancel(PPlanFragmentCancelReason.USER_CANCEL, "");
    }

    public abstract void cancel(PPlanFragmentCancelReason reason, String message);

    public abstract void onFinished();

    // ------------------------------------------------------------------------------------
    // Methods for query.
    // ------------------------------------------------------------------------------------

    public abstract RowBatch getNext() throws Exception;

    // ------------------------------------------------------------------------------------
    // Methods for load.
    // ------------------------------------------------------------------------------------

    public abstract boolean join(int timeoutSecond);

    public abstract boolean checkBackendState();

    public abstract boolean isThriftServerHighLoad();

    public abstract void setLoadJobType(TLoadJobType type);

    public abstract long getLoadJobId();

    public abstract void setLoadJobId(Long jobId);

    public abstract Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap();

    public abstract Map<Integer, TNetworkAddress> getChannelIdToBEPortMap();

    public abstract boolean isEnableLoadProfile();

    public abstract void clearExportStatus();

    // ------------------------------------------------------------------------------------
    // Methods for profile.
    // ------------------------------------------------------------------------------------

    public abstract void endProfile();

    public abstract void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier);

    public abstract void setExecPlanSupplier(Supplier<ExecPlan> execPlanSupplier);

    public abstract RuntimeProfile buildMergedQueryProfile(PQueryStatistics statistics);

    public abstract RuntimeProfile getQueryProfile();

    public abstract List<String> getDeltaUrls();

    public abstract Map<String, String> getLoadCounters();

    public abstract List<TTabletFailInfo> getFailInfos();

    public abstract List<TTabletCommitInfo> getCommitInfos();

    public abstract List<TSinkCommitInfo> getSinkCommitInfos();

    public abstract List<String> getExportFiles();

    public abstract String getTrackingUrl();

    public abstract List<String> getRejectedRecordPaths();

    public abstract List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos();

    // ------------------------------------------------------------------------------------
    // Common methods.
    // ------------------------------------------------------------------------------------

    public abstract Status getExecStatus();

    public abstract boolean isUsingBackend(Long backendID);

    public abstract boolean isDone();

    public abstract TUniqueId getQueryId();

    public abstract void setQueryId(TUniqueId queryId);

    public abstract List<ScanNode> getScanNodes();

    public abstract long getStartTimeMs();

    public abstract void setTimeoutSecond(int timeoutSecond);

    public abstract boolean isProfileAlreadyReported();

}
