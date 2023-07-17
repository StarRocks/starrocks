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

public interface ICoordinator {
    interface Factory {
        ICoordinator createQueryScheduler(ConnectContext context,
                                          List<PlanFragment> fragments,
                                          List<ScanNode> scanNodes,
                                          TDescriptorTable descTable);

        ICoordinator createInsertScheduler(ConnectContext context,
                                           List<PlanFragment> fragments,
                                           List<ScanNode> scanNodes,
                                           TDescriptorTable descTable);

        ICoordinator createBrokerLoadScheduler(LoadPlanner loadPlanner);

        ICoordinator createStreamLoadScheduler(LoadPlanner loadPlanner);

        ICoordinator createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address);

        ICoordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                          List<PlanFragment> fragments,
                                                          List<ScanNode> scanNodes, String timezone, long startTime,
                                                          Map<String, String> sessionVariables,
                                                          ConnectContext context, long execMemLimit);

        ICoordinator createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                 List<PlanFragment> fragments,
                                                 List<ScanNode> scanNodes, String timezone, long startTime,
                                                 Map<String, String> sessionVariables,
                                                 long execMemLimit);
    }

    // ------------------------------------------------------------------------------------
    // Common methods for scheduling.
    // ------------------------------------------------------------------------------------

    default void exec() throws Exception {
        startScheduling(true);
    }

    default void exec(boolean needDeploy) throws Exception {
        startScheduling(needDeploy);
    }

    void startScheduling(boolean needDeploy) throws Exception;

    void updateFragmentExecStatus(TReportExecStatusParams params);

    default void cancel() {
        cancel(PPlanFragmentCancelReason.USER_CANCEL, "");
    }

    void cancel(PPlanFragmentCancelReason reason, String message);

    void onFinished();

    // ------------------------------------------------------------------------------------
    // Methods for query.
    // ------------------------------------------------------------------------------------

    RowBatch getNext() throws Exception;

    // ------------------------------------------------------------------------------------
    // Methods for load.
    // ------------------------------------------------------------------------------------

    boolean join(int timeoutSecond);

    boolean checkBackendState();

    boolean isThriftServerHighLoad();

    void setLoadJobType(TLoadJobType type);

    long getLoadJobId();

    void setLoadJobId(Long jobId);

    Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap();

    Map<Integer, TNetworkAddress> getChannelIdToBEPortMap();

    boolean isEnableLoadProfile();

    void clearExportStatus();

    // ------------------------------------------------------------------------------------
    // Methods for profile.
    // ------------------------------------------------------------------------------------

    void endProfile();

    void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier);

    RuntimeProfile buildMergedQueryProfile(PQueryStatistics statistics);

    RuntimeProfile getQueryProfile();

    List<String> getDeltaUrls();

    Map<String, String> getLoadCounters();

    List<TTabletFailInfo> getFailInfos();

    List<TTabletCommitInfo> getCommitInfos();

    List<TSinkCommitInfo> getSinkCommitInfos();

    List<String> getExportFiles();

    String getTrackingUrl();

    List<String> getRejectedRecordPaths();

    List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos();

    // ------------------------------------------------------------------------------------
    // Common methods.
    // ------------------------------------------------------------------------------------

    Status getExecStatus();

    boolean isUsingBackend(Long backendID);

    boolean isDone();

    TUniqueId getQueryId();

    void setQueryId(TUniqueId queryId);

    List<ScanNode> getScanNodes();

    long getStartTimeMs();

    void setTimeout(int timeoutSecond);

    boolean isProfileAlreadyReported();

}
