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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java

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

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServer;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.QueryStatisticsItem.FragmentInstanceInfo;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(DefaultCoordinator.class);
    private static final int DEFAULT_PROFILE_TIMEOUT_SECOND = 2;

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    private final JobSpec jobSpec;

    // protects all fields below
    private final Lock lock = new ReentrantLock();
    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;
    private RuntimeProfile queryProfile;
    private List<RuntimeProfile> fragmentProfiles;
    private final Map<PlanFragmentId, Integer> fragmentId2fragmentProfileIds = Maps.newHashMap();
    // backend execute state
    private final ConcurrentNavigableMap<Integer, BackendExecState> backendExecStates = new ConcurrentSkipListMap<>();
    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private final List<BackendExecState> needCheckBackendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
    // number of instances of this query, equals to
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;
    private int numReceivedRows = 0;
    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;
    private final Set<String> rejectedRecordPaths = new HashSet<>();
    // for export
    private List<String> exportFiles;
    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TTabletFailInfo> failInfos = Lists.newArrayList();

    // for external table sink
    private final List<TSinkCommitInfo> sinkCommitInfos = Lists.newArrayList();
    // Input parameter
    private final ConnectContext connectContext;
    private final boolean needReport;

    // True indicates that the profile has been reported
    // When `enable_load_profile` is enabled,
    // if the time costs of stream load is less than `stream_load_profile_collect_second`,
    // the profile will not be reported to FE to reduce the overhead of profile under high-frequency import
    private boolean profileAlreadyReported = false;

    private final CoordinatorPreprocessor coordinatorPreprocessor;

    private boolean thriftServerHighLoad;

    private Supplier<RuntimeProfile> topProfileSupplier;
    private final AtomicLong lastRuntimeProfileUpdateTime = new AtomicLong(0L);

    public static class Factory implements Coordinator.Factory {

        @Override
        public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            JobSpec jobSpec =
                    JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.SELECT);
            return new DefaultCoordinator(context, jobSpec, context.getSessionVariable().isEnableProfile());
        }

        @Override
        public DefaultCoordinator createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                        List<ScanNode> scanNodes,
                                                        TDescriptorTable descTable) {
            JobSpec jobSpec = JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.LOAD);
            return new DefaultCoordinator(context, jobSpec, context.getSessionVariable().isEnableProfile());
        }

        @Override
        public DefaultCoordinator createBrokerLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromBrokerLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec, true);
        }

        @Override
        public DefaultCoordinator createStreamLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromStreamLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec, true);
        }

        @Override
        public DefaultCoordinator createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address) {
            JobSpec jobSpec = JobSpec.Factory.fromSyncStreamLoadSpec(planner);
            return new DefaultCoordinator(jobSpec, planner, address);
        }

        @Override
        public DefaultCoordinator createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                              List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                              String timezone,
                                                              long startTime, Map<String, String> sessionVariables,
                                                              long execMemLimit) {
            ConnectContext context = new ConnectContext();
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.getSessionVariable().setEnablePipelineEngine(true);
            context.getSessionVariable().setPipelineDop(0);

            JobSpec jobSpec = JobSpec.Factory.fromBrokerExportSpec(context, jobId, queryId, descTable,
                    fragments, scanNodes, timezone,
                    startTime, sessionVariables, execMemLimit);

            return new DefaultCoordinator(context, jobSpec, true);
        }

        @Override
        public DefaultCoordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId,
                                                                       DescriptorTable descTable,
                                                                       List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                                       String timezone,
                                                                       long startTime, Map<String, String> sessionVariables,
                                                                       ConnectContext context, long execMemLimit) {
            JobSpec jobSpec = JobSpec.Factory.fromNonPipelineBrokerLoadJobSpec(context, jobId, queryId, descTable,
                    fragments, scanNodes, timezone,
                    startTime, sessionVariables, execMemLimit);

            return new DefaultCoordinator(context, jobSpec, true);
        }
    }

    /**
     * Only used for sync stream load profile,
     * so only init relative data structure.
     */
    public DefaultCoordinator(JobSpec jobSpec, StreamLoadPlanner planner, TNetworkAddress address) {
        this.connectContext = planner.getConnectContext();
        this.jobSpec = jobSpec;

        TUniqueId queryId = jobSpec.getQueryId();

        LOG.info("Execution Profile " + DebugUtil.printId(queryId));
        queryProfile = new RuntimeProfile("Execution");

        fragmentProfiles = new ArrayList<>();
        fragmentProfiles.add(new RuntimeProfile("Fragment 0"));
        queryProfile.addChild(fragmentProfiles.get(0));
        profileDoneSignal = new MarkedCountDownLatch<>(1);
        profileDoneSignal.addMark(queryId, -1L /* value is meaningless */);

        BackendExecState backendExecState = new BackendExecState(queryId, address);
        backendExecStates.put(0, backendExecState);

        attachInstanceProfileToFragmentProfile();

        deltaUrls = Lists.newArrayList();
        loadCounters = Maps.newHashMap();

        this.coordinatorPreprocessor = null;
        this.needReport = true;
    }

    DefaultCoordinator(ConnectContext context, JobSpec jobSpec, boolean needReport) {
        this.connectContext = context;
        this.jobSpec = jobSpec;
        this.returnedAllResults = false;
        this.needReport = needReport;

        this.coordinatorPreprocessor = new CoordinatorPreprocessor(context, jobSpec);
    }

    @Override
    public long getLoadJobId() {
        return jobSpec.getLoadJobId();
    }

    @Override
    public void setLoadJobId(Long jobId) {
        jobSpec.setLoadJobId(jobId);
    }

    @Override
    public TUniqueId getQueryId() {
        return jobSpec.getQueryId();
    }

    @Override
    public void setQueryId(TUniqueId queryId) {
        jobSpec.setQueryId(queryId);
    }

    @Override
    public void setLoadJobType(TLoadJobType type) {
        jobSpec.setLoadJobType(type);
    }

    @Override
    public Status getExecStatus() {
        return queryStatus;
    }

    @Override
    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    @Override
    public List<String> getDeltaUrls() {
        return deltaUrls;
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return loadCounters;
    }

    @Override
    public String getTrackingUrl() {
        return trackingUrl;
    }

    @Override
    public List<String> getRejectedRecordPaths() {
        return new ArrayList<>(rejectedRecordPaths);
    }

    @Override
    public long getStartTimeMs() {
        return jobSpec.getStartTimeMs();
    }

    public JobSpec getJobSpec() {
        return jobSpec;
    }

    @Override
    public void setTimeoutSecond(int timeoutSecond) {
        jobSpec.setQueryTimeout(timeoutSecond);
    }

    public void addReplicateScanId(Integer scanId) {
        this.coordinatorPreprocessor.getReplicateScanIds().add(scanId);
    }

    @Override
    public void clearExportStatus() {
        lock.lock();
        try {
            this.backendExecStates.clear();
            this.queryStatus.setStatus(new Status());
            if (this.exportFiles == null) {
                this.exportFiles = Lists.newArrayList();
            }
            this.exportFiles.clear();
            this.needCheckBackendExecStates.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }

    @Override
    public List<TTabletFailInfo> getFailInfos() {
        return failInfos;
    }

    @Override
    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return sinkCommitInfos;
    }

    @Override
    public void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {
        this.topProfileSupplier = topProfileSupplier;
    }

    @Override
    public boolean isUsingBackend(Long backendID) {
        return coordinatorPreprocessor.getWorkerProvider().isWorkerSelected(backendID);
    }

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    public Collection<BackendExecState> getBackendExecutions() {
        return backendExecStates.values();
    }

    public Collection<Integer> getBackendNums() {
        return backendExecStates.keySet();
    }

    public Set<TUniqueId> getInstanceIds() {
        return coordinatorPreprocessor.getInstanceIds();
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void prepareExec() throws Exception {
        if (LOG.isDebugEnabled()) {
            if (!jobSpec.getScanNodes().isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                        DebugUtil.printId(jobSpec.getQueryId()),
                        jobSpec.getScanNodes().get(0).treeToThrift());
            }
            if (!jobSpec.getFragments().isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, fragment: {}",
                        DebugUtil.printId(jobSpec.getQueryId()),
                        jobSpec.getFragments().get(0).toThrift());
            }
            LOG.debug("debug: in Coordinator::exec. query id: {}, desc table: {}",
                    DebugUtil.printId(jobSpec.getQueryId()), jobSpec.getDescTable());
        }

        coordinatorPreprocessor.prepareExec();

        // create result receiver
        prepareResultSink();

        prepareProfile();
    }

    @Override
    public void onFinished() {
        // Do nothing.
    }

    public CoordinatorPreprocessor getPrepareInfo() {
        return coordinatorPreprocessor;
    }

    public Map<PlanFragmentId, CoordinatorPreprocessor.FragmentExecParams> getFragmentExecParamsMap() {
        return coordinatorPreprocessor.getFragmentExecParamsMap();
    }

    public List<PlanFragment> getFragments() {
        return jobSpec.getFragments();
    }

    public TDescriptorTable getDescTable() {
        return jobSpec.getDescTable();
    }

    public boolean isLoadType() {
        return jobSpec.isLoadType();
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return jobSpec.getScanNodes();
    }

    @Override
    public void startScheduling() throws Exception {

        QueryQueueManager.getInstance().maybeWait(connectContext, this);
        try (PlannerProfile.ScopedTimer timer = PlannerProfile.getScopedTimer("CoordPrepareExec")) {
            prepareExec();
        }

        try (PlannerProfile.ScopedTimer timer = PlannerProfile.getScopedTimer("CoordDeliverExec")) {
            deliverExecFragments();
        }
    }

    private void prepareProfile() {
        queryProfile = new RuntimeProfile("Execution");

        fragmentProfiles = new ArrayList<>();
        for (int i = 0; i < jobSpec.getFragments().size(); i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
            fragmentId2fragmentProfileIds.put(jobSpec.getFragments().get(i).getFragmentId(), i);
            queryProfile.addChild(fragmentProfiles.get(i));
        }

        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        profileDoneSignal = new MarkedCountDownLatch<>(coordinatorPreprocessor.getInstanceIds().size());
        for (TUniqueId instanceId : coordinatorPreprocessor.getInstanceIds()) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }
    }

    private void prepareResultSink() throws Exception {
        PlanFragmentId topId = jobSpec.getFragments().get(0).getFragmentId();
        CoordinatorPreprocessor.FragmentExecParams topParams =
                coordinatorPreprocessor.getFragmentExecParamsMap().get(topId);
        if (topParams.fragment.getSink() instanceof ResultSink) {
            long workerId = topParams.instanceExecParams.get(0).getWorkerId();
            ComputeNode worker = coordinatorPreprocessor.getWorkerProvider().getWorkerById(workerId);
            TNetworkAddress execBeAddr = worker.getAddress();
            receiver = new ResultReceiver(
                    topParams.instanceExecParams.get(0).instanceId,
                    workerId,
                    worker.getBrpcAddress(),
                    jobSpec.getQueryOptions().query_timeout * 1000);

            // Select top fragment as global runtime filter merge address
            setGlobalRuntimeFilterParams(topParams, worker.getBrpcAddress());

            if (LOG.isDebugEnabled()) {
                LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()), execBeAddr);
            }

            // set the broker address for OUTFILE sink
            ResultSink resultSink = (ResultSink) topParams.fragment.getSink();
            if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
                FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(resultSink.getBrokerName(),
                        execBeAddr.getHostname());
                resultSink.setBrokerAddr(broker.ip, broker.port);
                LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
            }

        } else {
            // This is a load process.
            this.jobSpec.getQueryOptions().setEnable_profile(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds();
            GlobalStateMgr.getCurrentState().getLoadMgr()
                    .initJobProgress(jobSpec.getLoadJobId(), jobSpec.getQueryId(),
                            coordinatorPreprocessor.getInstanceIds(),
                            relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()),
                    coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds());
        }
    }

    private void deliverExecFragments() throws Exception {
        deliverExecFragmentsRequests(coordinatorPreprocessor.isUsePipeline());
    }

    private void handleErrorBackendExecState(BackendExecState errorBackendExecState, TStatusCode errorCode,
                                             String errMessage)
            throws UserException, RpcException {
        if (errorBackendExecState != null) {
            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
            switch (Objects.requireNonNull(errorCode)) {
                case TIMEOUT:
                    throw new UserException("query timeout. backend id: " + errorBackendExecState.backend.getId());
                case THRIFT_RPC_ERROR:
                    SimpleScheduler.addToBlacklist(errorBackendExecState.backend.getId());
                    throw new RpcException(errorBackendExecState.backend.getHost(), "rpc failed");
                default:
                    throw new UserException(errMessage + " backend:" + errorBackendExecState.address.hostname);
            }
        }
    }

    /**
     * Compute the topological order of the fragment tree.
     * It will divide fragments to several groups.
     * - There is no data dependency among fragments in a group.
     * - All the upstream fragments of the fragments in a group must belong to the previous groups.
     * - Each group should be delivered sequentially, and fragments in a group can be delivered concurrently.
     * <p>
     * For example, the following tree will produce four groups: [[1], [2, 3, 4], [5, 6], [7]]
     * -     *         1
     * -     *         │
     * -     *    ┌────┼────┐
     * -     *    │    │    │
     * -     *    2    3    4
     * -     *    │    │    │
     * -     * ┌──┴─┐  │    │
     * -     * │    │  │    │
     * -     * 5    6  │    │
     * -     *      │  │    │
     * -     *      └──┼────┘
     * -     *         │
     * -     *         7
     *
     * @return multiple fragment groups.
     */
    private List<List<PlanFragment>> computeTopologicalOrderFragments() {
        Queue<PlanFragment> queue = Lists.newLinkedList();
        Map<PlanFragment, Integer> inDegrees = Maps.newHashMap();

        PlanFragment root = jobSpec.getFragments().get(0);

        // Compute in-degree of each fragment by BFS.
        // `queue` contains the fragments need to visit its in-edges.
        inDegrees.put(root, 0);
        queue.add(root);
        while (!queue.isEmpty()) {
            PlanFragment fragment = queue.poll();
            for (PlanFragment child : fragment.getChildren()) {
                Integer v = inDegrees.get(child);
                if (v != null) {
                    // Has added this child to queue before, don't add again.
                    inDegrees.put(child, v + 1);
                } else {
                    inDegrees.put(child, 1);
                    queue.add(child);
                }
            }
        }

        if (jobSpec.getFragments().size() != inDegrees.size()) {
            for (PlanFragment fragment : jobSpec.getFragments()) {
                if (!inDegrees.containsKey(fragment)) {
                    LOG.warn("This fragment does not belong to the fragment tree: {}", fragment.getFragmentId());
                }
            }
            throw new StarRocksPlannerException("Some fragments do not belong to the fragment tree",
                    ErrorType.INTERNAL_ERROR);
        }

        // Compute fragment groups by BFS.
        // `queue` contains the fragments whose in-degree is zero.
        queue.add(root);
        List<List<PlanFragment>> groups = Lists.newArrayList();
        int numOutputFragments = 0;
        while (!queue.isEmpty()) {
            int groupSize = queue.size();
            List<PlanFragment> group = new ArrayList<>(groupSize);
            // The next `groupSize` fragments can be delivered concurrently, because zero in-degree indicates that
            // they don't depend on each other and all the fragments depending on them have been delivered.
            for (int i = 0; i < groupSize; ++i) {
                PlanFragment fragment = queue.poll();
                group.add(fragment);

                for (PlanFragment child : fragment.getChildren()) {
                    int degree = inDegrees.compute(child, (k, v) -> v - 1);
                    if (degree == 0) {
                        queue.add(child);
                    }
                }
            }

            groups.add(group);
            numOutputFragments += groupSize;
        }

        if (jobSpec.getFragments().size() != numOutputFragments) {
            throw new StarRocksPlannerException("There are some circles in the fragment tree",
                    ErrorType.INTERNAL_ERROR);
        }

        return groups;
    }

    /**
     * Deliver multiple fragments concurrently according to the topological order.
     */
    private void deliverExecFragmentsRequests(boolean enablePipelineEngine) throws Exception {
        TQueryOptions queryOptions = jobSpec.getQueryOptions();
        long queryDeliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;
        List<List<PlanFragment>> fragmentGroups = computeTopologicalOrderFragments();

        lock();
        try {
            // execute all instances from up to bottom
            int backendNum = 0;

            jobSpec.getDescTable().setIs_cached(false);
            TDescriptorTable emptyDescTable = new TDescriptorTable();
            emptyDescTable.setIs_cached(true);
            emptyDescTable.setTupleDescriptors(Collections.emptyList());

            Set<Long> deployedWorkerIds = new HashSet<>();
            for (List<PlanFragment> fragmentGroup : fragmentGroups) {
                // Divide requests of fragments in the current group to two stages.
                // - stage 1, the first request to a host, which need send descTable.
                // - stage 2, the non-first requests to a host, which needn't send descTable.
                List<List<BackendExecState>> twoStageExecutionsToDeploy =
                        ImmutableList.of(new ArrayList<>(), new ArrayList<>());
                for (PlanFragment fragment : fragmentGroup) {
                    int profileFragmentId = fragmentId2fragmentProfileIds.get(fragment.getFragmentId());
                    CoordinatorPreprocessor.FragmentExecParams params =
                            coordinatorPreprocessor.getFragmentExecParamsMap().get(fragment.getFragmentId());
                    Preconditions.checkState(!params.instanceExecParams.isEmpty());

                    // Fragment instances' ordinals in FragmentExecParams.instanceExecParams determine
                    // shuffle partitions' ordinals in DataStreamSink. backendIds of Fragment instances that
                    // contain shuffle join determine the ordinals of GRF components in the GRF. For a
                    // shuffle join, its shuffle partitions and corresponding one-map-one GRF components
                    // should have the same ordinals. so here assign monotonic unique backendIds to
                    // Fragment instances to keep consistent order with Fragment instances in
                    // FragmentExecParams.instanceExecParams.
                    for (CoordinatorPreprocessor.FInstanceExecParam fInstanceExecParam : params.instanceExecParams) {
                        fInstanceExecParam.backendNum = backendNum++;
                    }

                    List<List<CoordinatorPreprocessor.FInstanceExecParam>> twoStageInstancesToDeploy =
                            ImmutableList.of(new ArrayList<>(), new ArrayList<>());
                    if (!enablePipelineEngine) {
                        twoStageInstancesToDeploy.get(0).addAll(params.instanceExecParams);
                    } else {
                        params.instanceExecParams.forEach(instance -> {
                            if (deployedWorkerIds.contains(instance.getWorkerId())) {
                                twoStageInstancesToDeploy.get(1).add(instance);
                            } else {
                                deployedWorkerIds.add(instance.getWorkerId());
                                twoStageInstancesToDeploy.get(0).add(instance);
                            }
                        });
                    }

                    // if pipeline is enable and current fragment contain olap table sink, in fe we will 
                    // calculate the number of all tablet sinks in advance and assign them to each fragment instance
                    boolean enablePipelineTableSinkDop = enablePipelineEngine &&
                            (fragment.hasOlapTableSink() || fragment.hasIcebergTableSink());
                    int tableSinkTotalDop = 0;
                    int accTabletSinkDop = 0;
                    if (enablePipelineTableSinkDop) {
                        tableSinkTotalDop = twoStageInstancesToDeploy.stream()
                                .flatMap(Collection::stream)
                                .mapToInt(CoordinatorPreprocessor.FInstanceExecParam::getTableSinkDop)
                                .sum();
                    }
                    Preconditions.checkState(tableSinkTotalDop >= 0,
                            "tableSinkTotalDop = " + tableSinkTotalDop + " should be >= 0");

                    for (int stageIndex = 0; stageIndex < twoStageInstancesToDeploy.size(); stageIndex++) {
                        List<CoordinatorPreprocessor.FInstanceExecParam> stageInstances =
                                twoStageInstancesToDeploy.get(stageIndex);
                        if (stageInstances.isEmpty()) {
                            continue;
                        }

                        TDescriptorTable descTable;
                        if (stageIndex == 0) {
                            descTable = jobSpec.getDescTable();
                        } else {
                            descTable = emptyDescTable;
                        }

                        Map<TUniqueId, Long> instanceId2WorkerId =
                                stageInstances.stream().collect(Collectors.toMap(
                                        CoordinatorPreprocessor.FInstanceExecParam::getInstanceId,
                                        CoordinatorPreprocessor.FInstanceExecParam::getWorkerId));
                        List<TExecPlanFragmentParams> tRequests =
                                params.toThrift(instanceId2WorkerId.keySet(), descTable, enablePipelineEngine,
                                        accTabletSinkDop, tableSinkTotalDop, false);
                        if (enablePipelineTableSinkDop) {
                            accTabletSinkDop += stageInstances.stream()
                                    .mapToInt(CoordinatorPreprocessor.FInstanceExecParam::getTableSinkDop)
                                    .sum();
                        }

                        // This is a load process, and it is the first fragment.
                        // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                        // so that we can check these backends' state when joining this Coordinator
                        boolean needCheckBackendState = isLoadType() && profileFragmentId == 0;

                        for (TExecPlanFragmentParams tRequest : tRequests) {
                            // TODO: pool of pre-formatted BackendExecStates?
                            Long workerId = instanceId2WorkerId.get(tRequest.params.fragment_instance_id);
                            ComputeNode worker = coordinatorPreprocessor.getWorkerProvider().getWorkerById(workerId);
                            BackendExecState execState =
                                    new BackendExecState(fragment.getFragmentId(), worker.getAddress(),
                                            profileFragmentId, tRequest,
                                            worker);
                            backendExecStates.put(tRequest.backend_num, execState);
                            if (needCheckBackendState) {
                                needCheckBackendExecStates.add(execState);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("add need check backend {} for fragment, {} job: {}",
                                            execState.backend.getId(),
                                            fragment.getFragmentId().asInt(), jobSpec.getLoadJobId());
                                }
                            }
                            twoStageExecutionsToDeploy.get(stageIndex).add(execState);
                        }
                    }
                }

                for (List<BackendExecState> stageExecutions : twoStageExecutionsToDeploy) {
                    // Send requests of the executions in this stage.
                    List<Pair<BackendExecState, Future<PExecPlanFragmentResult>>> futures =
                            new ArrayList<>(stageExecutions.size());
                    for (BackendExecState execution : stageExecutions) {
                        futures.add(Pair.create(execution, execution.execRemoteFragmentAsync()));
                    }

                    // Wait responses of the executions in this stage.
                    BackendExecState errorBackendExecState = null;
                    TStatusCode errorCode = null;
                    String errMessage = null;
                    for (Pair<BackendExecState, Future<PExecPlanFragmentResult>> pair : futures) {
                        BackendExecState execution = pair.first;
                        Future<PExecPlanFragmentResult> future = pair.second;

                        TStatusCode code;
                        String errMsg = null;
                        try {
                            PExecPlanFragmentResult result = future.get(queryDeliveryTimeoutMs, TimeUnit.MILLISECONDS);
                            code = TStatusCode.findByValue(result.status.statusCode);
                            if (result.status.errorMsgs != null && !result.status.errorMsgs.isEmpty()) {
                                errMsg = result.status.errorMsgs.get(0);
                            }
                        } catch (ExecutionException e) {
                            LOG.warn("catch a execute exception", e);
                            code = TStatusCode.THRIFT_RPC_ERROR;
                        } catch (InterruptedException e) { // NOSONAR
                            LOG.warn("catch a interrupt exception", e);
                            code = TStatusCode.INTERNAL_ERROR;
                        } catch (TimeoutException e) {
                            LOG.warn("catch a timeout exception", e);
                            code = TStatusCode.TIMEOUT;
                        }

                        if (code != TStatusCode.OK) {
                            if (errMsg == null) {
                                errMsg = "exec rpc error. backend id: " + execution.backend.getId();
                            }
                            queryStatus.setInternalErrorStatus(errMsg + " backend:" + execution.address.hostname);
                            LOG.warn("exec plan fragment failed, errmsg={}, code: {}, fragmentId={}, backend={}:{}",
                                    errMsg, code, execution.fragmentId,
                                    execution.address.hostname, execution.address.port);
                            if (errorBackendExecState == null) {
                                errorBackendExecState = execution;
                                errorCode = code;
                                errMessage = errMsg;
                            }
                            if (Objects.requireNonNull(code) == TStatusCode.TIMEOUT) {
                                break;
                            }
                        }
                    }

                    // Handle error results and cancel fragment instances, excluding TIMEOUT errors,
                    // until all the delivered fragment instances are completed.
                    // Otherwise, the cancellation RPC may arrive at BE before the delivery fragment instance RPC,
                    // causing the instances to become stale and only able to be released after a timeout.
                    handleErrorBackendExecState(errorBackendExecState, errorCode, errMessage);
                }
            }

            attachInstanceProfileToFragmentProfile();
        } finally {
            unlock();
        }
    }

    // choose at most num FInstances on difference BEs
    private List<CoordinatorPreprocessor.FInstanceExecParam> pickupFInstancesOnDifferentHosts(
            List<CoordinatorPreprocessor.FInstanceExecParam> instances, int num) {
        if (instances.size() <= num) {
            return instances;
        }

        Map<Long, List<CoordinatorPreprocessor.FInstanceExecParam>> workerId2instances = Maps.newHashMap();
        for (CoordinatorPreprocessor.FInstanceExecParam instance : instances) {
            workerId2instances.putIfAbsent(instance.getWorkerId(), Lists.newLinkedList());
            workerId2instances.get(instance.getWorkerId()).add(instance);
        }
        List<CoordinatorPreprocessor.FInstanceExecParam> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<CoordinatorPreprocessor.FInstanceExecParam> instancesPerHost : workerId2instances.values()) {
                if (instancesPerHost.isEmpty()) {
                    continue;
                }
                picked.add(instancesPerHost.remove(0));
            }
        }
        return picked;
    }

    private List<TRuntimeFilterDestination> mergeGRFProbers(List<TRuntimeFilterProberParams> probers) {
        Map<TNetworkAddress, List<TUniqueId>> host2probers = Maps.newHashMap();
        for (TRuntimeFilterProberParams prober : probers) {
            host2probers.putIfAbsent(prober.fragment_instance_address, Lists.newArrayList());
            host2probers.get(prober.fragment_instance_address).add(prober.fragment_instance_id);
        }
        return host2probers.entrySet().stream().map(
                e -> new TRuntimeFilterDestination().setAddress(e.getKey()).setFinstance_ids(e.getValue())
        ).collect(Collectors.toList());
    }

    private void setGlobalRuntimeFilterParams(CoordinatorPreprocessor.FragmentExecParams topParams,
                                              TNetworkAddress mergeHost)
            throws Exception {

        Map<Integer, List<TRuntimeFilterProberParams>> broadcastGRFProbersMap = Maps.newHashMap();
        List<RuntimeFilterDescription> broadcastGRFList = Lists.newArrayList();
        Map<Integer, List<TRuntimeFilterProberParams>> idToProbePrams = new HashMap<>();

        for (PlanFragment fragment : jobSpec.getFragments()) {
            fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            fragment.collectProbeRuntimeFilters(fragment.getPlanRoot());
            CoordinatorPreprocessor.FragmentExecParams params =
                    coordinatorPreprocessor.getFragmentExecParamsMap().get(fragment.getFragmentId());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = Lists.newArrayList();
                for (final CoordinatorPreprocessor.FInstanceExecParam instance : params.instanceExecParams) {
                    TRuntimeFilterProberParams probeParam = new TRuntimeFilterProberParams();
                    probeParam.setFragment_instance_id(instance.instanceId);
                    probeParam.setFragment_instance_address(
                            coordinatorPreprocessor.getAddressByWorkerId(instance.getWorkerId()));
                    probeParamList.add(probeParam);
                }
                if (coordinatorPreprocessor.isUsePipeline() && kv.getValue().isBroadcastJoin() &&
                        kv.getValue().isHasRemoteTargets()) {
                    broadcastGRFProbersMap.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                } else {
                    idToProbePrams.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                }
            }

            Set<TUniqueId> broadcastGRfSenders =
                    pickupFInstancesOnDifferentHosts(params.instanceExecParams, 3).stream().
                            map(instance -> instance.instanceId).collect(Collectors.toSet());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getBuildRuntimeFilters().entrySet()) {
                int rid = kv.getKey();
                RuntimeFilterDescription rf = kv.getValue();
                if (rf.isHasRemoteTargets()) {
                    if (rf.isBroadcastJoin()) {
                        // for broadcast join, we send at most 3 copy to probers, the first arrival wins.
                        topParams.runtimeFilterParams.putToRuntime_filter_builder_number(rid, 1);
                        if (coordinatorPreprocessor.isUsePipeline()) {
                            rf.setBroadcastGRFSenders(broadcastGRfSenders);
                            broadcastGRFList.add(rf);
                        } else {
                            rf.setSenderFragmentInstanceId(params.instanceExecParams.get(0).instanceId);
                        }
                    } else {
                        topParams.runtimeFilterParams
                                .putToRuntime_filter_builder_number(rid, params.instanceExecParams.size());
                    }
                }
            }
            fragment.setRuntimeFilterMergeNodeAddresses(fragment.getPlanRoot(), mergeHost);
        }
        topParams.runtimeFilterParams.setId_to_prober_params(idToProbePrams);

        broadcastGRFList.forEach(rf -> rf.setBroadcastGRFDestinations(
                mergeGRFProbers(broadcastGRFProbersMap.get(rf.getFilterId()))));

        if (connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            topParams.runtimeFilterParams.setRuntime_filter_max_size(
                    sessionVariable.getGlobalRuntimeFilterBuildMaxSize());
        }
    }

    @Override
    public List<String> getExportFiles() {
        return exportFiles;
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return coordinatorPreprocessor.getChannelIdToBEHTTPMap();
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        return coordinatorPreprocessor.getChannelIdToBEPortMap();
    }

    void updateExportFiles(List<String> files) {
        lock.lock();
        try {
            if (exportFiles == null) {
                exportFiles = Lists.newArrayList();
            }
            exportFiles.addAll(files);
        } finally {
            lock.unlock();
        }
    }

    void updateDeltas(List<String> urls) {
        lock.lock();
        try {
            deltaUrls.addAll(urls);
        } finally {
            lock.unlock();
        }
    }

    private void updateLoadCounters(Map<String, String> newLoadCounters) {
        lock.lock();
        try {
            long numRowsNormal = 0L;
            String value = this.loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal = Long.parseLong(value);
            }
            long numRowsAbnormal = 0L;
            value = this.loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal = Long.parseLong(value);
            }
            long numRowsUnselected = 0L;
            value = this.loadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected = Long.parseLong(value);
            }
            long numLoadBytesTotal = 0L;
            value = this.loadCounters.get(LoadJob.LOADED_BYTES);
            if (value != null) {
                numLoadBytesTotal = Long.parseLong(value);
            }

            // new load counters
            value = newLoadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal += Long.parseLong(value);
            }
            value = newLoadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal += Long.parseLong(value);
            }
            value = newLoadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected += Long.parseLong(value);
            }
            value = newLoadCounters.get(LoadJob.LOADED_BYTES);
            if (value != null) {
                numLoadBytesTotal += Long.parseLong(value);
            }

            this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, String.valueOf(numRowsNormal));
            this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, String.valueOf(numRowsAbnormal));
            this.loadCounters.put(LoadJob.UNSELECTED_ROWS, String.valueOf(numRowsUnselected));
            this.loadCounters.put(LoadJob.LOADED_BYTES, String.valueOf(numLoadBytesTotal));
        } finally {
            lock.unlock();
        }
    }

    private void updateCommitInfos(List<TTabletCommitInfo> commitInfos) {
        lock.lock();
        try {
            this.commitInfos.addAll(commitInfos);
        } finally {
            lock.unlock();
        }
    }

    private void updateFailInfos(List<TTabletFailInfo> failInfos) {
        lock.lock();
        try {
            this.failInfos.addAll(failInfos);
            LOG.info(failInfos);
        } finally {
            lock.unlock();
        }
    }

    private void updateStatus(Status status, TUniqueId instanceId) {
        lock.lock();
        try {
            // The query is done and we are just waiting for remote fragments to clean up.
            // Ignore their cancelled updates.
            if (returnedAllResults && status.isCancelled()) {
                return;
            }
            // nothing to update
            if (status.ok()) {
                return;
            }

            // don't override an error status; also, cancellation has already started
            if (!queryStatus.ok()) {
                return;
            }

            queryStatus.setStatus(status);
            LOG.warn(
                    "one instance report fail throw updateStatus(), need cancel. job id: {}, query id: {}, instance id: {}",
                    jobSpec.getLoadJobId(), DebugUtil.printId(jobSpec.getQueryId()),
                    instanceId != null ? DebugUtil.printId(instanceId) : "NaN");
            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RowBatch getNext() throws Exception {
        if (receiver == null) {
            throw new UserException("There is no receiver.");
        }

        RowBatch resultBatch;
        Status status = new Status();

        resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            connectContext.setErrorCodeOnce(status.getErrorCodeString());
            LOG.warn("get next fail, need cancel. status {}, query id: {}", status.toString(),
                    DebugUtil.printId(jobSpec.getQueryId()));
        }
        updateStatus(status, null /* no instance id */);

        Status copyStatus;
        lock();
        try {
            copyStatus = new Status(queryStatus);
        } finally {
            unlock();
        }

        if (!copyStatus.ok()) {
            if (Strings.isNullOrEmpty(copyStatus.getErrorMsg())) {
                copyStatus.rewriteErrorMsg();
            }

            if (copyStatus.isRemoteFileNotFound()) {
                throw new RemoteFileNotFoundException(copyStatus.getErrorMsg());
            }

            if (copyStatus.isRpcError()) {
                throw new RpcException("unknown", copyStatus.getErrorMsg());
            } else {
                String errMsg = copyStatus.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);

                // hide host info
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                throw new UserException(errMsg);
            }
        }

        if (resultBatch.isEos()) {
            this.returnedAllResults = true;

            // if this query is a block query do not cancel.
            long numLimitRows = jobSpec.getFragments().get(0).getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!jobSpec.isBlockQuery() && coordinatorPreprocessor.getInstanceIds().size() > 1 && hasLimit &&
                    numReceivedRows >= numLimitRows) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                cancelInternal(PPlanFragmentCancelReason.LIMIT_REACH);
            }
        } else {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        return resultBatch;
    }

    // Cancel execution of query. This includes the execution of the local plan
    // fragment,
    // if any, as well as all plan fragments on remote nodes.
    @Override
    public void cancel(PPlanFragmentCancelReason reason, String message) {
        lock();
        try {
            if (!queryStatus.ok()) {
                // we can't cancel twice
                return;
            } else {
                queryStatus.setStatus(Status.CANCELLED);
                queryStatus.setErrorMsg(message);
            }
            LOG.warn("cancel execution of query, this is outside invoke");
            cancelInternal(reason);
        } finally {
            try {
                // when enable_profile is true, it disable count down profileDoneSignal for collect all backend's profile
                // but if backend has crashed, we need count down profileDoneSignal since it will not report by itself
                if (connectContext.getSessionVariable().isEnableProfile() && profileDoneSignal != null
                        && message.equals(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR)) {
                    profileDoneSignal.countDownToZero(new Status());
                    LOG.info("count down profileDoneSignal since backend has crashed, query id: {}",
                            DebugUtil.printId(jobSpec.getQueryId()));
                }
            } finally {
                unlock();
            }
        }
    }

    private void cancelInternal(PPlanFragmentCancelReason cancelReason) {
        if (StringUtils.isEmpty(connectContext.getState().getErrorMessage())) {
            connectContext.getState().setError(cancelReason.toString());
        }
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(cancelReason);
        if (profileDoneSignal != null && cancelReason != PPlanFragmentCancelReason.LIMIT_REACH) {
            // count down to zero to notify all objects waiting for this
            if (!connectContext.getSessionVariable().isEnableProfile()) {
                profileDoneSignal.countDownToZero(new Status());
                LOG.info("unfinished instance: {}",
                        profileDoneSignal.getLeftMarks().stream().map(e -> DebugUtil.printId(e.getKey())).toArray());
            }
        }
    }

    private void cancelRemoteFragmentsAsync(PPlanFragmentCancelReason cancelReason) {
        for (BackendExecState backendExecState : backendExecStates.values()) {
            // If the execution fails to be cancelled, and it has been finished or not been deployed,
            // count down the profileDoneSignal of this execution immediately,
            // because the profile report will not arrive anymore for the finished or non-deployed execution.
            if (!backendExecState.cancelFragmentInstance(cancelReason) &&
                    (!backendExecState.initiated || backendExecState.done)) {
                profileDoneSignal.markedCountDown(backendExecState.getInstanceId(), -1L);
            }
        }

        coordinatorPreprocessor.getFragmentExecParamsMap().values()
                .stream().flatMap(execFragment -> execFragment.instanceExecParams.stream())
                .filter(instance -> !backendExecStates.containsKey(instance.getBackendNum()))
                .forEach(instance -> profileDoneSignal.markedCountDown(instance.getInstanceId(), -1L));
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        BackendExecState execState = backendExecStates.get(params.backend_num);
        if (execState == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.backend_num,
                    backendExecStates.keySet());
            return;
        }

        if (params.isSetProfile()) {
            profileAlreadyReported = true;
        }

        // Update runtime profile when query is still in process.
        //
        // We need to export profile to ProfileManager before update this profile, because:
        // Each fragment instance will report its state based on their on own timer, and basically, these
        // timers are consistent. So we can assume that all the instances will report profile in a very short
        // time range, if we choose to export the profile to profile manager after update this instance's profile,
        // the whole profile may include the information from the previous report, except for the current instance,
        // which leads to inconsistency.
        //
        // So the profile update strategy looks like this: During a short time interval, each instance will report
        // its execution information. However, when receiving the information reported by the first instance of the
        // current batch, the previous reported state will be synchronized to the profile manager.
        if (!execState.done) {
            long now = System.currentTimeMillis();
            long lastTime = lastRuntimeProfileUpdateTime.get();
            if (topProfileSupplier != null &&
                    connectContext != null &&
                    connectContext.getSessionVariable().isEnableProfile() &&
                    now - lastTime > connectContext.getSessionVariable().getRuntimeProfileReportInterval() * 1000L &&
                    lastRuntimeProfileUpdateTime.compareAndSet(lastTime, now)) {
                RuntimeProfile profile = topProfileSupplier.get();
                profile.addChild(buildMergedQueryProfile(null));
                ProfileManager.getInstance().pushProfile(profile);
            }
        }

        lock();
        try {
            if (!execState.updateProfile(params)) {
                return;
            }
        } finally {
            unlock();
        }

        // print fragment instance profile
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            execState.printProfile(builder);
            LOG.debug("profile for query_id={} instance_id={}\n{}",
                    DebugUtil.printId(jobSpec.getQueryId()),
                    DebugUtil.printId(params.getFragment_instance_id()),
                    builder.toString());
        }

        Status status = new Status(params.status);
        // for now, abort the query if we see any error except if the error is cancelled
        // and returned_all_results_ is true.
        // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
        if (!(returnedAllResults && status.isCancelled()) && !status.ok()) {
            ConnectContext ctx = connectContext;
            if (ctx != null) {
                ctx.setErrorCodeOnce(status.getErrorCodeString());
            }
            LOG.warn("exec state report failed status={}, query_id={}, instance_id={}",
                    status, DebugUtil.printId(jobSpec.getQueryId()),
                    DebugUtil.printId(params.getFragment_instance_id()));
            updateStatus(status, params.getFragment_instance_id());
        }
        if (execState.done) {
            if (params.isSetDelta_urls()) {
                updateDeltas(params.getDelta_urls());
            }
            if (params.isSetLoad_counters()) {
                updateLoadCounters(params.getLoad_counters());
            }
            if (params.isSetTracking_url()) {
                trackingUrl = params.tracking_url;
            }
            if (params.isSetExport_files()) {
                updateExportFiles(params.export_files);
            }
            if (params.isSetCommitInfos()) {
                updateCommitInfos(params.getCommitInfos());
            }
            if (params.isSetFailInfos()) {
                updateFailInfos(params.getFailInfos());
            }
            if (params.isSetRejected_record_path()) {
                rejectedRecordPaths.add(execState.address.hostname + ":" + params.getRejected_record_path());
            }
            if (params.isSetSink_commit_infos()) {
                sinkCommitInfos.addAll(params.sink_commit_infos);
            }
            profileDoneSignal.markedCountDown(params.getFragment_instance_id(), -1L);
        }

        if (params.isSetLoad_type()) {
            TLoadJobType loadJobType = params.getLoad_type();
            if (loadJobType == TLoadJobType.BROKER ||
                    loadJobType == TLoadJobType.INSERT_QUERY ||
                    loadJobType == TLoadJobType.INSERT_VALUES) {
                if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                        && params.isSetSource_load_bytes()) {
                    GlobalStateMgr.getCurrentState().getLoadMgr().updateJobPrgress(
                            jobSpec.getLoadJobId(), params);
                }
            }
        } else {
            if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                    && params.isSetSource_load_bytes()) {
                GlobalStateMgr.getCurrentState().getLoadMgr().updateJobPrgress(
                        jobSpec.getLoadJobId(), params);
            }
        }
    }

    public void endProfile() {
        if (backendExecStates.isEmpty()) {
            return;
        }

        // wait for all backends
        if (needReport) {
            try {
                int timeout;
                // connectContext can be null for broker export task coordinator
                if (connectContext != null) {
                    timeout = connectContext.getSessionVariable().getProfileTimeout();
                } else {
                    timeout = DEFAULT_PROFILE_TIMEOUT_SECOND;
                }
                // Waiting for other fragment instances to finish execution
                // Ideally, it should wait indefinitely, but out of defense, set timeout
                if (!profileDoneSignal.await(timeout, TimeUnit.SECONDS)) {
                    LOG.warn("failed to get profile within {} seconds", timeout);
                }
            } catch (InterruptedException e) { // NOSONAR
                LOG.warn("signal await error", e);
            }
        }
        lock();
        try {
            for (int i = 1; i < fragmentProfiles.size(); ++i) {
                fragmentProfiles.get(i).sortChildren();
            }
        } finally {
            unlock();
        }
    }

    /*
     * Waiting the coordinator finish executing.
     * return false if waiting timeout.
     * return true otherwise.
     * NOTICE: return true does not mean that coordinator executed success,
     * the caller should check queryStatus for result.
     *
     * We divide the entire waiting process into multiple rounds,
     * with a maximum of 5 seconds per round. And after each round of waiting,
     * check the status of the BE. If the BE status is abnormal, the wait is ended
     * and the result is returned. Otherwise, continue to the next round of waiting.
     * This method mainly avoids the problem that the Coordinator waits for a long time
     * after some BE can no long return the result due to some exception, such as BE is down.
     */
    @Override
    public boolean join(int timeoutS) {
        final long fixedMaxWaitTime = 5;

        long leftTimeoutS = timeoutS;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            boolean awaitRes = false;
            try {
                awaitRes = profileDoneSignal.await(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) { // NOSONAR
                // Do nothing
            }
            if (awaitRes) {
                return true;
            }

            if (!checkBackendState()) {
                return true;
            }

            if (ThriftServer.getExecutor() != null
                    && ThriftServer.getExecutor().getPoolSize() >= Config.thrift_server_max_worker_threads) {
                thriftServerHighLoad = true;
            }

            leftTimeoutS -= waitTime;
        }
        return false;
    }

    @Override
    public RuntimeProfile buildMergedQueryProfile(PQueryStatistics statistics) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();

        if (!sessionVariable.isEnableProfile()) {
            return queryProfile;
        }

        if (!coordinatorPreprocessor.isUsePipeline()) {
            return queryProfile;
        }

        int profileLevel = sessionVariable.getPipelineProfileLevel();
        if (profileLevel >= TPipelineProfileLevel.DETAIL.getValue()) {
            return queryProfile;
        }

        RuntimeProfile newQueryProfile = new RuntimeProfile(queryProfile.getName());
        newQueryProfile.copyAllInfoStringsFrom(queryProfile, null);
        newQueryProfile.copyAllCountersFrom(queryProfile);

        long maxQueryCumulativeCpuTime = 0;
        long maxQueryPeakMemoryUsage = 0;

        List<RuntimeProfile> newFragmentProfiles = Lists.newArrayList();
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            RuntimeProfile newFragmentProfile = new RuntimeProfile(fragmentProfile.getName());
            newFragmentProfiles.add(newFragmentProfile);
            newFragmentProfile.copyAllInfoStringsFrom(fragmentProfile, null);
            newFragmentProfile.copyAllCountersFrom(fragmentProfile);

            if (fragmentProfile.getChildList().isEmpty()) {
                continue;
            }

            List<RuntimeProfile> instanceProfiles = fragmentProfile.getChildList().stream()
                    .map(pair -> pair.first)
                    .collect(Collectors.toList());

            Set<String> backendAddresses = Sets.newHashSet();
            for (RuntimeProfile instanceProfile : instanceProfiles) {
                // Setup backend address infos
                backendAddresses.add(instanceProfile.getInfoString("Address"));

                // Get query level peak memory usage and cpu cost
                Counter toBeRemove = instanceProfile.getCounter("QueryCumulativeCpuTime");
                if (toBeRemove != null) {
                    maxQueryCumulativeCpuTime = Math.max(maxQueryCumulativeCpuTime, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QueryCumulativeCpuTime");

                toBeRemove = instanceProfile.getCounter("QueryPeakMemoryUsage");
                if (toBeRemove != null) {
                    maxQueryPeakMemoryUsage = Math.max(maxQueryPeakMemoryUsage, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QueryPeakMemoryUsage");
            }
            newFragmentProfile.addInfoString("BackendAddresses", String.join(",", backendAddresses));
            Counter backendNum = newFragmentProfile.addCounter("BackendNum", TUnit.UNIT, null);
            backendNum.setValue(backendAddresses.size());

            // Setup number of instance
            Counter counter = newFragmentProfile.addCounter("InstanceNum", TUnit.UNIT, null);
            counter.setValue(instanceProfiles.size());

            RuntimeProfile mergedInstanceProfile =
                    RuntimeProfile.mergeIsomorphicProfiles(instanceProfiles, Sets.newHashSet("Address"));
            Preconditions.checkState(mergedInstanceProfile != null);

            newFragmentProfile.copyAllInfoStringsFrom(mergedInstanceProfile, null);
            newFragmentProfile.copyAllCountersFrom(mergedInstanceProfile);

            mergedInstanceProfile.getChildList().forEach(pair -> {
                RuntimeProfile pipelineProfile = pair.first;
                foldUnnecessaryLimitOperators(pipelineProfile);
                setOperatorStatus(pipelineProfile);
                newFragmentProfile.addChild(pipelineProfile);
            });

            newQueryProfile.addChild(newFragmentProfile);
        }

        // Remove redundant MIN/MAX metrics if MIN and MAX are identical
        for (RuntimeProfile fragmentProfile : newFragmentProfiles) {
            RuntimeProfile.removeRedundantMinMaxMetrics(fragmentProfile);
        }

        long queryAllocatedMemoryUsage = 0;
        long queryDeallocatedMemoryUsage = 0;
        // Calculate ExecutionTotalTime, which comprising all operator's sync time and async time
        // We can get Operator's sync time from OperatorTotalTime, and for async time, only ScanOperator and
        // ExchangeOperator have async operations, we can get async time from ScanTime(for ScanOperator) and
        // NetworkTime(for ExchangeOperator)
        long queryCumulativeOperatorTime = 0;
        boolean foundResultSink = false;
        for (RuntimeProfile fragmentProfile : newFragmentProfiles) {
            Counter instanceAllocatedMemoryUsage = fragmentProfile.getCounter("InstanceAllocatedMemoryUsage");
            if (instanceAllocatedMemoryUsage != null) {
                queryAllocatedMemoryUsage += instanceAllocatedMemoryUsage.getValue();
            }
            Counter instanceDeallocatedMemoryUsage = fragmentProfile.getCounter("InstanceDeallocatedMemoryUsage");
            if (instanceDeallocatedMemoryUsage != null) {
                queryDeallocatedMemoryUsage += instanceDeallocatedMemoryUsage.getValue();
            }

            for (Pair<RuntimeProfile, Boolean> pipelineProfilePair : fragmentProfile.getChildList()) {
                RuntimeProfile pipelineProfile = pipelineProfilePair.first;
                for (Pair<RuntimeProfile, Boolean> operatorProfilePair : pipelineProfile.getChildList()) {
                    RuntimeProfile operatorProfile = operatorProfilePair.first;
                    if (!foundResultSink && (operatorProfile.getName().contains("RESULT_SINK") ||
                            operatorProfile.getName().contains("OLAP_TABLE_SINK"))) {
                        newQueryProfile.getCounterTotalTime().setValue(0);

                        long executionWallTime = pipelineProfile.getCounter("DriverTotalTime").getValue();
                        Counter executionTotalTime =
                                newQueryProfile.addCounter("QueryExecutionWallTime", TUnit.TIME_NS, null);
                        executionTotalTime.setValue(executionWallTime);

                        long resultDeliverTime = pipelineProfile.getCounter("OutputFullTime").getValue();
                        Counter resultDeliverTimer =
                                newQueryProfile.addCounter("ResultDeliverTime", TUnit.TIME_NS, null);
                        resultDeliverTimer.setValue(resultDeliverTime);

                        foundResultSink = true;
                    }

                    RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                    RuntimeProfile uniqueMetrics = operatorProfile.getChild("UniqueMetrics");
                    if (commonMetrics == null || uniqueMetrics == null) {
                        continue;
                    }
                    Counter operatorTotalTime = commonMetrics.getMaxCounter("OperatorTotalTime");
                    Preconditions.checkNotNull(operatorTotalTime);
                    queryCumulativeOperatorTime += operatorTotalTime.getValue();

                    Counter scanTime = uniqueMetrics.getMaxCounter("ScanTime");
                    if (scanTime != null) {
                        queryCumulativeOperatorTime += scanTime.getValue();
                    }

                    Counter networkTime = uniqueMetrics.getMaxCounter("NetworkTime");
                    if (networkTime != null) {
                        queryCumulativeOperatorTime += networkTime.getValue();
                    }
                }
            }
        }
        Counter queryAllocatedMemoryUsageCounter =
                newQueryProfile.addCounter("QueryAllocatedMemoryUsage", TUnit.BYTES, null);
        queryAllocatedMemoryUsageCounter.setValue(queryAllocatedMemoryUsage);
        Counter queryDeallocatedMemoryUsageCounter =
                newQueryProfile.addCounter("QueryDeallocatedMemoryUsage", TUnit.BYTES, null);
        queryDeallocatedMemoryUsageCounter.setValue(queryDeallocatedMemoryUsage);
        Counter queryCumulativeOperatorTimer =
                newQueryProfile.addCounter("QueryCumulativeOperatorTime", TUnit.TIME_NS, null);
        queryCumulativeOperatorTimer.setValue(queryCumulativeOperatorTime);
        newQueryProfile.getCounterTotalTime().setValue(0);

        Counter queryCumulativeCpuTime = newQueryProfile.addCounter("QueryCumulativeCpuTime", TUnit.TIME_NS, null);
        queryCumulativeCpuTime.setValue(statistics == null || statistics.cpuCostNs == null ?
                maxQueryCumulativeCpuTime : statistics.cpuCostNs);
        Counter queryPeakMemoryUsage = newQueryProfile.addCounter("QueryPeakMemoryUsage", TUnit.BYTES, null);
        queryPeakMemoryUsage.setValue(statistics == null || statistics.memCostBytes == null ?
                maxQueryPeakMemoryUsage : statistics.memCostBytes);

        return newQueryProfile;
    }

    /**
     * Remove unnecessary LimitOperator, which has same input rows and output rows
     * to keep the profile concise
     */
    private void foldUnnecessaryLimitOperators(RuntimeProfile pipelineProfile) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isProfileLimitFold()) {
            return;
        }

        List<String> foldNames = Lists.newArrayList();
        for (Pair<RuntimeProfile, Boolean> child : pipelineProfile.getChildList()) {
            RuntimeProfile operatorProfile = child.first;
            if (operatorProfile.getName().contains("LIMIT")) {
                RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                Preconditions.checkNotNull(commonMetrics);
                Counter pullRowNum = commonMetrics.getCounter("PullRowNum");
                Counter pushRowNum = commonMetrics.getCounter("PushRowNum");
                if (pullRowNum == null || pushRowNum == null) {
                    continue;
                }
                if (Objects.equals(pullRowNum.getValue(), pushRowNum.getValue())) {
                    foldNames.add(operatorProfile.getName());
                }
            }
        }

        foldNames.forEach(pipelineProfile::removeChild);
    }

    private void setOperatorStatus(RuntimeProfile pipelineProfile) {
        for (Pair<RuntimeProfile, Boolean> child : pipelineProfile.getChildList()) {
            RuntimeProfile operatorProfile = child.first;
            RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
            Preconditions.checkNotNull(commonMetrics);

            Counter closeTime = commonMetrics.getCounter("CloseTime");
            Counter minCloseTime = commonMetrics.getCounter("__MIN_OF_CloseTime");
            if (closeTime != null && closeTime.getValue() == 0 ||
                    minCloseTime != null && minCloseTime.getValue() == 0) {
                commonMetrics.addInfoString("Status", "Running");
            }
        }
    }

    /**
     * Check the state of backends in needCheckBackendExecStates.
     * return true if all of them are OK. Otherwise, return false.
     */
    @Override
    public boolean checkBackendState() {
        for (BackendExecState backendExecState : needCheckBackendExecStates) {
            if (!backendExecState.isBackendStateHealthy()) {
                queryStatus = new Status(TStatusCode.INTERNAL_ERROR,
                        "backend " + backendExecState.backend.getId() + " is down");
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isDone() {
        return profileDoneSignal.getCount() == 0;
    }

    @Override
    public boolean isEnableLoadProfile() {
        return connectContext != null && connectContext.getSessionVariable().isEnableLoadProfile();
    }

    // consistent with EXPLAIN's fragment index
    @Override
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        final List<QueryStatisticsItem.FragmentInstanceInfo> result = Lists.newArrayList();
        for (PlanFragment fragment : jobSpec.getFragments()) {
            for (BackendExecState backendExecState : backendExecStates.values()) {
                if (fragment.getFragmentId() != backendExecState.fragmentId) {
                    continue;
                }
                final FragmentInstanceInfo info = backendExecState.buildFragmentInstanceInfo();
                result.add(info);
            }
        }
        return result;
    }

    private void attachInstanceProfileToFragmentProfile() {
        for (BackendExecState backendExecState : backendExecStates.values()) {
            if (!backendExecState.computeTimeInProfile(fragmentProfiles.size())) {
                return;
            }
            fragmentProfiles.get(backendExecState.profileFragmentId).addChild(backendExecState.profile);
        }
    }

    @Override
    public boolean isThriftServerHighLoad() {
        return this.thriftServerHighLoad;
    }

    @Override
    public boolean isProfileAlreadyReported() {
        return this.profileAlreadyReported;
    }

    // record backend execute state
    // TODO(zhaochun): add profile information and others
    public class BackendExecState {
        TExecPlanFragmentParams commonRpcParams;
        TExecPlanFragmentParams uniqueRpcParams;
        PlanFragmentId fragmentId;
        boolean initiated;
        boolean done;
        boolean hasCanceled;
        int profileFragmentId;
        RuntimeProfile profile;
        TNetworkAddress address;
        final ComputeNode backend;
        long lastMissingHeartbeatTime = -1;

        // fake backendExecState, only user for stream load profile
        public BackendExecState(TUniqueId fragmentInstanceId, TNetworkAddress address) {
            String name = "Instance " + DebugUtil.printId(fragmentInstanceId);
            this.profile = new RuntimeProfile(name);
            this.profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
            this.backend = null;
        }

        public BackendExecState(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                TExecPlanFragmentParams rpcParams,
                                ComputeNode backend) {
            this(fragmentId, host, profileFragmentId, rpcParams, rpcParams, backend);
        }

        public BackendExecState(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                TExecPlanFragmentParams commonRpcParams, TExecPlanFragmentParams uniqueRpcParams,
                                ComputeNode backend) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.commonRpcParams = commonRpcParams;
            this.uniqueRpcParams = uniqueRpcParams;
            this.initiated = false;
            this.done = false;
            this.address = host;
            this.backend = backend;
            String name =
                    "Instance " + DebugUtil.printId(uniqueRpcParams.params.fragment_instance_id) + " (host=" + address +
                            ")";
            this.profile = new RuntimeProfile(name);
            this.profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
            this.hasCanceled = false;
            this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
        }

        public boolean isFinished() {
            return done;
        }

        public Integer getIndexInJob() {
            return uniqueRpcParams.getBackend_num();
        }

        public TUniqueId getInstanceId() {
            return uniqueRpcParams.getParams().getFragment_instance_id();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        // update profile.
        // return true if profile is updated. Otherwise, return false.
        public synchronized boolean updateProfile(TReportExecStatusParams params) {
            if (this.done) {
                // duplicate packet
                return false;
            }
            if (params.isSetProfile()) {
                profile.update(params.profile);
            }
            this.done = params.done;
            return true;
        }

        public synchronized void printProfile(StringBuilder builder) {
            this.profile.computeTimeInProfile();
            this.profile.prettyPrint(builder, "");
        }

        // cancel the fragment instance.
        // return true if cancel success. Otherwise, return false
        public synchronized boolean cancelFragmentInstance(PPlanFragmentCancelReason cancelReason) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "cancelRemoteFragments initiated={} done={} hasCanceled={} backend: {}, " +
                                "fragment instance id={}, reason: {}",
                        this.initiated, this.done, this.hasCanceled, backend.getId(),
                        DebugUtil.printId(fragmentInstanceId()), cancelReason.name());
            }
            try {
                if (!this.initiated) {
                    return false;
                }
                // don't cancel if it is already finished
                if (this.done) {
                    return false;
                }
                if (this.hasCanceled) {
                    return false;
                }

                TNetworkAddress brpcAddress = backend.getBrpcAddress();

                try {
                    BackendServiceClient.getInstance().cancelPlanFragmentAsync(brpcAddress,
                            jobSpec.getQueryId(), fragmentInstanceId(), cancelReason,
                            commonRpcParams.is_pipeline);
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(backend.getId());
                }

                this.hasCanceled = true;
            } catch (Exception e) {
                LOG.warn("catch a exception", e);
                return false;
            }
            return true;
        }

        public synchronized boolean computeTimeInProfile(int maxFragmentId) {
            if (this.profileFragmentId < 0 || this.profileFragmentId > maxFragmentId) {
                LOG.warn("profileFragmentId {} should be in [0, {})", profileFragmentId, maxFragmentId);
                return false;
            }
            profile.computeTimeInProfile();
            return true;
        }

        public boolean isBackendStateHealthy() {
            if (backend.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime) {
                LOG.warn("backend {} is down while joining the coordinator. job id: {}", backend.getId(),
                        jobSpec.getLoadJobId());
                return false;
            }
            return true;
        }

        public Future<PExecPlanFragmentResult> execRemoteFragmentAsync() throws TException {
            TNetworkAddress brpcAddress;
            try {
                brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            } catch (Exception e) {
                throw new TException(e.getMessage());
            }
            this.initiated = true;
            try {
                return BackendServiceClient.getInstance().execPlanFragmentAsync(brpcAddress, uniqueRpcParams);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return new Future<PExecPlanFragmentResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public PExecPlanFragmentResult get() {
                        PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                        StatusPB pStatus = new StatusPB();
                        pStatus.errorMsgs = Lists.newArrayList();
                        pStatus.errorMsgs.add(e.getMessage());
                        // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                        pStatus.statusCode = TStatusCode.THRIFT_RPC_ERROR.getValue();
                        result.status = pStatus;
                        return result;
                    }

                    @Override
                    public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                        return get();
                    }
                };
            }
        }

        public FragmentInstanceInfo buildFragmentInstanceInfo() {
            return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                    .instanceId(fragmentInstanceId()).fragmentId(String.valueOf(fragmentId)).address(this.address)
                    .build();
        }

        private TUniqueId fragmentInstanceId() {
            return this.uniqueRpcParams.params.getFragment_instance_id();
        }
    }
}
