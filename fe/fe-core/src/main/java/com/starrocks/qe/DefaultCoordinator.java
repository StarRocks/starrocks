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
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPipelineProfileLevel;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
    private final ExecutionDAG executionDAG;

    // protects all fields below
    private final Lock lock = new ReentrantLock();
    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;
    private RuntimeProfile queryProfile;
    private List<RuntimeProfile> fragmentProfiles;
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
    private Supplier<ExecPlan> execPlanSupplier;
    private final AtomicLong lastRuntimeProfileUpdateTime = new AtomicLong(System.currentTimeMillis());

    public static class Factory implements Coordinator.Factory {

        @Override
        public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            JobSpec jobSpec = JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.SELECT);
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
        this.executionDAG = ExecutionDAG.build(jobSpec);

        TUniqueId queryId = jobSpec.getQueryId();

        LOG.info("Execution Profile: {}", DebugUtil.printId(queryId));
        queryProfile = new RuntimeProfile("Execution");

        fragmentProfiles = new ArrayList<>();
        fragmentProfiles.add(new RuntimeProfile("Fragment 0"));
        queryProfile.addChild(fragmentProfiles.get(0));
        profileDoneSignal = new MarkedCountDownLatch<>(1);
        profileDoneSignal.addMark(queryId, -1L /* value is meaningless */);

        FragmentInstanceExecState execState = FragmentInstanceExecState.createFakeExecution(queryId, address);
        executionDAG.addExecution(execState);

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
        this.executionDAG = coordinatorPreprocessor.getExecutionDAG();
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

    @Override
    public void clearExportStatus() {
        lock.lock();
        try {
            executionDAG.resetExecutions();
            this.queryStatus.setStatus(new Status());
            if (this.exportFiles == null) {
                this.exportFiles = Lists.newArrayList();
            }
            this.exportFiles.clear();
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
    public void setExecPlanSupplier(Supplier<ExecPlan> execPlanSupplier) {
        this.execPlanSupplier = execPlanSupplier;
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

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    // Initiate asynchronous execState of query. Returns as soon as all plan fragments
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
        getFragmentExecParamsMap().values().forEach(execFragment -> execFragment.getWorkerStatsTracker().release());
    }

    public CoordinatorPreprocessor getPrepareInfo() {
        return coordinatorPreprocessor;
    }

    public List<PlanFragment> getFragments() {
        return jobSpec.getFragments();
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
        for (ExecutionFragment fragment : executionDAG.getFragmentsInCreatedOrder()) {
            RuntimeProfile fragmentProfile = new RuntimeProfile("Fragment " + fragment.getFragmentIndex());
            fragmentProfiles.add(fragmentProfile);
            queryProfile.addChild(fragmentProfile);
        }

        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execState has been initiated, otherwise we might try to cancel fragment
        // execState at backends where it hasn't even started
        profileDoneSignal = new MarkedCountDownLatch<>(executionDAG.getInstanceIds().size());
        for (TUniqueId instanceId : executionDAG.getInstanceIds()) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }
    }

    private void prepareResultSink() throws Exception {
        ExecutionFragment rootExecFragment = executionDAG.getRootFragment();
        if (rootExecFragment.getPlanFragment().getSink() instanceof ResultSink) {
            long workerId = rootExecFragment.getInstances().get(0).getWorkerId();
            ComputeNode worker = coordinatorPreprocessor.getWorkerProvider().getWorkerById(workerId);
            TNetworkAddress execBeAddr = worker.getAddress();
            receiver = new ResultReceiver(
                    rootExecFragment.getInstances().get(0).getInstanceId(),
                    workerId,
                    worker.getBrpcAddress(),
                    jobSpec.getQueryOptions().query_timeout * 1000);

            // Select top fragment as global runtime filter merge address
            setGlobalRuntimeFilterParams(rootExecFragment, worker.getBrpcAddress());

            if (LOG.isDebugEnabled()) {
                LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()), execBeAddr);
            }

            // set the broker address for OUTFILE sink
            ResultSink resultSink = (ResultSink) rootExecFragment.getPlanFragment().getSink();
            if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
                FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(resultSink.getBrokerName(),
                        execBeAddr.getHostname());
                resultSink.setBrokerAddr(broker.ip, broker.port);
                LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
            }
        } else {
            // This is a load process.
            jobSpec.getQueryOptions().setEnable_profile(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds();
            GlobalStateMgr.getCurrentState().getLoadMgr()
                    .initJobProgress(jobSpec.getLoadJobId(), jobSpec.getQueryId(),
                            executionDAG.getInstanceIds(),
                            relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()),
                    coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds());
        }
    }

    private void deliverExecFragments() throws RpcException, UserException {
        lock();
        try {
            Deployer deployer = new Deployer(connectContext, jobSpec, executionDAG, coordinatorPreprocessor.getCoordAddress(),
                    this::handleErrorExecution);
            for (List<ExecutionFragment> concurrentFragments : executionDAG.getFragmentsInTopologicalOrderFromRoot()) {
                deployer.deployFragments(concurrentFragments);
            }

            attachInstanceProfileToFragmentProfile();
        } finally {
            unlock();
        }
    }

    private void handleErrorExecution(Status status, FragmentInstanceExecState execution, Throwable failure)
            throws UserException, RpcException {
        cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
        switch (Objects.requireNonNull(status.getErrorCode())) {
            case TIMEOUT:
                throw new UserException("query timeout. backend id: " + execution.getWorker().getId());
            case THRIFT_RPC_ERROR:
                SimpleScheduler.addToBlacklist(execution.getWorker().getId());
                throw new RpcException(execution.getWorker().getHost(), "rpc failed");
            default:
                throw new UserException(status.getErrorMsg());
        }
    }

    // choose at most num FInstances on difference BEs
    private List<FragmentInstance> pickupFInstancesOnDifferentHosts(List<FragmentInstance> instances, int num) {
        if (instances.size() <= num) {
            return instances;
        }

        Map<Long, List<FragmentInstance>> workerId2instances = Maps.newHashMap();
        for (FragmentInstance instance : instances) {
            workerId2instances.putIfAbsent(instance.getWorkerId(), Lists.newLinkedList());
            workerId2instances.get(instance.getWorkerId()).add(instance);
        }
        List<FragmentInstance> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<FragmentInstance> instancesPerHost : workerId2instances.values()) {
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

    private void setGlobalRuntimeFilterParams(ExecutionFragment topParams,
                                              TNetworkAddress mergeHost) {

        Map<Integer, List<TRuntimeFilterProberParams>> broadcastGRFProbersMap = Maps.newHashMap();
        List<RuntimeFilterDescription> broadcastGRFList = Lists.newArrayList();
        Map<Integer, List<TRuntimeFilterProberParams>> idToProbePrams = new HashMap<>();

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            PlanFragment fragment = execFragment.getPlanFragment();
            fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            fragment.collectProbeRuntimeFilters(fragment.getPlanRoot());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = Lists.newArrayList();
                for (final FragmentInstance instance : execFragment.getInstances()) {
                    TRuntimeFilterProberParams probeParam = new TRuntimeFilterProberParams();
                    probeParam.setFragment_instance_id(instance.getInstanceId());
                    probeParam.setFragment_instance_address(coordinatorPreprocessor.getBrpcAddress(instance.getWorkerId()));
                    probeParamList.add(probeParam);
                }
                if (jobSpec.isEnablePipeline() && kv.getValue().isBroadcastJoin() &&
                        kv.getValue().isHasRemoteTargets()) {
                    broadcastGRFProbersMap.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                } else {
                    idToProbePrams.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                }
            }

            Set<TUniqueId> broadcastGRfSenders =
                    pickupFInstancesOnDifferentHosts(execFragment.getInstances(), 3).stream().
                            map(FragmentInstance::getInstanceId).collect(Collectors.toSet());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getBuildRuntimeFilters().entrySet()) {
                int rid = kv.getKey();
                RuntimeFilterDescription rf = kv.getValue();
                if (rf.isHasRemoteTargets()) {
                    if (rf.isBroadcastJoin()) {
                        // for broadcast join, we send at most 3 copy to probers, the first arrival wins.
                        topParams.getRuntimeFilterParams().putToRuntime_filter_builder_number(rid, 1);
                        if (jobSpec.isEnablePipeline()) {
                            rf.setBroadcastGRFSenders(broadcastGRfSenders);
                            broadcastGRFList.add(rf);
                        } else {
                            rf.setSenderFragmentInstanceId(execFragment.getInstances().get(0).getInstanceId());
                        }
                    } else {
                        topParams.getRuntimeFilterParams()
                                .putToRuntime_filter_builder_number(rid, execFragment.getInstances().size());
                    }
                }
            }
            fragment.setRuntimeFilterMergeNodeAddresses(fragment.getPlanRoot(), mergeHost);
        }
        topParams.getRuntimeFilterParams().setId_to_prober_params(idToProbePrams);

        broadcastGRFList.forEach(rf -> rf.setBroadcastGRFDestinations(
                mergeGRFProbers(broadcastGRFProbersMap.get(rf.getFilterId()))));

        if (connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            topParams.getRuntimeFilterParams().setRuntime_filter_max_size(
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
            LOG.warn("get next fail, need cancel. status {}, query id: {}", status,
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
            long numLimitRows = executionDAG.getRootFragment().getPlanFragment().getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!jobSpec.isBlockQuery() && executionDAG.getInstanceIds().size() > 1 && hasLimit &&
                    numReceivedRows >= numLimitRows) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                cancelInternal(PPlanFragmentCancelReason.LIMIT_REACH);
            }
        } else {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        return resultBatch;
    }

    // Cancel execState of query. This includes the execState of the local plan
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
            LOG.warn("cancel execState of query, this is outside invoke");
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
        for (FragmentInstanceExecState execState : executionDAG.getExecutions()) {
            // If the execState fails to be cancelled, and it has been finished or not been deployed,
            // count down the profileDoneSignal of this execState immediately,
            // because the profile report will not arrive anymore for the finished or non-deployed execState.
            if (!execState.cancelFragmentInstance(cancelReason) &&
                    (!execState.hasBeenDeployed() || execState.isFinished())) {
                profileDoneSignal.markedCountDown(execState.getInstanceId(), -1L);
            }
        }

        executionDAG.getInstances().stream()
                .filter(instance -> executionDAG.getExecution(instance.getIndexInJob()) == null)
                .forEach(instance -> profileDoneSignal.markedCountDown(instance.getInstanceId(), -1L));
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        FragmentInstanceExecState execState = executionDAG.getExecution(params.getBackend_num());
        if (execState == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.getBackend_num(),
                    executionDAG.getExecutionIndexesInJob());
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
        // its execState information. However, when receiving the information reported by the first instance of the
        // current batch, the previous reported state will be synchronized to the profile manager.
        long now = System.currentTimeMillis();
        long lastTime = lastRuntimeProfileUpdateTime.get();
        if (topProfileSupplier != null && execPlanSupplier != null && connectContext != null &&
                connectContext.getSessionVariable().isEnableProfile() &&
                // If it's the last done report, avoiding duplicate trigger
                (!execState.isFinished() || profileDoneSignal.getLeftMarks().size() > 1) &&
                // Interval * 0.95 * 1000 to allow a certain range of deviation
                now - lastTime > (connectContext.getSessionVariable().getRuntimeProfileReportInterval() * 950L) &&
                lastRuntimeProfileUpdateTime.compareAndSet(lastTime, now)) {
            RuntimeProfile profile = topProfileSupplier.get();
            ExecPlan execPlan = execPlanSupplier.get();
            profile.addChild(buildMergedQueryProfile(null));
            ProfileManager.getInstance().pushProfile(execPlan, profile);
        }

        lock();
        try {
            if (!execState.updateExecStatus(params)) {
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
                    builder);
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
        if (execState.isFinished()) {
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
                rejectedRecordPaths.add(execState.getAddress().hostname + ":" + params.getRejected_record_path());
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
        if (executionDAG.getExecutions().isEmpty()) {
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
                // Waiting for other fragment instances to finish execState
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

        if (!jobSpec.isEnablePipeline()) {
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
            Set<String> instanceIds = Sets.newHashSet();
            Set<String> missingInstanceIds = Sets.newHashSet();
            for (RuntimeProfile instanceProfile : instanceProfiles) {
                // Setup backend meta infos
                backendAddresses.add(instanceProfile.getInfoString("Address"));
                instanceIds.add(instanceProfile.getInfoString("InstanceId"));
                if (CollectionUtils.isEmpty(instanceProfile.getChildList())) {
                    missingInstanceIds.add(instanceProfile.getInfoString("InstanceId"));
                }

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
            newFragmentProfile.addInfoString("InstanceIds", String.join(",", instanceIds));
            if (!missingInstanceIds.isEmpty()) {
                newFragmentProfile.addInfoString("MissingInstanceIds", String.join(",", missingInstanceIds));
            }
            Counter backendNum = newFragmentProfile.addCounter("BackendNum", TUnit.UNIT, null);
            backendNum.setValue(backendAddresses.size());

            // Setup number of instance
            Counter counter = newFragmentProfile.addCounter("InstanceNum", TUnit.UNIT, null);
            counter.setValue(instanceProfiles.size());

            RuntimeProfile mergedInstanceProfile =
                    RuntimeProfile.mergeIsomorphicProfiles(instanceProfiles, Sets.newHashSet("Address", "InstanceId"));
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

                        Counter outputFullTime = pipelineProfile.getCounter("OutputFullTime");
                        if (outputFullTime != null) {
                            long resultDeliverTime = outputFullTime.getValue();
                            Counter resultDeliverTimer =
                                    newQueryProfile.addCounter("ResultDeliverTime", TUnit.TIME_NS, null);
                            resultDeliverTimer.setValue(resultDeliverTime);
                        }

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
        for (FragmentInstanceExecState execState : executionDAG.getNeedCheckExecutions()) {
            if (!execState.isBackendStateHealthy()) {
                queryStatus = new Status(TStatusCode.INTERNAL_ERROR,
                        "backend " + execState.getWorker().getId() + " is down");
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
        return executionDAG.getFragmentInstanceInfos();
    }

    private void attachInstanceProfileToFragmentProfile() {
        for (FragmentInstanceExecState execState : executionDAG.getExecutions()) {
            if (!execState.computeTimeInProfile(fragmentProfiles.size())) {
                return;
            }
            fragmentProfiles.get(execState.getFragmentIndex()).addChild(execState.getProfile());
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
}
