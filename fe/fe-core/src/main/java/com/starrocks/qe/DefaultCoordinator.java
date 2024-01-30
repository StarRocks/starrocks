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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServer;
import com.starrocks.common.UserException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.AuditStatisticsUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.mysql.MysqlCommand;
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
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(DefaultCoordinator.class);

    private static final int DEFAULT_PROFILE_TIMEOUT_SECOND = 2;

    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final ConnectContext connectContext;

    private final CoordinatorPreprocessor coordinatorPreprocessor;

    /**
     * Protects all the fields below.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * Overall status of the entire query.
     * <p> Set to the first reported fragment error status or to CANCELLED, if {@link #cancel(String cancelledMessage)} is called.
     */
    private Status queryStatus = new Status();

    private PQueryStatistics auditStatistics;

    private final QueryRuntimeProfile queryProfile;

    private ResultReceiver receiver;
    private int numReceivedRows = 0;

    /**
     * True indicates the query is done returning all results.
     * <p> It is possible that the coordinator still needs to wait for cleanup on remote fragments (e.g. queries with limit)
     * Once this is set to true, errors from remote fragments are ignored.
     */
    private boolean returnedAllResults;

    private boolean thriftServerHighLoad;

    private LogicalSlot slot = null;

    private ShortCircuitExecutor shortCircuitExecutor = null;
    private boolean isShortCircuit = false;
    private boolean isBinaryRow = false;

    public static class Factory implements Coordinator.Factory {

        @Override
        public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            JobSpec jobSpec =
                    JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.SELECT);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                        List<ScanNode> scanNodes,
                                                        TDescriptorTable descTable) {
            JobSpec jobSpec = JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.LOAD);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createBrokerLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromBrokerLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createStreamLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromStreamLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec);
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

            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createRefreshDictionaryCacheScheduler(ConnectContext context, TUniqueId queryId,
                                                                        DescriptorTable descTable, List<PlanFragment> fragments,
                                                                        List<ScanNode> scanNodes) {

            JobSpec jobSpec = JobSpec.Factory.fromRefreshDictionaryCacheSpec(context, queryId, descTable, fragments,
                    scanNodes);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId,
                                                                       DescriptorTable descTable,
                                                                       List<PlanFragment> fragments,
                                                                       List<ScanNode> scanNodes,
                                                                       String timezone,
                                                                       long startTime,
                                                                       Map<String, String> sessionVariables,
                                                                       ConnectContext context, long execMemLimit) {
            JobSpec jobSpec = JobSpec.Factory.fromNonPipelineBrokerLoadJobSpec(context, jobId, queryId, descTable,
                    fragments, scanNodes, timezone,
                    startTime, sessionVariables, execMemLimit);

            return new DefaultCoordinator(context, jobSpec);
        }
    }

    /**
     * Only used for sync stream load profile, and only init relative data structure.
     */
    public DefaultCoordinator(JobSpec jobSpec, StreamLoadPlanner planner, TNetworkAddress address) {
        this.connectContext = planner.getConnectContext();
        this.jobSpec = jobSpec;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        TUniqueId queryId = jobSpec.getQueryId();

        LOG.info("Execution Profile: {}", DebugUtil.printId(queryId));

        FragmentInstanceExecState execState = FragmentInstanceExecState.createFakeExecution(queryId, address);
        executionDAG.addExecution(execState);

        this.queryProfile = new QueryRuntimeProfile(connectContext, jobSpec, 1);
        queryProfile.attachInstances(Collections.singletonList(queryId));
        queryProfile.attachExecutionProfiles(executionDAG.getExecutions());

        this.coordinatorPreprocessor = null;
    }

    DefaultCoordinator(ConnectContext context, JobSpec jobSpec) {
        this.connectContext = context;
        this.jobSpec = jobSpec;
        this.returnedAllResults = false;

        this.coordinatorPreprocessor = new CoordinatorPreprocessor(context, jobSpec);
        this.executionDAG = coordinatorPreprocessor.getExecutionDAG();

        this.queryProfile = new QueryRuntimeProfile(connectContext, jobSpec, executionDAG.getFragmentsInCreatedOrder().size());
        List<PlanFragment> fragments = jobSpec.getFragments();
        List<ScanNode> scanNodes = jobSpec.getScanNodes();
        TDescriptorTable descTable = jobSpec.getDescTable();

        if (connectContext.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
            isBinaryRow = true;
        }

        shortCircuitExecutor = ShortCircuitExecutor.create(context, fragments, scanNodes, descTable,
                isBinaryRow, jobSpec.isNeedReport());

        if (null != shortCircuitExecutor) {
            isShortCircuit = true;
        }
    }

    @Override
    public LogicalSlot getSlot() {
        return slot;
    }

    public void setSlot(LogicalSlot slot) {
        this.slot = slot;
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
        return queryProfile.getQueryProfile();
    }

    @Override
    public List<String> getDeltaUrls() {
        return queryProfile.getDeltaUrls();
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return queryProfile.getLoadCounters();
    }

    @Override
    public String getTrackingUrl() {
        return queryProfile.getTrackingUrl();
    }

    @Override
    public List<String> getRejectedRecordPaths() {
        return queryProfile.getRejectedRecordPaths();
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
            queryStatus.setStatus(new Status());
            queryProfile.clearExportStatus();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return queryProfile.getCommitInfos();
    }

    @Override
    public List<TTabletFailInfo> getFailInfos() {
        return queryProfile.getFailInfos();
    }

    @Override
    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return queryProfile.getSinkCommitInfos();
    }

    @Override
    public List<String> getExportFiles() {
        return queryProfile.getExportFiles();
    }

    @Override
    public void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {
        queryProfile.setTopProfileSupplier(topProfileSupplier);
    }

    @Override
    public void setExecPlan(ExecPlan execPlan) {
        queryProfile.setExecPlan(execPlan);
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

        if (slot != null && slot.getPipelineDop() > 0 &&
                slot.getPipelineDop() != jobSpec.getQueryOptions().getPipeline_dop()) {
            jobSpec.getFragments().forEach(fragment -> fragment.limitMaxPipelineDop(slot.getPipelineDop()));
        }

        coordinatorPreprocessor.prepareExec();

        prepareResultSink();

        prepareProfile();
    }

    @Override
    public void onFinished() {
        jobSpec.getSlotProvider().cancelSlotRequirement(slot);
        jobSpec.getSlotProvider().releaseSlot(slot);
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
    public void startScheduling(boolean needDeploy) throws Exception {
        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Pending")) {
            QueryQueueManager.getInstance().maybeWait(connectContext, this);
        }

        if (isShortCircuit) {
            execShortCircuit();
            return;
        }

        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Prepare")) {
            prepareExec();
        }

        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Deploy")) {
            deliverExecFragments(needDeploy);
        }

        // Prevent `explain scheduler` from waiting until the profile timeout.
        if (!needDeploy) {
            queryProfile.finishAllInstances(Status.OK);
        }
    }

    @Override
    public String getSchedulerExplain() {
        return executionDAG.getFragmentsInPreorder().stream()
                .map(ExecutionFragment::getExplainString)
                .collect(Collectors.joining("\n"));
    }

    private void prepareProfile() {
        ExecutionFragment rootExecFragment = executionDAG.getRootFragment();
        boolean isLoadType = !(rootExecFragment.getPlanFragment().getSink() instanceof ResultSink);
        if (isLoadType) {
            jobSpec.getQueryOptions().setEnable_profile(true);
            List<Long> relatedBackendIds = coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds();
            GlobalStateMgr.getCurrentState().getLoadMgr().initJobProgress(
                    jobSpec.getLoadJobId(), jobSpec.getQueryId(), executionDAG.getInstanceIds(), relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()),
                    coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds());
        }

        queryProfile.attachInstances(executionDAG.getInstanceIds());
    }

    private void prepareResultSink() throws AnalysisException {
        ExecutionFragment rootExecFragment = executionDAG.getRootFragment();
        long workerId = rootExecFragment.getInstances().get(0).getWorkerId();
        ComputeNode worker = coordinatorPreprocessor.getWorkerProvider().getWorkerById(workerId);
        // Select top fragment as global runtime filter merge address
        setGlobalRuntimeFilterParams(rootExecFragment, worker.getBrpcIpAddress());
        boolean isLoadType = !(rootExecFragment.getPlanFragment().getSink() instanceof ResultSink);
        if (isLoadType) {
            // TODO (by satanson): Other DataSink except ResultSink can not support global
            //  runtime filter merging at present, we should support it in future.
            // pipeline-level runtime filter needs to derive RuntimeFilterLayout, so we collect
            // RuntimeFilterDescription
            for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
                PlanFragment fragment = execFragment.getPlanFragment();
                fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            }
            return;
        }

        TNetworkAddress execBeAddr = worker.getAddress();
        receiver = new ResultReceiver(
                rootExecFragment.getInstances().get(0).getInstanceId(),
                workerId,
                worker.getBrpcAddress(),
                jobSpec.getQueryOptions().query_timeout * 1000);

        if (LOG.isDebugEnabled()) {
            LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()), execBeAddr);
        }

        // set the broker address for OUTFILE sink
        ResultSink resultSink = (ResultSink) rootExecFragment.getPlanFragment().getSink();
        if (isBinaryRow) {
            resultSink.setBinaryRow(true);
        }
        if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
            FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(resultSink.getBrokerName(),
                    execBeAddr.getHostname());
            resultSink.setBrokerAddr(broker.ip, broker.port);
            LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
        }
    }

    private void deliverExecFragments(boolean needDeploy) throws RpcException, UserException {
        lock();
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployLockInternalTime")) {
            Deployer deployer =
                    new Deployer(connectContext, jobSpec, executionDAG, coordinatorPreprocessor.getCoordAddress(),
                            this::handleErrorExecution);
            for (List<ExecutionFragment> concurrentFragments : executionDAG.getFragmentsInTopologicalOrderFromRoot()) {
                deployer.deployFragments(concurrentFragments, needDeploy);
            }

            queryProfile.attachExecutionProfiles(executionDAG.getExecutions());
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
                SimpleScheduler.addToBlocklist(execution.getWorker().getId());
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
                    probeParam.setFragment_instance_address(
                            coordinatorPreprocessor.getBrpcIpAddress(instance.getWorkerId()));
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
    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return coordinatorPreprocessor.getChannelIdToBEHTTPMap();
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        return coordinatorPreprocessor.getChannelIdToBEPortMap();
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
        if (isShortCircuit) {
            return shortCircuitExecutor.getNext();
        }
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

    /**
     * Cancel execState of query. This includes the execState of the local plan fragment,
     * if any, as well as all plan fragments on remote nodes.
     */
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
                // Disable count down profileDoneSignal for collect all backend's profile
                // but if backend has crashed, we need count down profileDoneSignal since it will not report by itself
                if (message.equals(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR)) {
                    queryProfile.finishAllInstances(Status.OK);
                    LOG.info("count down profileDoneSignal since backend has crashed, query id: {}",
                            DebugUtil.printId(jobSpec.getQueryId()));
                }
            } finally {
                unlock();
            }
        }
    }

    private void cancelInternal(PPlanFragmentCancelReason cancelReason) {
        jobSpec.getSlotProvider().cancelSlotRequirement(slot);
        if (StringUtils.isEmpty(connectContext.getState().getErrorMessage())) {
            connectContext.getState().setError(cancelReason.toString());
        }
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(cancelReason);
        if (cancelReason != PPlanFragmentCancelReason.LIMIT_REACH) {
            // count down to zero to notify all objects waiting for this
            if (!connectContext.isProfileEnabled()) {
                queryProfile.finishAllInstances(Status.OK);
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
                queryProfile.finishInstance(execState.getInstanceId());
            }
        }

        executionDAG.getInstances().stream()
                .filter(instance -> executionDAG.getExecution(instance.getIndexInJob()) == null)
                .forEach(instance -> queryProfile.finishInstance(instance.getInstanceId()));
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        FragmentInstanceExecState execState = executionDAG.getExecution(params.getBackend_num());
        if (execState == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.getBackend_num(),
                    executionDAG.getExecutionIndexesInJob());
            return;
        }

        queryProfile.updateProfile(execState, params);

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
            lock();
            try {
                queryProfile.updateLoadInformation(execState, params);
            } finally {
                unlock();
            }

            queryProfile.finishInstance(params.getFragment_instance_id());
        }

        updateJobProgress(params);
    }

    @Override
    public synchronized void updateAuditStatistics(TReportAuditStatisticsParams params) {
        PQueryStatistics newAuditStatistics = AuditStatisticsUtil.toProtobuf(params.audit_statistics);
        if (auditStatistics == null) {
            auditStatistics = newAuditStatistics;
        } else {
            AuditStatisticsUtil.mergeProtobuf(newAuditStatistics, auditStatistics);
        }
    }

    private void updateJobProgress(TReportExecStatusParams params) {
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

    public void collectProfileSync() {
        if (executionDAG.getExecutions().isEmpty()) {
            return;
        }

        // wait for all backends
        if (jobSpec.isNeedReport()) {
            int timeout;
            // connectContext can be null for broker export task coordinator
            if (connectContext != null) {
                timeout = connectContext.getSessionVariable().getProfileTimeout();
            } else {
                timeout = DEFAULT_PROFILE_TIMEOUT_SECOND;
            }

            // Waiting for other fragment instances to finish execState
            // Ideally, it should wait indefinitely, but out of defense, set timeout
            queryProfile.waitForProfileFinished(timeout, TimeUnit.SECONDS);
        }

        lock();
        try {
            queryProfile.finalizeProfile();
        } finally {
            unlock();
        }
    }

    public boolean tryProcessProfileAsync(Consumer<Boolean> task) {
        if (executionDAG.getExecutions().isEmpty()) {
            return false;
        }
        if (!jobSpec.isNeedReport()) {
            return false;
        }
        boolean enableAsyncProfile = true;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            enableAsyncProfile = connectContext.getSessionVariable().isEnableAsyncProfile();
        }
        TUniqueId queryId = null;
        if (connectContext != null) {
            queryId = connectContext.getExecutionId();
        }

        if (!enableAsyncProfile || !queryProfile.addListener(task)) {
            if (enableAsyncProfile) {
                LOG.info("Profile task is full, execute in sync mode, query id = {}", DebugUtil.printId(queryId));
            }
            collectProfileSync();
            task.accept(false);
            return false;
        }
        return true;
    }

    /**
     * Waiting the coordinator finish executing.
     * return false if waiting timeout.
     * return true otherwise.
     * NOTICE: return true does not mean that coordinator executed success,
     * the caller should check queryStatus for result.
     * <p>
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
            boolean awaitRes = queryProfile.waitForProfileFinished(waitTime, TimeUnit.SECONDS);
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
    public RuntimeProfile buildQueryProfile(boolean needMerge) {
        return queryProfile.buildQueryProfile(needMerge);
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
        return queryProfile.isFinished();
    }

    @Override
    public boolean isEnableLoadProfile() {
        return connectContext != null && connectContext.getSessionVariable().isEnableLoadProfile();
    }

    /**
     * Get information of each fragment instance.
     *
     * @return the fragment instance information list, consistent with the fragment index of {@code EXPLAIN}.
     */
    @Override
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return executionDAG.getFragmentInstanceInfos();
    }

    @Override
    public PQueryStatistics getAuditStatistics() {
        return auditStatistics;
    }

    @Override
    public boolean isThriftServerHighLoad() {
        return this.thriftServerHighLoad;
    }

    @Override
    public boolean isProfileAlreadyReported() {
        return this.queryProfile.isProfileAlreadyReported();
    }

    private void execShortCircuit() {
        shortCircuitExecutor.exec();
        Optional<RuntimeProfile> runtimeProfile = shortCircuitExecutor.getRuntimeProfile();
        if (jobSpec.isNeedReport() && runtimeProfile.isPresent()) {
            RuntimeProfile profile = runtimeProfile.get();
            profile.setName("Short Circuit Executor");
            queryProfile.getQueryProfile().addChild(profile);
        }
    }
}
