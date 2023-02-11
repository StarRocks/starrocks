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
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.QueryStatisticsItem.FragmentInstanceInfo;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecBatchPlanFragmentsParams;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Coordinator {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);
    private static final int DEFAULT_PROFILE_TIMEOUT_SECOND = 2;

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    // copied from TQueryExecRequest; constant across all fragments
    private final TDescriptorTable descTable;
    // Why we use query global?
    // When `NOW()` function is in sql, we need only one now(),
    // but, we execute `NOW()` distributed.
    // So we make a query global value here to make one `now()` value in one query process.
    private final TQueryGlobals queryGlobals;
    private final TQueryOptions queryOptions;
    // protects all fields below
    private final Lock lock = new ReentrantLock();
    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;
    private RuntimeProfile queryProfile;
    private List<RuntimeProfile> fragmentProfiles;

    private final List<PlanFragment> fragments;
    // backend execute state
    private final ConcurrentNavigableMap<Integer, BackendExecState> backendExecStates = new ConcurrentSkipListMap<>();
    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private final List<BackendExecState> needCheckBackendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
    private final List<ScanNode> scanNodes;
    // number of instances of this query, equals to
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;
    private final boolean isBlockQuery;
    private int numReceivedRows = 0;
    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;
    // for export
    private List<String> exportFiles;
    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TTabletFailInfo> failInfos = Lists.newArrayList();
    // Input parameter
    private long jobId = -1; // job which this task belongs to
    private TUniqueId queryId;
    private final ConnectContext connectContext;
    private final boolean needReport;

    private final CoordinatorPreprocessor coordinatorPreprocessor;

    // Used for new planner
    public Coordinator(ConnectContext context, List<PlanFragment> fragments, List<ScanNode> scanNodes,
                       TDescriptorTable descTable) {
        this.isBlockQuery = false;
        this.queryId = context.getExecutionId();
        this.connectContext = context;
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.descTable = descTable;
        this.returnedAllResults = false;
        this.queryOptions = context.getSessionVariable().toThrift();
        long startTime = context.getStartTime();
        String timezone = context.getSessionVariable().getTimeZone();
        this.queryGlobals = CoordinatorPreprocessor.genQueryGlobals(startTime, timezone);
        if (context.getLastQueryId() != null) {
            this.queryGlobals.setLast_query_id(context.getLastQueryId().toString());
        }
        this.needReport = context.getSessionVariable().isEnableProfile();

        this.coordinatorPreprocessor =
                new CoordinatorPreprocessor(queryId, context, fragments, scanNodes, descTable, queryGlobals,
                        queryOptions);
    }

    // Used for broker export task coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
                       List<ScanNode> scanNodes, String timezone, long startTime,
                       Map<String, String> sessionVariables) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setQualifiedUser(AuthenticationManager.ROOT_USER);
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setPipelineDop(0);
        this.connectContext = connectContext;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        if (sessionVariables.containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
            final TCompressionType loadCompressionType = CompressionUtils
                    .findTCompressionByName(
                            sessionVariables.get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
            if (loadCompressionType != null) {
                this.queryOptions.setLoad_transmission_compression_type(loadCompressionType);
            }
        }
        this.queryGlobals = CoordinatorPreprocessor.genQueryGlobals(startTime, timezone);
        this.needReport = true;

        this.coordinatorPreprocessor =
                new CoordinatorPreprocessor(queryId, connectContext, fragments, scanNodes, this.descTable, queryGlobals,
                        queryOptions);
    }

    // Used for broker load task coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
                       List<ScanNode> scanNodes, String timezone, long startTime, Map<String, String> sessionVariables,
                       ConnectContext context) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.connectContext = context;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        if (sessionVariables.containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
            final TCompressionType loadCompressionType = CompressionUtils
                    .findTCompressionByName(
                            sessionVariables.get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
            if (loadCompressionType != null) {
                this.queryOptions.setLoad_transmission_compression_type(loadCompressionType);
            }
        }
        this.queryGlobals = CoordinatorPreprocessor.genQueryGlobals(startTime, timezone);
        this.needReport = true;

        this.coordinatorPreprocessor =
                new CoordinatorPreprocessor(queryId, context, fragments, scanNodes, this.descTable, queryGlobals,
                        queryOptions);
    }

    public Coordinator(LoadPlanner loadPlanner) {
        ConnectContext context = loadPlanner.getContext();
        this.isBlockQuery = true;
        this.jobId = loadPlanner.getLoadJobId();
        this.queryId = loadPlanner.getLoadId();
        this.connectContext = context;
        this.descTable = loadPlanner.getDescTable().toThrift();
        this.fragments = loadPlanner.getFragments();
        this.scanNodes = loadPlanner.getScanNodes();

        this.queryOptions = context.getSessionVariable().toThrift();
        this.queryOptions.setQuery_type(TQueryType.LOAD);
        this.queryOptions.setQuery_timeout((int) loadPlanner.getTimeout());
        this.queryOptions.setMem_limit(loadPlanner.getExecMemLimit());
        this.queryOptions.setLoad_mem_limit(loadPlanner.getLoadMemLimit());
        Map<String, String> sessionVariables = loadPlanner.getSessionVariables();
        if (sessionVariables != null) {
            if (sessionVariables.containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
                final TCompressionType loadCompressionType = CompressionUtils
                        .findTCompressionByName(
                                sessionVariables.get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
                if (loadCompressionType != null) {
                    this.queryOptions.setLoad_transmission_compression_type(loadCompressionType);
                }
            }
        }

        this.queryGlobals = CoordinatorPreprocessor.genQueryGlobals(loadPlanner.getStartTime(),
                loadPlanner.getTimeZone());
        if (context.getLastQueryId() != null) {
            this.queryGlobals.setLast_query_id(context.getLastQueryId().toString());
        }

        this.needReport = true;
        this.coordinatorPreprocessor =
                new CoordinatorPreprocessor(queryId, context, fragments, scanNodes, descTable, queryGlobals,
                        queryOptions);
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
        if (this.coordinatorPreprocessor != null) {
            this.coordinatorPreprocessor.setQueryId(queryId);
        }
    }

    public void setQueryType(TQueryType type) {
        this.queryOptions.setQuery_type(type);
    }

    public void setLoadJobType(TLoadJobType type) {
        this.queryOptions.setLoad_job_type(type);
    }

    public Status getExecStatus() {
        return queryStatus;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public List<String> getDeltaUrls() {
        return deltaUrls;
    }

    public Map<String, String> getLoadCounters() {
        return loadCounters;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public long getStartTime() {
        return this.queryGlobals.getTimestamp_ms();
    }

    public void setExecMemoryLimit(long execMemoryLimit) {
        this.queryOptions.setMem_limit(execMemoryLimit);
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.queryOptions.setLoad_mem_limit(loadMemLimit);
    }

    public void setTimeout(int timeout) {
        this.queryOptions.setQuery_timeout(timeout);
    }

    public void addReplicateScanId(Integer scanId) {
        this.coordinatorPreprocessor.getReplicateScanIds().add(scanId);
    }

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

    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }

    public List<TTabletFailInfo> getFailInfos() {
        return failInfos;
    }

    public boolean isUsingBackend(Long backendID) {
        return coordinatorPreprocessor.getUsedBackendIDs().contains(backendID);
    }

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void prepareExec() throws Exception {
        if (LOG.isDebugEnabled()) {
            if (!scanNodes.isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                        DebugUtil.printId(queryId), scanNodes.get(0).treeToThrift());
            }
            if (!fragments.isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, fragment: {}",
                        DebugUtil.printId(queryId), fragments.get(0).toThrift());
            }
            LOG.debug("debug: in Coordinator::exec. query id: {}, desc table: {}",
                    DebugUtil.printId(queryId), descTable);
        }

        coordinatorPreprocessor.prepareExec();

        // create result receiver
        prepareResultSink();

        prepareProfile();
    }

    public CoordinatorPreprocessor getPrepareInfo() {
        return coordinatorPreprocessor;
    }

    public Map<PlanFragmentId, CoordinatorPreprocessor.FragmentExecParams> getFragmentExecParamsMap() {
        return coordinatorPreprocessor.getFragmentExecParamsMap();
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public TDescriptorTable getDescTable() {
        return descTable;
    }

    public boolean isLoadType() {
        return queryOptions.getQuery_type() == TQueryType.LOAD;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public void exec() throws Exception {
        QueryQueueManager.getInstance().maybeWait(connectContext, this);
        try (PlannerProfile.ScopedTimer timer = PlannerProfile.getScopedTimer("CoordPrepareExec")) {
            prepareExec();
        }

        try (PlannerProfile.ScopedTimer timer = PlannerProfile.getScopedTimer("CoordDeliverExec")) {
            deliverExecFragments();
        }
    }

    private void prepareProfile() {
        queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));

        fragmentProfiles = new ArrayList<>();
        for (int i = 0; i < fragments.size(); i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
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
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        CoordinatorPreprocessor.FragmentExecParams topParams =
                coordinatorPreprocessor.getFragmentExecParamsMap().get(topId);
        if (topParams.fragment.getSink() instanceof ResultSink) {
            TNetworkAddress execBeAddr = topParams.instanceExecParams.get(0).host;
            receiver = new ResultReceiver(
                    topParams.instanceExecParams.get(0).instanceId,
                    coordinatorPreprocessor.getAddressToBackendID().get(execBeAddr),
                    SystemInfoService.toBrpcHost(execBeAddr),
                    queryOptions.query_timeout * 1000);

            // Select top fragment as global runtime filter merge address
            setGlobalRuntimeFilterParams(topParams, SystemInfoService.toBrpcHost(execBeAddr));

            if (LOG.isDebugEnabled()) {
                LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(queryId),
                        topParams.instanceExecParams.get(0).host);
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
            this.queryOptions.setEnable_profile(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = Lists.newArrayList(coordinatorPreprocessor.getAddressToBackendID().values());
            GlobalStateMgr.getCurrentState().getLoadManager()
                    .initJobProgress(jobId, queryId, coordinatorPreprocessor.getInstanceIds(),
                            relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId),
                    coordinatorPreprocessor.getAddressToBackendID().keySet());
        }
    }

    private void deliverExecFragments() throws Exception {
        // Only pipeline uses deliver_batch_fragments.
        boolean enableDeliverBatchFragments =
                coordinatorPreprocessor.isUsePipeline() &&
                        connectContext.getSessionVariable().isEnableDeliverBatchFragments();

        if (enableDeliverBatchFragments) {
            deliverExecBatchFragmentsRequests(coordinatorPreprocessor.isUsePipeline());
        } else {
            deliverExecFragmentRequests(coordinatorPreprocessor.isUsePipeline());
        }
    }

    private void deliverExecFragmentRequests(boolean enablePipelineEngine) throws Exception {
        long queryDeliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;
        lock();
        try {
            // execute all instances from up to bottom
            int backendId = 0;
            int profileFragmentId = 0;

            Set<Long> dbIds = connectContext != null ? connectContext.getCurrentSqlDbIds() : null;

            Set<TNetworkAddress> firstDeliveryAddresses = new HashSet<>();
            for (PlanFragment fragment : fragments) {
                CoordinatorPreprocessor.FragmentExecParams params =
                        coordinatorPreprocessor.getFragmentExecParamsMap().get(fragment.getFragmentId());

                // set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                List<List<CoordinatorPreprocessor.FInstanceExecParam>> infightExecParamList = new LinkedList<>();

                // Fragment instances' ordinals in FragmentExecParams.instanceExecParams determine
                // shuffle partitions' ordinals in DataStreamSink. backendIds of Fragment instances that
                // contain shuffle join determine the ordinals of GRF components in the GRF. For a
                // shuffle join, its shuffle partitions and corresponding one-map-one GRF components
                // should have the same ordinals. so here assign monotonic unique backendIds to
                // Fragment instances to keep consistent order with Fragment instances in
                // FragmentExecParams.instanceExecParams.
                for (CoordinatorPreprocessor.FInstanceExecParam fInstanceExecParam : params.instanceExecParams) {
                    fInstanceExecParam.backendNum = backendId++;
                }
                if (enablePipelineEngine) {
                    List<CoordinatorPreprocessor.FInstanceExecParam> firstFInstanceParamList = new ArrayList<>();
                    List<CoordinatorPreprocessor.FInstanceExecParam> remainingFInstanceParamList = new ArrayList<>();

                    for (CoordinatorPreprocessor.FInstanceExecParam fInstanceExecParam : params.instanceExecParams) {
                        if (!firstDeliveryAddresses.contains(fInstanceExecParam.host)) {
                            firstDeliveryAddresses.add(fInstanceExecParam.host);
                            firstFInstanceParamList.add(fInstanceExecParam);
                        } else {
                            remainingFInstanceParamList.add(fInstanceExecParam);
                        }
                    }
                    infightExecParamList.add(firstFInstanceParamList);
                    infightExecParamList.add(remainingFInstanceParamList);
                } else {
                    infightExecParamList.add(params.instanceExecParams);
                }

                // if pipeline is enable and current fragment contain olap table sink, in fe we will 
                // calculate the number of all tablet sinks in advance and assign them to each fragment instance
                boolean enablePipelineTableSinkDop = enablePipelineEngine && fragment.hasOlapTableSink();
                boolean forceSetTableSinkDop = fragment.forceSetTableSinkDop();
                int tabletSinkTotalDop = 0;
                int accTabletSinkDop = 0;
                if (enablePipelineTableSinkDop) {
                    for (List<CoordinatorPreprocessor.FInstanceExecParam> fInstanceExecParamList : infightExecParamList) {
                        for (CoordinatorPreprocessor.FInstanceExecParam instanceExecParam : fInstanceExecParamList) {
                            if (!forceSetTableSinkDop) {
                                tabletSinkTotalDop += instanceExecParam.getPipelineDop();
                            } else {
                                tabletSinkTotalDop += fragment.getPipelineDop();
                            }
                        }
                    }
                }

                if (tabletSinkTotalDop < 0) {
                    throw new UserException(
                            "tabletSinkTotalDop = " + tabletSinkTotalDop + " should be >= 0");
                }

                boolean isFirst = true;
                for (List<CoordinatorPreprocessor.FInstanceExecParam> fInstanceExecParamList : infightExecParamList) {
                    TDescriptorTable descTable = new TDescriptorTable();
                    descTable.setIs_cached(true);
                    descTable.setTupleDescriptors(Collections.emptyList());
                    if (isFirst) {
                        descTable = this.descTable;
                        descTable.setIs_cached(false);
                        isFirst = false;
                    }

                    if (fInstanceExecParamList.isEmpty()) {
                        continue;
                    }

                    Map<TUniqueId, TNetworkAddress> instanceId2Host =
                            fInstanceExecParamList.stream().collect(Collectors.toMap(f -> f.instanceId, f -> f.host));
                    List<TExecPlanFragmentParams> tParams =
                            params.toThrift(instanceId2Host.keySet(), descTable, enablePipelineEngine,
                                    accTabletSinkDop, tabletSinkTotalDop, false);
                    if (enablePipelineTableSinkDop) {
                        for (CoordinatorPreprocessor.FInstanceExecParam instanceExecParam : fInstanceExecParamList) {
                            if (!forceSetTableSinkDop) {
                                accTabletSinkDop += instanceExecParam.getPipelineDop();
                            } else {
                                accTabletSinkDop += fragment.getPipelineDop();
                            }
                        }
                    }
                    List<Pair<BackendExecState, Future<PExecPlanFragmentResult>>> futures = Lists.newArrayList();

                    // This is a load process, and it is the first fragment.
                    // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                    // so that we can check these backends' state when joining this Coordinator
                    boolean needCheckBackendState = isLoadType() && profileFragmentId == 0;

                    for (TExecPlanFragmentParams tParam : tParams) {
                        // TODO: pool of pre-formatted BackendExecStates?
                        TNetworkAddress host = instanceId2Host.get(tParam.params.fragment_instance_id);
                        BackendExecState execState = new BackendExecState(fragment.getFragmentId(), host,
                                profileFragmentId, tParam, coordinatorPreprocessor.getAddressToBackendID());
                        backendExecStates.put(tParam.backend_num, execState);
                        if (needCheckBackendState) {
                            needCheckBackendExecStates.add(execState);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("add need check backend {} for fragment, {} job: {}",
                                        execState.backend.getId(),
                                        fragment.getFragmentId().asInt(), jobId);
                            }
                        }
                        futures.add(Pair.create(execState, execState.execRemoteFragmentAsync()));
                    }
                    for (Pair<BackendExecState, Future<PExecPlanFragmentResult>> pair : futures) {
                        TStatusCode code;
                        String errMsg = null;
                        try {
                            PExecPlanFragmentResult result =
                                    pair.second.get(queryDeliveryTimeoutMs, TimeUnit.MILLISECONDS);
                            code = TStatusCode.findByValue(result.status.statusCode);
                            if (result.status.errorMsgs != null && !result.status.errorMsgs.isEmpty()) {
                                errMsg = result.status.errorMsgs.get(0);
                            }
                        } catch (ExecutionException e) {
                            LOG.warn("catch a execute exception", e);
                            code = TStatusCode.THRIFT_RPC_ERROR;
                        } catch (InterruptedException e) {
                            LOG.warn("catch a interrupt exception", e);
                            code = TStatusCode.INTERNAL_ERROR;
                        } catch (TimeoutException e) {
                            LOG.warn("catch a timeout exception", e);
                            code = TStatusCode.TIMEOUT;
                        }

                        if (code != TStatusCode.OK) {
                            if (errMsg == null) {
                                errMsg = "exec rpc error. backend id: " + pair.first.backend.getId();
                            }
                            queryStatus.setStatus(errMsg);
                            LOG.warn("exec plan fragment failed, errmsg={}, code: {}, fragmentId={}, backend={}:{}",
                                    errMsg, code, fragment.getFragmentId(),
                                    pair.first.address.hostname, pair.first.address.port);
                            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
                            switch (Objects.requireNonNull(code)) {
                                case TIMEOUT:
                                    throw new UserException("query timeout. backend id: " + pair.first.backend.getId());
                                case THRIFT_RPC_ERROR:
                                    SimpleScheduler.addToBlacklist(pair.first.backend.getId());
                                    throw new RpcException(pair.first.backend.getHost(), "rpc failed");
                                default:
                                    throw new UserException(errMsg);
                            }
                        }
                    }
                }
                profileFragmentId += 1;
            }
            attachInstanceProfileToFragmentProfile();
        } finally {
            unlock();
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

        PlanFragment root = fragments.get(0);

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

        if (fragments.size() != inDegrees.size()) {
            for (PlanFragment fragment : fragments) {
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

        if (fragments.size() != numOutputFragments) {
            throw new StarRocksPlannerException("There are some circles in the fragment tree",
                    ErrorType.INTERNAL_ERROR);
        }

        return groups;
    }

    /**
     * Deliver multiple fragments concurrently according to the topological order,
     * and all the instances of a fragment to the same destination host are delivered in the same request.
     */
    private void deliverExecBatchFragmentsRequests(boolean enablePipelineEngine) throws Exception {
        long queryDeliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;
        List<List<PlanFragment>> fragmentGroups = computeTopologicalOrderFragments();

        lock();
        try {
            // execute all instances from up to bottom
            int backendNum = 0;
            int profileFragmentId = 0;

            Set<Long> dbIds = connectContext != null ? connectContext.getCurrentSqlDbIds() : null;

            this.descTable.setIs_cached(false);
            TDescriptorTable emptyDescTable = new TDescriptorTable();
            emptyDescTable.setIs_cached(true);
            emptyDescTable.setTupleDescriptors(Collections.emptyList());

            // Record the first groupIndex of each host.
            // Each host only sends descTable once in the first batch request.
            Map<TNetworkAddress, Integer> host2firstGroupIndex = Maps.newHashMap();
            for (int groupIndex = 0; groupIndex < fragmentGroups.size(); ++groupIndex) {
                List<PlanFragment> fragmentGroup = fragmentGroups.get(groupIndex);

                // Divide requests of fragments in the current group to two stages.
                // If a request need send descTable, the other requests to the same host will be in the second stage.
                // Otherwise, the request will be in the first stage, including
                // - the request need send descTable.
                // - the request to the host, where some request in the previous group has already sent descTable.
                List<List<Pair<List<BackendExecState>, TExecBatchPlanFragmentsParams>>> inflightRequestsList =
                        ImmutableList.of(new ArrayList<>(), new ArrayList<>());
                for (PlanFragment fragment : fragmentGroup) {
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

                    Map<TNetworkAddress, List<CoordinatorPreprocessor.FInstanceExecParam>> requestsPerHost =
                            params.instanceExecParams.stream()
                                    .collect(Collectors.groupingBy(CoordinatorPreprocessor.FInstanceExecParam::getHost,
                                            HashMap::new,
                                            Collectors.mapping(Function.identity(), Collectors.toList())));
                    // if pipeline is enable and current fragment contain olap table sink, in fe we will 
                    // calculate the number of all tablet sinks in advance and assign them to each fragment instance
                    boolean enablePipelineTableSinkDop = enablePipelineEngine && fragment.hasOlapTableSink();
                    boolean forceSetTableSinkDop = fragment.forceSetTableSinkDop();
                    int tabletSinkTotalDop = 0;
                    int accTabletSinkDop = 0;
                    if (enablePipelineTableSinkDop) {
                        for (Map.Entry<TNetworkAddress, List<CoordinatorPreprocessor.FInstanceExecParam>> hostAndRequests :
                                requestsPerHost.entrySet()) {
                            List<CoordinatorPreprocessor.FInstanceExecParam> requests = hostAndRequests.getValue();
                            for (CoordinatorPreprocessor.FInstanceExecParam request : requests) {
                                if (!forceSetTableSinkDop) {
                                    tabletSinkTotalDop += request.getPipelineDop();
                                } else {
                                    tabletSinkTotalDop += fragment.getPipelineDop();
                                }
                            }
                        }
                    }

                    if (tabletSinkTotalDop < 0) {
                        throw new UserException(
                                "tabletSinkTotalDop = " + tabletSinkTotalDop + " should be >= 0");
                    }

                    for (Map.Entry<TNetworkAddress, List<CoordinatorPreprocessor.FInstanceExecParam>> hostAndRequests :
                            requestsPerHost.entrySet()) {
                        TNetworkAddress host = hostAndRequests.getKey();
                        List<CoordinatorPreprocessor.FInstanceExecParam> requests = hostAndRequests.getValue();
                        if (requests.isEmpty()) {
                            continue;
                        }

                        int inflightIndex = 0;
                        TDescriptorTable curDescTable = this.descTable;
                        if (enablePipelineEngine) {
                            Integer firstGroupIndex = host2firstGroupIndex.get(host);
                            if (firstGroupIndex == null) {
                                // Hasn't sent descTable for this host,
                                // so send descTable this time.
                                host2firstGroupIndex.put(host, groupIndex);
                            } else if (firstGroupIndex < groupIndex) {
                                // Has sent descTable for this host in the previous fragment group,
                                // so needn't wait and use cached descTable.
                                curDescTable = emptyDescTable;
                            } else {
                                // The previous fragment for this host int the current fragment group will send descTable,
                                // so this fragment need wait until the previous one finishes delivering.
                                inflightIndex = 1;
                                curDescTable = emptyDescTable;
                            }
                        }

                        Set<TUniqueId> curInstanceIds = requests.stream()
                                .map(CoordinatorPreprocessor.FInstanceExecParam::getInstanceId)
                                .collect(Collectors.toSet());
                        TExecBatchPlanFragmentsParams tRequest =
                                params.toThriftInBatch(curInstanceIds, host, curDescTable, enablePipelineEngine,
                                        accTabletSinkDop, tabletSinkTotalDop);
                        if (enablePipelineTableSinkDop) {
                            for (CoordinatorPreprocessor.FInstanceExecParam request : requests) {
                                if (!forceSetTableSinkDop) {
                                    accTabletSinkDop += request.getPipelineDop();
                                } else {
                                    accTabletSinkDop += fragment.getPipelineDop();
                                }
                            }
                        }
                        TExecPlanFragmentParams tCommonParams = tRequest.getCommon_param();
                        List<TExecPlanFragmentParams> tUniqueParamsList = tRequest.getUnique_param_per_instance();
                        Preconditions.checkState(!tUniqueParamsList.isEmpty());

                        // this is a load process, and it is the first fragment.
                        // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                        // so that we can check these backends' state when joining this Coordinator
                        boolean needCheckBackendState = isLoadType() && profileFragmentId == 0;

                        // Create ExecState for each fragment instance.
                        List<BackendExecState> execStates = Lists.newArrayList();
                        for (TExecPlanFragmentParams tUniquePrams : tUniqueParamsList) {
                            // TODO: pool of pre-formatted BackendExecStates?
                            BackendExecState execState = new BackendExecState(fragment.getFragmentId(), host,
                                    profileFragmentId, tCommonParams, tUniquePrams,
                                    coordinatorPreprocessor.getAddressToBackendID());
                            execStates.add(execState);
                            backendExecStates.put(tUniquePrams.backend_num, execState);
                            if (needCheckBackendState) {
                                needCheckBackendExecStates.add(execState);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("add need check backend {} for fragment, {} job: {}",
                                            execState.backend.getId(),
                                            fragment.getFragmentId().asInt(), jobId);
                                }
                            }
                        }

                        inflightRequestsList.get(inflightIndex).add(Pair.create(execStates, tRequest));
                    }

                    profileFragmentId += 1;
                }

                for (List<Pair<List<BackendExecState>, TExecBatchPlanFragmentsParams>> inflightRequests :
                        inflightRequestsList) {
                    List<Pair<BackendExecState, Future<PExecBatchPlanFragmentsResult>>> futures = Lists.newArrayList();
                    for (Pair<List<BackendExecState>, TExecBatchPlanFragmentsParams> inflightRequest : inflightRequests) {
                        List<BackendExecState> execStates = inflightRequest.first;
                        execStates.forEach(execState -> execState.setInitiated(true));

                        Preconditions.checkState(!execStates.isEmpty());
                        // Just choose any instance ExecState to send the batch RPC request.
                        BackendExecState firstExecState = execStates.get(0);
                        futures.add(Pair.create(firstExecState,
                                firstExecState.execRemoteBatchFragmentsAsync(inflightRequest.second)));
                    }

                    for (Pair<BackendExecState, Future<PExecBatchPlanFragmentsResult>> pair : futures) {
                        TStatusCode code;
                        String errMsg = null;
                        try {
                            PExecBatchPlanFragmentsResult result =
                                    pair.second.get(queryDeliveryTimeoutMs, TimeUnit.MILLISECONDS);
                            code = TStatusCode.findByValue(result.status.statusCode);
                            if (result.status.errorMsgs != null && !result.status.errorMsgs.isEmpty()) {
                                errMsg = result.status.errorMsgs.get(0);
                            }
                        } catch (ExecutionException e) {
                            LOG.warn("catch a execute exception", e);
                            code = TStatusCode.THRIFT_RPC_ERROR;
                        } catch (InterruptedException e) {
                            LOG.warn("catch a interrupt exception", e);
                            code = TStatusCode.INTERNAL_ERROR;
                        } catch (TimeoutException e) {
                            LOG.warn("catch a timeout exception", e);
                            code = TStatusCode.TIMEOUT;
                        }

                        if (code != TStatusCode.OK) {
                            if (errMsg == null) {
                                errMsg = "exec rpc error. backend id: " + pair.first.backend.getId();
                            }
                            queryStatus.setStatus(errMsg + " backend:" + pair.first.address.hostname);
                            LOG.warn("exec plan fragment failed, errmsg={}, code: {}, fragmentId={}, backend={}:{}",
                                    errMsg, code, pair.first.fragmentId,
                                    pair.first.address.hostname, pair.first.address.port);
                            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
                            switch (Objects.requireNonNull(code)) {
                                case TIMEOUT:
                                    throw new UserException("query timeout. backend id: " + pair.first.backend.getId());
                                case THRIFT_RPC_ERROR:
                                    SimpleScheduler.addToBlacklist(pair.first.backend.getId());
                                    throw new RpcException(pair.first.backend.getHost(), "rpc failed");
                                default:
                                    throw new UserException(errMsg);
                            }
                        }
                    }
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

        Map<TNetworkAddress, List<CoordinatorPreprocessor.FInstanceExecParam>> host2instances = Maps.newHashMap();
        for (CoordinatorPreprocessor.FInstanceExecParam instance : instances) {
            host2instances.putIfAbsent(instance.host, Lists.newLinkedList());
            host2instances.get(instance.host).add(instance);
        }
        List<CoordinatorPreprocessor.FInstanceExecParam> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<CoordinatorPreprocessor.FInstanceExecParam> instancesPerHost : host2instances.values()) {
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

        for (PlanFragment fragment : fragments) {
            fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            fragment.collectProbeRuntimeFilters(fragment.getPlanRoot());
            CoordinatorPreprocessor.FragmentExecParams params =
                    coordinatorPreprocessor.getFragmentExecParamsMap().get(fragment.getFragmentId());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = Lists.newArrayList();
                for (final CoordinatorPreprocessor.FInstanceExecParam instance : params.instanceExecParams) {
                    TRuntimeFilterProberParams probeParam = new TRuntimeFilterProberParams();
                    probeParam.setFragment_instance_id(instance.instanceId);
                    probeParam.setFragment_instance_address(SystemInfoService.toBrpcHost(instance.host));
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

    public List<String> getExportFiles() {
        return exportFiles;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return coordinatorPreprocessor.getChannelIdToBEHTTPMap();
    }

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

            this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "" + numRowsNormal);
            this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "" + numRowsAbnormal);
            this.loadCounters.put(LoadJob.UNSELECTED_ROWS, "" + numRowsUnselected);
            this.loadCounters.put(LoadJob.LOADED_BYTES, "" + numLoadBytesTotal);
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
                    jobId, DebugUtil.printId(queryId), instanceId != null ? DebugUtil.printId(instanceId) : "NaN");
            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
        } finally {
            lock.unlock();
        }
    }

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
                    DebugUtil.printId(queryId));
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
            long numLimitRows = fragments.get(0).getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!isBlockQuery && coordinatorPreprocessor.getInstanceIds().size() > 1 && hasLimit &&
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
    public void cancel(PPlanFragmentCancelReason cancelReason, String cancelledMessage) {
        lock();
        try {
            if (!queryStatus.ok()) {
                // we can't cancel twice
                return;
            } else {
                queryStatus.setStatus(Status.CANCELLED);
                queryStatus.setErrorMsg(cancelledMessage);
            }
            LOG.warn("cancel execution of query, this is outside invoke");
            cancelInternal(cancelReason);
        } finally {
            unlock();
        }
    }

    public void cancel() {
        cancel(PPlanFragmentCancelReason.USER_CANCEL, "");
    }

    private void cancelInternal(PPlanFragmentCancelReason cancelReason) {
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(cancelReason);
        if (profileDoneSignal != null && cancelReason != PPlanFragmentCancelReason.LIMIT_REACH) {
            // count down to zero to notify all objects waiting for this
            profileDoneSignal.countDownToZero(new Status());
            LOG.info("unfinished instance: {}",
                    profileDoneSignal.getLeftMarks().stream().map(e -> DebugUtil.printId(e.getKey())).toArray());
        }
    }

    private void cancelRemoteFragmentsAsync(PPlanFragmentCancelReason cancelReason) {
        for (BackendExecState backendExecState : backendExecStates.values()) {
            backendExecState.cancelFragmentInstance(cancelReason);
        }
    }

    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        BackendExecState execState = backendExecStates.get(params.backend_num);
        if (execState == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.backend_num,
                    backendExecStates.keySet());
            return;
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
                    DebugUtil.printId(queryId),
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
            LOG.warn("one instance report fail {}, params={} query_id={} instance_id={}",
                    status, params, DebugUtil.printId(queryId),
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
            profileDoneSignal.markedCountDown(params.getFragment_instance_id(), -1L);
        }

        if (params.isSetLoad_type()) {
            TLoadJobType loadJobType = params.getLoad_type();
            if (loadJobType == TLoadJobType.BROKER ||
                    loadJobType == TLoadJobType.INSERT_QUERY ||
                    loadJobType == TLoadJobType.INSERT_VALUES) {
                if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                        && params.isSetSource_load_bytes()) {
                    GlobalStateMgr.getCurrentState().getLoadManager().updateJobPrgress(
                            jobId, params.backend_id, params.query_id, params.fragment_instance_id, params.loaded_rows,
                            params.sink_load_bytes, params.source_load_rows, params.source_load_bytes, params.done);
                }
            }
        } else {
            if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                    && params.isSetSource_load_bytes()) {
                GlobalStateMgr.getCurrentState().getLoadManager().updateJobPrgress(
                        jobId, params.backend_id, params.query_id, params.fragment_instance_id, params.loaded_rows,
                        params.sink_load_bytes, params.source_load_rows, params.source_load_bytes, params.done);
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
            } catch (InterruptedException e) {
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
     * with a maximum of 30 seconds per round. And after each round of waiting,
     * check the status of the BE. If the BE status is abnormal, the wait is ended
     * and the result is returned. Otherwise, continue to the next round of waiting.
     * This method mainly avoids the problem that the Coordinator waits for a long time
     * after some BE can no long return the result due to some exception, such as BE is down.
     */
    public boolean join(int timeoutS) {
        final long fixedMaxWaitTime = 30;

        long leftTimeoutS = timeoutS;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            boolean awaitRes = false;
            try {
                awaitRes = profileDoneSignal.await(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Do nothing
            }
            if (awaitRes) {
                return true;
            }

            if (!checkBackendState()) {
                return true;
            }

            leftTimeoutS -= waitTime;
        }
        return false;
    }

    public void mergeIsomorphicProfiles(PQueryStatistics statistics) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();

        if (!sessionVariable.isEnableProfile()) {
            return;
        }

        if (!coordinatorPreprocessor.isUsePipeline()) {
            return;
        }

        int profileLevel = sessionVariable.getPipelineProfileLevel();
        if (profileLevel >= TPipelineProfileLevel.DETAIL.getValue()) {
            return;
        }

        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            if (fragmentProfile.getChildList().isEmpty()) {
                continue;
            }

            RuntimeProfile instanceProfile0 = fragmentProfile.getChildList().get(0).first;
            if (instanceProfile0.getChildList().isEmpty()) {
                continue;
            }
            RuntimeProfile pipelineProfile0 = instanceProfile0.getChildList().get(0).first;

            // pipeline engine must have a counter named DegreeOfParallelism
            // some fragment may still execute in non-pipeline mode
            if (pipelineProfile0.getCounter("DegreeOfParallelism") == null) {
                continue;
            }

            List<RuntimeProfile> instanceProfiles = fragmentProfile.getChildList().stream()
                    .map(pair -> pair.first)
                    .collect(Collectors.toList());

            // Setup backend address infos
            Set<String> backendAddresses = Sets.newHashSet();
            instanceProfiles.forEach(instanceProfile -> {
                backendAddresses.add(instanceProfile.getInfoString("Address"));
                instanceProfile.removeInfoString("Address");
            });
            fragmentProfile.addInfoString("BackendAddresses", String.join(",", backendAddresses));

            // Setup number of instance
            Counter counter = fragmentProfile.addCounter("InstanceNum", TUnit.UNIT, null);
            counter.setValue(instanceProfiles.size());

            // After merge, all merged metrics will gather into the first profile
            // which is instanceProfile0
            RuntimeProfile.mergeIsomorphicProfiles(instanceProfiles);

            fragmentProfile.copyAllInfoStringsFrom(instanceProfile0);
            fragmentProfile.copyAllCountersFrom(instanceProfile0);

            // Remove the instance profile from the hierarchy
            fragmentProfile.removeAllChildren();
            instanceProfile0.getChildList().forEach(pair -> {
                RuntimeProfile pipelineProfile = pair.first;
                foldUnnecessaryLimitOperators(pipelineProfile);
                fragmentProfile.addChild(pipelineProfile);
            });
        }

        // Remove redundant MIN/MAX metrics if MIN and MAX are identical
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            RuntimeProfile.removeRedundantMinMaxMetrics(fragmentProfile);
        }

        // Set backend number
        for (int i = 0; i < fragments.size(); i++) {
            PlanFragment fragment = fragments.get(i);
            RuntimeProfile profile = fragmentProfiles.get(i);

            Set<TNetworkAddress> networkAddresses =
                    coordinatorPreprocessor.getFragmentExecParamsMap()
                            .get(fragment.getFragmentId()).instanceExecParams.stream()
                            .map(param -> param.host)
                            .collect(Collectors.toSet());

            Counter backendNum = profile.addCounter("BackendNum", TUnit.UNIT, null);
            backendNum.setValue(networkAddresses.size());
        }

        long queryAllocatedMemoryUsage = 0;
        long queryDeallocatedMemoryUsage = 0;
        // Calculate ExecutionTotalTime, which comprising all operator's sync time and async time
        // We can get Operator's sync time from OperatorTotalTime, and for async time, only ScanOperator and
        // ExchangeOperator have async operations, we can get async time from ScanTime(for ScanOperator) and
        // NetworkTime(for ExchangeOperator)
        long queryCumulativeOperatorTime = 0;
        boolean foundResultSink = false;
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
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
                    if (!foundResultSink & operatorProfile.getName().contains("RESULT_SINK")) {
                        long executionWallTime = pipelineProfile.getCounter("DriverTotalTime").getValue();
                        Counter executionTotalTime =
                                queryProfile.addCounter("QueryExecutionWallTime", TUnit.TIME_NS, null);
                        queryProfile.getCounterTotalTime().setValue(0);
                        executionTotalTime.setValue(executionWallTime);
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
                queryProfile.addCounter("QueryAllocatedMemoryUsage", TUnit.BYTES, null);
        queryAllocatedMemoryUsageCounter.setValue(queryAllocatedMemoryUsage);
        Counter queryDeallocatedMemoryUsageCounter =
                queryProfile.addCounter("QueryDeallocatedMemoryUsage", TUnit.BYTES, null);
        queryDeallocatedMemoryUsageCounter.setValue(queryDeallocatedMemoryUsage);
        Counter queryCumulativeOperatorTimer =
                queryProfile.addCounter("QueryCumulativeOperatorTime", TUnit.TIME_NS, null);
        queryCumulativeOperatorTimer.setValue(queryCumulativeOperatorTime);
        queryProfile.getCounterTotalTime().setValue(0);

        Counter queryCumulativeCpuTime = queryProfile.addCounter("QueryCumulativeCpuTime", TUnit.TIME_NS, null);
        queryCumulativeCpuTime.setValue(statistics == null || statistics.cpuCostNs == null ? 0 : statistics.cpuCostNs);
        Counter queryPeakMemoryUsage = queryProfile.addCounter("QueryPeakMemoryUsage", TUnit.BYTES, null);
        queryPeakMemoryUsage.setValue(
                statistics == null || statistics.memCostBytes == null ? 0 : statistics.memCostBytes);
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
                Preconditions.checkNotNull(pullRowNum);
                Preconditions.checkNotNull(pushRowNum);
                if (Objects.equals(pullRowNum.getValue(), pushRowNum.getValue())) {
                    foldNames.add(operatorProfile.getName());
                }
            }
        }

        foldNames.forEach(pipelineProfile::removeChild);
    }

    /*
     * Check the state of backends in needCheckBackendExecStates.
     * return true if all of them are OK. Otherwise, return false.
     */
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

    public boolean isDone() {
        return profileDoneSignal.getCount() == 0;
    }

    // consistent with EXPLAIN's fragment index
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        final List<QueryStatisticsItem.FragmentInstanceInfo> result =
                Lists.newArrayList();
        for (int index = 0; index < fragments.size(); index++) {
            for (BackendExecState backendExecState : backendExecStates.values()) {
                if (fragments.get(index).getFragmentId() != backendExecState.fragmentId) {
                    continue;
                }
                final QueryStatisticsItem.FragmentInstanceInfo info = backendExecState.buildFragmentInstanceInfo();
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
        ComputeNode backend;
        long lastMissingHeartbeatTime = -1;

        public BackendExecState(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                TExecPlanFragmentParams rpcParams,
                                Map<TNetworkAddress, Long> addressToBackendID) {
            this(fragmentId, host, profileFragmentId, rpcParams, rpcParams, addressToBackendID);
        }

        public BackendExecState(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                TExecPlanFragmentParams commonRpcParams, TExecPlanFragmentParams uniqueRpcParams,
                                Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.commonRpcParams = commonRpcParams;
            this.uniqueRpcParams = uniqueRpcParams;
            this.initiated = false;
            this.done = false;
            this.address = host;
            this.backend = coordinatorPreprocessor.getIdToBackend().get(addressToBackendID.get(address));
            // if useComputeNode and it's olapScan now, backend is null ,need get from olapScanNodeIdToComputeNode
            if (backend == null) {
                backend = coordinatorPreprocessor.getIdToComputeNode().get(addressToBackendID.get(address));
            }
            String name =
                    "Instance " + DebugUtil.printId(uniqueRpcParams.params.fragment_instance_id) + " (host=" + address +
                            ")";
            this.profile = new RuntimeProfile(name);
            this.profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
            this.hasCanceled = false;
            this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
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

                TNetworkAddress brpcAddress = SystemInfoService.toBrpcHost(address);

                try {
                    BackendServiceClient.getInstance().cancelPlanFragmentAsync(brpcAddress,
                            queryId, fragmentInstanceId(), cancelReason, commonRpcParams.is_pipeline);
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(coordinatorPreprocessor.getAddressToBackendID().get(brpcAddress));
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
                LOG.warn("backend {} is down while joining the coordinator. job id: {}", backend.getId(), jobId);
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

        public Future<PExecBatchPlanFragmentsResult> execRemoteBatchFragmentsAsync(
                TExecBatchPlanFragmentsParams tRequest) throws TException {
            TNetworkAddress brpcAddress;
            try {
                brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            } catch (Exception e) {
                throw new TException(e.getMessage());
            }
            this.initiated = true;
            try {
                return BackendServiceClient.getInstance().execBatchPlanFragmentsAsync(brpcAddress, tRequest);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return new Future<PExecBatchPlanFragmentsResult>() {
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
                    public PExecBatchPlanFragmentsResult get() {
                        PExecBatchPlanFragmentsResult result = new PExecBatchPlanFragmentsResult();
                        StatusPB pStatus = new StatusPB();
                        pStatus.errorMsgs = Lists.newArrayList();
                        pStatus.errorMsgs.add(e.getMessage());
                        // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                        pStatus.statusCode = TStatusCode.THRIFT_RPC_ERROR.getValue();
                        result.status = pStatus;
                        return result;
                    }

                    @Override
                    public PExecBatchPlanFragmentsResult get(long timeout, TimeUnit unit) {
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

        public void setInitiated(boolean initiated) {
            this.initiated = initiated;
        }
    }
}
