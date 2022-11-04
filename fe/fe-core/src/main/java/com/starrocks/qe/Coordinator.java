// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.WorkGroup;
import com.starrocks.catalog.WorkGroupClassifier;
import com.starrocks.common.Config;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ListUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.MultiCastDataSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.QueryStatisticsItem.FragmentInstanceInfo;
import com.starrocks.rpc.BackendServiceProxy;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.Backend;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TEsScanRange;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterParams;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Coordinator {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String localIP = FrontendOptions.getLocalHostAddress();

    // Random is used to shuffle instances of partitioned
    private static final Random instanceRandom = new Random();
    // parallel execute
    private final TUniqueId nextInstanceId;
    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();
    // save of related backends of this query
    Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();
    private ImmutableMap<Long, Backend> idToBackend = ImmutableMap.of();
    private final Set<Long> usedBackendIDs = Sets.newConcurrentHashSet();
    // copied from TQueryExecRequest; constant across all fragments
    private final TDescriptorTable descTable;
    // Why we use query global?
    // When `NOW()` function is in sql, we need only one now(),
    // but, we execute `NOW()` distributed.
    // So we make a query global value here to make one `now()` value in one query process.
    private final TQueryGlobals queryGlobals = new TQueryGlobals();
    private final TQueryOptions queryOptions;
    private TNetworkAddress coordAddress;
    // protects all fields below
    private final Lock lock = new ReentrantLock();
    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;
    private RuntimeProfile queryProfile;
    private List<RuntimeProfile> fragmentProfiles;
    // populated in computeFragmentExecParams()
    private final Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();
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
    private final Set<TUniqueId> instanceIds = Sets.newHashSet();
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
    // Input parameter
    private long jobId = -1; // job which this task belongs to
    private TUniqueId queryId;
    private final ConnectContext connectContext;
    private final boolean needReport;
    private final String clusterName;
    // force schedule local be for HybridBackendSelector
    // only for hive now
    private boolean forceScheduleLocal = false;
    private final Set<Integer> colocateFragmentIds = new HashSet<>();
    private final Set<Integer> replicateFragmentIds = new HashSet<>();
    private final Set<Integer> replicateScanIds = new HashSet<>();
    private final Set<Integer> bucketShuffleFragmentIds = new HashSet<>();
    private final Set<Integer> rightOrFullBucketShuffleFragmentIds = new HashSet<>();

    private final boolean usePipeline;

    // Resource group
    WorkGroup workGroup = null;

    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
    // fragment_id -> < bucket_seq -> < scannode_id -> scan_range_params >>
    private final Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = Maps.newHashMap();
    // fragment_id -> bucket_num
    private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap = Maps.newHashMap();
    // fragment_id -> < be_id -> bucket_count >
    private final Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBackendIdBucketCountMap = Maps.newHashMap();

    private final Map<PlanFragmentId, List<Integer>> fragmentIdToSeqToInstanceMap = Maps.newHashMap();

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
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            this.queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            this.queryGlobals.setTime_zone(context.getSessionVariable().getTimeZone());
        }
        String nowString =
                DATE_FORMAT.format(Instant.ofEpochMilli(startTime).atZone(ZoneId.of(queryGlobals.time_zone)));
        this.queryGlobals.setNow_string(nowString);
        this.queryGlobals.setTimestamp_ms(startTime);
        if (context.getLastQueryId() != null) {
            this.queryGlobals.setLast_query_id(context.getLastQueryId().toString());
        }
        this.needReport = context.getSessionVariable().isReportSucc();
        this.clusterName = context.getClusterName();
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
        this.forceScheduleLocal = context.getSessionVariable().isForceScheduleLocal();

        this.usePipeline = canUsePipeline(this.connectContext, this.fragments);
    }

    // Used for broker load task/export task coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
                       List<ScanNode> scanNodes, String cluster, String timezone, long startTime) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.connectContext = null;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        String nowString = DATE_FORMAT.format(Instant.ofEpochMilli(startTime).atZone(ZoneId.of(timezone)));
        this.queryGlobals.setNow_string(nowString);
        this.queryGlobals.setTimestamp_ms(startTime);
        this.queryGlobals.setTime_zone(timezone);
        this.needReport = true;
        this.clusterName = cluster;
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);

        this.usePipeline = canUsePipeline(this.connectContext, this.fragments);
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public void setQueryType(TQueryType type) {
        this.queryOptions.setQuery_type(type);
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

    public void setExecMemoryLimit(long execMemoryLimit) {
        this.queryOptions.setMem_limit(execMemoryLimit);
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.queryOptions.setLoad_mem_limit(loadMemLimit);
    }

    public void setTimeout(int timeout) {
        this.queryOptions.setQuery_timeout(timeout);
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

    public boolean isUsingBackend(Long backendID) {
        return usedBackendIDs.contains(backendID);
    }

    // Initialize
    private void prepare() {
        for (PlanFragment fragment : fragments) {
            fragmentExecParamsMap.put(fragment.getFragmentId(), new FragmentExecParams(fragment));
        }

        coordAddress = new TNetworkAddress(localIP, Config.rpc_port);

        int fragmentSize = fragments.size();
        queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));

        fragmentProfiles = new ArrayList<>();
        for (int i = 0; i < fragmentSize; i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
            queryProfile.addChild(fragmentProfiles.get(i));
        }

        this.idToBackend = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();
        if (LOG.isDebugEnabled()) {
            LOG.debug("idToBackend size={}", idToBackend.size());
            for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
                Long backendID = entry.getKey();
                Backend backend = entry.getValue();
                LOG.debug("backend: {}-{}-{}", backendID, backend.getHost(), backend.getBePort());
            }
        }
    }

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    private void traceInstance() {
        if (LOG.isDebugEnabled()) {
            // TODO(zc): add a switch to close this function
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            sb.append("query id=").append(DebugUtil.printId(queryId)).append(",");
            sb.append("fragment=[");
            for (Map.Entry<PlanFragmentId, FragmentExecParams> entry : fragmentExecParamsMap.entrySet()) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(entry.getKey());
                entry.getValue().appendTo(sb);
            }
            sb.append("]");
            LOG.debug(sb.toString());
        }
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void exec() throws Exception {
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

        // prepare information
        prepare();

        // prepare workgroup
        this.workGroup = prepareWorkGroup(connectContext);

        // compute Fragment Instance
        computeScanRangeAssignment();

        computeFragmentExecParams();

        traceInstance();

        prepareResultSink();
    }

    public static WorkGroup prepareWorkGroup(ConnectContext connect) {
        WorkGroup workgroup = null;
        if (connect == null || !connect.getSessionVariable().isEnableResourceGroup()) {
            return workgroup;
        }
        SessionVariable sessionVariable = connect.getSessionVariable();

        // 1. try to use the resource group specified by the variable
        if (StringUtils.isNotEmpty(sessionVariable.getResourceGroup())) {
            String rgName = sessionVariable.getResourceGroup();
            workgroup = GlobalStateMgr.getCurrentState().getWorkGroupMgr().chooseWorkGroupByName(rgName);
        }

        // 2. try to use the resource group specified by workgroup_id
        long workgroupId = connect.getSessionVariable().getWorkGroupId();
        if (workgroup == null && workgroupId > 0) {
            workgroup = new WorkGroup();
            workgroup.setId(workgroupId);
        }

        // 3. if the specified resource group not exist try to use the default one
        if (workgroup == null) {
            Set<Long> dbIds = connect.getCurrentSqlDbIds();
            workgroup = GlobalStateMgr.getCurrentState().getWorkGroupMgr().chooseWorkGroup(
                    connect, WorkGroupClassifier.QueryType.SELECT, dbIds);
        }

        if (workgroup != null) {
            connect.getAuditEventBuilder().setResourceGroup(workgroup.getName());
            connect.setWorkGroup(workgroup);
        }

        return workgroup;
    }

    private void prepareResultSink() throws Exception {
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        FragmentExecParams topParams = fragmentExecParamsMap.get(topId);
        if (topParams.fragment.getSink() instanceof ResultSink) {
            TNetworkAddress execBeAddr = topParams.instanceExecParams.get(0).host;
            receiver = new ResultReceiver(
                    topParams.instanceExecParams.get(0).instanceId,
                    addressToBackendID.get(execBeAddr),
                    toBrpcHost(execBeAddr),
                    queryOptions.query_timeout * 1000);

            // Select top fragment as global runtime filter merge address
            setGlobalRuntimeFilterParams(topParams, toBrpcHost(execBeAddr));

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
            this.queryOptions.setIs_report_success(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = Lists.newArrayList(addressToBackendID.values());
            GlobalStateMgr.getCurrentState().getLoadManager().initJobProgress(jobId, queryId, instanceIds,
                    relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), addressToBackendID.keySet());
        }

        computeBeInstanceNumbers();

        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
        for (TUniqueId instanceId : instanceIds) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }
        long queryDeliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;
        lock();
        try {
            // execute all instances from up to bottom
            int backendId = 0;
            int profileFragmentId = 0;

            Set<Long> dbIds = connectContext != null ? connectContext.getCurrentSqlDbIds() : null;

            Set<TNetworkAddress> firstDeliveryAddresses = new HashSet<>();
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

                // set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                List<List<FInstanceExecParam>> infightFInstanceExecParamList = new LinkedList<>();

                // Fragment instances' ordinals in FragmentExecParams.instanceExecParams determine
                // shuffle partitions' ordinals in DataStreamSink. backendIds of Fragment instances that
                // contains shuffle join determine the ordinals of GRF components in the GRF. For a
                // shuffle join, its shuffle partitions and corresponding one-map-one GRF components
                // should have the same ordinals. so here assign monotonic unique backendIds to
                // Fragment instances to keep consistent order with Fragment instances in
                // FragmentExecParams.instanceExecParams.
                for (FInstanceExecParam fInstanceExecParam : params.instanceExecParams) {
                    fInstanceExecParam.backendId = backendId++;
                }
                if (usePipeline) {
                    List<FInstanceExecParam> firstFInstanceParamList = new ArrayList<>();
                    List<FInstanceExecParam> remainingFInstanceParamList = new ArrayList<>();

                    for (FInstanceExecParam fInstanceExecParam : params.instanceExecParams) {
                        if (!firstDeliveryAddresses.contains(fInstanceExecParam.host)) {
                            firstDeliveryAddresses.add(fInstanceExecParam.host);
                            firstFInstanceParamList.add(fInstanceExecParam);
                        } else {
                            remainingFInstanceParamList.add(fInstanceExecParam);
                        }
                    }
                    infightFInstanceExecParamList.add(firstFInstanceParamList);
                    infightFInstanceExecParamList.add(remainingFInstanceParamList);
                } else {
                    infightFInstanceExecParamList.add(params.instanceExecParams);
                }

                boolean isFirst = true;
                for (List<FInstanceExecParam> fInstanceExecParamList : infightFInstanceExecParamList) {
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
                            params.toThrift(instanceId2Host.keySet(), descTable, dbIds, usePipeline);
                    List<Pair<BackendExecState, Future<PExecPlanFragmentResult>>> futures = Lists.newArrayList();

                    // This is a load process, and it is the first fragment.
                    // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                    // so that we can check these backends' state when joining this Coordinator
                    boolean needCheckBackendState = queryOptions.getQuery_type() == TQueryType.LOAD && profileFragmentId == 0;

                    for (TExecPlanFragmentParams tParam : tParams) {
                        // TODO: pool of pre-formatted BackendExecStates?
                        TNetworkAddress host = instanceId2Host.get(tParam.params.fragment_instance_id);
                        BackendExecState execState = new BackendExecState(fragment.getFragmentId(), host,
                                profileFragmentId, tParam, this.addressToBackendID);
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

    private final Map<TNetworkAddress, Integer> hostToNumbers = Maps.newHashMap();

    // Compute the fragment instance numbers in every BE for one query
    private void computeBeInstanceNumbers() {
        hostToNumbers.clear();
        for (PlanFragment fragment : fragments) {
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());
            for (final FInstanceExecParam instance : params.instanceExecParams) {
                Integer number = hostToNumbers.getOrDefault(instance.host, 0);
                hostToNumbers.put(instance.host, ++number);
            }
        }
    }

    // choose at most num FInstances on difference BEs
    private List<FInstanceExecParam> pickupFInstancesOnDifferentHosts(List<FInstanceExecParam> instances, int num) {
        if (instances.size() <= num) {
            return instances;
        }

        Map<TNetworkAddress, List<FInstanceExecParam>> host2instances = Maps.newHashMap();
        for (FInstanceExecParam instance : instances) {
            host2instances.putIfAbsent(instance.host, Lists.newLinkedList());
            host2instances.get(instance.host).add(instance);
        }
        List<FInstanceExecParam> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<FInstanceExecParam> instancesPerHost : host2instances.values()) {
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

    private void setGlobalRuntimeFilterParams(FragmentExecParams topParams, TNetworkAddress mergeHost)
            throws Exception {

        Map<Integer, List<TRuntimeFilterProberParams>> broadcastGRFProbersMap = Maps.newHashMap();
        List<RuntimeFilterDescription> broadcastGRFList = Lists.newArrayList();

        for (PlanFragment fragment : fragments) {
            fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            fragment.collectProbeRuntimeFilters(fragment.getPlanRoot());
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = Lists.newArrayList();
                for (final FInstanceExecParam instance : params.instanceExecParams) {
                    TRuntimeFilterProberParams probeParam = new TRuntimeFilterProberParams();
                    probeParam.setFragment_instance_id(instance.instanceId);
                    probeParam.setFragment_instance_address(toBrpcHost(instance.host));
                    probeParamList.add(probeParam);
                }
                if (usePipeline && kv.getValue().isBroadcastJoin() && kv.getValue().isHasRemoteTargets()) {
                    broadcastGRFProbersMap.put(kv.getKey(), probeParamList);
                } else {
                    topParams.runtimeFilterParams.putToId_to_prober_params(kv.getKey(), probeParamList);
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
                        if (usePipeline) {
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
            if (!isBlockQuery && instanceIds.size() > 1 && hasLimit && numReceivedRows >= numLimitRows) {
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

    private void computeFragmentExecParams() throws Exception {
        // fill hosts field in fragmentExecParams
        computeFragmentHosts();

        // assign instance ids
        instanceIds.clear();
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("fragment {} has instances {}", params.fragment.getFragmentId(),
                        params.instanceExecParams.size());
            }

            if (params.fragment.getSink() instanceof ResultSink && params.instanceExecParams.size() > 1) {
                throw new StarRocksPlannerException("This sql plan has multi result sinks",
                        ErrorType.INTERNAL_ERROR);
            }

            for (int j = 0; j < params.instanceExecParams.size(); ++j) {
                // we add instance_num to query_id.lo to create a
                // globally-unique instance id
                TUniqueId instanceId = new TUniqueId();
                instanceId.setHi(queryId.hi);
                instanceId.setLo(queryId.lo + instanceIds.size() + 1);
                params.instanceExecParams.get(j).instanceId = instanceId;
                instanceIds.add(instanceId);
            }
        }

        // compute destinations and # senders per exchange node
        // (the root fragment doesn't have a destination)

        // MultiCastFragment params
        handleMultiCastFragmentParams();

        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (params.fragment instanceof MultiCastPlanFragment) {
                continue;
            }

            PlanFragment destFragment = params.fragment.getDestFragment();

            if (destFragment == null) {
                // root plan fragment
                continue;
            }
            FragmentExecParams destParams = fragmentExecParamsMap.get(destFragment.getFragmentId());

            // set # of senders
            DataSink sink = params.fragment.getSink();
            // we can only handle unpartitioned (= broadcast) and
            // hash-partitioned
            // output at the moment

            // set params for pipeline level shuffle
            params.fragment.getDestNode().setPartitionType(params.fragment.getOutputPartition().getType());
            if (sink instanceof DataStreamSink) {
                DataStreamSink dataStreamSink = (DataStreamSink) sink;
                dataStreamSink.setExchDop(destParams.fragment.getPipelineDop());
            }

            PlanNodeId exchId = sink.getExchNodeId();
            if (destParams.perExchNumSenders.get(exchId.asInt()) == null) {
                destParams.perExchNumSenders.put(exchId.asInt(), params.instanceExecParams.size());
            } else {
                // we might have multiple fragments sending to this exchange node
                // (distributed MERGE), which is why we need to add up the #senders
                // e.g. sort-merge
                destParams.perExchNumSenders.put(exchId.asInt(),
                        params.instanceExecParams.size() + destParams.perExchNumSenders.get(exchId.asInt()));
            }

            if (needScheduleByShuffleJoin(destFragment.getFragmentId().asInt(), sink)) {
                int bucketSeq = 0;
                int bucketNum = getFragmentBucketNum(destFragment.getFragmentId());
                TNetworkAddress dummyServer = new TNetworkAddress("0.0.0.0", 0);

                while (bucketSeq < bucketNum) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();
                    // dest bucket may be pruned, these bucket dest should be set an invalid value
                    // and will be deal with in BE's DataStreamSender
                    dest.fragment_instance_id = new TUniqueId(-1, -1);
                    dest.server = dummyServer;
                    dest.setBrpc_server(dummyServer);

                    for (FInstanceExecParam instanceExecParams : destParams.instanceExecParams) {
                        if (instanceExecParams.bucketSeqSet.contains(bucketSeq)) {
                            dest.fragment_instance_id = instanceExecParams.instanceId;
                            dest.server = toRpcHost(instanceExecParams.host);
                            dest.setBrpc_server(toBrpcHost(instanceExecParams.host));
                            break;
                        }
                    }
                    Preconditions.checkState(dest.isSetFragment_instance_id());
                    bucketSeq++;
                    params.destinations.add(dest);
                }
            } else {
                // add destination host to this fragment's destination
                for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();
                    dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                    dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                    dest.setBrpc_server(toBrpcHost(destParams.instanceExecParams.get(j).host));
                    params.destinations.add(dest);
                }
            }
        }

    }

    private void handleMultiCastFragmentParams() throws Exception {
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (!(params.fragment instanceof MultiCastPlanFragment)) {
                continue;
            }

            MultiCastPlanFragment multi = (MultiCastPlanFragment) params.fragment;
            Preconditions.checkState(multi.getSink() instanceof MultiCastDataSink);
            // set # of senders
            MultiCastDataSink multiSink = (MultiCastDataSink) multi.getSink();

            for (int i = 0; i < multi.getDestFragmentList().size(); i++) {
                PlanFragment destFragment = multi.getDestFragmentList().get(i);
                DataSink sink = multiSink.getDataStreamSinks().get(i);

                if (destFragment == null) {
                    continue;
                }
                FragmentExecParams destParams = fragmentExecParamsMap.get(destFragment.getFragmentId());

                PlanNodeId exchId = sink.getExchNodeId();
                // MultiCastSink only send to itself, destination exchange only one senders
                // and it's don't support sort-merge
                Preconditions.checkState(!destParams.perExchNumSenders.containsKey(exchId.asInt()));
                destParams.perExchNumSenders.put(exchId.asInt(), 1);

                if (needScheduleByShuffleJoin(destFragment.getFragmentId().asInt(), sink)) {
                    int bucketSeq = 0;
                    int bucketNum = getFragmentBucketNum(destFragment.getFragmentId());
                    TNetworkAddress dummyServer = new TNetworkAddress("0.0.0.0", 0);

                    while (bucketSeq < bucketNum) {
                        TPlanFragmentDestination dest = new TPlanFragmentDestination();
                        // dest bucket may be pruned, these bucket dest should be set an invalid value
                        // and will be deal with in BE's DataStreamSender
                        dest.fragment_instance_id = new TUniqueId(-1, -1);
                        dest.server = dummyServer;
                        dest.setBrpc_server(dummyServer);

                        for (FInstanceExecParam instanceExecParams : destParams.instanceExecParams) {
                            if (instanceExecParams.bucketSeqSet.contains(bucketSeq)) {
                                dest.fragment_instance_id = instanceExecParams.instanceId;
                                dest.server = toRpcHost(instanceExecParams.host);
                                dest.setBrpc_server(toBrpcHost(instanceExecParams.host));
                                break;
                            }
                        }
                        Preconditions.checkState(dest.isSetFragment_instance_id());
                        bucketSeq++;
                        multiSink.getDestinations().get(i).add(dest);
                    }
                } else {
                    // add destination host to this fragment's destination
                    for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                        TPlanFragmentDestination dest = new TPlanFragmentDestination();
                        dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                        dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                        dest.setBrpc_server(toBrpcHost(destParams.instanceExecParams.get(j).host));
                        multiSink.getDestinations().get(i).add(dest);
                    }
                }
            }
        }
    }

    private boolean needScheduleByShuffleJoin(int fragmentId, DataSink sink) {
        if (isBucketShuffleJoin(fragmentId)) {
            if (sink instanceof DataStreamSink) {
                DataStreamSink streamSink = (DataStreamSink) sink;
                return streamSink.getOutputPartition().isBucketShuffle();
            }
        }
        return false;
    }

    private TNetworkAddress toRpcHost(TNetworkAddress host) throws Exception {
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBeRpcPort());
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }
        if (backend.getBrpcPort() < 0) {
            return null;
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    // For each fragment in fragments, computes hosts on which to run the instances
    // and stores result in fragmentExecParams.hosts.
    private void computeFragmentHosts() throws Exception {
        // compute hosts of producer fragment before those of consumer fragment(s),
        // the latter might inherit the set of hosts from the former
        // compute hosts *bottom up*.

        for (int i = fragments.size() - 1; i >= 0; --i) {
            PlanFragment fragment = fragments.get(i);
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

            boolean dopAdaptionEnabled =
                    usePipeline && connectContext.getSessionVariable().isPipelineDopAdaptionEnabled();

            // If left child is MultiCastDataFragment(only support left now), will keep same instance with child.
            if (fragment.getChildren().size() > 0 && fragment.getChild(0) instanceof MultiCastPlanFragment) {
                FragmentExecParams childFragmentParams =
                        fragmentExecParamsMap.get(fragment.getChild(0).getFragmentId());
                for (FInstanceExecParam childInstanceParam : childFragmentParams.instanceExecParams) {
                    params.instanceExecParams.add(new FInstanceExecParam(null, childInstanceParam.host, 0, params));
                }
                continue;
            }

            if (fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
                Reference<Long> backendIdRef = new Reference<>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    LOG.warn("DataPartition UNPARTITIONED, no scanNode Backend");
                    throw new UserException("Backend not found. Check if any backend is down or not");
                }
                recordUsedBackend(execHostport, backendIdRef.getRef());
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport,
                        0, params);
                params.instanceExecParams.add(instanceParam);
                continue;
            }

            PlanNode leftMostNode = findLeftmostNode(fragment.getPlanRoot());

            /*
             * Case A:
             *      if the left most is ScanNode, which means there is no child fragment,
             *      we should assign fragment instances on every scan node hosts.
             * Case B:
             *      if not, there should be exchange nodes to collect all data from child fragments(input fragments),
             *      so we should assign fragment instances corresponding to the child fragments' host
             */
            if (!(leftMostNode instanceof ScanNode)) {
                // (Case B)
                // there is no leftmost scan; we assign the same hosts as those of our
                //  input fragment which has a higher instance_number
                int inputFragmentIndex = 0;
                int maxParallelism = 0;
                for (int j = 0; j < fragment.getChildren().size(); j++) {
                    int currentChildFragmentParallelism =
                            fragmentExecParamsMap.get(fragment.getChild(j).getFragmentId()).instanceExecParams.size();
                    // when dop adaptation enabled, numInstances * pipelineDop is equivalent to numInstances in
                    // non-pipeline engine and pipeline engine(dop adaptation disabled).
                    if (dopAdaptionEnabled) {
                        currentChildFragmentParallelism *= fragment.getChild(j).getPipelineDop();
                    }
                    if (currentChildFragmentParallelism > maxParallelism) {
                        maxParallelism = currentChildFragmentParallelism;
                        inputFragmentIndex = j;
                    }
                }

                PlanFragmentId inputFragmentId = fragment.getChild(inputFragmentIndex).getFragmentId();
                FragmentExecParams maxParallelismFragmentExecParams = fragmentExecParamsMap.get(inputFragmentId);

                // hostSet contains target backends to whom fragment instances of the current PlanFragment will be
                // delivered. when pipeline parallelization is adopted, the number of instances should be the size
                // of hostSet, that it to say, each backend has exactly one fragment.
                Set<TNetworkAddress> hostSet = Sets.newHashSet();
                for (FInstanceExecParam execParams : maxParallelismFragmentExecParams.instanceExecParams) {
                    hostSet.add(execParams.host);
                }

                if (dopAdaptionEnabled) {
                    Preconditions.checkArgument(leftMostNode instanceof ExchangeNode);
                    maxParallelism = hostSet.size();
                }

                // AddAll() soft copy()
                int exchangeInstances = -1;
                if (connectContext != null && connectContext.getSessionVariable() != null) {
                    exchangeInstances = connectContext.getSessionVariable().getExchangeInstanceParallel();
                }
                if (exchangeInstances > 0 && maxParallelism > exchangeInstances) {
                    // random select some instance
                    // get distinct host,  when parallel_fragment_exec_instance_num > 1, single host may execute several instances
                    List<TNetworkAddress> hosts = Lists.newArrayList(hostSet);
                    Collections.shuffle(hosts, instanceRandom);

                    for (int index = 0; index < exchangeInstances; index++) {
                        FInstanceExecParam instanceParam =
                                new FInstanceExecParam(null, hosts.get(index % hosts.size()), 0, params);
                        params.instanceExecParams.add(instanceParam);
                    }
                } else {
                    List<TNetworkAddress> hosts = Lists.newArrayList(hostSet);
                    for (int index = 0; index < maxParallelism; ++index) {
                        TNetworkAddress host = hosts.get(index % hosts.size());
                        FInstanceExecParam instanceParam = new FInstanceExecParam(null, host, 0, params);
                        params.instanceExecParams.add(instanceParam);
                    }
                }

                // When group by cardinality is smaller than number of backend, only some backends always
                // process while other has no data to process.
                // So we shuffle instances to make different backends handle different queries.
                Collections.shuffle(params.instanceExecParams, instanceRandom);

                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            int parallelExecInstanceNum = fragment.getParallelExecNum();
            boolean hasColocate = (isColocateFragment(fragment.getPlanRoot()) &&
                    fragmentIdToSeqToAddressMap.containsKey(fragment.getFragmentId())
                    && fragmentIdToSeqToAddressMap.get(fragment.getFragmentId()).size() > 0);
            boolean hasBucketShuffle = isBucketShuffleJoin(fragment.getFragmentId().asInt());

            if (hasColocate || hasBucketShuffle) {
                computeColocatedJoinInstanceParam(fragment.getFragmentId(), parallelExecInstanceNum, params);
                computeBucketSeq2InstanceOrdinal(params, fragmentIdToBucketNumMap.get(fragment.getFragmentId()));
            } else {
                for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> tNetworkAddressMapEntry :
                        fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment.entrySet()) {
                    TNetworkAddress key = tNetworkAddressMapEntry.getKey();
                    Map<Integer, List<TScanRangeParams>> value = tNetworkAddressMapEntry.getValue();

                    // 1. Handle normal scan node firstly
                    for (Integer planNodeId : value.keySet()) {
                        if (replicateScanIds.contains(planNodeId)) {
                            continue;
                        }
                        List<TScanRangeParams> perNodeScanRanges = value.get(planNodeId);
                        int expectedInstanceNum = 1;
                        if (parallelExecInstanceNum > 1) {
                            //the scan instance num should not larger than the tablets num
                            expectedInstanceNum = Math.min(perNodeScanRanges.size(), parallelExecInstanceNum);
                        }
                        List<List<TScanRangeParams>> perInstanceScanRanges = ListUtil.splitBySize(perNodeScanRanges,
                                expectedInstanceNum);

                        for (List<TScanRangeParams> scanRangeParams : perInstanceScanRanges) {
                            FInstanceExecParam instanceParam = new FInstanceExecParam(null, key, 0, params);
                            instanceParam.perNodeScanRanges.put(planNodeId, scanRangeParams);
                            params.instanceExecParams.add(instanceParam);
                        }
                    }

                    // 1. Handle replicated scan node if need
                    boolean isReplicated = isReplicatedFragment(fragment.getPlanRoot());
                    if (isReplicated) {
                        for (Integer planNodeId : value.keySet()) {
                            if (!replicateScanIds.contains(planNodeId)) {
                                continue;
                            }
                            List<TScanRangeParams> perNodeScanRanges = value.get(planNodeId);
                            for (FInstanceExecParam instanceParam : params.instanceExecParams) {
                                instanceParam.perNodeScanRanges.put(planNodeId, perNodeScanRanges);
                            }
                        }
                    }
                }
            }

            if (params.instanceExecParams.isEmpty()) {
                Reference<Long> backendIdRef = new Reference<>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    throw new UserException("Backend not found. Check if any backend is down or not");
                }
                this.recordUsedBackend(execHostport, backendIdRef.getRef());
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport,
                        0, params);
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    static final int BUCKET_ABSENT = 2147483647;

    public void computeBucketSeq2InstanceOrdinal(FragmentExecParams params, int numBuckets) {
        Integer[] bucketSeq2InstanceOrdinal = new Integer[numBuckets];
        // some buckets are pruned, so set the corresponding instance ordinal to BUCKET_ABSENT to indicate
        // absence of buckets.
        for (int bucketSeq = 0; bucketSeq < numBuckets; ++bucketSeq) {
            bucketSeq2InstanceOrdinal[bucketSeq] = BUCKET_ABSENT;
        }
        for (int i = 0; i < params.instanceExecParams.size(); ++i) {
            FInstanceExecParam instance = params.instanceExecParams.get(i);
            for (Integer bucketSeq : instance.bucketSeqSet) {
                Preconditions.checkArgument(bucketSeq < numBuckets, "bucketSeq exceeds bucketNum in colocate Fragment");
                bucketSeq2InstanceOrdinal[bucketSeq] = i;
            }
        }
        fragmentIdToSeqToInstanceMap.put(params.fragment.getFragmentId(), Arrays.asList(bucketSeq2InstanceOrdinal));
    }

    private boolean isColocateFragment(PlanNode node) {
        // Cache the colocateFragmentIds
        if (colocateFragmentIds.contains(node.getFragmentId().asInt())) {
            return true;
        }
        // can not cross fragment
        if (node instanceof ExchangeNode) {
            return false;
        }

        if (node.isColocate()) {
            colocateFragmentIds.add(node.getFragmentId().asInt());
            return true;
        }

        boolean childHasColocate = false;
        if (node.isReplicated()) {
            // Only check left if node is replicate join
            childHasColocate = isColocateFragment(node.getChild(0));
        } else {
            for (PlanNode childNode : node.getChildren()) {
                childHasColocate |= isColocateFragment(childNode);
            }
        }

        return childHasColocate;
    }

    private boolean isReplicatedFragment(PlanNode node) {
        if (replicateFragmentIds.contains(node.getFragmentId().asInt())) {
            return true;
        }

        // can not cross fragment
        if (node instanceof ExchangeNode) {
            return false;
        }

        if (node.isReplicated()) {
            replicateFragmentIds.add(node.getFragmentId().asInt());
            return true;
        }

        boolean childHasReplicated = false;
        for (PlanNode childNode : node.getChildren()) {
            childHasReplicated |= isReplicatedFragment(childNode);
        }

        return childHasReplicated;
    }

    // check whether the node fragment is bucket shuffle join fragment
    private boolean isBucketShuffleJoin(int fragmentId, PlanNode node) {
        // check the node is be the part of the fragment
        if (fragmentId != node.getFragmentId().asInt()) {
            return false;
        }

        if (bucketShuffleFragmentIds.contains(fragmentId)) {
            return true;
        }
        // can not cross fragment
        if (node instanceof ExchangeNode) {
            return false;
        }

        // One fragment could only have one HashJoinNode
        if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            if (joinNode.isLocalHashBucket()) {
                bucketShuffleFragmentIds.add(joinNode.getFragmentId().asInt());
                if (joinNode.getJoinOp().isFullOuterJoin() || joinNode.getJoinOp().isRightJoin()) {
                    rightOrFullBucketShuffleFragmentIds.add(joinNode.getFragmentId().asInt());
                }
                return true;
            }
        }

        boolean childHasBucketShuffle = false;
        for (PlanNode childNode : node.getChildren()) {
            childHasBucketShuffle |= isBucketShuffleJoin(fragmentId, childNode);
        }

        return childHasBucketShuffle;
    }

    private boolean isBucketShuffleJoin(int fragmentId) {
        return bucketShuffleFragmentIds.contains(fragmentId);
    }

    // Returns the id of the leftmost node of any of the gives types in 'plan_root',
    // or INVALID_PLAN_NODE_ID if no such node present.
    private PlanNode findLeftmostNode(PlanNode plan) {
        PlanNode newPlan = plan;
        while (newPlan.getChildren().size() != 0 && !(newPlan instanceof ExchangeNode)) {
            newPlan = newPlan.getChild(0);
        }
        return newPlan;
    }

    private <K, V> V findOrInsert(HashMap<K, V> m, final K key, final V defaultVal) {
        V value = m.get(key);
        if (value == null) {
            m.put(key, defaultVal);
            value = defaultVal;
        }
        return value;
    }

    // weather we can overwrite the first parameter or not?
    private List<TScanRangeParams> findOrInsert(Map<Integer, List<TScanRangeParams>> m, Integer key,
                                                ArrayList<TScanRangeParams> defaultVal) {
        List<TScanRangeParams> value = m.get(key);
        if (value == null) {
            m.put(key, defaultVal);
            value = defaultVal;
        }
        return value;
    }

    private void computeColocatedJoinInstanceParam(PlanFragmentId fragmentId, int parallelExecInstanceNum,
                                                   FragmentExecParams params) {
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(fragmentId);

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<TNetworkAddress, List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges =
                Maps.newHashMap();
        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> scanRanges : bucketSeqToScanRange.entrySet()) {
            TNetworkAddress address = bucketSeqToAddress.get(scanRanges.getKey());

            if (!addressToScanRanges.containsKey(address)) {
                addressToScanRanges.put(address, Lists.newArrayList());
            }
            addressToScanRanges.get(address).add(scanRanges);
        }

        for (Map.Entry<TNetworkAddress, List<Map.Entry<Integer, Map<Integer,
                List<TScanRangeParams>>>>> addressScanRange : addressToScanRanges.entrySet()) {
            List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
            int expectedInstanceNum = 1;
            if (parallelExecInstanceNum > 1) {
                //the scan instance num should not larger than the tablets num
                expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
            }

            // 2. split how many scanRange one instance should scan
            List<List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> perInstanceScanRanges =
                    ListUtil.splitBySize(scanRange,
                            expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scan by instance
            for (List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>> perInstanceScanRange : perInstanceScanRanges) {
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), 0, params);
                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicateScanSet = new HashSet<>();
                for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : perInstanceScanRange) {
                    instanceParam.addBucketSeq(nodeScanRangeMap.getKey());
                    for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange : nodeScanRangeMap.getValue()
                            .entrySet()) {
                        int scanId = nodeScanRange.getKey();
                        if (!instanceParam.perNodeScanRanges.containsKey(scanId)) {
                            instanceParam.perNodeScanRanges.put(scanId, new ArrayList<>());
                        }
                        if (replicateScanIds.contains(scanId)) {
                            if (!instanceReplicateScanSet.contains(scanId)) {
                                instanceParam.perNodeScanRanges.get(scanId).addAll(nodeScanRange.getValue());
                                instanceReplicateScanSet.add(scanId);
                            }
                        } else {
                            instanceParam.perNodeScanRanges.get(scanId).addAll(nodeScanRange.getValue());
                        }
                    }
                }
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    private void computeScanRangeAssignment() throws Exception {
        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : scanNodes) {
            // the parameters of getScanRangeLocations may ignore, It dosn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            FragmentScanRangeAssignment assignment =
                    fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
            if ((scanNode instanceof HdfsScanNode) || (scanNode instanceof IcebergScanNode) ||
                    scanNode instanceof HudiScanNode) {
                HDFSBackendSelector selector = new HDFSBackendSelector(scanNode, locations, assignment,
                        ScanRangeAssignType.SCAN_DATA_SIZE, usedBackendIDs);
                List<Long> scanRangesBytes = Lists.newArrayList();
                for (TScanRangeLocations scanRangeLocations : locations) {
                    scanRangesBytes.add(scanRangeLocations.scan_range.hdfs_scan_range.length);
                }
                selector.setScanRangesBytes(scanRangesBytes);
                selector.computeScanRangeAssignment();
            } else {
                boolean hasColocate = isColocateFragment(scanNode.getFragment().getPlanRoot());
                boolean hasBucket =
                        isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());
                boolean hasReplicated = isReplicatedFragment(scanNode.getFragment().getPlanRoot());
                if (assignment.size() > 0 && hasReplicated && scanNode.canDoReplicatedJoin()) {
                    BackendSelector selector = new RelicatedBackendSelector(scanNode, locations, assignment);
                    selector.computeScanRangeAssignment();
                    replicateScanIds.add(scanNode.getId().asInt());
                } else if (hasColocate || hasBucket) {
                    BackendSelector selector = new ColocatedBackendSelector((OlapScanNode) scanNode, assignment);
                    selector.computeScanRangeAssignment();
                } else {
                    BackendSelector selector = new NormalBackendSelector(scanNode, locations, assignment);
                    selector.computeScanRangeAssignment();
                }
            }
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
            LOG.warn("one instance report fail {}, query_id={} instance_id={}",
                    status, DebugUtil.printId(queryId), DebugUtil.printId(params.getFragment_instance_id()));
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
            profileDoneSignal.markedCountDown(params.getFragment_instance_id(), -1L);
        }

        if (params.isSetLoaded_rows()) {
            GlobalStateMgr.getCurrentState().getLoadManager().updateJobPrgress(
                    jobId, params.backend_id, params.query_id, params.fragment_instance_id, params.loaded_rows,
                    params.done);
        }
    }

    public void endProfile() {
        if (backendExecStates.isEmpty()) {
            return;
        }

        // wait for all backends
        if (needReport) {
            try {
                int timeout = connectContext.getSessionVariable().getProfileTimeout();
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

    public void mergeIsomorphicProfiles() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();

        if (!sessionVariable.isReportSucc()) {
            return;
        }

        if (!usePipeline) {
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
            Counter counter = fragmentProfile.addCounter("InstanceNum", TUnit.UNIT);
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
                    fragmentExecParamsMap.get(fragment.getFragmentId()).instanceExecParams.stream()
                            .map(param -> param.host)
                            .collect(Collectors.toSet());

            Counter backendNum = profile.addCounter("BackendNum", TUnit.UNIT);
            backendNum.setValue(networkAddresses.size());
        }

        // Calculate ExecutionTotalTime, which comprising all operator's sync time and async time
        // We can get Operator's sync time from OperatorTotalTime, and for async time, only ScanOperator and
        // ExchangeOperator have async operations, we can get async time from ScanTime(for ScanOperator) and
        // NetworkTime(for ExchangeOperator)
        long executionTime = 0;
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            for (Pair<RuntimeProfile, Boolean> pipelineProfilePair : fragmentProfile.getChildList()) {
                RuntimeProfile pipelineProfile = pipelineProfilePair.first;
                for (Pair<RuntimeProfile, Boolean> operatorProfilePair : pipelineProfile.getChildList()) {
                    RuntimeProfile operatorProfile = operatorProfilePair.first;
                    RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                    RuntimeProfile uniqueMetrics = operatorProfile.getChild("UniqueMetrics");
                    if (commonMetrics == null || uniqueMetrics == null) {
                        continue;
                    }
                    Counter operatorTotalTime = commonMetrics.getMaxCounter("OperatorTotalTime");
                    Preconditions.checkNotNull(operatorTotalTime);
                    executionTime += operatorTotalTime.getValue();

                    Counter scanTime = uniqueMetrics.getMaxCounter("ScanTime");
                    if (scanTime != null) {
                        executionTime += scanTime.getValue();
                    }

                    Counter networkTime = uniqueMetrics.getMaxCounter("NetworkTime");
                    if (networkTime != null) {
                        executionTime += networkTime.getValue();
                    }
                }
            }
        }
        Counter executionTotalTime = queryProfile.addCounter("ExecutionTotalTime", TUnit.TIME_NS);
        queryProfile.getCounterTotalTime().setValue(0);
        executionTotalTime.setValue(executionTime);
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

    // For HybridBackendSelector
    private enum ScanRangeAssignType {
        SCAN_RANGE_NUM,
        SCAN_DATA_SIZE
    }

    private interface BackendSelector {
        void computeScanRangeAssignment() throws Exception;
    }

    // fragment instance exec param, it is used to assemble
    // the per-instance TPlanFragmentExecParas, as a member of
    // FragmentExecParams
    static class FInstanceExecParam {
        TUniqueId instanceId;
        TNetworkAddress host;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();

        int perFragmentInstanceIdx;

        Set<Integer> bucketSeqSet = Sets.newHashSet();

        int backendId;

        FragmentExecParams fragmentExecParams;

        public void addBucketSeq(int bucketSeq) {
            this.bucketSeqSet.add(bucketSeq);
        }

        public FInstanceExecParam(TUniqueId id, TNetworkAddress host,
                                  int perFragmentInstanceIdx, FragmentExecParams fragmentExecParams) {
            this.instanceId = id;
            this.host = host;
            this.perFragmentInstanceIdx = perFragmentInstanceIdx;
            this.fragmentExecParams = fragmentExecParams;
        }

        public PlanFragment fragment() {
            return fragmentExecParams.fragment;
        }
    }

    // map from an impalad host address to the per-node assigned scan ranges;
    // records scan range assignment for a single fragment
    static class FragmentScanRangeAssignment
            extends HashMap<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> {
    }

    static class BucketSeqToScanRange extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {

    }

    private int getFragmentBucketNum(PlanFragmentId fragmentId) {
        return fragmentIdToBucketNumMap.get(fragmentId);
    }

    // record backend execute state
    // TODO(zhaochun): add profile information and others
    public class BackendExecState {
        TExecPlanFragmentParams rpcParams;
        PlanFragmentId fragmentId;
        boolean initiated;
        boolean done;
        boolean hasCanceled;
        int profileFragmentId;
        RuntimeProfile profile;
        TNetworkAddress address;
        Backend backend;
        long lastMissingHeartbeatTime = -1;

        public BackendExecState(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            this.address = host;
            this.backend = idToBackend.get(addressToBackendID.get(address));

            String name =
                    "Instance " + DebugUtil.printId(rpcParams.params.fragment_instance_id) + " (host=" + address + ")";
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
                TNetworkAddress brpcAddress = toBrpcHost(address);

                try {
                    BackendServiceProxy.getInstance().cancelPlanFragmentAsync(brpcAddress,
                            queryId, fragmentInstanceId(), cancelReason, rpcParams.is_pipeline);
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(addressToBackendID.get(brpcAddress));
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
                return BackendServiceProxy.getInstance().execPlanFragmentAsync(brpcAddress, rpcParams);
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
            return this.rpcParams.params.getFragment_instance_id();
        }
    }

    // execution parameters for a single fragment,
    // per-fragment can have multiple FInstanceExecParam,
    // used to assemble TPlanFragmentExecParas
    protected class FragmentExecParams {
        public PlanFragment fragment;
        public List<TPlanFragmentDestination> destinations = Lists.newArrayList();
        public Map<Integer, Integer> perExchNumSenders = Maps.newHashMap();

        public List<FInstanceExecParam> instanceExecParams = Lists.newArrayList();
        public FragmentScanRangeAssignment scanRangeAssignment = new FragmentScanRangeAssignment();
        TRuntimeFilterParams runtimeFilterParams = new TRuntimeFilterParams();
        public boolean bucketSeqToInstanceForFilterIsSet = false;

        public FragmentExecParams(PlanFragment fragment) {
            this.fragment = fragment;
        }

        void setBucketSeqToInstanceForRuntimeFilters() {
            if (bucketSeqToInstanceForFilterIsSet) {
                return;
            }
            bucketSeqToInstanceForFilterIsSet = true;
            List<Integer> seqToInstance = fragmentIdToSeqToInstanceMap.get(fragment.getFragmentId());
            if (seqToInstance == null || seqToInstance.isEmpty()) {
                return;
            }
            for (RuntimeFilterDescription rf : fragment.getBuildRuntimeFilters().values()) {
                if (!rf.isColocateOrBucketShuffle()) {
                    continue;
                }
                rf.setBucketSeqToInstance(seqToInstance);
            }
        }

        List<TExecPlanFragmentParams> toThrift(Set<TUniqueId> inFlightInstanceIds,
                                               TDescriptorTable descTable,
                                               Set<Long> dbIds,
                                               boolean isEnablePipelineEngine) throws Exception {
            // add instance number in file name prefix when export job
            DataSink sink = fragment.getSink();
            ExportSink exportSink = null;
            String fileNamePrefix = null;
            if (sink instanceof ExportSink) {
                exportSink = (ExportSink) sink;
                fileNamePrefix = exportSink.getFileNamePrefix();
            }

            setBucketSeqToInstanceForRuntimeFilters();
            List<TExecPlanFragmentParams> paramsList = Lists.newArrayList();
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                if (!inFlightInstanceIds.contains(instanceExecParam.instanceId)) {
                    continue;
                }
                TExecPlanFragmentParams params = new TExecPlanFragmentParams();

                if (exportSink != null && fileNamePrefix != null) {
                    exportSink.setFileNamePrefix(fileNamePrefix + i + "_");
                }

                params.setProtocol_version(InternalServiceVersion.V1);
                params.setFragment(fragment.toThrift());

                /*
                 * For MultiCastDataFragment, output only send to local, and the instance is keep
                 * same with MultiCastDataFragment
                 * */
                if (fragment instanceof MultiCastPlanFragment) {
                    List<List<TPlanFragmentDestination>> multiFragmentDestinations =
                            params.getFragment().getOutput_sink().getMulti_cast_stream_sink().getDestinations();
                    List<List<TPlanFragmentDestination>> newDestinations = Lists.newArrayList();
                    for (List<TPlanFragmentDestination> destinations : multiFragmentDestinations) {
                        Preconditions.checkState(instanceExecParams.size() == destinations.size());
                        TPlanFragmentDestination ndes = destinations.get(i);

                        Preconditions.checkState(ndes.getServer().equals(toRpcHost(instanceExecParam.host)));
                        newDestinations.add(Lists.newArrayList(ndes));
                    }
                    params.getFragment().getOutput_sink().getMulti_cast_stream_sink().setDestinations(newDestinations);
                }

                params.setDesc_tbl(descTable);
                params.setParams(new TPlanFragmentExecParams());
                params.setFunc_version(3);
                params.params.setUse_vectorized(true);
                params.params.setQuery_id(queryId);
                params.params.setFragment_instance_id(instanceExecParam.instanceId);
                Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                }

                params.params.setPer_node_scan_ranges(scanRanges);
                params.params.setPer_exch_num_senders(perExchNumSenders);

                params.params.setDestinations(destinations);
                params.params.setSender_id(i);
                params.params.setNum_senders(instanceExecParams.size());
                if (runtimeFilterParams.isSetRuntime_filter_builder_number()) {
                    params.params.setRuntime_filter_params(runtimeFilterParams);
                }
                params.setCoord(coordAddress);
                params.setBackend_num(instanceExecParam.backendId);
                params.setQuery_globals(queryGlobals);
                if (isEnablePipelineEngine) {
                    params.setQuery_options(new TQueryOptions(queryOptions));
                } else {
                    params.setQuery_options(queryOptions);
                }
                params.params.setSend_query_statistics_with_every_batch(
                        fragment.isTransferQueryStatisticsWithEveryBatch());
                params.params.setInstances_number(hostToNumbers.get(instanceExecParams.get(i).host));
                // For broker load, the ConnectContext.get() is null
                if (connectContext != null) {
                    SessionVariable sessionVariable = connectContext.getSessionVariable();

                    if (isEnablePipelineEngine) {
                        params.setIs_pipeline(true);
                        params.getQuery_options().setBatch_size(SessionVariable.PIPELINE_BATCH_SIZE);
                        params.setPipeline_dop(fragment.getPipelineDop());

                        boolean enableResourceGroup = sessionVariable.isEnableResourceGroup();
                        params.setEnable_resource_group(enableResourceGroup);
                        if (enableResourceGroup && workGroup != null) {
                            params.setWorkgroup(workGroup.toThrift());
                        }
                    }

                    params.params.setEnable_exchange_pass_through(sessionVariable.isEnableExchangePassThrough());
                }
                paramsList.add(params);
            }
            return paramsList;
        }

        // Append range information
        // [tablet_id(version),tablet_id(version)]
        public void appendScanRange(StringBuilder sb, List<TScanRangeParams> params) {
            sb.append("range=[");
            int idx = 0;
            for (TScanRangeParams range : params) {
                TInternalScanRange internalScanRange = range.getScan_range().getInternal_scan_range();
                if (internalScanRange != null) {
                    if (idx++ != 0) {
                        sb.append(",");
                    }
                    sb.append("{tid=").append(internalScanRange.getTablet_id())
                            .append(",ver=").append(internalScanRange.getVersion()).append("}");
                }
                TEsScanRange esScanRange = range.getScan_range().getEs_scan_range();
                if (esScanRange != null) {
                    sb.append("{ index=").append(esScanRange.getIndex())
                            .append(", shardid=").append(esScanRange.getShard_id())
                            .append("}");
                }
            }
            sb.append("]");
        }

        public void appendTo(StringBuilder sb) {
            // append fragment
            sb.append("{plan=");
            fragment.getPlanRoot().appendTrace(sb);
            sb.append(",instance=[");
            // append instance
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                if (i != 0) {
                    sb.append(",");
                }
                TNetworkAddress address = instanceExecParams.get(i).host;
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        scanRangeAssignment.get(address);
                sb.append("{");
                sb.append("id=").append(DebugUtil.printId(instanceExecParams.get(i).instanceId));
                sb.append(",host=").append(instanceExecParams.get(i).host);
                if (scanRanges == null) {
                    sb.append("}");
                    continue;
                }
                sb.append(",range=[");
                int eIdx = 0;
                for (Map.Entry<Integer, List<TScanRangeParams>> entry : scanRanges.entrySet()) {
                    if (eIdx++ != 0) {
                        sb.append(",");
                    }
                    sb.append("id").append(entry.getKey()).append(",");
                    appendScanRange(sb, entry.getValue());
                }
                sb.append("]");
                sb.append("}");
            }
            sb.append("]"); // end of instances
            sb.append("}");
        }
    }

    private class NormalBackendSelector implements BackendSelector {
        private final ScanNode scanNode;
        private final List<TScanRangeLocations> locations;
        private final FragmentScanRangeAssignment assignment;

        public NormalBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                     FragmentScanRangeAssignment assignment) {
            this.scanNode = scanNode;
            this.locations = locations;
            this.assignment = assignment;
        }

        @Override
        public void computeScanRangeAssignment() throws Exception {
            HashMap<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
            for (TScanRangeLocations scanRangeLocations : locations) {
                // assign this scan range to the host w/ the fewest assigned bytes
                Long minAssignedBytes = Long.MAX_VALUE;
                TScanRangeLocation minLocation = null;
                for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    Long assignedBytes = findOrInsert(assignedBytesPerHost, location.server, 0L);
                    if (assignedBytes < minAssignedBytes) {
                        minAssignedBytes = assignedBytes;
                        minLocation = location;
                    }
                }
                assignedBytesPerHost.put(minLocation.server,
                        assignedBytesPerHost.get(minLocation.server) + 1);

                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostPort = SimpleScheduler.getHost(minLocation.backend_id,
                        scanRangeLocations.getLocations(),
                        idToBackend, backendIdRef);
                if (execHostPort == null) {
                    throw new UserException("Backend not found. Check if any backend is down or not");
                }
                recordUsedBackend(execHostPort, backendIdRef.getRef());

                Map<Integer, List<TScanRangeParams>> scanRanges = findOrInsert(
                        assignment, execHostPort, new HashMap<Integer, List<TScanRangeParams>>());
                List<TScanRangeParams> scanRangeParamsList = findOrInsert(
                        scanRanges, scanNode.getId().asInt(), new ArrayList<TScanRangeParams>());
                // add scan range
                TScanRangeParams scanRangeParams = new TScanRangeParams();
                scanRangeParams.scan_range = scanRangeLocations.scan_range;
                scanRangeParamsList.add(scanRangeParams);
            }
        }
    }

    private class RelicatedBackendSelector implements BackendSelector {
        private final ScanNode scanNode;
        private final List<TScanRangeLocations> locations;
        private final FragmentScanRangeAssignment assignment;

        public RelicatedBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                        FragmentScanRangeAssignment assignment) {
            this.scanNode = scanNode;
            this.locations = locations;
            this.assignment = assignment;
        }

        @Override
        public void computeScanRangeAssignment() {
            for (TScanRangeLocations scanRangeLocations : locations) {
                for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> kv : assignment.entrySet()) {
                    Map<Integer, List<TScanRangeParams>> scanRanges = kv.getValue();
                    List<TScanRangeParams> scanRangeParamsList = findOrInsert(
                            scanRanges, scanNode.getId().asInt(), new ArrayList<>());
                    // add scan range
                    TScanRangeParams scanRangeParams = new TScanRangeParams();
                    scanRangeParams.scan_range = scanRangeLocations.scan_range;
                    scanRangeParamsList.add(scanRangeParams);
                }
            }
            // If this fragment has bucket/colocate join, there need to fill fragmentIdBucketSeqToScanRangeMap here.
            // For example:
            //                       join(replicated)
            //                    /                    \
            //            join(bucket/colocate)       scan(C)
            //              /           \
            //            scan(A)         scan(B)
            // There are replicate join and bucket/colocate join in same fragment. for each bucket A,B used, we need to
            // add table C all tablet because of the character of the replicate join.
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());
            if (bucketSeqToScanRange != null && !bucketSeqToScanRange.isEmpty()) {
                for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> entry : bucketSeqToScanRange.entrySet()) {
                    for (TScanRangeLocations scanRangeLocations : locations) {
                        List<TScanRangeParams> scanRangeParamsList = findOrInsert(
                                entry.getValue(), scanNode.getId().asInt(), new ArrayList<>());
                        // add scan range
                        TScanRangeParams scanRangeParams = new TScanRangeParams();
                        scanRangeParams.scan_range = scanRangeLocations.scan_range;
                        scanRangeParamsList.add(scanRangeParams);
                    }
                }
            }
        }
    }

    private class ColocatedBackendSelector implements BackendSelector {
        private final OlapScanNode scanNode;
        private final FragmentScanRangeAssignment assignment;

        public ColocatedBackendSelector(OlapScanNode scanNode, FragmentScanRangeAssignment assignment) {
            this.scanNode = scanNode;
            this.assignment = assignment;
        }

        @Override
        public void computeScanRangeAssignment() throws Exception {
            PlanFragmentId fragmentId = scanNode.getFragmentId();
            if (!fragmentIdToSeqToAddressMap.containsKey(fragmentId)) {
                fragmentIdToSeqToAddressMap.put(fragmentId, Maps.newHashMap());
                fragmentIdBucketSeqToScanRangeMap.put(fragmentId, new BucketSeqToScanRange());
                fragmentIdToBackendIdBucketCountMap.put(fragmentId, new HashMap<>());
                fragmentIdToBucketNumMap.put(fragmentId,
                        scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum());
                if (scanNode.getSelectedPartitionIds().size() <= 1) {
                    for (Long pid : scanNode.getSelectedPartitionIds()) {
                        fragmentIdToBucketNumMap.put(fragmentId,
                                scanNode.getOlapTable().getPartition(pid).getDistributionInfo().getBucketNum());
                    }
                }
            }
            Map<Integer, TNetworkAddress> bucketSeqToAddress =
                    fragmentIdToSeqToAddressMap.get(fragmentId);
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

            for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
                //fill scanRangeParamsList
                List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), fragmentId, bucketSeq, idToBackend);
                }

                for (TScanRangeLocations location : locations) {
                    Map<Integer, List<TScanRangeParams>> scanRanges =
                            bucketSeqToScanRange.computeIfAbsent(bucketSeq, k -> Maps.newHashMap());

                    List<TScanRangeParams> scanRangeParamsList =
                            scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());

                    // add scan range
                    TScanRangeParams scanRangeParams = new TScanRangeParams();
                    scanRangeParams.scan_range = location.scan_range;
                    scanRangeParamsList.add(scanRangeParams);
                }
            }
            // Because of the right table will not send data to the bucket which has been pruned, the right join or full join will get wrong result.
            // So if this bucket shuffle is right join or full join, we need to add empty bucket scan range which is pruned by predicate.
            if (rightOrFullBucketShuffleFragmentIds.contains(fragmentId.asInt())) {
                int bucketNum = getFragmentBucketNum(fragmentId);

                for (int bucketSeq = 0; bucketSeq < bucketNum; ++bucketSeq) {
                    if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                        Reference<Long> backendIdRef = new Reference<>();
                        TNetworkAddress execHostport = SimpleScheduler.getHost(idToBackend, backendIdRef);
                        if (execHostport == null) {
                            throw new UserException("Backend not found. Check if any backend is down or not");
                        }
                        recordUsedBackend(execHostport, backendIdRef.getRef());
                        bucketSeqToAddress.put(bucketSeq, execHostport);
                    }
                    if (!bucketSeqToScanRange.containsKey(bucketSeq)) {
                        bucketSeqToScanRange.put(bucketSeq, Maps.newHashMap());
                        bucketSeqToScanRange.get(bucketSeq).put(scanNode.getId().asInt(), Lists.newArrayList());
                    }
                }
            }

            // use bucketSeqToScanRange to fill FragmentScanRangeAssignment
            for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> entry : bucketSeqToScanRange.entrySet()) {
                Integer bucketSeq = entry.getKey();
                // fill FragmentScanRangeAssignment only when there are scan id in the bucket
                if (entry.getValue().containsKey(scanNode.getId().asInt())) {
                    Map<Integer, List<TScanRangeParams>> scanRanges =
                            assignment.computeIfAbsent(bucketSeqToAddress.get(bucketSeq), k -> Maps.newHashMap());
                    List<TScanRangeParams> scanRangeParamsList =
                            scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
                    scanRangeParamsList.addAll(entry.getValue().get(scanNode.getId().asInt()));
                }
            }
        }

        // Make sure each host have average bucket to scan
        private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation,
                                                              PlanFragmentId fragmentId, Integer bucketSeq,
                                                              ImmutableMap<Long, Backend> idToBackend)
                throws Exception {
            Map<Long, Integer> buckendIdToBucketCountMap = fragmentIdToBackendIdBucketCountMap.get(fragmentId);
            int maxBucketNum = Integer.MAX_VALUE;
            long buckendId = Long.MAX_VALUE;
            for (TScanRangeLocation location : seqLocation.locations) {
                if (buckendIdToBucketCountMap.containsKey(location.backend_id)) {
                    if (buckendIdToBucketCountMap.get(location.backend_id) < maxBucketNum) {
                        maxBucketNum = buckendIdToBucketCountMap.get(location.backend_id);
                        buckendId = location.backend_id;
                    }
                } else {
                    buckendId = location.backend_id;
                    buckendIdToBucketCountMap.put(buckendId, 0);
                    break;
                }
            }

            buckendIdToBucketCountMap.put(buckendId, buckendIdToBucketCountMap.get(buckendId) + 1);
            Reference<Long> backendIdRef = new Reference<Long>();
            TNetworkAddress execHostPort =
                    SimpleScheduler.getHost(buckendId, seqLocation.locations, idToBackend, backendIdRef);
            if (execHostPort == null) {
                throw new UserException("Backend not found. Check if any backend is down or not");
            }

            recordUsedBackend(execHostPort, backendIdRef.getRef());
            fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
        }
    }

    /**
     * Hybrid backend selector for hive table.
     * Support hybrid and independent deployment with datanode.
     * <p>
     * Assign scan ranges to backend:
     * 1. local backend first,
     * 2. and smallest assigned scan ranges num or scan bytes.
     * <p>
     * If force_schedule_local variable is set, HybridBackendSelector will force to
     * assign scan ranges to local backend if there has one.
     */
    private class HDFSBackendSelector implements BackendSelector {
        // be -> assigned scans
        // type:
        //     SCAN_RANGE_NUM: assigned scan range num
        //     SCAN_DATA_SIZE: assigned scan data size
        Map<Backend, Long> assignedScansPerBe = Maps.newHashMap();
        // be host -> bes
        Multimap<String, Backend> hostToBes = HashMultimap.create();
        private final ScanNode scanNode;
        private final List<TScanRangeLocations> locations;
        private final FragmentScanRangeAssignment assignment;
        private final ScanRangeAssignType assignType;
        // for SCAN_DATA_SIZE assign type
        private List<Long> scanRangesBytes = Lists.newArrayList();
        private final List<Long> remoteScanRangesBytes = Lists.newArrayList();

        private final Set<Long> usedBackendIDs;
        // TODO: disk stats

        public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                   FragmentScanRangeAssignment assignment, ScanRangeAssignType assignType,
                                   Set<Long> usedBackendIDs) {
            this.scanNode = scanNode;
            this.locations = locations;
            this.assignment = assignment;
            this.assignType = assignType;
            this.usedBackendIDs = usedBackendIDs;
        }

        /**
         * Must be set when using SCAN_DATA_SIZE assign type
         *
         * @param scanRangesBytes: has the same size with locations
         */
        public void setScanRangesBytes(List<Long> scanRangesBytes) {
            this.scanRangesBytes = scanRangesBytes;
        }

        @Override
        public void computeScanRangeAssignment() throws Exception {
            Preconditions.checkArgument(assignType != ScanRangeAssignType.SCAN_DATA_SIZE
                    || locations.size() == scanRangesBytes.size());

            for (Backend backend : idToBackend.values()) {
                if (!backend.isAlive() || SimpleScheduler.isInBlacklist(backend.getId())) {
                    continue;
                }

                assignedScansPerBe.put(backend, 0L);
                hostToBes.put(backend.getHost(), backend);
            }
            if (hostToBes.isEmpty()) {
                throw new UserException("Backend not found. Check if any backend is down or not");
            }

            // total scans / alive bes
            long avgScansPerBe = -1;
            if (!forceScheduleLocal) {
                int numBes = assignedScansPerBe.size();
                if (assignType == ScanRangeAssignType.SCAN_DATA_SIZE) {
                    long totalBytes = 0L;
                    for (long scanRangeBytes : scanRangesBytes) {
                        totalBytes += scanRangeBytes;
                    }
                    avgScansPerBe = totalBytes / numBes + (totalBytes % numBes == 0 ? 0 : 1);
                } else {
                    // SCAN_RANGE_NUM
                    int numLocations = locations.size();
                    avgScansPerBe = numLocations / numBes + (numLocations % numBes == 0 ? 0 : 1);
                }
            }

            List<TScanRangeLocations> remoteScanRangeLocations = Lists.newArrayList();
            for (int i = 0; i < locations.size(); ++i) {
                TScanRangeLocations scanRangeLocations = locations.get(i);
                long minAssignedScanRanges = Long.MAX_VALUE;
                Backend minBe = null;
                for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    Collection<Backend> bes = hostToBes.get(location.getServer().getHostname());
                    if (bes == null || bes.isEmpty()) {
                        continue;
                    }
                    for (Backend backend : bes) {
                        long assignedScanRanges = assignedScansPerBe.get(backend);
                        if (!forceScheduleLocal && assignedScanRanges >= avgScansPerBe) {
                            continue;
                        }
                        if (assignedScanRanges < minAssignedScanRanges) {
                            minAssignedScanRanges = assignedScanRanges;
                            minBe = backend;
                        }
                    }
                }
                if (minBe == null) {
                    remoteScanRangeLocations.add(scanRangeLocations);
                    if (assignType == ScanRangeAssignType.SCAN_DATA_SIZE) {
                        remoteScanRangesBytes.add(scanRangesBytes.get(i));
                    }
                    continue;
                }
                long scansToAdd = (assignType == ScanRangeAssignType.SCAN_DATA_SIZE ? scanRangesBytes.get(i) : 1);
                recordScanRangeAssignment(minBe, scanRangeLocations, scansToAdd);
            }

            if (remoteScanRangeLocations.isEmpty()) {
                return;
            }

            Preconditions.checkArgument(assignType != ScanRangeAssignType.SCAN_DATA_SIZE
                    || remoteScanRangeLocations.size() == remoteScanRangesBytes.size());
            for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
                TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
                long minAssignedScanRanges = Long.MAX_VALUE;
                Backend minBe = null;
                for (Map.Entry<Backend, Long> entry : assignedScansPerBe.entrySet()) {
                    Backend backend = entry.getKey();
                    long assignedScanRanges = entry.getValue();
                    if (assignedScanRanges < minAssignedScanRanges) {
                        minAssignedScanRanges = assignedScanRanges;
                        minBe = backend;
                    }
                }
                long scansToAdd = (assignType == ScanRangeAssignType.SCAN_DATA_SIZE ? remoteScanRangesBytes.get(i) : 1);
                recordScanRangeAssignment(minBe, scanRangeLocations, scansToAdd);
            }
        }

        private void recordScanRangeAssignment(Backend minBe, TScanRangeLocations scanRangeLocations,
                                               long addedScans) {
            TNetworkAddress minBeAddress = new TNetworkAddress(minBe.getHost(), minBe.getBePort());
            usedBackendIDs.add(minBe.getId());
            addressToBackendID.put(minBeAddress, minBe.getId());

            // update statistic
            assignedScansPerBe.put(minBe, assignedScansPerBe.get(minBe) + addedScans);

            // add in assignment
            Map<Integer, List<TScanRangeParams>> scanRanges = findOrInsert(
                    assignment, minBeAddress, new HashMap<>());
            List<TScanRangeParams> scanRangeParamsList = findOrInsert(
                    scanRanges, scanNode.getId().asInt(), new ArrayList<TScanRangeParams>());
            // add scan range params
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = scanRangeLocations.scan_range;
            scanRangeParamsList.add(scanRangeParams);
        }
    }

    /**
     * Whether it can use pipeline engine.
     * @param connectContext It is null for broker broker export.
     * @param fragments All the fragments need to execute.
     * @return true if enabling pipeline in the session variable and all the fragments can use pipeline,
     * otherwise false.
     */
    private boolean canUsePipeline(ConnectContext connectContext, List<PlanFragment> fragments) {
        return connectContext != null &&
                connectContext.getSessionVariable().isEnablePipelineEngine() &&
                fragments.stream().allMatch(PlanFragment::canUsePipeline);
    }

    private void recordUsedBackend(TNetworkAddress addr, Long backendID) {
        usedBackendIDs.add(backendID);
        addressToBackendID.put(addr, backendID);
    }
}

