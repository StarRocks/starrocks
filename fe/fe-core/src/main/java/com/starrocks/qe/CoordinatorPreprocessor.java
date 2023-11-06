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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.DnsCache;
import com.starrocks.common.util.ListUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.MultiCastDataSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.planner.UnionNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TAdaptiveDopParam;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TEsScanRange;
import com.starrocks.thrift.TExecBatchPlanFragmentsParams;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFunctionVersion;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryQueueOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TRuntimeFilterParams;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.TINY_SCALE_ROWS_LIMIT;

public class CoordinatorPreprocessor {
    private static final Logger LOG = LogManager.getLogger(CoordinatorPreprocessor.class);
    private static final String LOCAL_IP = FrontendOptions.getLocalHostAddress();
    private static final int BUCKET_ABSENT = 2147483647;
    static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Random random = new Random();

    private TNetworkAddress coordAddress;
    private TUniqueId queryId;
    private final ConnectContext connectContext;
    private final TQueryGlobals queryGlobals;
    private final TQueryOptions queryOptions;

    private final boolean usePipeline;
    // if it has compute node, hasComputeNode is true
    private boolean hasComputeNode = false;
    // when use compute node, usedComputeNode is true,
    // if hasComputeNode but preferComputeNode is false and no hdfsScanNode, usedComputeNode still false
    private boolean usedComputeNode = false;

    private final Set<Integer> colocateFragmentIds = new HashSet<>();
    private final Set<Integer> replicateFragmentIds = new HashSet<>();
    private final Set<Integer> replicateScanIds = new HashSet<>();
    private final Set<Integer> bucketShuffleFragmentIds = new HashSet<>();
    private final Set<Integer> rightOrFullBucketShuffleFragmentIds = new HashSet<>();
    private final Set<TUniqueId> instanceIds = Sets.newHashSet();

    private final TDescriptorTable descriptorTable;
    private final List<PlanFragment> fragments;
    private final List<ScanNode> scanNodes;

    // populated in computeFragmentExecParams()
    private final Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();
    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
    // fragment_id -> < bucket_seq -> < scannode_id -> scan_range_params >>
    private final Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = Maps.newHashMap();
    // fragment_id -> bucket_num
    private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap = Maps.newHashMap();
    // fragment_id -> < be_id -> bucket_count >
    private final Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBackendIdBucketCountMap = Maps.newHashMap();
    private final Map<PlanFragmentId, List<Integer>> fragmentIdToSeqToInstanceMap = Maps.newHashMap();

    // used only by channel stream load, records the mapping from channel id to target BE's address
    private final Map<Integer, TNetworkAddress> channelIdToBEHTTP = Maps.newHashMap();
    private final Map<Integer, TNetworkAddress> channelIdToBEPort = Maps.newHashMap();
    // used only by channel stream load, records the mapping from be port to be's webserver port
    private final Map<TNetworkAddress, TNetworkAddress> bePortToBeWebServerPort = Maps.newHashMap();

    // backends which this query will use
    private ImmutableMap<Long, ComputeNode> idToBackend;
    // compute node which this query will use
    private ImmutableMap<Long, ComputeNode> idToComputeNode;

    // save of related backends of this query
    private final Set<Long> usedBackendIDs = Sets.newConcurrentHashSet();
    private final Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();

    // Resource group
    private final TWorkGroup resourceGroup;

    private boolean enableQueue = false;
    private boolean needCheckQueued = false;
    private boolean enableGroupLevelQueue = false;

    public CoordinatorPreprocessor(TUniqueId queryId, ConnectContext context, List<PlanFragment> fragments,
                                   List<ScanNode> scanNodes, TDescriptorTable descriptorTable,
                                   TQueryGlobals queryGlobals, TQueryOptions queryOptions) {
        this.connectContext = context;
        this.queryId = queryId;
        this.descriptorTable = descriptorTable;
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryGlobals = queryGlobals;
        this.queryOptions = queryOptions;
        this.usePipeline = canUsePipeline(this.connectContext, this.fragments);

        // prepare workgroup
        resourceGroup = prepareResourceGroup(connectContext,
                queryOptions.getQuery_type() == TQueryType.LOAD ? ResourceGroupClassifier.QueryType.INSERT
                        : ResourceGroupClassifier.QueryType.SELECT);
    }

    @VisibleForTesting
    CoordinatorPreprocessor(List<PlanFragment> fragments, List<ScanNode> scanNodes) {
        this.scanNodes = scanNodes;
        this.connectContext = StatisticUtils.buildConnectContext();
        this.queryId = connectContext.getExecutionId();
        this.queryGlobals =
                genQueryGlobals(System.currentTimeMillis(), connectContext.getSessionVariable().getTimeZone());
        this.queryOptions = connectContext.getSessionVariable().toThrift();
        this.usePipeline = true;
        this.descriptorTable = null;
        this.fragments = fragments;

        this.idToComputeNode = buildComputeNodeInfo();
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            this.idToBackend = this.idToComputeNode;
        } else {
            this.idToBackend = ImmutableMap.copyOf(GlobalStateMgr.getCurrentSystemInfo().getIdToBackend());
        }

        Map<PlanFragmentId, PlanFragment> fragmentMap =
                fragments.stream().collect(Collectors.toMap(PlanFragment::getFragmentId, x -> x));
        for (ScanNode scan : scanNodes) {
            PlanFragmentId id = scan.getFragmentId();
            PlanFragment fragment = fragmentMap.get(id);
            if (fragment == null) {
                // Fake a fragment for this node
                fragment = new PlanFragment(id, scan, DataPartition.RANDOM);
            }
            fragmentExecParamsMap.put(scan.getFragmentId(), new FragmentExecParams(fragment));
        }

        // prepare workgroup
        resourceGroup = prepareResourceGroup(connectContext,
                queryOptions.getQuery_type() == TQueryType.LOAD ? ResourceGroupClassifier.QueryType.INSERT
                        : ResourceGroupClassifier.QueryType.SELECT);
    }

    public static TQueryGlobals genQueryGlobals(long startTime, String timezone) {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        String nowString = DATE_FORMAT.format(Instant.ofEpochMilli(startTime).atZone(ZoneId.of(timezone)));
        queryGlobals.setNow_string(nowString);
        queryGlobals.setTimestamp_ms(startTime);
        if (timezone.equals("CST")) {
            queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            queryGlobals.setTime_zone(timezone);
        }
        return queryGlobals;
    }

    public TNetworkAddress getCoordAddress() {
        return coordAddress;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public boolean isUsePipeline() {
        return usePipeline;
    }

    public TQueryGlobals getQueryGlobals() {
        return queryGlobals;
    }

    public TQueryOptions getQueryOptions() {
        return queryOptions;
    }

    public Set<Integer> getColocateFragmentIds() {
        return colocateFragmentIds;
    }

    public Set<Integer> getReplicateFragmentIds() {
        return replicateFragmentIds;
    }

    public Set<Integer> getReplicateScanIds() {
        return replicateScanIds;
    }

    public Set<Integer> getBucketShuffleFragmentIds() {
        return bucketShuffleFragmentIds;
    }

    public Set<Integer> getRightOrFullBucketShuffleFragmentIds() {
        return rightOrFullBucketShuffleFragmentIds;
    }

    public Set<TUniqueId> getInstanceIds() {
        return instanceIds;
    }

    public TDescriptorTable getDescriptorTable() {
        return descriptorTable;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public Map<PlanFragmentId, FragmentExecParams> getFragmentExecParamsMap() {
        return fragmentExecParamsMap;
    }

    public Map<PlanFragmentId, Map<Integer, TNetworkAddress>> getFragmentIdToSeqToAddressMap() {
        return fragmentIdToSeqToAddressMap;
    }

    public Map<PlanFragmentId, BucketSeqToScanRange> getFragmentIdBucketSeqToScanRangeMap() {
        return fragmentIdBucketSeqToScanRangeMap;
    }

    public Map<PlanFragmentId, Integer> getFragmentIdToBucketNumMap() {
        return fragmentIdToBucketNumMap;
    }

    public Map<PlanFragmentId, Map<Long, Integer>> getFragmentIdToBackendIdBucketCountMap() {
        return fragmentIdToBackendIdBucketCountMap;
    }

    public Map<PlanFragmentId, List<Integer>> getFragmentIdToSeqToInstanceMap() {
        return fragmentIdToSeqToInstanceMap;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTP() {
        return channelIdToBEHTTP;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPort() {
        return channelIdToBEPort;
    }

    public Map<TNetworkAddress, TNetworkAddress> getBePortToBeWebServerPort() {
        return bePortToBeWebServerPort;
    }

    public ImmutableMap<Long, ComputeNode> getIdToBackend() {
        return idToBackend;
    }

    public ImmutableMap<Long, ComputeNode> getIdToComputeNode() {
        return idToComputeNode;
    }

    public TNetworkAddress getBrpcAddress(TNetworkAddress beAddress) {
        long beId = Preconditions.checkNotNull(addressToBackendID.get(beAddress),
                "backend not found: " + beAddress);
        ComputeNode be = Preconditions.checkNotNull(idToBackend.get(beId),
                "backend not found: " + beId);
        return be.getBrpcAddress();
    }

    public boolean isHasComputeNode() {
        return hasComputeNode;
    }

    public boolean isUsedComputeNode() {
        return usedComputeNode;
    }

    public Set<Long> getUsedBackendIDs() {
        return usedBackendIDs;
    }

    public Map<TNetworkAddress, Long> getAddressToBackendID() {
        return addressToBackendID;
    }

    public TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public boolean isEnableQueue() {
        return enableQueue;
    }

    public void setEnableQueue(boolean enableQueue) {
        this.enableQueue = enableQueue;
    }

    public boolean isNeedCheckQueued() {
        return needCheckQueued;
    }

    public void setNeedCheckQueued(boolean needCheckQueued) {
        this.needCheckQueued = needCheckQueued;
    }

    public boolean isEnableGroupLevelQueue() {
        return enableGroupLevelQueue;
    }

    public void setEnableGroupLevelQueue(boolean enableGroupLevelQueue) {
        this.enableGroupLevelQueue = enableGroupLevelQueue;
    }

    public Map<TNetworkAddress, Integer> getHostToNumbers() {
        return hostToNumbers;
    }

    public boolean isLoadType() {
        return queryOptions.getQuery_type() == TQueryType.LOAD;
    }

    public void prepareExec() throws Exception {
        // prepare information
        resetFragmentState();
        prepareFragments();

        computeScanRangeAssignment();
        computeFragmentExecParams();
        traceInstance();
        computeBeInstanceNumbers();
    }

    /**
     * Reset state of all the fragments set in Coordinator, when retrying the same query with the fragments.
     */
    private void resetFragmentState() {
        for (PlanFragment fragment : fragments) {
            if (fragment instanceof MultiCastPlanFragment) {
                MultiCastDataSink multiSink = (MultiCastDataSink) fragment.getSink();
                for (List<TPlanFragmentDestination> destination : multiSink.getDestinations()) {
                    destination.clear();
                }
            }
        }
    }

    @VisibleForTesting
    void prepareFragments() {
        for (PlanFragment fragment : fragments) {
            fragmentExecParamsMap.put(fragment.getFragmentId(), new FragmentExecParams(fragment));
        }

        coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.idToComputeNode = buildComputeNodeInfo();
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            this.idToBackend = this.idToComputeNode;
        } else {
            this.idToBackend = ImmutableMap.copyOf(GlobalStateMgr.getCurrentSystemInfo().getIdToBackend());
        }

        //if it has compute node and contains hdfsScanNode,will use compute node,even though preferComputeNode is false
        boolean preferComputeNode = connectContext.getSessionVariable().isPreferComputeNode();
        if (idToComputeNode != null && idToComputeNode.size() > 0) {
            hasComputeNode = true;
            if (preferComputeNode || RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
                usedComputeNode = true;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("idToBackend size={}", idToBackend.size());
            for (Map.Entry<Long, ComputeNode> entry : idToBackend.entrySet()) {
                Long backendID = entry.getKey();
                ComputeNode backend = entry.getValue();
                LOG.debug("backend: {}-{}-{}", backendID, backend.getHost(), backend.getBePort());
            }

            LOG.debug("idToComputeNode: {}", idToComputeNode);
        }
    }

    private ImmutableMap<Long, ComputeNode> buildComputeNodeInfo() {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            return GlobalStateMgr.getCurrentWarehouseMgr().getComputeNodesFromWarehouse();
        }

        ImmutableMap<Long, ComputeNode> idToComputeNode
                = ImmutableMap.copyOf(GlobalStateMgr.getCurrentSystemInfo().getIdComputeNode());

        int useComputeNodeNumber = connectContext.getSessionVariable().getUseComputeNodes();
        if (useComputeNodeNumber < 0
                || useComputeNodeNumber >= idToComputeNode.size()) {
            return idToComputeNode;
        } else {
            Map<Long, ComputeNode> computeNodes = new HashMap<>();
            for (int i = 0; i < useComputeNodeNumber; i++) {
                ComputeNode computeNode = SimpleScheduler.getComputeNode(idToComputeNode);
                if (computeNode == null) {
                    continue;
                }
                computeNodes.put(computeNode.getId(), computeNode);
            }
            return ImmutableMap.copyOf(computeNodes);
        }
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

    private void recordUsedBackend(TNetworkAddress addr, Long backendID) {
        if (this.queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD &&
                !bePortToBeWebServerPort.containsKey(addr)) {
            ComputeNode backend = idToBackend.get(backendID);
            bePortToBeWebServerPort.put(addr, new TNetworkAddress(backend.getHost(), backend.getHttpPort()));
        }
        usedBackendIDs.add(backendID);
        addressToBackendID.put(addr, backendID);
    }

    /**
     * Whether it can use pipeline engine.
     *
     * @param connectContext It is null for broker broker export.
     * @param fragments      All the fragments need to execute.
     * @return true if enabling pipeline in the session variable and all the fragments can use pipeline,
     * otherwise false.
     */
    private boolean canUsePipeline(ConnectContext connectContext, List<PlanFragment> fragments) {
        return connectContext != null &&
                connectContext.getSessionVariable().isEnablePipelineEngine() &&
                fragments.stream().allMatch(PlanFragment::canUsePipeline);
    }

    /**
     * Split scan range params into groupNum groups by each group's row count.
     */
    private List<List<TScanRangeParams>> splitScanRangeParamByRowCount(List<TScanRangeParams> scanRangeParams, int groupNum) {
        List<List<TScanRangeParams>> result = new ArrayList<>(groupNum);
        for (int i = 0; i < groupNum; i++) {
            result.add(new ArrayList<>());
        }
        long[] dataSizePerGroup = new long[groupNum];
        for (TScanRangeParams scanRangeParam : scanRangeParams) {
            int minIndex = 0;
            long minDataSize = dataSizePerGroup[0];
            for (int i = 1; i < groupNum; i++) {
                if (dataSizePerGroup[i] < minDataSize) {
                    minIndex = i;
                    minDataSize = dataSizePerGroup[i];
                }
            }
            dataSizePerGroup[minIndex] += scanRangeParam.getScan_range().getInternal_scan_range().getRow_count();
            result.get(minIndex).add(scanRangeParam);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("dataSizePerGroup: {}", dataSizePerGroup);
        }

        return result;
    }

    // For each fragment in fragments, computes hosts on which to run the instances
    // and stores result in fragmentExecParams.hosts.
    private void computeFragmentHosts() throws Exception {
        // compute hosts of producer fragment before those of consumer fragment(s),
        // the latter might inherit the set of hosts from the former
        // compute hosts *bottom up*.
        boolean isGatherOutput = fragments.get(0).getDataPartition() == DataPartition.UNPARTITIONED;

        for (int i = fragments.size() - 1; i >= 0; --i) {
            PlanFragment fragment = fragments.get(i);
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

            boolean dopAdaptionEnabled = usePipeline &&
                    connectContext.getSessionVariable().isEnablePipelineAdaptiveDop();

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
                TNetworkAddress execHostport;
                if (usedComputeNode) {
                    execHostport = SimpleScheduler.getComputeNodeHost(this.idToComputeNode, backendIdRef);
                } else {
                    execHostport = SimpleScheduler.getBackendHost(this.idToBackend, backendIdRef);
                }
                if (execHostport == null) {
                    String msg = FeConstants.getNodeNotFoundError(usedComputeNode);
                    LOG.warn("DataPartition UNPARTITIONED. " + msg);
                    throw new UserException(msg + backendInfosString(usedComputeNode));
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
                final Set<TNetworkAddress> hostSet = Sets.newHashSet();
                final Set<TNetworkAddress> childHosts = Sets.newHashSet();
                // don't use all nodes in shared_data running mode to avoid heavy exchange cost
                if (connectContext.getSessionVariable().isPreferComputeNode() && hasComputeNode) {
                    Map<Long, ComputeNode> candidates = getAliveNodes(idToComputeNode);
                    candidates.values().stream().forEach(e -> childHosts.add(e.getAddress()));
                    // make olapScan maxParallelism equals prefer compute node number
                    List<Pair<Long, TNetworkAddress>> chosenNodes = adaptiveChooseNodes(fragment, candidates, childHosts);
                    chosenNodes.stream().forEach(e -> {
                        hostSet.add(e.second);
                        recordUsedBackend(e.second, e.first);
                    });
                    maxParallelism = hostSet.size() * fragment.getParallelExecNum();

                } else {
                    if (isUnionFragment(fragment) && isGatherOutput) {
                        // union fragment use all children's host
                        // if output fragment isn't gather, all fragment must keep 1 instance

                        for (PlanFragment child : fragment.getChildren()) {
                            FragmentExecParams childParams = fragmentExecParamsMap.get(child.getFragmentId());
                            childParams.instanceExecParams.stream().map(e -> e.host).forEach(hostSet::add);
                        }
                        //make olapScan maxParallelism equals prefer compute node number
                        maxParallelism = hostSet.size() * fragment.getParallelExecNum();
                    } else {
                        for (FInstanceExecParam execParams : maxParallelismFragmentExecParams.instanceExecParams) {
                            childHosts.add(execParams.host);
                        }
                        List<Pair<Long, TNetworkAddress>> chosenNodes = adaptiveChooseNodes(fragment,
                                getAliveNodes(idToBackend), childHosts);

                        chosenNodes.stream().forEach(e -> {
                            hostSet.add(e.second);
                            recordUsedBackend(e.second, e.first);
                        });
                    }
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
                    Collections.shuffle(hosts, random);

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
                Collections.shuffle(params.instanceExecParams, random);

                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            int parallelExecInstanceNum = fragment.getParallelExecNum();
            int pipelineDop = fragment.getPipelineDop();
            boolean hasColocate = (isColocateFragment(fragment.getPlanRoot()) &&
                    fragmentIdToSeqToAddressMap.containsKey(fragment.getFragmentId())
                    && fragmentIdToSeqToAddressMap.get(fragment.getFragmentId()).size() > 0);
            boolean hasBucketShuffle = isBucketShuffleJoin(fragment.getFragmentId().asInt());

            if (hasColocate || hasBucketShuffle) {
                computeColocatedJoinInstanceParam(fragmentIdToSeqToAddressMap.get(fragment.getFragmentId()),
                        fragmentIdBucketSeqToScanRangeMap.get(fragment.getFragmentId()),
                        parallelExecInstanceNum, pipelineDop, usePipeline, params);
                computeBucketSeq2InstanceOrdinal(params, fragmentIdToBucketNumMap.get(fragment.getFragmentId()));
            } else {
                boolean assignScanRangesPerDriverSeq = usePipeline &&
                        (fragment.isAssignScanRangesPerDriverSeq() || fragment.isForceAssignScanRangesPerDriverSeq());
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
                            params.instanceExecParams.add(instanceParam);

                            boolean assignPerDriverSeq = assignScanRangesPerDriverSeq &&
                                    (enableAssignScanRangesPerDriverSeq(scanRangeParams, pipelineDop)
                                            || fragment.isForceAssignScanRangesPerDriverSeq());
                            if (!assignPerDriverSeq) {
                                instanceParam.perNodeScanRanges.put(planNodeId, scanRangeParams);
                            } else {
                                int expectedDop = Math.max(1, Math.min(pipelineDop, scanRangeParams.size()));
                                List<List<TScanRangeParams>> scanRangeParamsPerDriverSeq = null;
                                if (Config.enable_schedule_insert_query_by_row_count && isLoadType()
                                        && scanRangeParams.size() > 0
                                        && scanRangeParams.get(0).getScan_range().isSetInternal_scan_range()) {
                                    scanRangeParamsPerDriverSeq = splitScanRangeParamByRowCount(scanRangeParams, expectedDop);
                                } else {
                                    scanRangeParamsPerDriverSeq = ListUtil.splitBySize(scanRangeParams, expectedDop);
                                }
                                if (fragment.isForceAssignScanRangesPerDriverSeq() && scanRangeParamsPerDriverSeq.size()
                                        != pipelineDop) {
                                    fragment.setPipelineDop(scanRangeParamsPerDriverSeq.size());
                                }
                                instanceParam.pipelineDop = scanRangeParamsPerDriverSeq.size();
                                if (fragment.isUseRuntimeAdaptiveDop()) {
                                    instanceParam.pipelineDop = Utils.computeMinGEPower2(instanceParam.pipelineDop);
                                }
                                Map<Integer, List<TScanRangeParams>> scanRangesPerDriverSeq = new HashMap<>();
                                instanceParam.nodeToPerDriverSeqScanRanges.put(planNodeId, scanRangesPerDriverSeq);
                                for (int driverSeq = 0; driverSeq < scanRangeParamsPerDriverSeq.size(); ++driverSeq) {
                                    scanRangesPerDriverSeq.put(driverSeq, scanRangeParamsPerDriverSeq.get(driverSeq));
                                }
                                for (int driverSeq = scanRangeParamsPerDriverSeq.size();
                                        driverSeq < instanceParam.pipelineDop; ++driverSeq) {
                                    scanRangesPerDriverSeq.put(driverSeq, Lists.newArrayList());
                                }
                            }
                            if (this.queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD) {
                                for (TScanRangeParams scanRange : scanRangeParams) {
                                    int channelId = scanRange.scan_range.broker_scan_range.channel_id;
                                    TNetworkAddress beHttpAddress = bePortToBeWebServerPort.get(key);
                                    channelIdToBEHTTP.put(channelId, beHttpAddress);
                                    channelIdToBEPort.put(channelId, key);
                                }
                            }
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
                TNetworkAddress execHostport;
                if (usedComputeNode) {
                    execHostport = SimpleScheduler.getComputeNodeHost(this.idToComputeNode, backendIdRef);
                } else {
                    execHostport = SimpleScheduler.getBackendHost(this.idToBackend, backendIdRef);
                }
                if (execHostport == null) {
                    throw new UserException(
                            FeConstants.getNodeNotFoundError(usedComputeNode) + backendInfosString(usedComputeNode));
                }
                this.recordUsedBackend(execHostport, backendIdRef.getRef());
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport,
                        0, params);
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    private boolean isUnionFragment(PlanFragment fragment) {
        Deque<PlanNode> dq = new LinkedList<>();
        dq.offer(fragment.getPlanRoot());

        while (!dq.isEmpty()) {
            PlanNode nd = dq.poll();

            if (nd instanceof UnionNode) {
                return true;
            }
            if (!(nd instanceof ExchangeNode)) {
                nd.getChildren().forEach(dq::offer);
            }
        }
        return false;
    }

    public void computeBucketSeq2InstanceOrdinal(FragmentExecParams params, int numBuckets) {
        Integer[] bucketSeq2InstanceOrdinal = new Integer[numBuckets];
        // some buckets are pruned, so set the corresponding instance ordinal to BUCKET_ABSENT to indicate
        // absence of buckets.
        for (int bucketSeq = 0; bucketSeq < numBuckets; ++bucketSeq) {
            bucketSeq2InstanceOrdinal[bucketSeq] = BUCKET_ABSENT;
        }
        for (int i = 0; i < params.instanceExecParams.size(); ++i) {
            FInstanceExecParam instance = params.instanceExecParams.get(i);
            for (Integer bucketSeq : instance.bucketSeqToDriverSeq.keySet()) {
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

    // Returns the id of the leftmost node of any of the gives types in 'plan_root'.
    private PlanNode findLeftmostNode(PlanNode plan) {
        PlanNode newPlan = plan;
        while (newPlan.getChildren().size() != 0 && !(newPlan instanceof ExchangeNode)) {
            newPlan = newPlan.getChild(0);
        }
        return newPlan;
    }

    /**
     * This strategy assigns buckets to each driver sequence to avoid local shuffle.
     * If the number of buckets assigned to a fragment instance is less than pipelineDop,
     * pipelineDop will be set to num_buckets, which will reduce the degree of operator parallelism.
     * Therefore, when there are few buckets (<=pipeline_dop/2), insert local shuffle instead of using this strategy
     * to improve the degree of parallelism.
     *
     * @param scanRanges  The buckets assigned to a fragment instance.
     * @param pipelineDop The expected pipelineDop.
     * @return Whether using the strategy of assigning scanRanges to each driver sequence.
     */
    private <T> boolean enableAssignScanRangesPerDriverSeq(List<T> scanRanges, int pipelineDop) {
        boolean enableTabletInternalParallel =
                connectContext != null && connectContext.getSessionVariable().isEnableTabletInternalParallel();
        return !enableTabletInternalParallel || scanRanges.size() > pipelineDop / 2;
    }

    public void computeColocatedJoinInstanceParam(Map<Integer, TNetworkAddress> bucketSeqToAddress,
                                                  BucketSeqToScanRange bucketSeqToScanRange,
                                                  int parallelExecInstanceNum, int pipelineDop, boolean enablePipeline,
                                                  FragmentExecParams params) {
        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<TNetworkAddress, List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges =
                Maps.newHashMap();
        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> bucketSeqAndScanRanges : bucketSeqToScanRange.entrySet()) {
            TNetworkAddress address = bucketSeqToAddress.get(bucketSeqAndScanRanges.getKey());
            addressToScanRanges
                    .computeIfAbsent(address, k -> Lists.newArrayList())
                    .add(bucketSeqAndScanRanges);
        }

        boolean assignPerDriverSeq =
                enablePipeline && addressToScanRanges.values().stream()
                        .allMatch(scanRanges -> enableAssignScanRangesPerDriverSeq(scanRanges, pipelineDop));

        for (Map.Entry<TNetworkAddress, List<Map.Entry<Integer, Map<Integer,
                List<TScanRangeParams>>>>> addressScanRange : addressToScanRanges.entrySet()) {
            List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();

            int expectedInstanceNum = 1;
            if (parallelExecInstanceNum > 1) {
                // The scan instance num should not larger than the tablets num
                expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
            }

            // 2. split how many scanRange one instance should scan
            List<List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> scanRangesPerInstance =
                    ListUtil.splitBySize(scanRange, expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scan by instance
            for (List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>> scanRangePerInstance : scanRangesPerInstance) {
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), 0, params);
                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicateScanSet = new HashSet<>();

                int expectedDop = 1;
                if (pipelineDop > 1) {
                    expectedDop = Math.min(scanRangePerInstance.size(), pipelineDop);
                }
                List<List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> scanRangesPerDriverSeq =
                        ListUtil.splitBySize(scanRangePerInstance, expectedDop);

                if (assignPerDriverSeq) {
                    instanceParam.pipelineDop = scanRangesPerDriverSeq.size();
                    if (params.fragment.isUseRuntimeAdaptiveDop()) {
                        instanceParam.pipelineDop = Utils.computeMinGEPower2(instanceParam.pipelineDop);
                    }
                }

                for (int driverSeq = 0; driverSeq < scanRangesPerDriverSeq.size(); ++driverSeq) {
                    final int finalDriverSeq = driverSeq;
                    scanRangesPerDriverSeq.get(finalDriverSeq).forEach(bucketSeqAndScanRanges -> {
                        if (assignPerDriverSeq) {
                            instanceParam.addBucketSeqAndDriverSeq(bucketSeqAndScanRanges.getKey(), finalDriverSeq);
                        } else {
                            instanceParam.addBucketSeq(bucketSeqAndScanRanges.getKey());
                        }

                        bucketSeqAndScanRanges.getValue().forEach((scanId, scanRanges) -> {
                            List<TScanRangeParams> destScanRanges;
                            if (!assignPerDriverSeq) {
                                destScanRanges = instanceParam.perNodeScanRanges
                                        .computeIfAbsent(scanId, k -> new ArrayList<>());
                            } else {
                                destScanRanges = instanceParam.nodeToPerDriverSeqScanRanges
                                        .computeIfAbsent(scanId, k -> new HashMap<>())
                                        .computeIfAbsent(finalDriverSeq, k -> new ArrayList<>());
                            }

                            if (replicateScanIds.contains(scanId)) {
                                if (!instanceReplicateScanSet.contains(scanId)) {
                                    destScanRanges.addAll(scanRanges);
                                    instanceReplicateScanSet.add(scanId);
                                }
                            } else {
                                destScanRanges.addAll(scanRanges);
                            }
                        });
                    });
                }

                if (assignPerDriverSeq) {
                    instanceParam.nodeToPerDriverSeqScanRanges.forEach((scanId, perDriverSeqScanRanges) -> {
                        for (int driverSeq = 0; driverSeq < instanceParam.pipelineDop; ++driverSeq) {
                            perDriverSeqScanRanges.computeIfAbsent(driverSeq, k -> new ArrayList<>());
                        }
                    });
                }

                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    public ImmutableCollection<ComputeNode> getSelectorComputeNodes(boolean whenUseComputeNode) {
        if (whenUseComputeNode) {
            return idToComputeNode.values();
        } else {
            return ImmutableList.copyOf(idToBackend.values());
        }
    }

    public FragmentScanRangeAssignment getFragmentScanRangeAssignment(PlanFragmentId fragmentId) {
        return fragmentExecParamsMap.get(fragmentId).scanRangeAssignment;
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    @VisibleForTesting
    void computeScanRangeAssignment() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();

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
            if (scanNode instanceof SchemaScanNode) {
                BackendSelector selector = new NormalBackendSelector(scanNode, locations, assignment);
                selector.computeScanRangeAssignment();
            } else if ((scanNode instanceof HdfsScanNode) || (scanNode instanceof IcebergScanNode) ||
                    scanNode instanceof HudiScanNode || scanNode instanceof DeltaLakeScanNode ||
                    scanNode instanceof FileTableScanNode || scanNode instanceof PaimonScanNode) {

                HDFSBackendSelector selector =
                        new HDFSBackendSelector(scanNode, locations, assignment, addressToBackendID, usedBackendIDs,
                                getSelectorComputeNodes(hasComputeNode),
                                hasComputeNode,
                                sv.getForceScheduleLocal(),
                                sv.getHDFSBackendSelectorScanRangeShuffle());
                selector.computeScanRangeAssignment();
            } else {
                boolean hasColocate = isColocateFragment(scanNode.getFragment().getPlanRoot());
                boolean hasBucket =
                        isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());
                boolean hasReplicated = isReplicatedFragment(scanNode.getFragment().getPlanRoot());
                if (assignment.size() > 0 && hasReplicated && scanNode.canDoReplicatedJoin()) {
                    BackendSelector selector = new ReplicatedBackendSelector(scanNode, locations, assignment);
                    selector.computeScanRangeAssignment();
                    replicateScanIds.add(scanNode.getId().asInt());
                } else if (hasColocate || hasBucket) {
                    BackendSelector selector = new ColocatedBackendSelector((OlapScanNode) scanNode, assignment);
                    selector.computeScanRangeAssignment();
                } else {
                    BackendSelector selector = new NormalBackendSelector(scanNode, locations, assignment, isLoadType());
                    selector.computeScanRangeAssignment();
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(assignment.toDebugString());
            }
        }
    }

    // Because there is no DNS cache when compute node is in container.
    // Try to avoid flooding the DNS server, we translate the hostname to IP here.
    private TNetworkAddress toIpAddress(TNetworkAddress address) throws UnknownHostException {
        return DnsCache.lookup(address);
    }

    @VisibleForTesting
    void computeFragmentExecParams() throws Exception {
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

            // Set params for pipeline level shuffle.
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
                        Integer driverSeq = instanceExecParams.bucketSeqToDriverSeq.get(bucketSeq);
                        if (driverSeq != null) {
                            dest.fragment_instance_id = instanceExecParams.instanceId;
                            dest.server = toRpcHost(instanceExecParams.host);
                            dest.setBrpc_server(toIpAddress(SystemInfoService.toBrpcHost(instanceExecParams.host)));
                            if (driverSeq != FInstanceExecParam.ABSENT_DRIVER_SEQUENCE) {
                                dest.setPipeline_driver_sequence(driverSeq);
                            }
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
                    dest.setBrpc_server(toIpAddress(SystemInfoService.toBrpcHost(destParams.instanceExecParams.get(j).host)));
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
                DataStreamSink sink = multiSink.getDataStreamSinks().get(i);

                if (destFragment == null) {
                    continue;
                }
                FragmentExecParams destParams = fragmentExecParamsMap.get(destFragment.getFragmentId());

                // Set params for pipeline level shuffle.
                multi.getDestNode(i).setPartitionType(params.fragment.getOutputPartition().getType());
                sink.setExchDop(destFragment.getPipelineDop());

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
                            Integer driverSeq = instanceExecParams.bucketSeqToDriverSeq.get(bucketSeq);
                            if (driverSeq != null) {
                                dest.fragment_instance_id = instanceExecParams.instanceId;
                                dest.server = toRpcHost(instanceExecParams.host);
                                dest.setBrpc_server(SystemInfoService.toBrpcHost(instanceExecParams.host));
                                if (driverSeq != FInstanceExecParam.ABSENT_DRIVER_SEQUENCE) {
                                    dest.setPipeline_driver_sequence(driverSeq);
                                }
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
                        dest.setBrpc_server(SystemInfoService.toBrpcHost(destParams.instanceExecParams.get(j).host));
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

    private int getFragmentBucketNum(PlanFragmentId fragmentId) {
        return fragmentIdToBucketNumMap.get(fragmentId);
    }

    public TNetworkAddress toRpcHost(TNetworkAddress host) throws Exception {
        ComputeNode computeNode = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode =
                    GlobalStateMgr.getCurrentSystemInfo().getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
            }
        }
        return new TNetworkAddress(computeNode.getHost(), computeNode.getBeRpcPort());
    }

    private String backendInfosString(boolean chooseComputeNode) {
        if (chooseComputeNode) {
            String infoStr = "compute node: ";
            for (Map.Entry<Long, ComputeNode> entry : this.idToComputeNode.entrySet()) {
                Long backendID = entry.getKey();
                ComputeNode backend = entry.getValue();
                infoStr += String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID));
            }
            return infoStr;
        } else {
            if (MapUtils.isEmpty(this.idToBackend)) {
                return "";
            }
            StringBuilder infoStr = new StringBuilder("backend: ");
            for (Map.Entry<Long, ComputeNode> entry : this.idToBackend.entrySet()) {
                Long backendID = entry.getKey();
                ComputeNode backend = entry.getValue();
                infoStr.append(String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID)));
            }
            return infoStr.toString();
        }
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        if (this.queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD) {
            return channelIdToBEHTTP;
        }
        return null;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        if (this.queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD) {
            return channelIdToBEPort;
        }
        return null;
    }

    public static TWorkGroup prepareResourceGroup(ConnectContext connect, ResourceGroupClassifier.QueryType queryType) {
        if (connect == null || !connect.getSessionVariable().isEnableResourceGroup()) {
            return null;
        }
        SessionVariable sessionVariable = connect.getSessionVariable();
        TWorkGroup resourceGroup = null;

        // 1. try to use the resource group specified by the variable
        if (StringUtils.isNotEmpty(sessionVariable.getResourceGroup())) {
            String rgName = sessionVariable.getResourceGroup();
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByName(rgName);
            if (rgName.equalsIgnoreCase(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME)) {
                ResourceGroup defaultMVResourceGroup = new ResourceGroup();
                defaultMVResourceGroup.setId(ResourceGroup.DEFAULT_MV_WG_ID);
                defaultMVResourceGroup.setName(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME);
                defaultMVResourceGroup.setVersion(ResourceGroup.DEFAULT_MV_VERSION);
                resourceGroup = defaultMVResourceGroup.toThrift();
            }
        }

        // 2. try to use the resource group specified by workgroup_id
        long workgroupId = connect.getSessionVariable().getResourceGroupId();
        if (resourceGroup == null && workgroupId > 0) {
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByID(workgroupId);
        }

        // 3. if the specified resource group not exist try to use the default one
        if (resourceGroup == null) {
            Set<Long> dbIds = connect.getCurrentSqlDbIds();
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                    connect, queryType, dbIds);
        }

        if (resourceGroup != null) {
            connect.getAuditEventBuilder().setResourceGroup(resourceGroup.getName());
            connect.setResourceGroup(resourceGroup);
        } else {
            connect.getAuditEventBuilder().setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
        }

        return resourceGroup;
    }

    private List<Pair<Long, TNetworkAddress>> adaptiveChooseNodes(PlanFragment fragment, Map<Long, ComputeNode> candidates,
                                                                  Set<TNetworkAddress> childUsedHosts) {
        List<Pair<Long, TNetworkAddress>> childHosts = Lists.newArrayList();
        for (Map.Entry<Long, ComputeNode> entry : candidates.entrySet()) {
            TNetworkAddress address = entry.getValue().getAddress();
            if (childUsedHosts.contains(address)) {
                childHosts.add(Pair.create(entry.getKey(), address));
            }
        }

        // sometimes we may reserve the right fragment and add the children of left fragment
        // in the children list of right fragment like SHUFFLE_HASH_BUCKET plan, so we need sort
        // the list to ensure the most left child is at the 0 index position.
        List<PlanFragment> sortedFragments = fragment.getChildren().stream()
                .sorted(Comparator.comparing(e -> e.getPlanRoot().getId().asInt()))
                .collect(Collectors.toList());

        long maxOutputOfRightChild = sortedFragments.stream().skip(1)
                .map(e -> e.getPlanRoot().getCardinality()).reduce(Long::max)
                .orElse(fragment.getChild(0).getPlanRoot().getCardinality());
        long outputOfMostLeftChild = sortedFragments.get(0).getPlanRoot().getCardinality();

        long baseNodeNums = Math.max(1, maxOutputOfRightChild / TINY_SCALE_ROWS_LIMIT / fragment.getPipelineDop());
        double base = Math.max(Math.E, baseNodeNums);

        long amplifyFactor = Math.round(Math.max(1,
                Math.log(outputOfMostLeftChild / TINY_SCALE_ROWS_LIMIT / fragment.getPipelineDop()) / Math.log(base)));

        long nodeNums = Math.min(amplifyFactor * baseNodeNums, candidates.size());

        SessionVariableConstants.ChooseInstancesMode mode = connectContext.getSessionVariable()
                .getChooseExecuteInstancesMode();
        if (mode.enableIncreaseInstance() && nodeNums > childUsedHosts.size()) {
            for (Map.Entry<Long, ComputeNode> entry : candidates.entrySet()) {
                TNetworkAddress address = new TNetworkAddress(entry.getValue().getHost(), entry.getValue().getBePort());
                if (!childUsedHosts.contains(address)) {
                    childHosts.add(Pair.create(entry.getKey(), address));
                    if (childHosts.size() == nodeNums) {
                        break;
                    }
                }
            }
            return childHosts;
        } else if (mode.enableDecreaseInstance() && nodeNums < childUsedHosts.size()
                && candidates.size() >= Config.adaptive_choose_instances_threshold) {
            Collections.shuffle(childHosts, random);
            return childHosts.stream().limit(nodeNums).collect(Collectors.toList());
        } else {
            return childHosts;
        }
    }

    private Map<Long, ComputeNode> getAliveNodes(ImmutableMap<Long, ComputeNode> nodeMap) {
        Map<Long, ComputeNode> candidates = Maps.newHashMap();
        for (Map.Entry<Long, ComputeNode> entry : nodeMap.entrySet()) {
            ComputeNode computeNode = entry.getValue();
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }
            candidates.put(entry.getKey(), entry.getValue());
        }
        return candidates;
    }

    // fragment instance exec param, it is used to assemble
    // the per-instance TPlanFragmentExecParas, as a member of
    // FragmentExecParams
    public static class FInstanceExecParam {
        static final int ABSENT_PIPELINE_DOP = -1;
        static final int ABSENT_DRIVER_SEQUENCE = -1;

        TUniqueId instanceId;
        TNetworkAddress host;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();
        Map<Integer, Map<Integer, List<TScanRangeParams>>> nodeToPerDriverSeqScanRanges = Maps.newHashMap();

        int perFragmentInstanceIdx;

        Map<Integer, Integer> bucketSeqToDriverSeq = Maps.newHashMap();

        int backendNum;

        FragmentExecParams fragmentExecParams;

        int pipelineDop = ABSENT_PIPELINE_DOP;

        public void addBucketSeqAndDriverSeq(int bucketSeq, int driverSeq) {
            this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, driverSeq);
        }

        public void addBucketSeq(int bucketSeq) {
            this.bucketSeqToDriverSeq.putIfAbsent(bucketSeq, ABSENT_DRIVER_SEQUENCE);
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

        public boolean isSetPipelineDop() {
            return pipelineDop != ABSENT_PIPELINE_DOP;
        }

        public int getPipelineDop() {
            return pipelineDop;
        }

        public Map<Integer, Integer> getBucketSeqToDriverSeq() {
            return bucketSeqToDriverSeq;
        }

        public Map<Integer, List<TScanRangeParams>> getPerNodeScanRanges() {
            return perNodeScanRanges;
        }

        public Map<Integer, Map<Integer, List<TScanRangeParams>>> getNodeToPerDriverSeqScanRanges() {
            return nodeToPerDriverSeqScanRanges;
        }

        public int getBackendNum() {
            return backendNum;
        }

        public void setBackendNum(int backendNum) {
            this.backendNum = backendNum;
        }

        public TNetworkAddress getHost() {
            return host;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }
    }

    static class BucketSeqToScanRange
            extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {
    }

    // execution parameters for a single fragment,
    // per-fragment can have multiple FInstanceExecParam,
    // used to assemble TPlanFragmentExecParas
    public class FragmentExecParams {
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

        /**
         * Set the common fields of all the fragment instances to the destination common thrift params.
         *
         * @param commonParams           The destination common thrift params.
         * @param destHost               The destination host to delivery these instances.
         * @param descTable              The descriptor table, empty for the non-first instance
         *                               when enable pipeline and disable multi fragments in one request.
         * @param isEnablePipelineEngine Whether enable pipeline engine.
         */
        private void toThriftForCommonParams(TExecPlanFragmentParams commonParams,
                                             TNetworkAddress destHost, TDescriptorTable descTable,
                                             boolean isEnablePipelineEngine, int tableSinkTotalDop,
                                             boolean isEnableStreamPipeline) {
            boolean enablePipelineTableSinkDop = isEnablePipelineEngine &&
                    (fragment.hasOlapTableSink() || fragment.hasIcebergTableSink());
            commonParams.setProtocol_version(InternalServiceVersion.V1);
            commonParams.setFragment(fragment.toThrift());
            commonParams.setDesc_tbl(descTable);
            commonParams.setFunc_version(TFunctionVersion.RUNTIME_FILTER_SERIALIZE_VERSION_2.getValue());
            commonParams.setCoord(coordAddress);

            commonParams.setParams(new TPlanFragmentExecParams());
            commonParams.params.setUse_vectorized(true);
            commonParams.params.setQuery_id(queryId);
            commonParams.params.setInstances_number(hostToNumbers.get(destHost));
            commonParams.params.setDestinations(destinations);
            if (enablePipelineTableSinkDop) {
                commonParams.params.setNum_senders(tableSinkTotalDop);
            } else {
                commonParams.params.setNum_senders(instanceExecParams.size());
            }
            commonParams.setIs_stream_pipeline(isEnableStreamPipeline);
            commonParams.params.setPer_exch_num_senders(perExchNumSenders);
            if (runtimeFilterParams.isSetRuntime_filter_builder_number()) {
                commonParams.params.setRuntime_filter_params(runtimeFilterParams);
            }
            commonParams.params.setSend_query_statistics_with_every_batch(
                    fragment.isTransferQueryStatisticsWithEveryBatch());

            commonParams.setQuery_globals(queryGlobals);
            if (isEnablePipelineEngine) {
                commonParams.setQuery_options(new TQueryOptions(queryOptions));
            } else {
                commonParams.setQuery_options(queryOptions);
            }
            // For broker load, the ConnectContext.get() is null
            if (connectContext != null) {
                SessionVariable sessionVariable = connectContext.getSessionVariable();

                if (isEnablePipelineEngine) {
                    commonParams.setIs_pipeline(true);
                    commonParams.getQuery_options().setBatch_size(SessionVariable.PIPELINE_BATCH_SIZE);
                    commonParams.setEnable_shared_scan(sessionVariable.isEnableSharedScan());
                    commonParams.params.setEnable_exchange_pass_through(sessionVariable.isEnableExchangePassThrough());
                    commonParams.params.setEnable_exchange_perf(sessionVariable.isEnableExchangePerf());

                    commonParams.setEnable_resource_group(true);
                    if (resourceGroup != null) {
                        commonParams.setWorkgroup(resourceGroup);
                    }
                    if (fragment.isUseRuntimeAdaptiveDop()) {
                        commonParams.setAdaptive_dop_param(new TAdaptiveDopParam());
                        commonParams.adaptive_dop_param.setMax_block_rows_per_driver_seq(
                                sessionVariable.getAdaptiveDopMaxBlockRowsPerDriverSeq());
                        commonParams.adaptive_dop_param.setMax_output_amplification_factor(
                                sessionVariable.getAdaptiveDopMaxOutputAmplificationFactor());
                    }
                    if (isEnableQueue()) {
                        TQueryQueueOptions queryQueueOptions = new TQueryQueueOptions();
                        queryQueueOptions.setEnable_global_query_queue(isEnableQueue());
                        queryQueueOptions.setEnable_group_level_query_queue(isEnableGroupLevelQueue());

                        TQueryOptions queryOptions = commonParams.getQuery_options();
                        queryOptions.setQuery_queue_options(queryQueueOptions);
                    }
                }

            }
        }

        /**
         * Set the unique fields for a fragment instance to the destination unique thrift params, including:
         * - backend_num
         * - pipeline_dop (used when isEnablePipelineEngine is true)
         * - params.fragment_instance_id
         * - params.sender_id
         * - params.per_node_scan_ranges
         * - fragment.output_sink (only for MultiCastDataStreamSink and ExportSink)
         *
         * @param uniqueParams         The destination unique thrift params.
         * @param fragmentIndex        The index of this instance in this.instanceExecParams.
         * @param instanceExecParam    The instance param.
         * @param enablePipelineEngine Whether enable pipeline engine.
         */
        private void toThriftForUniqueParams(TExecPlanFragmentParams uniqueParams, int fragmentIndex,
                                             FInstanceExecParam instanceExecParam, boolean enablePipelineEngine,
                                             int accTabletSinkDop, int curTableSinkDop)
                throws Exception {
            // if pipeline is enable and current fragment contain olap table sink, in fe we will
            // calculate the number of all tablet sinks in advance and assign them to each fragment instance
            boolean enablePipelineTableSinkDop = enablePipelineEngine &&
                    (fragment.hasOlapTableSink() || fragment.hasIcebergTableSink());

            uniqueParams.setProtocol_version(InternalServiceVersion.V1);
            uniqueParams.setBackend_num(instanceExecParam.backendNum);
            if (enablePipelineEngine) {
                if (instanceExecParam.isSetPipelineDop()) {
                    uniqueParams.setPipeline_dop(instanceExecParam.pipelineDop);
                } else {
                    uniqueParams.setPipeline_dop(fragment.getPipelineDop());
                }
            }

            /// Set thrift fragment with the unique fields.

            // Add instance number in file name prefix when export job.
            if (fragment.getSink() instanceof ExportSink) {
                ExportSink exportSink = (ExportSink) fragment.getSink();
                if (exportSink.getFileNamePrefix() != null) {
                    exportSink.setFileNamePrefix(exportSink.getFileNamePrefix() + fragmentIndex + "_");
                }
            }
            if (!uniqueParams.isSetFragment()) {
                uniqueParams.setFragment(fragment.toThriftForUniqueFields());
            }
            /*
             * For MultiCastDataFragment, output only send to local, and the instance is keep
             * same with MultiCastDataFragment
             * */
            if (fragment instanceof MultiCastPlanFragment) {
                List<List<TPlanFragmentDestination>> multiFragmentDestinations =
                        uniqueParams.getFragment().getOutput_sink().getMulti_cast_stream_sink().getDestinations();
                List<List<TPlanFragmentDestination>> newDestinations = Lists.newArrayList();
                for (List<TPlanFragmentDestination> destinations : multiFragmentDestinations) {
                    Preconditions.checkState(instanceExecParams.size() == destinations.size());
                    TPlanFragmentDestination ndes = destinations.get(fragmentIndex);

                    Preconditions.checkState(ndes.getServer().equals(toRpcHost(instanceExecParam.host)));
                    newDestinations.add(Lists.newArrayList(ndes));
                }

                uniqueParams.getFragment().getOutput_sink().getMulti_cast_stream_sink()
                        .setDestinations(newDestinations);
            }

            if (!uniqueParams.isSetParams()) {
                uniqueParams.setParams(new TPlanFragmentExecParams());
            }
            uniqueParams.params.setFragment_instance_id(instanceExecParam.instanceId);

            Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
            if (scanRanges == null) {
                scanRanges = Maps.newHashMap();
            }
            uniqueParams.params.setPer_node_scan_ranges(scanRanges);
            uniqueParams.params.setNode_to_per_driver_seq_scan_ranges(instanceExecParam.nodeToPerDriverSeqScanRanges);

            if (enablePipelineTableSinkDop) {
                uniqueParams.params.setSender_id(accTabletSinkDop);
                uniqueParams.params.setPipeline_sink_dop(curTableSinkDop);
            } else {
                uniqueParams.params.setSender_id(fragmentIndex);
            }
        }

        /**
         * Fill required fields of thrift params with meaningless values.
         *
         * @param params The thrift params need to be filled required fields.
         */
        private void fillRequiredFieldsToThrift(TExecPlanFragmentParams params) {
            TPlanFragmentExecParams fragmentExecParams = params.getParams();

            if (!fragmentExecParams.isSetFragment_instance_id()) {
                fragmentExecParams.setFragment_instance_id(new TUniqueId(0, 0));
            }

            if (!fragmentExecParams.isSetInstances_number()) {
                fragmentExecParams.setInstances_number(0);
            }

            if (!fragmentExecParams.isSetSender_id()) {
                fragmentExecParams.setSender_id(0);
            }

            if (!fragmentExecParams.isSetPer_node_scan_ranges()) {
                fragmentExecParams.setPer_node_scan_ranges(Maps.newHashMap());
            }

            if (!fragmentExecParams.isSetPer_exch_num_senders()) {
                fragmentExecParams.setPer_exch_num_senders(Maps.newHashMap());
            }

            if (!fragmentExecParams.isSetQuery_id()) {
                fragmentExecParams.setQuery_id(new TUniqueId(0, 0));
            }
        }

        public List<TExecPlanFragmentParams> toThrift(Set<TUniqueId> inFlightInstanceIds,
                                                      TDescriptorTable descTable,
                                                      boolean enablePipelineEngine, int accTabletSinkDop,
                                                      int tableSinkTotalDop,
                                                      boolean isEnableStreamPipeline) throws Exception {
            boolean forceSetTableSinkDop = fragment.forceSetTableSinkDop();
            setBucketSeqToInstanceForRuntimeFilters();

            List<TExecPlanFragmentParams> paramsList = Lists.newArrayList();
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                if (!inFlightInstanceIds.contains(instanceExecParam.instanceId)) {
                    continue;
                }
                int curTableSinkDop = 0;
                if (forceSetTableSinkDop) {
                    DataSink dataSink = fragment.getSink();
                    int dop = fragment.getPipelineDop();
                    if (!(dataSink instanceof IcebergTableSink)) {
                        curTableSinkDop = dop;
                    } else {
                        int sessionVarSinkDop = ConnectContext.get().getSessionVariable().getPipelineSinkDop();
                        if (sessionVarSinkDop > 0) {
                            curTableSinkDop = Math.min(dop, sessionVarSinkDop);
                        } else {
                            curTableSinkDop = Math.min(dop, IcebergTableSink.ICEBERG_SINK_MAX_DOP);
                        }
                    }
                } else {
                    curTableSinkDop = instanceExecParam.getPipelineDop();
                }
                TExecPlanFragmentParams params = new TExecPlanFragmentParams();

                toThriftForCommonParams(params, instanceExecParam.getHost(), descTable, enablePipelineEngine,
                        tableSinkTotalDop, isEnableStreamPipeline);
                toThriftForUniqueParams(params, i, instanceExecParam, enablePipelineEngine,
                        accTabletSinkDop, curTableSinkDop);

                paramsList.add(params);
                accTabletSinkDop += curTableSinkDop;
            }
            return paramsList;
        }

        TExecBatchPlanFragmentsParams toThriftInBatch(
                Set<TUniqueId> inFlightInstanceIds, TNetworkAddress destHost, TDescriptorTable descTable,
                boolean enablePipelineEngine, int accTabletSinkDop,
                int tableSinkTotalDop) throws Exception {

            boolean forceSetTableSinkDop = fragment.forceSetTableSinkDop();

            setBucketSeqToInstanceForRuntimeFilters();

            TExecPlanFragmentParams commonParams = new TExecPlanFragmentParams();
            toThriftForCommonParams(commonParams, destHost, descTable, enablePipelineEngine, tableSinkTotalDop, false);
            fillRequiredFieldsToThrift(commonParams);

            List<TExecPlanFragmentParams> uniqueParamsList = Lists.newArrayList();
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                if (!inFlightInstanceIds.contains(instanceExecParam.instanceId)) {
                    continue;
                }
                int curTableSinkDop = 0;
                if (forceSetTableSinkDop) {
                    DataSink dataSink = fragment.getSink();
                    int dop = fragment.getPipelineDop();
                    if (!(dataSink instanceof IcebergTableSink)) {
                        curTableSinkDop = dop;
                    } else {
                        int sessionVarSinkDop = ConnectContext.get().getSessionVariable().getPipelineSinkDop();
                        if (sessionVarSinkDop > 0) {
                            curTableSinkDop = Math.min(dop, sessionVarSinkDop);
                        } else {
                            curTableSinkDop = Math.min(dop, IcebergTableSink.ICEBERG_SINK_MAX_DOP);
                        }
                    }
                } else {
                    curTableSinkDop = instanceExecParam.getPipelineDop();
                }

                TExecPlanFragmentParams uniqueParams = new TExecPlanFragmentParams();
                toThriftForUniqueParams(uniqueParams, i, instanceExecParam, enablePipelineEngine,
                        accTabletSinkDop, curTableSinkDop);
                fillRequiredFieldsToThrift(uniqueParams);

                uniqueParamsList.add(uniqueParams);
                accTabletSinkDop += curTableSinkDop;
            }

            TExecBatchPlanFragmentsParams request = new TExecBatchPlanFragmentsParams();
            request.setCommon_param(commonParams);
            request.setUnique_param_per_instance(uniqueParamsList);
            return request;
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
                THdfsScanRange hdfsScanRange = range.getScan_range().getHdfs_scan_range();
                if (hdfsScanRange != null) {
                    sb.append("{relative_path=").append(hdfsScanRange.getRelative_path())
                            .append(", offset=").append(hdfsScanRange.getOffset())
                            .append(", length=").append(hdfsScanRange.getLength())
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

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            appendTo(sb);
            return sb.toString();
        }
    }

    private class NormalBackendSelector implements BackendSelector {
        private final ScanNode scanNode;
        private final List<TScanRangeLocations> locations;
        private final FragmentScanRangeAssignment assignment;
        private final boolean isLoad;

        public NormalBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                     FragmentScanRangeAssignment assignment) {
            this.scanNode = scanNode;
            this.locations = locations;
            this.assignment = assignment;
            this.isLoad = false;
        }

        public NormalBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                     FragmentScanRangeAssignment assignment, boolean isLoad) {
            this.scanNode = scanNode;
            this.locations = locations;
            this.assignment = assignment;
            this.isLoad = isLoad;
        }

        private boolean isEnableScheduleByRowCnt(TScanRangeLocations scanRangeLocations) {
            // only enable for load now, The insert into select performance problem caused by data skew is the most serious
            return Config.enable_schedule_insert_query_by_row_count && isLoad
                    && scanRangeLocations.getScan_range().isSetInternal_scan_range()
                    && scanRangeLocations.getScan_range().getInternal_scan_range().isSetRow_count()
                    && scanRangeLocations.getScan_range().getInternal_scan_range().getRow_count() > 0;
        }

        @Override
        public void computeScanRangeAssignment() throws Exception {
            HashMap<TNetworkAddress, Long> assignedRowCountPerHost = Maps.newHashMap();
            // sort the scan ranges by row count
            // only sort the scan range when it is load job
            // but when there are too many scan ranges, we will not sort them since performance issue
            if (locations.size() < 10240 && locations.size() > 0 && isEnableScheduleByRowCnt(locations.get(0))) {
                locations.sort(new Comparator<TScanRangeLocations>() {
                    @Override
                    public int compare(TScanRangeLocations l, TScanRangeLocations r) {
                        return Long.compare(r.getScan_range().getInternal_scan_range().getRow_count(),
                                l.getScan_range().getInternal_scan_range().getRow_count());
                    }
                });
            }
            for (TScanRangeLocations scanRangeLocations : locations) {
                // assign this scan range to the host w/ the fewest assigned row count
                Long minAssignedBytes = Long.MAX_VALUE;
                TScanRangeLocation minLocation = null;
                for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    Long assignedBytes = BackendSelector.findOrInsert(assignedRowCountPerHost, location.server, 0L);
                    if (assignedBytes < minAssignedBytes) {
                        minAssignedBytes = assignedBytes;
                        minLocation = location;
                    }
                }
                if (minLocation == null) {
                    throw new UserException("Scan range not found" + backendInfosString(false));
                }

                // only enable for load now, The insert into select performance problem caused by data skew is the most serious
                if (isEnableScheduleByRowCnt(scanRangeLocations)) {
                    assignedRowCountPerHost.put(minLocation.server, assignedRowCountPerHost.get(minLocation.server)
                            + scanRangeLocations.getScan_range().getInternal_scan_range().getRow_count());
                } else {
                    // use tablet num as assigned row count
                    assignedRowCountPerHost.put(minLocation.server, assignedRowCountPerHost.get(minLocation.server) + 1);
                }

                Reference<Long> backendIdRef = new Reference<>();
                TNetworkAddress execHostPort = SimpleScheduler.getHost(minLocation.backend_id,
                        scanRangeLocations.getLocations(),
                        idToBackend, backendIdRef);

                if (execHostPort == null) {
                    throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR
                            + backendInfosString(false));
                }
                recordUsedBackend(execHostPort, backendIdRef.getRef());

                Map<Integer, List<TScanRangeParams>> scanRanges = BackendSelector.findOrInsert(
                        assignment, execHostPort, new HashMap<>());
                List<TScanRangeParams> scanRangeParamsList = BackendSelector.findOrInsert(
                        scanRanges, scanNode.getId().asInt(), new ArrayList<>());
                // add scan range
                TScanRangeParams scanRangeParams = new TScanRangeParams();
                scanRangeParams.scan_range = scanRangeLocations.scan_range;
                scanRangeParamsList.add(scanRangeParams);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("assignedRowCountPerHost: {}", assignedRowCountPerHost);
            }
        }
    }

    private class ReplicatedBackendSelector implements BackendSelector {
        private final ScanNode scanNode;
        private final List<TScanRangeLocations> locations;
        private final FragmentScanRangeAssignment assignment;

        public ReplicatedBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
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
                    List<TScanRangeParams> scanRangeParamsList = BackendSelector.findOrInsert(
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
                        List<TScanRangeParams> scanRangeParamsList = BackendSelector.findOrInsert(
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
                    getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), fragmentId, bucketSeq,
                            idToBackend);
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
                        TNetworkAddress execHostport = SimpleScheduler.getBackendHost(idToBackend, backendIdRef);
                        if (execHostport == null) {
                            throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR
                                    + backendInfosString(false));
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
                                                              ImmutableMap<Long, ComputeNode> idToBackend)
                throws UserException {
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
            Reference<Long> backendIdRef = new Reference<>();
            TNetworkAddress execHostPort = SimpleScheduler.getHost(buckendId, seqLocation.locations,
                    idToBackend, backendIdRef);
            if (execHostPort == null) {
                throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR
                        + backendInfosString(false));
            }

            recordUsedBackend(execHostPort, backendIdRef.getRef());
            fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
        }
    }

}
