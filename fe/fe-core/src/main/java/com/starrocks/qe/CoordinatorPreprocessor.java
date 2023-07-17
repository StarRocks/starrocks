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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
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
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.ComputeNode;
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
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TRuntimeFilterParams;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final Map<PlanFragmentId, List<Integer>> fragmentIdToSeqToInstanceMap = Maps.newHashMap();

    // used only by channel stream load, records the mapping from channel id to target BE's address
    private final Map<Integer, TNetworkAddress> channelIdToBEHTTP = Maps.newHashMap();
    private final Map<Integer, TNetworkAddress> channelIdToBEPort = Maps.newHashMap();

    private final WorkerProvider.Factory workerProviderFactory = new DefaultWorkerProvider.Factory();
    private WorkerProvider workerProvider;

    // Resource group
    private TWorkGroup resourceGroup = null;

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

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());
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

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

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

    public Map<PlanFragmentId, List<Integer>> getFragmentIdToSeqToInstanceMap() {
        return fragmentIdToSeqToInstanceMap;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTP() {
        return channelIdToBEHTTP;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPort() {
        return channelIdToBEPort;
    }

    public TNetworkAddress getBrpcAddress(long workerId) {
        return workerProvider.getWorkerById(workerId).getBrpcAddress();
    }

    public WorkerProvider getWorkerProvider() {
        return workerProvider;
    }

    public TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public boolean isLoadType() {
        return queryOptions.getQuery_type() == TQueryType.LOAD;
    }

    public void prepareExec() throws Exception {
        // prepare information
        resetExec();
        prepareFragments();

        // prepare workgroup
        resourceGroup = prepareResourceGroup(connectContext,
                ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));

        computeScanRangeAssignment();
        computeFragmentExecParams();
        traceInstance();
        computeBeInstanceNumbers();
    }

    /**
     * Reset state of all the fragments set in Coordinator, when retrying the same query with the fragments.
     */
    private void resetExec() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        fragments.forEach(PlanFragment::reset);
    }

    @VisibleForTesting
    void prepareFragments() {
        for (PlanFragment fragment : fragments) {
            fragmentExecParamsMap.put(fragment.getFragmentId(), new FragmentExecParams(fragment));
        }

        coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);
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
    private List<List<TScanRangeParams>> splitScanRangeParamByRowCount(List<TScanRangeParams> scanRangeParams,
                                                                       int groupNum) {
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
                    params.instanceExecParams.add(
                            new FInstanceExecParam(null, childInstanceParam.getWorkerId(), params));
                }
                continue;
            }

            if (fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
                long workerId = workerProvider.selectNextWorker();
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, workerId, params);
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
                Set<Long> workerIdSet = Sets.newHashSet();

                List<Long> selectedComputedNodes = workerProvider.selectAllComputeNodes();
                if (!selectedComputedNodes.isEmpty()) {
                    workerIdSet.addAll(selectedComputedNodes);
                    //make olapScan maxParallelism equals prefer compute node number
                    maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
                } else {
                    if (fragment.isUnionFragment() && isGatherOutput) {
                        // union fragment use all children's host
                        // if output fragment isn't gather, all fragment must keep 1 instance
                        for (PlanFragment child : fragment.getChildren()) {
                            FragmentExecParams childParams = fragmentExecParamsMap.get(child.getFragmentId());
                            childParams.instanceExecParams.stream()
                                    .map(FInstanceExecParam::getWorkerId)
                                    .forEach(workerIdSet::add);
                        }
                        //make olapScan maxParallelism equals prefer compute node number
                        maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
                    } else {
                        for (FInstanceExecParam execParams : maxParallelismFragmentExecParams.instanceExecParams) {
                            workerIdSet.add(execParams.getWorkerId());
                        }
                    }
                }

                if (dopAdaptionEnabled) {
                    Preconditions.checkArgument(leftMostNode instanceof ExchangeNode);
                    maxParallelism = workerIdSet.size();
                }

                // AddAll() soft copy()
                int exchangeInstances = -1;
                if (connectContext != null && connectContext.getSessionVariable() != null) {
                    exchangeInstances = connectContext.getSessionVariable().getExchangeInstanceParallel();
                }
                if (exchangeInstances > 0 && maxParallelism > exchangeInstances) {
                    // random select some instance
                    // get distinct host,  when parallel_fragment_exec_instance_num > 1, single host may execute several instances
                    List<Long> hosts = Lists.newArrayList(workerIdSet);
                    Collections.shuffle(hosts, random);

                    for (int index = 0; index < exchangeInstances; index++) {
                        FInstanceExecParam instanceParam =
                                new FInstanceExecParam(null, hosts.get(index % hosts.size()), params);
                        params.instanceExecParams.add(instanceParam);
                    }
                } else {
                    List<Long> workerIds = Lists.newArrayList(workerIdSet);
                    for (int index = 0; index < maxParallelism; ++index) {
                        Long workerId = workerIds.get(index % workerIds.size());
                        FInstanceExecParam instanceParam = new FInstanceExecParam(null, workerId, params);
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
            ColocatedBackendSelector.Assignment colocatedAssignment = params.colocatedAssignment;
            boolean hasColocate = isColocateFragment(fragment.getPlanRoot())
                    && colocatedAssignment != null
                    && colocatedAssignment.getSeqToWorkerId().size() > 0;
            boolean hasBucketShuffle =
                    isBucketShuffleJoin(fragment.getFragmentId().asInt()) && colocatedAssignment != null;

            if (hasColocate || hasBucketShuffle) {
                computeColocatedJoinInstanceParam(colocatedAssignment.getSeqToWorkerId(),
                        colocatedAssignment.getSeqToScanRange(),
                        parallelExecInstanceNum, pipelineDop, usePipeline, params);
                computeBucketSeq2InstanceOrdinal(params, colocatedAssignment.getBucketNum());
            } else {
                boolean assignScanRangesPerDriverSeq = usePipeline &&
                        (fragment.isAssignScanRangesPerDriverSeq() || fragment.isForceAssignScanRangesPerDriverSeq());
                for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> workerIdMapEntry :
                        fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment.entrySet()) {
                    Long workerId = workerIdMapEntry.getKey();
                    Map<Integer, List<TScanRangeParams>> value = workerIdMapEntry.getValue();

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
                            FInstanceExecParam instanceParam = new FInstanceExecParam(null, workerId, params);
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
                                    scanRangeParamsPerDriverSeq =
                                            splitScanRangeParamByRowCount(scanRangeParams, expectedDop);
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
                                    ComputeNode worker = workerProvider.getWorkerById(workerId);
                                    channelIdToBEHTTP.put(channelId, worker.getHttpAddress());
                                    channelIdToBEPort.put(channelId, worker.getAddress());
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
                long workerId = workerProvider.selectNextWorker();
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, workerId, params);
                params.instanceExecParams.add(instanceParam);
            }
        }
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

    public void computeColocatedJoinInstanceParam(Map<Integer, Long> bucketSeqToWorkerId,
                                                  ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange,
                                                  int parallelExecInstanceNum, int pipelineDop, boolean enablePipeline,
                                                  FragmentExecParams params) {
        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<Long, List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> workerIdToScanRanges =
                Maps.newHashMap();
        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> bucketSeqAndScanRanges : bucketSeqToScanRange.entrySet()) {
            Long workerId = bucketSeqToWorkerId.get(bucketSeqAndScanRanges.getKey());
            workerIdToScanRanges
                    .computeIfAbsent(workerId, k -> Lists.newArrayList())
                    .add(bucketSeqAndScanRanges);
        }

        boolean assignPerDriverSeq =
                enablePipeline && workerIdToScanRanges.values().stream()
                        .allMatch(scanRanges -> enableAssignScanRangesPerDriverSeq(scanRanges, pipelineDop));

        for (Map.Entry<Long, List<Map.Entry<Integer, Map<Integer,
                List<TScanRangeParams>>>>> addressScanRange : workerIdToScanRanges.entrySet()) {
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
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), params);
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

    public FragmentScanRangeAssignment getFragmentScanRangeAssignment(PlanFragmentId fragmentId) {
        return fragmentExecParamsMap.get(fragmentId).scanRangeAssignment;
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    @VisibleForTesting
    void computeScanRangeAssignment() throws UserException {
        SessionVariable sv = connectContext.getSessionVariable();

        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : scanNodes) {
            // the parameters of getScanRangeLocations may ignore, It dosn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            FragmentExecParams fragmentExecParams = fragmentExecParamsMap.get(scanNode.getFragmentId());
            FragmentScanRangeAssignment assignment = fragmentExecParams.scanRangeAssignment;

            if (scanNode instanceof SchemaScanNode) {
                BackendSelector selector =
                        new NormalBackendSelector(scanNode, locations, assignment, workerProvider, false);
                selector.computeScanRangeAssignment();
            } else if ((scanNode instanceof HdfsScanNode) || (scanNode instanceof IcebergScanNode) ||
                    scanNode instanceof HudiScanNode || scanNode instanceof DeltaLakeScanNode ||
                    scanNode instanceof FileTableScanNode || scanNode instanceof PaimonScanNode) {

                HDFSBackendSelector selector =
                        new HDFSBackendSelector(scanNode, locations, assignment, workerProvider,
                                sv.getForceScheduleLocal(),
                                sv.getHDFSBackendSelectorScanRangeShuffle());
                selector.computeScanRangeAssignment();
            } else {
                boolean hasColocate = isColocateFragment(scanNode.getFragment().getPlanRoot());
                boolean hasBucket =
                        isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());
                boolean hasReplicated = isReplicatedFragment(scanNode.getFragment().getPlanRoot());
                if (assignment.size() > 0 && hasReplicated && scanNode.canDoReplicatedJoin()) {
                    BackendSelector selector = new ReplicatedBackendSelector(scanNode, locations, assignment,
                            fragmentExecParams.colocatedAssignment);
                    selector.computeScanRangeAssignment();
                    replicateScanIds.add(scanNode.getId().asInt());
                } else if (hasColocate || hasBucket) {
                    if (fragmentExecParams.colocatedAssignment == null) {
                        fragmentExecParams.colocatedAssignment =
                                new ColocatedBackendSelector.Assignment((OlapScanNode) scanNode);
                    }
                    ColocatedBackendSelector.Assignment colocatedAssignment = fragmentExecParams.colocatedAssignment;
                    boolean isRightOrFullBucketShuffleFragment =
                            rightOrFullBucketShuffleFragmentIds.contains(scanNode.getFragmentId().asInt());
                    BackendSelector selector = new ColocatedBackendSelector((OlapScanNode) scanNode, assignment,
                            colocatedAssignment, isRightOrFullBucketShuffleFragment, workerProvider);
                    selector.computeScanRangeAssignment();
                } else {
                    BackendSelector selector =
                            new NormalBackendSelector(scanNode, locations, assignment, workerProvider, isLoadType());
                    selector.computeScanRangeAssignment();
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(assignment.toDebugString());
            }
        }
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

                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(dummyServer);

                    dest.setBrpc_server(dummyServer);

                    for (FInstanceExecParam instanceExecParams : destParams.instanceExecParams) {
                        Integer driverSeq = instanceExecParams.bucketSeqToDriverSeq.get(bucketSeq);
                        if (driverSeq != null) {
                            dest.fragment_instance_id = instanceExecParams.instanceId;

                            ComputeNode worker = workerProvider.getWorkerById(instanceExecParams.workerId);
                            // NOTE(zc): can be removed in version 4.0
                            dest.setDeprecated_server(worker.getAddress());
                            dest.setBrpc_server(worker.getBrpcAddress());

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

                    ComputeNode worker = workerProvider.getWorkerById(destParams.instanceExecParams.get(j).workerId);
                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(worker.getAddress());
                    dest.setBrpc_server(worker.getBrpcAddress());

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

                        // NOTE(zc): can be removed in version 4.0
                        dest.setDeprecated_server(dummyServer);

                        dest.setBrpc_server(dummyServer);

                        for (FInstanceExecParam instanceExecParams : destParams.instanceExecParams) {
                            Integer driverSeq = instanceExecParams.bucketSeqToDriverSeq.get(bucketSeq);
                            if (driverSeq != null) {
                                dest.fragment_instance_id = instanceExecParams.instanceId;

                                ComputeNode worker = workerProvider.getWorkerById(instanceExecParams.workerId);
                                // NOTE(zc): can be removed in version 4.0
                                dest.setDeprecated_server(worker.getAddress());
                                dest.setBrpc_server(worker.getBrpcAddress());

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

                        ComputeNode worker =
                                workerProvider.getWorkerById(destParams.instanceExecParams.get(j).workerId);
                        // NOTE(zc): can be removed in version 4.0
                        dest.setDeprecated_server(worker.getAddress());
                        dest.setBrpc_server(worker.getBrpcAddress());

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

    private final Map<Long, Integer> workerIdToNumInstances = Maps.newHashMap();

    // Compute the fragment instance numbers in every BE for one query
    private void computeBeInstanceNumbers() {
        workerIdToNumInstances.clear();
        for (PlanFragment fragment : fragments) {
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());
            for (final FInstanceExecParam instance : params.instanceExecParams) {
                long workerId = instance.getWorkerId();
                Integer number = workerIdToNumInstances.getOrDefault(workerId, 0);
                workerIdToNumInstances.put(workerId, ++number);
            }
        }
    }

    private int getFragmentBucketNum(PlanFragmentId fragmentId) {
        ColocatedBackendSelector.Assignment colocatedAssignment =
                fragmentExecParamsMap.get(fragmentId).colocatedAssignment;
        return colocatedAssignment == null ? 0 : colocatedAssignment.getBucketNum();
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

    // fragment instance exec param, it is used to assemble
    // the per-instance TPlanFragmentExecParas, as a member of
    // FragmentExecParams
    public static class FInstanceExecParam {
        static final int ABSENT_PIPELINE_DOP = -1;
        static final int ABSENT_DRIVER_SEQUENCE = -1;

        TUniqueId instanceId;
        final Long workerId;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();
        Map<Integer, Map<Integer, List<TScanRangeParams>>> nodeToPerDriverSeqScanRanges = Maps.newHashMap();

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

        public FInstanceExecParam(TUniqueId id, Long workerId, FragmentExecParams fragmentExecParams) {
            this.instanceId = id;
            this.workerId = workerId;
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

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public Long getWorkerId() {
            return workerId;
        }
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
        private ColocatedBackendSelector.Assignment colocatedAssignment = null;
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
         * @param workerId               The destination id to delivery these instances.
         * @param descTable              The descriptor table, empty for the non-first instance
         *                               when enable pipeline and disable multi fragments in one request.
         * @param isEnablePipelineEngine Whether enable pipeline engine.
         */
        private void toThriftForCommonParams(TExecPlanFragmentParams commonParams,
                                             long workerId, TDescriptorTable descTable,
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
            commonParams.params.setInstances_number(workerIdToNumInstances.get(workerId));
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

                toThriftForCommonParams(params, instanceExecParam.getWorkerId(), descTable, enablePipelineEngine,
                        tableSinkTotalDop, isEnableStreamPipeline);
                toThriftForUniqueParams(params, i, instanceExecParam, enablePipelineEngine,
                        accTabletSinkDop, curTableSinkDop);

                paramsList.add(params);
                accTabletSinkDop += curTableSinkDop;
            }
            return paramsList;
        }

        TExecBatchPlanFragmentsParams toThriftInBatch(
                Set<TUniqueId> inFlightInstanceIds, long workerId, TDescriptorTable descTable,
                boolean enablePipelineEngine, int accTabletSinkDop,
                int tableSinkTotalDop) throws Exception {

            boolean forceSetTableSinkDop = fragment.forceSetTableSinkDop();

            setBucketSeqToInstanceForRuntimeFilters();

            TExecPlanFragmentParams commonParams = new TExecPlanFragmentParams();
            toThriftForCommonParams(commonParams, workerId, descTable, enablePipelineEngine, tableSinkTotalDop, false);
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

                FInstanceExecParam instanceExecParam = instanceExecParams.get(i);

                Map<Integer, List<TScanRangeParams>> scanRanges =
                        scanRangeAssignment.get(instanceExecParam.getWorkerId());
                sb.append("{");
                sb.append("id=").append(DebugUtil.printId(instanceExecParams.get(i).instanceId));
                sb.append(",host=").append(getAddressByWorkerId(instanceExecParam.getWorkerId()));
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

    public TNetworkAddress getAddressByWorkerId(long workerId) {
        ComputeNode worker = workerProvider.getWorkerById(workerId);
        return worker.getAddress();
    }
}
