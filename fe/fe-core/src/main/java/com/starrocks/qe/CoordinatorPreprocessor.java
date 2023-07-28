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
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.TFragmentInstanceFactory;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryGlobals;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CoordinatorPreprocessor {
    private static final Logger LOG = LogManager.getLogger(CoordinatorPreprocessor.class);
    private static final String LOCAL_IP = FrontendOptions.getLocalHostAddress();
    public static final int BUCKET_ABSENT = 2147483647;

    static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Random random = new Random();

    private final TNetworkAddress coordAddress;
    private final ConnectContext connectContext;
    private final Set<Integer> replicateScanIds = Sets.newHashSet();

    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final WorkerProvider.Factory workerProviderFactory = new DefaultWorkerProvider.Factory();
    private WorkerProvider workerProvider;

    public CoordinatorPreprocessor(ConnectContext context, JobSpec jobSpec) {
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = Preconditions.checkNotNull(context);
        this.jobSpec = jobSpec;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

    }

    @VisibleForTesting
    CoordinatorPreprocessor(List<PlanFragment> fragments, List<ScanNode> scanNodes) {
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = StatisticUtils.buildConnectContext();
        this.jobSpec = JobSpec.Factory.mockJobSpec(connectContext, fragments, scanNodes);
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        Map<PlanFragmentId, PlanFragment> fragmentMap =
                fragments.stream().collect(Collectors.toMap(PlanFragment::getFragmentId, Function.identity()));
        for (ScanNode scan : scanNodes) {
            PlanFragmentId id = scan.getFragmentId();
            PlanFragment fragment = fragmentMap.get(id);
            if (fragment == null) {
                // Fake a fragment for this node
                fragment = new PlanFragment(id, scan, DataPartition.RANDOM);
                executionDAG.attachFragments(Collections.singletonList(fragment));
            }
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

    public TUniqueId getQueryId() {
        return jobSpec.getQueryId();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    public TDescriptorTable getDescriptorTable() {
        return jobSpec.getDescTable();
    }

    @VisibleForTesting
    Map<PlanFragmentId, ExecutionFragment> getIdToExecFragment() {
        return executionDAG.getIdToFragment();
    }

    public List<ExecutionFragment> getFragmentsInPreorder() {
        return executionDAG.getFragmentsInPreorder();
    }

    public TNetworkAddress getCoordAddress() {
        return coordAddress;
    }

    public TNetworkAddress getBrpcAddress(long workerId) {
        return workerProvider.getWorkerById(workerId).getBrpcAddress();
    }

    public TNetworkAddress getAddress(long workerId) {
        ComputeNode worker = workerProvider.getWorkerById(workerId);
        return worker.getAddress();
    }

    public WorkerProvider getWorkerProvider() {
        return workerProvider;
    }

    public TWorkGroup getResourceGroup() {
        return jobSpec.getResourceGroup();
    }

    public boolean isLoadType() {
        return jobSpec.isLoadType();
    }

    public void prepareExec() throws Exception {
        // prepare information
        resetExec();

        computeScanRangeAssignment();
        computeFragmentExecParams();
        traceInstance();

        executionDAG.finalizeDAG();
    }

    /**
     * Reset state of all the fragments set in Coordinator, when retrying the same query with the fragments.
     */
    private void resetExec() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        jobSpec.getFragments().forEach(PlanFragment::reset);
    }

    private void traceInstance() {
        if (LOG.isDebugEnabled()) {
            // TODO(zc): add a switch to close this function
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            sb.append("query id=").append(DebugUtil.printId(jobSpec.getQueryId())).append(",");
            sb.append("fragment=[");
            for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(execFragment.getFragmentId());
                execFragment.appendTo(sb);
            }
            sb.append("]");
            LOG.debug(sb.toString());
        }
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
        boolean isGatherOutput = jobSpec.getFragments().get(0).getDataPartition() == DataPartition.UNPARTITIONED;

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPostorder()) {
            PlanFragment fragment = execFragment.getPlanFragment();

            boolean dopAdaptionEnabled = jobSpec.isEnablePipeline() &&
                    connectContext.getSessionVariable().isEnablePipelineAdaptiveDop();

            // If left child is MultiCastDataFragment(only support left now), will keep same instance with child.
            if (fragment.getChildren().size() > 0 && fragment.getChild(0) instanceof MultiCastPlanFragment) {
                ExecutionFragment childExecFragment = execFragment.getChild(0);
                for (FragmentInstance childInstance : childExecFragment.getInstances()) {
                    execFragment.addInstance(new FragmentInstance(childInstance.getWorker(), execFragment));
                }
                continue;
            }

            if (fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
                long workerId = workerProvider.selectNextWorker();
                FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
                execFragment.addInstance(instance);
                continue;
            }

            PlanNode leftMostNode = execFragment.getLeftMostNode();

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
                    int currentChildFragmentParallelism = execFragment.getChild(j).getInstances().size();
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

                ExecutionFragment maxParallelismExecFragment = execFragment.getChild(inputFragmentIndex);

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
                        for (int i = 0; i < execFragment.childrenSize(); i++) {
                            ExecutionFragment childExecFragment = execFragment.getChild(i);
                            childExecFragment.getInstances().stream()
                                    .map(FragmentInstance::getWorkerId)
                                    .forEach(workerIdSet::add);
                        }
                        //make olapScan maxParallelism equals prefer compute node number
                        maxParallelism = workerIdSet.size() * fragment.getParallelExecNum();
                    } else {
                        for (FragmentInstance execParams : maxParallelismExecFragment.getInstances()) {
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
                    List<Long> workerIds = Lists.newArrayList(workerIdSet);
                    Collections.shuffle(workerIds, random);

                    for (int index = 0; index < exchangeInstances; index++) {
                        Long workerId = workerIds.get(index % workerIds.size());
                        FragmentInstance instance =
                                new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
                        execFragment.addInstance(instance);
                    }
                } else {
                    List<Long> workerIds = Lists.newArrayList(workerIdSet);
                    for (int index = 0; index < maxParallelism; ++index) {
                        Long workerId = workerIds.get(index % workerIds.size());
                        FragmentInstance instance =
                                new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
                        execFragment.addInstance(instance);
                    }
                }

                // When group by cardinality is smaller than number of backend, only some backends always
                // process while other has no data to process.
                // So we shuffle instances to make different backends handle different queries.
                execFragment.shuffleInstances(random);

                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            int parallelExecInstanceNum = fragment.getParallelExecNum();
            int pipelineDop = fragment.getPipelineDop();
            ColocatedBackendSelector.Assignment colocatedAssignment = execFragment.getColocatedAssignment();
            boolean hasColocate = execFragment.isColocated()
                    && colocatedAssignment != null
                    && colocatedAssignment.getSeqToWorkerId().size() > 0;
            boolean hasBucketShuffle = execFragment.isBucketShuffleJoin() && colocatedAssignment != null;

            if (hasColocate || hasBucketShuffle) {
                computeColocatedJoinInstanceParam(colocatedAssignment.getSeqToWorkerId(),
                        colocatedAssignment.getSeqToScanRange(),
                        parallelExecInstanceNum, pipelineDop, jobSpec.isEnablePipeline(), execFragment);
            } else {
                boolean assignScanRangesPerDriverSeq = jobSpec.isEnablePipeline() &&
                        (fragment.isAssignScanRangesPerDriverSeq() || fragment.isForceAssignScanRangesPerDriverSeq());
                for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> workerIdMapEntry :
                        execFragment.getScanRangeAssignment().entrySet()) {
                    Long workerId = workerIdMapEntry.getKey();
                    ComputeNode worker = workerProvider.getWorkerById(workerId);
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
                            FragmentInstance instance = new FragmentInstance(worker, execFragment);
                            execFragment.addInstance(instance);

                            boolean assignPerDriverSeq = assignScanRangesPerDriverSeq &&
                                    (enableAssignScanRangesPerDriverSeq(scanRangeParams, pipelineDop)
                                            || fragment.isForceAssignScanRangesPerDriverSeq());
                            if (!assignPerDriverSeq) {
                                instance.getNode2ScanRanges().put(planNodeId, scanRangeParams);
                            } else {
                                int expectedDop = Math.max(1, Math.min(pipelineDop, scanRangeParams.size()));
                                List<List<TScanRangeParams>> scanRangeParamsPerDriverSeq;
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
                                instance.setPipelineDop(scanRangeParamsPerDriverSeq.size());
                                Map<Integer, List<TScanRangeParams>> scanRangesPerDriverSeq = new HashMap<>();
                                instance.getNode2DriverSeqToScanRanges().put(planNodeId, scanRangesPerDriverSeq);
                                for (int driverSeq = 0; driverSeq < scanRangeParamsPerDriverSeq.size(); ++driverSeq) {
                                    scanRangesPerDriverSeq.put(driverSeq, scanRangeParamsPerDriverSeq.get(driverSeq));
                                }
                                for (int driverSeq = scanRangeParamsPerDriverSeq.size();
                                        driverSeq < instance.getPipelineDop(); ++driverSeq) {
                                    scanRangesPerDriverSeq.put(driverSeq, Lists.newArrayList());
                                }
                            }
                        }
                    }

                    // 1. Handle replicated scan node if need
                    boolean isReplicated = execFragment.isReplicated();
                    if (isReplicated) {
                        for (Integer planNodeId : value.keySet()) {
                            if (!replicateScanIds.contains(planNodeId)) {
                                continue;
                            }
                            List<TScanRangeParams> perNodeScanRanges = value.get(planNodeId);
                            for (FragmentInstance instanceParam : execFragment.getInstances()) {
                                instanceParam.getNode2ScanRanges().put(planNodeId, perNodeScanRanges);
                            }
                        }
                    }
                }
            }

            if (execFragment.getInstances().isEmpty()) {
                long workerId = workerProvider.selectNextWorker();
                ComputeNode worker = workerProvider.getWorkerById(workerId);
                FragmentInstance instance = new FragmentInstance(worker, execFragment);
                execFragment.addInstance(instance);
            }
        }
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
                                                  ExecutionFragment execFragment) {
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
                List<TScanRangeParams>>>>> workerIdScanRange : workerIdToScanRanges.entrySet()) {
            ComputeNode worker = workerProvider.getWorkerById(workerIdScanRange.getKey());
            List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = workerIdScanRange.getValue();

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
                FragmentInstance instance = new FragmentInstance(worker, execFragment);
                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicateScanSet = new HashSet<>();

                int expectedDop = 1;
                if (pipelineDop > 1) {
                    expectedDop = Math.min(scanRangePerInstance.size(), pipelineDop);
                }
                List<List<Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>>>> scanRangesPerDriverSeq =
                        ListUtil.splitBySize(scanRangePerInstance, expectedDop);

                if (assignPerDriverSeq) {
                    instance.setPipelineDop(scanRangesPerDriverSeq.size());
                }

                for (int driverSeq = 0; driverSeq < scanRangesPerDriverSeq.size(); ++driverSeq) {
                    final int finalDriverSeq = driverSeq;
                    scanRangesPerDriverSeq.get(finalDriverSeq).forEach(bucketSeqAndScanRanges -> {
                        if (assignPerDriverSeq) {
                            instance.addBucketSeqAndDriverSeq(bucketSeqAndScanRanges.getKey(), finalDriverSeq);
                        } else {
                            instance.addBucketSeq(bucketSeqAndScanRanges.getKey());
                        }

                        bucketSeqAndScanRanges.getValue().forEach((scanId, scanRanges) -> {
                            List<TScanRangeParams> destScanRanges;
                            if (!assignPerDriverSeq) {
                                destScanRanges = instance.getNode2ScanRanges()
                                        .computeIfAbsent(scanId, k -> new ArrayList<>());
                            } else {
                                destScanRanges = instance.getNode2DriverSeqToScanRanges()
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
                    instance.getNode2DriverSeqToScanRanges().forEach((scanId, perDriverSeqScanRanges) -> {
                        for (int driverSeq = 0; driverSeq < instance.getPipelineDop(); ++driverSeq) {
                            perDriverSeqScanRanges.computeIfAbsent(driverSeq, k -> new ArrayList<>());
                        }
                    });
                }

                execFragment.addInstance(instance);
            }
        }
    }

    public FragmentScanRangeAssignment getFragmentScanRangeAssignment(PlanFragmentId fragmentId) {
        return executionDAG.getFragment(fragmentId).getScanRangeAssignment();
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    @VisibleForTesting
    void computeScanRangeAssignment() throws UserException {
        SessionVariable sv = connectContext.getSessionVariable();

        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : jobSpec.getScanNodes()) {
            // the parameters of getScanRangeLocations may ignore, It dosn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            ExecutionFragment execFragment = executionDAG.getFragment(scanNode.getFragmentId());
            FragmentScanRangeAssignment assignment = execFragment.getScanRangeAssignment();

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
                boolean hasColocate = execFragment.isColocated();
                boolean hasBucket = execFragment.isBucketShuffleJoin();
                boolean hasReplicated = execFragment.isReplicated();
                if (assignment.size() > 0 && hasReplicated && scanNode.canDoReplicatedJoin()) {
                    BackendSelector selector = new ReplicatedBackendSelector(scanNode, locations, assignment,
                            execFragment.getColocatedAssignment());
                    selector.computeScanRangeAssignment();
                    replicateScanIds.add(scanNode.getId().asInt());
                } else if (hasColocate || hasBucket) {
                    ColocatedBackendSelector.Assignment colocatedAssignment =
                            execFragment.getOrCreateColocatedAssignment((OlapScanNode) scanNode);
                    boolean isRightOrFullBucketShuffleFragment = execFragment.isRightOrFullBucketShuffle();
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
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("fragment {} has instances {}", execFragment.getFragmentId(),
                        execFragment.getInstances().size());
            }

            if (execFragment.getPlanFragment().getSink() instanceof ResultSink &&
                    execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException("This sql plan has multi result sinks",
                        ErrorType.INTERNAL_ERROR);
            }
        }

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            executionDAG.initFragment(execFragment);
        }
    }

    public TFragmentInstanceFactory createTFragmentInstanceFactory() {
        return new TFragmentInstanceFactory(connectContext, jobSpec, executionDAG, coordAddress);
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEHTTP();
        }
        return null;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEPort();
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
}
