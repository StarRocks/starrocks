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

package com.starrocks.qe.scheduler.assignment;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListUtil;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.BackendSelector;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The assignment strategy for fragments whose left most node is a scan node.
 * <p> It firstly assigns scan ranges to workers, and then dispatches scan ranges, assigned to each worker, to fragment instances.
 */
public class LocalFragmentAssignmentStrategy implements FragmentAssignmentStrategy {
    private static final Logger LOG = LogManager.getLogger(LocalFragmentAssignmentStrategy.class);

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final boolean usePipeline;
    private final boolean isLoadType;

    private final Set<Integer> replicatedScanIds = Sets.newHashSet();

    public LocalFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider, boolean usePipeline,
                                           boolean isLoadType) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.usePipeline = usePipeline;
        this.isLoadType = isLoadType;
    }

    @Override
    public void assignFragmentToWorker(ExecutionFragment execFragment) throws UserException {
        for (ScanNode scanNode : execFragment.getScanNodes()) {
            assignScanRangesToWorker(execFragment, scanNode);
        }

        assignScanRangesToFragmentInstancePerWorker(execFragment);

        // The fragment which only contains scan nodes without scan ranges,
        // such as SchemaScanNode, is assigned to an arbitrary worker.
        if (execFragment.getInstances().isEmpty()) {
            long workerId = workerProvider.selectNextWorker();
            ComputeNode worker = workerProvider.getWorkerById(workerId);
            FragmentInstance instance = new FragmentInstance(worker, execFragment);
            execFragment.addInstance(instance);
        }
    }

    private void assignScanRangesToWorker(ExecutionFragment execFragment, ScanNode scanNode) throws UserException {
        BackendSelector backendSelector = BackendSelectorFactory.create(
                scanNode, isLoadType, execFragment, workerProvider, connectContext, replicatedScanIds);

        backendSelector.computeScanRangeAssignment();

        if (LOG.isDebugEnabled()) {
            LOG.debug(execFragment.getScanRangeAssignment().toDebugString());
        }
    }

    private void assignScanRangesToFragmentInstancePerWorker(ExecutionFragment execFragment) {
        ColocatedBackendSelector.Assignment colocatedAssignment = execFragment.getColocatedAssignment();
        boolean hasColocate = execFragment.isColocated()
                && colocatedAssignment != null && !colocatedAssignment.getSeqToWorkerId().isEmpty();
        boolean hasBucketShuffle = execFragment.isLocalBucketShuffleJoin() && colocatedAssignment != null;

        if (hasColocate || hasBucketShuffle) {
            assignScanRangesToColocateFragmentInstancePerWorker(execFragment,
                    colocatedAssignment.getSeqToWorkerId(),
                    colocatedAssignment.getSeqToScanRange());
        } else {
            assignScanRangesToNormalFragmentInstancePerWorker(execFragment);
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

    private boolean enableAssignScanRangesPerDriverSeq(PlanFragment fragment, List<TScanRangeParams> scanRanges) {
        if (!usePipeline) {
            return false;
        }

        if (fragment.isForceAssignScanRangesPerDriverSeq()) {
            return true;
        }

        return fragment.isAssignScanRangesPerDriverSeq() &&
                enableAssignScanRangesPerDriverSeq(scanRanges, fragment.getPipelineDop());
    }

    private boolean needAddScanRanges(Set<Integer> visitedReplicatedScanIds, Integer scanId) {
        if (!replicatedScanIds.contains(scanId)) {
            return true;
        }
        return visitedReplicatedScanIds.add(scanId);
    }

    private void assignScanRangesToColocateFragmentInstancePerWorker(
            ExecutionFragment execFragment,
            Map<Integer, Long> bucketSeqToWorkerId,
            ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        final int parallelExecInstanceNum = fragment.getParallelExecNum();
        final int pipelineDop = fragment.getPipelineDop();

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<Long, List<Integer>> workerIdToBucketSeqs = bucketSeqToWorkerId.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));

        // TODO(stephen): Support iceberg multi-bucket columns un-assignPerDriverSeq
        boolean assignPerDriverSeq = usePipeline && workerIdToBucketSeqs.values().stream()
                .allMatch(bucketSeqs -> enableAssignScanRangesPerDriverSeq(bucketSeqs, pipelineDop)) ||
                execFragment.getColocatedAssignment().getTableType() == Table.TableType.ICEBERG;

        if (!assignPerDriverSeq) {
            // these optimize depend on assignPerDriverSeq.
            fragment.disablePhysicalPropertyOptimize();
        }

        workerIdToBucketSeqs.forEach((workerId, bucketSeqsOfWorker) -> {
            ComputeNode worker = workerProvider.getWorkerById(workerId);

            // 2. split how many scanRange one instance should scan
            int expectedInstanceNum = Math.max(1, parallelExecInstanceNum);
            List<List<Integer>> bucketSeqsPerInstance = ListUtil.splitBySize(bucketSeqsOfWorker, expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scanned by instance
            bucketSeqsPerInstance.forEach(bucketSeqsOfInstance -> {
                FragmentInstance instance = new FragmentInstance(worker, execFragment);
                execFragment.addInstance(instance);

                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicatedScanIds = new HashSet<>();

                if (!assignPerDriverSeq) {
                    bucketSeqsOfInstance.forEach(bucketSeq -> {
                        instance.addBucketSeq(bucketSeq);
                        bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                            if (needAddScanRanges(instanceReplicatedScanIds, scanId)) {
                                instance.addScanRanges(scanId, scanRanges);
                            }
                        });
                    });
                } else {
                    int expectedDop = Math.max(1, pipelineDop);
                    List<List<Integer>> bucketSeqsPerDriverSeq = ListUtil.splitBySize(bucketSeqsOfInstance, expectedDop);

                    instance.setPipelineDop(bucketSeqsPerDriverSeq.size());

                    for (int driverSeq = 0; driverSeq < bucketSeqsPerDriverSeq.size(); driverSeq++) {
                        int finalDriverSeq = driverSeq;
                        bucketSeqsPerDriverSeq.get(driverSeq).forEach(bucketSeq -> {
                            instance.addBucketSeqAndDriverSeq(bucketSeq, finalDriverSeq);
                            bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                                if (needAddScanRanges(instanceReplicatedScanIds, scanId)) {
                                    instance.addScanRanges(scanId, finalDriverSeq, scanRanges);
                                }
                            });
                        });
                    }

                    instance.paddingScanRanges();
                }
            });
        });
    }

    private void assignScanRangesToNormalFragmentInstancePerWorker(ExecutionFragment execFragment) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        final int parallelExecInstanceNum = fragment.getParallelExecNum();
        final int pipelineDop = fragment.getPipelineDop();

        execFragment.getScanRangeAssignment().forEach((workerId, scanRangesPerWorker) -> {
            // 1. Handle normal scan node firstly
            scanRangesPerWorker.forEach((scanId, scanRangesOfNode) -> {
                if (replicatedScanIds.contains(scanId)) {
                    return;
                }

                int expectedInstanceNum = Math.max(1, parallelExecInstanceNum);
                List<List<TScanRangeParams>> scanRangesPerInstance = ListUtil.splitBySize(scanRangesOfNode, expectedInstanceNum);

                for (List<TScanRangeParams> scanRanges : scanRangesPerInstance) {
                    FragmentInstance instance = new FragmentInstance(workerProvider.getWorkerById(workerId), execFragment);
                    execFragment.addInstance(instance);

                    if (!enableAssignScanRangesPerDriverSeq(fragment, scanRanges)) {
                        instance.addScanRanges(scanId, scanRanges);
                        fragment.disablePhysicalPropertyOptimize();
                    } else {
                        int expectedDop = Math.max(1, Math.min(pipelineDop, scanRanges.size()));
                        List<List<TScanRangeParams>> scanRangesPerDriverSeq;
                        if (Config.enable_schedule_insert_query_by_row_count && isLoadType
                                && !scanRanges.isEmpty()
                                && scanRanges.get(0).getScan_range().isSetInternal_scan_range()) {
                            scanRangesPerDriverSeq = splitScanRangeParamByRowCount(scanRanges, expectedDop);
                        } else {
                            scanRangesPerDriverSeq = ListUtil.splitBySize(scanRanges, expectedDop);
                        }

                        if (fragment.isForceAssignScanRangesPerDriverSeq() && scanRangesPerDriverSeq.size() != pipelineDop) {
                            fragment.setPipelineDop(scanRangesPerDriverSeq.size());
                        }
                        instance.setPipelineDop(scanRangesPerDriverSeq.size());

                        for (int driverSeq = 0; driverSeq < scanRangesPerDriverSeq.size(); ++driverSeq) {
                            instance.addScanRanges(scanId, driverSeq, scanRangesPerDriverSeq.get(driverSeq));
                        }
                        instance.paddingScanRanges();
                    }
                }
            });

            // 2. Handle replicated scan node, if needed
            boolean isReplicated = execFragment.isReplicated();
            if (isReplicated) {
                scanRangesPerWorker.forEach((scanId, scanRangesPerNode) -> {
                    if (!replicatedScanIds.contains(scanId)) {
                        return;
                    }

                    for (FragmentInstance instance : execFragment.getInstances()) {
                        instance.addScanRanges(scanId, scanRangesPerNode);
                    }
                });
            }
        });
    }

    /**
     * Split scan range params into groupNum groups by each group's row count.
     */
    private static List<List<TScanRangeParams>> splitScanRangeParamByRowCount(List<TScanRangeParams> scanRangeParams,
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
            dataSizePerGroup[minIndex] += Math.max(1, scanRangeParam.getScan_range().getInternal_scan_range().getRow_count());
            result.get(minIndex).add(scanRangeParam);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("dataSizePerGroup: {}", dataSizePerGroup);
        }

        return result;
    }
}
