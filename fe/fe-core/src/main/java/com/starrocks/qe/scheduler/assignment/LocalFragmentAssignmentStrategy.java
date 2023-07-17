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
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListUtil;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeVisitor;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.qe.BackendSelector;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.FragmentScanRangeAssignment;
import com.starrocks.qe.HDFSBackendSelector;
import com.starrocks.qe.NormalBackendSelector;
import com.starrocks.qe.ReplicatedBackendSelector;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.SchedulerException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobInformation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LocalFragmentAssignmentStrategy implements FragmentAssignmentStrategy {
    private static final Logger LOG = LogManager.getLogger(LocalFragmentAssignmentStrategy.class);

    private final ConnectContext connectContext;
    private final WorkerProvider workerProvider;
    private final ExecutionDAG executionDAG;
    private final boolean usePipeline;

    private final Set<Integer> replicatedScanIds = Sets.newHashSet();

    private final boolean isLoad;

    public LocalFragmentAssignmentStrategy(ConnectContext connectContext, WorkerProvider workerProvider,
                                           ExecutionDAG executionDAG,
                                           boolean usePipeline) {
        this.connectContext = connectContext;
        this.workerProvider = workerProvider;
        this.executionDAG = executionDAG;
        this.usePipeline = usePipeline;

        JobInformation jobInformation = executionDAG.getJobInformation();
        this.isLoad = jobInformation.isLoadType();
    }

    @Override
    public void assignWorkerToFragment(ExecutionFragment execFragment) throws UserException {
        for (ScanNode scanNode : execFragment.getScanNodes()) {
            assignScanRangesToWorkers(execFragment, scanNode);
        }

        assignWorkerScanRangeToFragmentInstance(execFragment);

        // The fragment which only contains scan nodes without scan ranges,
        // such as SchemaScanNode, is assigned to an arbitrary worker.
        if (execFragment.getInstances().isEmpty()) {
            Long workerId = workerProvider.selectNextWorker();
            FragmentInstance instance =
                    new FragmentInstance(workerProvider.getWorkerById(workerId), workerId, execFragment);
            execFragment.addInstance(instance);
        }
    }

    private void assignWorkerScanRangeToFragmentInstance(ExecutionFragment execFragment) {
        ColocatedBackendSelector.Assignment colocatedAssignment = execFragment.getColocatedAssignment();
        boolean hasColocate = execFragment.isColocated()
                && colocatedAssignment != null
                && colocatedAssignment.getSeqToWorkerId().size() > 0;
        boolean hasBucketShuffle = execFragment.isBucketShuffleJoin() && colocatedAssignment != null;

        if (hasColocate || hasBucketShuffle) {
            assignToLocalColocatedFragment(execFragment,
                    colocatedAssignment.getSeqToWorkerId(),
                    colocatedAssignment.getSeqToScanRange());
        } else {
            assignToLocalNormalFragment(execFragment);
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

    private boolean shouldAddScanRanges(Set<Integer> visitedReplicatedScanIds, Integer scanId) {
        if (!replicatedScanIds.contains(scanId)) {
            return true;
        }
        if (visitedReplicatedScanIds.contains(scanId)) {
            return false;
        }
        visitedReplicatedScanIds.add(scanId);
        return true;
    }

    private void assignToLocalColocatedFragment(ExecutionFragment execFragment,
                                                Map<Integer, Long> bucketSeqToWorkerId,
                                                ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        int parallelExecInstanceNum = fragment.getParallelExecNum();
        int pipelineDop = fragment.getPipelineDop();

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<Long, List<Integer>> workerIdToBucketSeqs = bucketSeqToWorkerId.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));

        boolean assignPerDriverSeq = usePipeline && workerIdToBucketSeqs.values().stream()
                .allMatch(bucketSeqs -> enableAssignScanRangesPerDriverSeq(bucketSeqs, pipelineDop));

        workerIdToBucketSeqs.forEach((workerId, bucketSeqsOfAddr) -> {
            // 2. split how many scanRange one instance should scan
            List<List<Integer>> bucketSeqsPerInstance = ListUtil.splitBySize(bucketSeqsOfAddr, parallelExecInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scanned by instance
            bucketSeqsPerInstance.forEach(bucketSeqsOfInstance -> {
                FragmentInstance instance =
                        new FragmentInstance(workerProvider.getWorkerById(workerId), workerId, execFragment);
                execFragment.addInstance(instance);

                // record each instance replicate scan id in set, to avoid add replicate scan range repeatedly when they are in different buckets
                Set<Integer> instanceReplicatedScanIds = new HashSet<>();

                if (!assignPerDriverSeq) {
                    bucketSeqsOfInstance.forEach(bucketSeq -> {
                        instance.addBucketSeq(bucketSeq);
                        bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                            if (shouldAddScanRanges(instanceReplicatedScanIds, scanId)) {
                                instance.addScanRanges(scanId, scanRanges);
                            }
                        });
                    });
                } else {
                    List<List<Integer>> bucketSeqsPerDriverSeq =
                            ListUtil.splitBySize(bucketSeqsOfInstance, pipelineDop);

                    instance.setPipelineDop(bucketSeqsPerDriverSeq.size());

                    IntStream.range(0, bucketSeqsPerDriverSeq.size())
                            .forEachOrdered(driverSeq -> bucketSeqsPerDriverSeq.get(driverSeq).forEach(bucketSeq -> {
                                instance.addBucketSeqAndDriverSeq(bucketSeq, driverSeq);
                                bucketSeqToScanRange.get(bucketSeq).forEach((scanId, scanRanges) -> {
                                    if (shouldAddScanRanges(instanceReplicatedScanIds, scanId)) {
                                        instance.addScanRanges(scanId, driverSeq, scanRanges);
                                    }
                                });
                            }));

                    instance.paddingScanRanges();
                }
            });
        });
    }

    private void assignToLocalNormalFragment(ExecutionFragment execFragment) {
        final PlanFragment fragment = execFragment.getPlanFragment();
        int parallelExecInstanceNum = fragment.getParallelExecNum();
        int pipelineDop = fragment.getPipelineDop();

        execFragment.getScanRangeAssignment().forEach((workerId, scanRangesPerAddr) -> {
            // 1. Handle normal scan node firstly
            scanRangesPerAddr.forEach((scanId, scanRangesOfNode) -> {
                if (replicatedScanIds.contains(scanId)) {
                    return;
                }

                List<List<TScanRangeParams>> scanRangesPerInstance = ListUtil.splitBySize(scanRangesOfNode,
                        parallelExecInstanceNum);

                for (List<TScanRangeParams> scanRanges : scanRangesPerInstance) {
                    FragmentInstance instance =
                            new FragmentInstance(workerProvider.getWorkerById(workerId), workerId, execFragment);
                    execFragment.addInstance(instance);

                    if (!enableAssignScanRangesPerDriverSeq(fragment, scanRanges)) {
                        instance.addScanRanges(scanId, scanRanges);
                    } else {
                        List<List<TScanRangeParams>> scanRangesPerDriverSeq =
                                ListUtil.splitBySize(scanRanges, pipelineDop);

                        if (fragment.isForceAssignScanRangesPerDriverSeq() &&
                                scanRangesPerDriverSeq.size() != pipelineDop) {
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
                scanRangesPerAddr.forEach((scanId, scanRangesPerNode) -> {
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

    private void assignScanRangesToWorkers(ExecutionFragment executionFragment, ScanNode scanNode)
            throws UserException {
        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        if (locations == null) {
            // only analysis olap scan node
            return;
        }

        BackendSelectorFactory factory = new BackendSelectorFactory();
        BackendSelectorFactory.Context context = factory.new Context(executionFragment, locations);
        BackendSelector backendSelector = factory.create(scanNode, context);
        backendSelector.computeScanRangeAssignment();

        if (LOG.isDebugEnabled()) {
            LOG.debug(executionFragment.getScanRangeAssignment().toDebugString());
        }
    }

    private class BackendSelectorFactory implements PlanNodeVisitor<BackendSelector, BackendSelectorFactory.Context> {
        private class Context {
            private final ExecutionFragment executionFragment;
            private final List<TScanRangeLocations> locations;

            public Context(ExecutionFragment executionFragment, List<TScanRangeLocations> locations) {
                this.executionFragment = executionFragment;
                this.locations = locations;
            }
        }

        public BackendSelector create(ScanNode scanNode, Context context) throws SchedulerException {
            return scanNode.accept(this, context);
        }

        @Override
        public BackendSelector visit(PlanNode node, Context ctx) throws SchedulerException {
            String msg = String.format("BackendSelectorFactory cannot handle plan node [%s]", node);
            throw new SchedulerException(msg);
        }

        @Override
        public BackendSelector visitScanNode(ScanNode node, Context ctx) {
            FragmentScanRangeAssignment assignment = ctx.executionFragment.getScanRangeAssignment();

            boolean hasColocated = ctx.executionFragment.isColocated();
            boolean hasBucket = ctx.executionFragment.isBucketShuffleJoin();
            boolean hasReplicated = ctx.executionFragment.isReplicated();

            if (assignment.size() > 0 && hasReplicated && node.canDoReplicatedJoin()) {
                replicatedScanIds.add(node.getId().asInt());
                return new ReplicatedBackendSelector(node, ctx.locations, assignment,
                        ctx.executionFragment.getColocatedAssignment());
            } else if (hasColocated || hasBucket) {
                ColocatedBackendSelector.Assignment colocatedAssignment =
                        ctx.executionFragment.getOrCreateColocatedAssignment(
                                (OlapScanNode) node);
                boolean isRightOrFullBucketShuffleFragment = ctx.executionFragment.isRightOrFullBucketShuffle();
                return new ColocatedBackendSelector((OlapScanNode) node, assignment,
                        colocatedAssignment, isRightOrFullBucketShuffleFragment, workerProvider);
            } else {
                return new NormalBackendSelector(node, ctx.locations, assignment, workerProvider, isLoad);
            }
        }

        @Override
        public BackendSelector visitSchemaScanNode(SchemaScanNode node, Context ctx) {
            return new NormalBackendSelector(node, ctx.locations, ctx.executionFragment.getScanRangeAssignment(),
                    workerProvider, false);
        }

        private BackendSelector visitConnectorScanNode(ScanNode node, Context ctx) {
            SessionVariable sv = connectContext.getSessionVariable();
            return new HDFSBackendSelector(node, ctx.locations, ctx.executionFragment.getScanRangeAssignment(),
                    workerProvider,
                    sv.getForceScheduleLocal(),
                    sv.getHDFSBackendSelectorScanRangeShuffle());
        }

        @Override
        public BackendSelector visitHdfsScanNode(HdfsScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }

        @Override
        public BackendSelector visitIcebergScanNode(IcebergScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }

        @Override
        public BackendSelector visitHudiScanNode(HudiScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }

        @Override
        public BackendSelector visitDeltaLakeScanNode(DeltaLakeScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }

        @Override
        public BackendSelector visitFileTableScanNode(FileTableScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }

        @Override
        public BackendSelector visitPaimonScanNode(PaimonScanNode node, Context ctx) {
            return visitConnectorScanNode(node, ctx);
        }
    }
}
