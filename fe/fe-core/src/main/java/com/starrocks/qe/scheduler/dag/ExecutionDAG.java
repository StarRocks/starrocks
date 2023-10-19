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

package com.starrocks.qe.scheduler.dag;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.MultiCastDataSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.SchedulerException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The execution DAG represents the distributed execution of a job, mainly including parallel instances of fragments and the
 * communication between them.
 *
 * <p>The execution DAG consists of the following components:
 *
 * <ul>
 *  <li>The {@link ExecutionFragment} represents a collection of multiple parallel instances of a {@link PlanFragment}.
 *  <li>The {@link FragmentInstance} represents a parallel instance of a {@link PlanFragment}.
 *  <li> The {@link FragmentInstanceExecState} represents the execution state of a {@link FragmentInstance} for a single attempt.
 * </ul>
 */
public class ExecutionDAG {
    private static final Logger LOG = LogManager.getLogger(ExecutionDAG.class);

    private final JobSpec jobSpec;
    private final List<ExecutionFragment> fragments;
    private final Map<PlanFragmentId, ExecutionFragment> idToFragment;

    /**
     * {@code instanceIdToInstance} and {@link #workerIdToNumInstances} will be calculated in {@link #finalizeDAG()},
     * after all the fragment instances have already been added to the DAG .
     */
    private final Map<TUniqueId, FragmentInstance> instanceIdToInstance;
    private Map<Long, Integer> workerIdToNumInstances = Maps.newHashMap();

    /**
     * The executions will be added to {@code indexInJobToExecState}, when it is deploying.
     */
    private final ConcurrentMap<Integer, FragmentInstanceExecState> indexInJobToExecState = new ConcurrentSkipListMap<>();

    /**
     * Backend which state need to be checked when joining this coordinator.
     * It is supposed to be the subset of backendExecStates.
     */
    private final List<FragmentInstanceExecState> needCheckExecutions = Lists.newArrayList();
    /**
     * Used only by channel stream load, records the mapping from channel id to target BE's address
     */
    private final Map<Integer, TNetworkAddress> channelIdToBEHTTP = Maps.newHashMap();
    private final Map<Integer, TNetworkAddress> channelIdToBEPort = Maps.newHashMap();

    private ExecutionDAG(JobSpec jobSpec) {
        this.jobSpec = jobSpec;
        this.fragments = Lists.newArrayList();
        this.idToFragment = Maps.newHashMap();
        this.instanceIdToInstance = Maps.newHashMap();
    }

    public static ExecutionDAG build(JobSpec jobSpec) {
        ExecutionDAG executionDAG = new ExecutionDAG(jobSpec);

        executionDAG.attachFragments(jobSpec.getFragments());

        return executionDAG;
    }

    public void attachFragments(List<PlanFragment> planFragments) {
        for (PlanFragment planFragment : planFragments) {
            ExecutionFragment fragment = new ExecutionFragment(this, planFragment, fragments.size());
            fragments.add(fragment);
            idToFragment.put(planFragment.getFragmentId(), fragment);
        }
    }

    public Set<TUniqueId> getInstanceIds() {
        return instanceIdToInstance.keySet();
    }

    public Collection<FragmentInstance> getInstances() {
        return instanceIdToInstance.values();
    }

    public FragmentInstance getInstanceByInstanceId(TUniqueId instanceId) {
        return instanceIdToInstance.get(instanceId);
    }

    public ExecutionFragment getRootFragment() {
        return fragments.get(0);
    }

    public Collection<ScanNode> getScanNodes() {
        return fragments.stream()
                .flatMap(fragment -> fragment.getScanNodes().stream())
                .collect(Collectors.toList());
    }

    public List<ExecutionFragment> getFragmentsInCreatedOrder() {
        return fragments;
    }

    public List<ExecutionFragment> getFragmentsInPreorder() {
        return fragments;
    }

    public List<ExecutionFragment> getFragmentsInPostorder() {
        return Lists.reverse(fragments);
    }

    /**
     * Compute the topological order of the fragment tree.
     * It will divide fragments to several groups.
     * - There is no data dependency among fragments in a group.
     * - All the upstream fragments of the fragments in a group must belong to the previous groups.
     * - Each group should be delivered sequentially, and fragments in a group can be delivered concurrently.
     *
     * <p>For example, the following tree will produce four groups: [[1], [2, 3, 4], [5, 6], [7]]
     * <pre>{@code
     *                 1
     *                 │
     *            ┌────┼────┐
     *            │    │    │
     *            2    3    4
     *            │    │    │
     *         ┌──┴─┐  │    │
     *         │    │  │    │
     *         5    6  │    │
     *              │  │    │
     *              └──┼────┘
     *                 │
     *                 7
     * }</pre>
     *
     * @return multiple fragment groups.
     */
    public List<List<ExecutionFragment>> getFragmentsInTopologicalOrderFromRoot() {
        Queue<ExecutionFragment> queue = Lists.newLinkedList();
        Map<ExecutionFragment, Integer> inDegrees = Maps.newHashMap();

        ExecutionFragment root = getRootFragment();

        // Compute in-degree of each fragment by BFS.
        // `queue` contains the fragments need to visit its in-edges.
        inDegrees.put(root, 0);
        queue.add(root);
        while (!queue.isEmpty()) {
            ExecutionFragment fragment = queue.poll();
            for (int i = 0; i < fragment.childrenSize(); i++) {
                ExecutionFragment child = fragment.getChild(i);
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
            for (ExecutionFragment fragment : fragments) {
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
        List<List<ExecutionFragment>> groups = Lists.newArrayList();
        int numOutputFragments = 0;
        while (!queue.isEmpty()) {
            int groupSize = queue.size();
            List<ExecutionFragment> group = new ArrayList<>(groupSize);
            // The next `groupSize` fragments can be delivered concurrently, because zero in-degree indicates that
            // they don't depend on each other and all the fragments depending on them have been delivered.
            for (int i = 0; i < groupSize; ++i) {
                ExecutionFragment fragment = Preconditions.checkNotNull(queue.poll());
                group.add(fragment);

                for (int j = 0; j < fragment.childrenSize(); j++) {
                    ExecutionFragment child = fragment.getChild(j);
                    int degree = inDegrees.compute(child, (k, v) -> Preconditions.checkNotNull(v) - 1);
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

    public Map<PlanFragmentId, ExecutionFragment> getIdToFragment() {
        return idToFragment;
    }

    public ExecutionFragment getFragment(PlanFragmentId fragmentId) {
        return idToFragment.get(fragmentId);
    }

    public boolean isGatherOutput() {
        return fragments.get(0).getPlanFragment().getDataPartition() == DataPartition.UNPARTITIONED;
    }

    /**
     * Do the finalize work after all the fragment instances have already been added to the DAG, including:
     *
     * <ul>
     *  <li>Assign instance id to fragment instances.
     *  <li>Assign monotonic unique indexInJob to fragment instances to keep consistent order with indexInFragment.
     *      Also see {@link FragmentInstance#getIndexInJob()}.
     *  <li>Connect each fragment to its destination fragments
     *  <li>Setup {@link #workerIdToNumInstances}, {@link #channelIdToBEHTTP}, and {@link #channelIdToBEPort}
     * </ul>
     *
     * @throws SchedulerException when there is something wrong for the plan or execution information.
     */
    public void finalizeDAG() throws SchedulerException {
        // Assign monotonic unique indexInJob to fragment instances to keep consistent order with indexInFragment.
        int index = 0;
        for (ExecutionFragment fragment : fragments) {
            for (FragmentInstance instance : fragment.getInstances()) {
                setInstanceId(instance);
                instance.setIndexInJob(index++);
            }
        }

        for (ExecutionFragment fragment : fragments) {
            connectFragmentToDestFragments(fragment);
        }

        workerIdToNumInstances = fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .collect(Collectors.groupingBy(
                        FragmentInstance::getWorkerId,
                        Collectors.summingInt(instance -> 1)
                ));

        if (jobSpec.isStreamLoad()) {
            fragments.stream()
                    .flatMap(fragment -> fragment.getInstances().stream())
                    .forEach(instance -> Stream.concat(
                                    instance.getNode2ScanRanges().values().stream(),
                                    instance.getNode2DriverSeqToScanRanges().values().stream()
                                            .flatMap(driverSeqToScanRanges -> driverSeqToScanRanges.values().stream()))
                            .flatMap(Collection::stream)
                            .forEach(scanRange -> {
                                int channelId = scanRange.scan_range.broker_scan_range.channel_id;
                                ComputeNode worker = instance.getWorker();
                                channelIdToBEHTTP.put(channelId, worker.getHttpAddress());
                                channelIdToBEPort.put(channelId, worker.getAddress());
                            })
                    );
        }
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTP() {
        return channelIdToBEHTTP;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPort() {
        return channelIdToBEPort;
    }

    public int getNumInstancesOfWorkerId(Long addr) {
        return workerIdToNumInstances.get(addr);
    }

    public void addExecution(FragmentInstanceExecState execution) {
        FragmentInstance instance = instanceIdToInstance.get(execution.getInstanceId());
        if (instance != null) {
            instance.setExecution(execution);
        }
        indexInJobToExecState.put(execution.getIndexInJob(), execution);
    }

    public void addNeedCheckExecution(FragmentInstanceExecState execution) {
        needCheckExecutions.add(execution);
    }

    public List<FragmentInstanceExecState> getNeedCheckExecutions() {
        return needCheckExecutions;
    }

    public Collection<Integer> getExecutionIndexesInJob() {
        return indexInJobToExecState.keySet();
    }

    public Collection<FragmentInstanceExecState> getExecutions() {
        return indexInJobToExecState.values();
    }

    public FragmentInstanceExecState getExecution(int indexInJob) {
        return indexInJobToExecState.get(indexInJob);
    }

    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .map(FragmentInstance::getExecution)
                .filter(Objects::nonNull)
                .map(FragmentInstanceExecState::buildFragmentInstanceInfo)
                .collect(Collectors.toList());
    }

    public void resetExecutions() {
        fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .forEach(instance -> instance.setExecution(null));
        indexInJobToExecState.clear();
        needCheckExecutions.clear();
        instanceIdToInstance.clear();
    }

    /**
     * Compute destinations and # senders per exchange node.
     *
     * @param execFragment The fragment to be initialized.
     * @throws SchedulerException when there is something wrong for the plan or execution information.
     */
    private void connectFragmentToDestFragments(ExecutionFragment execFragment) throws SchedulerException {
        if (execFragment.getPlanFragment() instanceof MultiCastPlanFragment) {
            connectMultiCastFragmentToDestFragments(execFragment, (MultiCastPlanFragment) execFragment.getPlanFragment());
        } else {
            connectNormalFragmentToDestFragments(execFragment);
        }
    }

    private boolean needScheduleByLocalBucketShuffleJoin(ExecutionFragment destFragment, DataSink sourceSink) {
        if (destFragment.isLocalBucketShuffleJoin() && sourceSink instanceof DataStreamSink) {
            DataStreamSink streamSink = (DataStreamSink) sourceSink;
            return streamSink.getOutputPartition().isBucketShuffle();
        }
        return false;
    }

    private void connectMultiCastFragmentToDestFragments(ExecutionFragment execFragment, MultiCastPlanFragment fragment)
            throws SchedulerException {
        Preconditions.checkState(fragment.getSink() instanceof MultiCastDataSink);
        MultiCastDataSink multiSink = (MultiCastDataSink) fragment.getSink();

        // set # of senders

        for (int i = 0; i < fragment.getDestFragmentList().size(); i++) {
            PlanFragment destFragment = fragment.getDestFragmentList().get(i);

            if (destFragment == null) {
                continue;
            }

            ExecutionFragment destExecFragment = idToFragment.get(destFragment.getFragmentId());
            DataStreamSink sink = multiSink.getDataStreamSinks().get(i);

            // Set params for pipeline level shuffle.
            fragment.getDestNode(i).setPartitionType(fragment.getOutputPartition().getType());
            sink.setExchDop(destFragment.getPipelineDop());

            Integer exchangeId = sink.getExchNodeId().asInt();
            // MultiCastSink only send to itself, destination exchange only one sender,
            // and it doesn't support sort-merge
            Preconditions.checkState(!destExecFragment.getNumSendersPerExchange().containsKey(exchangeId));
            destExecFragment.getNumSendersPerExchange().put(exchangeId, 1);

            if (needScheduleByLocalBucketShuffleJoin(destExecFragment, sink)) {
                throw new NonRecoverableException("CTE consumer fragment cannot be bucket shuffle join");
            } else {
                // add destination host to this fragment's destination
                for (FragmentInstance destInstance : destExecFragment.getInstances()) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();

                    dest.setFragment_instance_id(destInstance.getInstanceId());
                    ComputeNode worker = destInstance.getWorker();
                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(worker.getAddress());
                    dest.setBrpc_server(worker.getBrpcIpAddress());

                    multiSink.getDestinations().get(i).add(dest);
                }
            }
        }
    }

    private void connectNormalFragmentToDestFragments(ExecutionFragment execFragment) {
        PlanFragment fragment = execFragment.getPlanFragment();
        PlanFragment destFragment = fragment.getDestFragment();

        if (destFragment == null) {
            // root plan fragment
            return;
        }

        ExecutionFragment destExecFragment = idToFragment.get(destFragment.getFragmentId());
        DataSink sink = fragment.getSink();

        // Set params for pipeline level shuffle.
        fragment.getDestNode().setPartitionType(fragment.getOutputPartition().getType());
        if (sink instanceof DataStreamSink) {
            DataStreamSink dataStreamSink = (DataStreamSink) sink;
            dataStreamSink.setExchDop(destFragment.getPipelineDop());
        }

        Integer exchangeId = sink.getExchNodeId().asInt();
        destExecFragment.getNumSendersPerExchange().compute(exchangeId, (k, oldNumSenders) -> {
            if (oldNumSenders == null) {
                return execFragment.getInstances().size();
            }
            // we might have multiple fragments sending to this exchange node
            // (distributed MERGE), which is why we need to add up the #senders
            // e.g. sort-merge
            return oldNumSenders + execFragment.getInstances().size();
        });

        // We can only handle unpartitioned (= broadcast) and hash-partitioned output at the moment.
        if (needScheduleByLocalBucketShuffleJoin(destExecFragment, sink)) {
            Map<Integer, FragmentInstance> bucketSeqToDestInstance = Maps.newHashMap();
            for (FragmentInstance destInstance : destExecFragment.getInstances()) {
                for (int bucketSeq : destInstance.getBucketSeqs()) {
                    bucketSeqToDestInstance.put(bucketSeq, destInstance);
                }
            }

            TNetworkAddress dummyServer = new TNetworkAddress("0.0.0.0", 0);
            int bucketNum = destExecFragment.getBucketNum();
            for (int bucketSeq = 0; bucketSeq < bucketNum; bucketSeq++) {
                TPlanFragmentDestination dest = new TPlanFragmentDestination();

                FragmentInstance destInstance = bucketSeqToDestInstance.get(bucketSeq);
                if (destInstance == null) {
                    // dest bucket may be pruned, these bucket dest should be set an invalid value
                    // and will be deal with in BE's DataStreamSender
                    dest.setFragment_instance_id(new TUniqueId(-1, -1));
                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(dummyServer);
                    dest.setBrpc_server(dummyServer);
                } else {
                    dest.setFragment_instance_id(destInstance.getInstanceId());
                    ComputeNode worker = destInstance.getWorker();
                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(worker.getAddress());
                    dest.setBrpc_server(worker.getBrpcIpAddress());

                    int driverSeq = destInstance.getDriverSeqOfBucketSeq(bucketSeq);
                    if (driverSeq != FragmentInstance.ABSENT_DRIVER_SEQUENCE) {
                        dest.setPipeline_driver_sequence(driverSeq);
                    }
                }
                execFragment.addDestination(dest);
            }
        } else {
            // add destination host to this fragment's destination
            for (FragmentInstance destInstance : destExecFragment.getInstances()) {
                TPlanFragmentDestination dest = new TPlanFragmentDestination();

                dest.setFragment_instance_id(destInstance.getInstanceId());
                ComputeNode worker = destInstance.getWorker();
                // NOTE(zc): can be removed in version 4.0
                dest.setDeprecated_server(worker.getAddress());
                dest.setBrpc_server(worker.getBrpcIpAddress());

                execFragment.addDestination(dest);
            }
        }
    }

    private void setInstanceId(FragmentInstance instance) {
        TUniqueId jobId = jobSpec.getQueryId();
        TUniqueId instanceId = new TUniqueId();
        instanceId.setHi(jobId.hi);
        instanceId.setLo(jobId.lo + instanceIdToInstance.size() + 1);

        instance.setInstanceId(instanceId);
        instanceIdToInstance.put(instanceId, instance);
    }

}
