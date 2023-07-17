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
import com.starrocks.qe.scheduler.ExecutionFragmentInstance;
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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecutionDAG {
    private static final Logger LOG = LogManager.getLogger(ExecutionDAG.class);

    private final JobInformation jobInformation;
    private final List<ExecutionFragment> fragments;
    private final Map<PlanFragmentId, ExecutionFragment> idToFragment;

    private final Map<TUniqueId, FragmentInstance> instanceIdToInstance;
    private Map<Long, Integer> workerIdToNumInstances = Maps.newHashMap();
    private final ConcurrentNavigableMap<Integer, ExecutionFragmentInstance> indexInJobToExecution =
            new ConcurrentSkipListMap<>();

    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private final List<ExecutionFragmentInstance> needCheckExecutions = Lists.newArrayList();
    // Used by stream load.
    private final Map<Integer, TNetworkAddress> channelIdToBEHTTP = Maps.newHashMap();
    private final Map<Integer, TNetworkAddress> channelIdToBEPort = Maps.newHashMap();

    private ExecutionDAG(JobInformation jobInformation) {
        this.jobInformation = jobInformation;
        this.fragments = Lists.newArrayList();
        this.idToFragment = Maps.newHashMap();
        this.instanceIdToInstance = Maps.newHashMap();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("ExecutionDAG{\n");
        for (ExecutionFragment fragment : fragments) {
            builder.append(fragment.getFragmentId()).append(":\n");
            for (FragmentInstance instance : fragment.getInstances()) {
                builder.append("\t- ").append(instance).append("\n");
            }
        }
        builder.append("}");

        return builder.toString();
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTP() {
        return channelIdToBEHTTP;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPort() {
        return channelIdToBEPort;
    }

    public void attachFragments(List<PlanFragment> planFragments) {
        for (PlanFragment planFragment : planFragments) {
            ExecutionFragment fragment = new ExecutionFragment(this, planFragment);
            planFragment.setExecutionFragment(fragment);
            this.fragments.add(fragment);
            this.idToFragment.put(planFragment.getFragmentId(), fragment);
        }
    }

    public void initFragment(ExecutionFragment execFragment) throws SchedulerException {
        for (FragmentInstance instance : execFragment.getInstances()) {
            setInstanceId(instance);
        }

        if (execFragment.getPlanFragment() instanceof MultiCastPlanFragment) {
            initMultiCastFragment(execFragment, (MultiCastPlanFragment) execFragment.getPlanFragment());
        } else {
            initNormalFragment(execFragment);
        }
    }

    public JobInformation getJobInformation() {
        return jobInformation;
    }

    private boolean needScheduleByBucketShuffleJoin(ExecutionFragment destFragment, DataSink sourceSink) {
        if (destFragment.isBucketShuffleJoin()) {
            if (sourceSink instanceof DataStreamSink) {
                DataStreamSink streamSink = (DataStreamSink) sourceSink;
                return streamSink.getOutputPartition().isBucketShuffle();
            }
        }
        return false;
    }

    private void initMultiCastFragment(ExecutionFragment execFragment, MultiCastPlanFragment fragment)
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

            if (needScheduleByBucketShuffleJoin(destExecFragment, sink)) {
                throw new SchedulerException("CTE consumer fragment cannot be bucket shuffle join");
            } else {
                // add destination host to this fragment's destination
                for (FragmentInstance destInstance : destExecFragment.getInstances()) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();

                    dest.setFragment_instance_id(destInstance.getInstanceId());
                    ComputeNode worker = destInstance.getWorker();
                    // NOTE(zc): can be removed in version 4.0
                    dest.setDeprecated_server(worker.getAddress());
                    dest.setBrpc_server(worker.getBrpcAddress());

                    multiSink.getDestinations().get(i).add(dest);
                }
            }
        }
    }

    private void initNormalFragment(ExecutionFragment execFragment) {
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
        if (needScheduleByBucketShuffleJoin(destExecFragment, sink)) {
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
                    dest.setBrpc_server(worker.getBrpcAddress());

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
                dest.setBrpc_server(worker.getBrpcAddress());

                execFragment.addDestination(dest);
            }
        }
    }

    public void setInstanceId(FragmentInstance instance) {
        TUniqueId jobId = jobInformation.getQueryId();
        TUniqueId instanceId = new TUniqueId();
        instanceId.setHi(jobId.hi);
        instanceId.setLo(jobId.lo + instanceIdToInstance.size() + 1);

        instance.setInstanceId(instanceId);
        instanceIdToInstance.put(instanceId, instance);
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

    public int getNumFragments() {
        return fragments.size();
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
                ExecutionFragment fragment = queue.poll();
                group.add(fragment);

                for (int j = 0; j < fragment.childrenSize(); j++) {
                    ExecutionFragment child = fragment.getChild(j);
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

    public ExecutionFragment getFragment(PlanFragmentId fragmentId) {
        return idToFragment.get(fragmentId);
    }

    public boolean isGatherOutput() {
        return fragments.get(0).getPlanFragment().getDataPartition() == DataPartition.UNPARTITIONED;
    }

    public void finalizeDAG() {
        workerIdToNumInstances = fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .collect(Collectors.groupingBy(
                        FragmentInstance::getWorkerId,
                        Collectors.summingInt(instance -> 1)
                ));

        // For a shuffle join, its shuffle partitions and corresponding one-map-one GRF components should have the same ordinals.
        // - Fragment instances' ordinals in ExecutionFragment.instances determine shuffle partitions' ordinals in DataStreamSink.
        // - IndexInJob of Fragment instances that contain shuffle join determine the ordinals of GRF components in the GRF.
        // Therefore, here assign monotonic unique indexInJob to Fragment instances to keep consistent order with Fragment
        // instances in ExecutionFragment.instances.
        int index = 0;
        for (ExecutionFragment fragment : fragments) {
            for (FragmentInstance instance : fragment.getInstances()) {
                instance.setIndexInJob(index++);
            }
        }

        if (jobInformation.isStreamLoad()) {
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

    public int getNumInstancesOfWorkerId(Long addr) {
        return workerIdToNumInstances.get(addr);
    }

    public void addExecution(ExecutionFragmentInstance execution) {
        FragmentInstance instance = execution.getInstance();
        if (instance != null) {
            instance.setExecution(execution);
        }
        indexInJobToExecution.put(execution.getIndexInJob(), execution);
    }

    public void addNeedCheckExecution(ExecutionFragmentInstance execution) {
        needCheckExecutions.add(execution);
    }

    public List<ExecutionFragmentInstance> getNeedCheckExecutions() {
        return needCheckExecutions;
    }

    public Collection<Integer> getExecutionIndexesInJob() {
        return indexInJobToExecution.keySet();
    }

    public Collection<ExecutionFragmentInstance> getExecutions() {
        return indexInJobToExecution.values();
    }

    public ExecutionFragmentInstance getExecution(int indexInJob) {
        return indexInJobToExecution.get(indexInJob);
    }

    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .map(FragmentInstance::getExecution)
                .filter(Objects::nonNull)
                .map(ExecutionFragmentInstance::buildFragmentInstanceInfo)
                .collect(Collectors.toList());
    }

    public void clearExportStatus() {
        fragments.stream()
                .flatMap(fragment -> fragment.getInstances().stream())
                .forEach(instance -> instance.setExecution(null));
        indexInJobToExecution.clear();
        needCheckExecutions.clear();
    }

    public static ExecutionDAG build(JobInformation jobInformation) {
        ExecutionDAG executionDAG = new ExecutionDAG(jobInformation);

        executionDAG.attachFragments(jobInformation.getFragments());

        return executionDAG;
    }

}
