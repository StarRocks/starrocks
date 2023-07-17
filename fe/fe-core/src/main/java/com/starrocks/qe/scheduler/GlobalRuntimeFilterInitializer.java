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

package com.starrocks.qe.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GlobalRuntimeFilterInitializer {
    private final ExecutionDAG executionDAG;
    private final ConnectContext connectContext;
    private final boolean usePipeline;

    public GlobalRuntimeFilterInitializer(ExecutionDAG executionDAG, ConnectContext connectContext,
                                          boolean usePipeline) {
        this.executionDAG = executionDAG;
        this.connectContext = connectContext;
        this.usePipeline = usePipeline;
    }

    public void setGlobalRuntimeFilterParams(ExecutionFragment rootFragment, TNetworkAddress mergeHost) {
        Map<Integer, List<TRuntimeFilterProberParams>> broadcastGRFProbersMap = Maps.newHashMap();
        List<RuntimeFilterDescription> broadcastGRFList = Lists.newArrayList();
        Map<Integer, List<TRuntimeFilterProberParams>> idToProbePrams = new HashMap<>();

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            PlanFragment fragment = execFragment.getPlanFragment();

            fragment.collectBuildRuntimeFilters(fragment.getPlanRoot());
            fragment.collectProbeRuntimeFilters(fragment.getPlanRoot());

            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = execFragment.getInstances().stream()
                        .map(instance -> new TRuntimeFilterProberParams()
                                .setFragment_instance_id(instance.getInstanceId())
                                .setFragment_instance_address(instance.getWorker().getBrpcAddress()))
                        .collect(Collectors.toList());
                if (usePipeline && kv.getValue().isBroadcastJoin() && kv.getValue().isHasRemoteTargets()) {
                    broadcastGRFProbersMap.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                } else {
                    idToProbePrams.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                }
            }

            Set<TUniqueId> broadcastGRfSenders = pickupInstancesOnDifferentHosts(execFragment.getInstances(), 3)
                    .stream()
                    .map(FragmentInstance::getInstanceId)
                    .collect(Collectors.toSet());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getBuildRuntimeFilters().entrySet()) {
                int rid = kv.getKey();
                RuntimeFilterDescription rf = kv.getValue();
                if (rf.isHasRemoteTargets()) {
                    if (rf.isBroadcastJoin()) {
                        // for broadcast join, we send at most 3 copy to probers, the first arrival wins.
                        rootFragment.getRuntimeFilterParams()
                                .putToRuntime_filter_builder_number(rid, 1);
                        if (usePipeline) {
                            rf.setBroadcastGRFSenders(broadcastGRfSenders);
                            broadcastGRFList.add(rf);
                        } else {
                            rf.setSenderFragmentInstanceId(execFragment.getInstances().get(0).getInstanceId());
                        }
                    } else {
                        rootFragment.getRuntimeFilterParams()
                                .putToRuntime_filter_builder_number(rid, execFragment.getInstances().size());
                    }
                }
            }
            fragment.setRuntimeFilterMergeNodeAddresses(fragment.getPlanRoot(), mergeHost);

            for (RuntimeFilterDescription rf : fragment.getBuildRuntimeFilters().values()) {
                if (!rf.isColocateOrBucketShuffle()) {
                    continue;
                }
                rf.setBucketSeqToInstance(execFragment.getBucketSeqToInstance());
            }
        }
        rootFragment.getRuntimeFilterParams().setId_to_prober_params(idToProbePrams);

        broadcastGRFList.forEach(rf ->
                rf.setBroadcastGRFDestinations(mergeGRFProbers(broadcastGRFProbersMap.get(rf.getFilterId()))));

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        rootFragment.getRuntimeFilterParams()
                .setRuntime_filter_max_size(sessionVariable.getGlobalRuntimeFilterBuildMaxSize());

    }

    // choose at most num FInstances on difference BEs
    private List<FragmentInstance> pickupInstancesOnDifferentHosts(List<FragmentInstance> instances, int num) {
        if (instances.size() <= num) {
            return instances;
        }

        Map<Long, List<FragmentInstance>> host2instances = instances.stream()
                .collect(Collectors.groupingBy(
                        FragmentInstance::getWorkerId,
                        Collectors.mapping(Function.identity(), Collectors.toList())
                ));

        List<FragmentInstance> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<FragmentInstance> instancesPerHost : host2instances.values()) {
                if (instancesPerHost.isEmpty()) {
                    continue;
                }
                picked.add(instancesPerHost.remove(0));
            }
        }
        return picked;
    }

    private List<TRuntimeFilterDestination> mergeGRFProbers(List<TRuntimeFilterProberParams> probers) {
        Map<TNetworkAddress, List<TUniqueId>> host2probers = probers.stream()
                .collect(Collectors.groupingBy(
                        TRuntimeFilterProberParams::getFragment_instance_address,
                        Collectors.mapping(TRuntimeFilterProberParams::getFragment_instance_id, Collectors.toList())
                ));
        return host2probers.entrySet()
                .stream()
                .map(e -> new TRuntimeFilterDestination()
                        .setAddress(e.getKey())
                        .setFinstance_ids(e.getValue()))
                .collect(Collectors.toList());
    }
}
