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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.qe.WorkerProviderHelper.getNextWorker;

/**
 * SkipBlacklistWorkerProvider extends DefaultWorkerProvider and skips backend blacklist verification.
 * This provider filters workers based only on availability (alive status), ignoring the blacklist.
 */
public class SkipBlacklistWorkerProvider extends DefaultWorkerProvider {
    private static final Logger LOG = LogManager.getLogger(SkipBlacklistWorkerProvider.class);

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public SkipBlacklistWorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                                                   boolean preferComputeNode, int numUsedComputeNodes,
                                                                   ComputationFragmentSchedulingPolicy schedulingPolicy,
                                                                   ComputeResource computeResource) {

            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    buildComputeNodeInfoSkipBlacklist(systemInfoService, numUsedComputeNodes,
                            schedulingPolicy, computeResource);

            ImmutableMap<Long, ComputeNode> idToBackend = ImmutableMap.copyOf(systemInfoService.getIdToBackend());

            if (LOG.isDebugEnabled()) {
                LOG.debug("SkipBlacklistWorkerProvider - idToBackend size={}", idToBackend.size());
                for (Map.Entry<Long, ComputeNode> entry : idToBackend.entrySet()) {
                    Long backendID = entry.getKey();
                    ComputeNode backend = entry.getValue();
                    LOG.debug("backend: {}-{}-{}", backendID, backend.getHost(), backend.getBePort());
                }

                LOG.debug("idToComputeNode: {}", idToComputeNode);
            }

            return new SkipBlacklistWorkerProvider(idToBackend, idToComputeNode,
                    filterAvailableWorkersSkipBlacklist(idToBackend),
                    filterAvailableWorkersSkipBlacklist(idToComputeNode),
                    preferComputeNode, computeResource);
        }

        private static ImmutableMap<Long, ComputeNode> buildComputeNodeInfoSkipBlacklist(
                SystemInfoService systemInfoService,
                int numUsedComputeNodes,
                ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                ComputeResource computeResource) {
            //define Node Pool
            Map<Long, ComputeNode> computeNodes = new HashMap<>();

            //get CN and BE from systemInfoService
            ImmutableMap<Long, ComputeNode> idToComputeNode
                    = ImmutableMap.copyOf(systemInfoService.getIdComputeNode());
            ImmutableMap<Long, ComputeNode> idToBackend
                    = ImmutableMap.copyOf(systemInfoService.getIdToBackend());

            //add CN and BE to Node Pool
            if (numUsedComputeNodes <= 0) {
                computeNodes.putAll(idToComputeNode);
                if (computationFragmentSchedulingPolicy == ComputationFragmentSchedulingPolicy.ALL_NODES) {
                    computeNodes.putAll(idToBackend);
                }
            } else {
                for (int i = 0; i < idToComputeNode.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                    ComputeNode computeNode =
                            getNextWorker(idToComputeNode, DefaultWorkerProvider::getNextComputeNodeIndex,
                                    computeResource);
                    Preconditions.checkNotNull(computeNode);
                    if (!isWorkerAvailableSkipBlacklist(computeNode)) {
                        continue;
                    }
                    computeNodes.put(computeNode.getId(), computeNode);
                }
                if (computationFragmentSchedulingPolicy == ComputationFragmentSchedulingPolicy.ALL_NODES) {
                    for (int i = 0; i < idToBackend.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                        ComputeNode backend =
                                getNextWorker(idToBackend, DefaultWorkerProvider::getNextBackendIndex, computeResource);
                        Preconditions.checkNotNull(backend);
                        if (!isWorkerAvailableSkipBlacklist(backend)) {
                            continue;
                        }
                        computeNodes.put(backend.getId(), backend);
                    }

                }
            }

            //return Node Pool
            return ImmutableMap.copyOf(computeNodes);
        }

        private static <C extends ComputeNode> ImmutableMap<Long, C> filterAvailableWorkersSkipBlacklist(
                ImmutableMap<Long, C> workers) {
            return ImmutableMap.copyOf(
                    workers.entrySet().stream()
                            .filter(entry -> isWorkerAvailableSkipBlacklist(entry.getValue()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        private static boolean isWorkerAvailableSkipBlacklist(ComputeNode worker) {
            // Only check if worker is alive, skip blacklist verification
            return worker.isAlive();
        }
    }

    protected SkipBlacklistWorkerProvider(ImmutableMap<Long, ComputeNode> id2Backend,
                                          ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                          ImmutableMap<Long, ComputeNode> availableID2Backend,
                                          ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                                          boolean preferComputeNode, ComputeResource computeResource) {
        super(id2Backend, id2ComputeNode, availableID2Backend, availableID2ComputeNode,
                preferComputeNode, computeResource);
    }
}
