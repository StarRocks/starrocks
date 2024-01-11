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

package com.starrocks.system;

import com.google.api.client.util.Sets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.clone.TabletChecker;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Select nodes when creating table or loading data.
 */
public class NodeSelector {
    private static final Logger LOG = LogManager.getLogger(NodeSelector.class);

    private final SystemInfoService systemInfoService;

    private long lastNodeIdForCreation = -1;
    private long lastNodeIdForOther = -1;

    private final Random locBackendRandSelector = new Random();

    public NodeSelector(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable,
                                                         boolean isCreate, Multimap<String, String> locReq,
                                                         TStorageMedium storageMedium) {

        return seqChooseBackendIds(backendNum, needAvailable, isCreate, locReq,
                v -> !v.checkDiskExceedLimitForCreate(storageMedium));
    }

    public Long seqChooseBackendOrComputeId() throws UserException {
        List<Long> backendIds = seqChooseBackendIds(1, true, false, null);
        if (CollectionUtils.isNotEmpty(backendIds)) {
            return backendIds.get(0);
        }
        if (RunMode.isSharedNothingMode()) {
            throw new UserException("No backend alive.");
        }
        List<Long> computeNodes = seqChooseComputeNodes(1, true, false);
        if (CollectionUtils.isNotEmpty(computeNodes)) {
            return computeNodes.get(0);
        }
        throw new UserException("No backend or compute node alive.");
    }

    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable,
                                          boolean isCreate, Multimap<String, String> locReq) {
        if (isCreate) {
            return seqChooseBackendIds(backendNum, needAvailable, true, locReq,
                    v -> !v.checkDiskExceedLimitForCreate());
        } else {
            return seqChooseBackendIds(backendNum, needAvailable, false, locReq,
                    v -> !v.checkDiskExceedLimit());
        }
    }

    public List<Long> seqChooseComputeNodes(int computeNodeNum, boolean needAvailable, boolean isCreate) {

        final List<ComputeNode> candidateComputeNodes =
                needAvailable ? systemInfoService.getAvailableComputeNodes() : systemInfoService.getComputeNodes();
        if (CollectionUtils.isEmpty(candidateComputeNodes)) {
            LOG.warn("failed to find any compute nodes, needAvailable={}", needAvailable);
            return Collections.emptyList();
        }

        return seqChooseNodeIds(computeNodeNum, isCreate, null, candidateComputeNodes);
    }

    private List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable,
                                           boolean isCreate, Multimap<String, String> locReq,
                                           Predicate<? super Backend> predicate) {
        final List<Backend> candidateBackends =
                needAvailable ? systemInfoService.getAvailableBackends() : systemInfoService.getBackends();
        if (CollectionUtils.isEmpty(candidateBackends)) {
            LOG.warn("failed to find any backend, needAvailable={}", needAvailable);
            return Collections.emptyList();
        }

        final List<ComputeNode> filteredBackends = candidateBackends.stream()
                .filter(predicate)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(filteredBackends)) {
            String backendInfo = candidateBackends.stream()
                    .map(backend -> "[host=" + backend.getHost() + ", bePort=" + backend.getBePort() + "]")
                    .collect(Collectors.joining("; "));

            LOG.warn("failed to find any backend with qualified disk usage from {}" +
                            " candidate backends, needAvailable={}, [{}]",
                    candidateBackends.size(), needAvailable, backendInfo);
            return Collections.emptyList();
        }
        return seqChooseNodeIds(backendNum, isCreate, locReq, filteredBackends);
    }

    /**
     * Get the matched backends, save in `locBackendIdList`.
     *
     * @return the number of backends in `locToBackendIds` which are on different hosts and
     * has different location with each other.
     */
    public static int getLocationMatchedBackendIdList(List<List<Long>> locBackendIdList,
                                                      final List<ComputeNode> srcNodes,
                                                      Multimap<String, String> locReq,
                                                      SystemInfoService systemInfoService) {
        // Location -> matched backend id list, e.g. rack:rack1 -> [1, 2, 3].
        Multimap<String, Long> locToBackendIds = HashMultimap.create();
        // Find all backends that match the location requirement.
        for (ComputeNode node : srcNodes) {
            if (node instanceof Backend) {
                Backend backend = (Backend) node;
                Pair<String, String> backendLocKV = backend.getSingleLevelLocationKV();
                if (backendLocKV != null &&
                        TabletChecker.isLocationMatch(backendLocKV.first, backendLocKV.second, locReq)) {
                    locToBackendIds.put(backendLocKV.first + ":" + backendLocKV.second, backend.getId());
                }
            }
        }

        // Count the number of backends with different hosts in `locToBackendIds`.
        int locBackendWithDiffHostLocNum = 0;
        Set<String> checkedHosts = Sets.newHashSet();
        Set<Pair<String, String>> checkedLocs = Sets.newHashSet();

        for (String loc : locToBackendIds.keySet()) {
            Collection<Long> backendIds = locToBackendIds.get(loc);
            List<Long> backendIdList = new ArrayList<>(backendIds);
            locBackendIdList.add(backendIdList);
            for (Long backendId : backendIds) {
                Backend backend = systemInfoService.getBackend(backendId);
                if (backend != null && !checkedHosts.contains(backend.getHost()) &&
                        !checkedLocs.contains(backend.getSingleLevelLocationKV())) {
                    locBackendWithDiffHostLocNum++;
                    checkedHosts.add(backend.getHost());
                    checkedLocs.add(backend.getSingleLevelLocationKV());
                }
            }
        }

        return  locBackendWithDiffHostLocNum;
    }

    /**
     * Choose nodes(currently only backend supported) matching the location requirement.
     * This is a **best-effort** choosing, if the location requirement cannot be met, for example,
     * user has specified 3-replica table and "rack:*" as location property, but we only have
     * 2 different rack tags, the creation won't fail, and the replicas will scatter on the 2 racks
     * as much as possible.
     */
    private List<Long> chooseNodesMatchLocReq(int nodeNum, Multimap<String, String> locReq,
                                              final List<ComputeNode> srcNodes) {
        // convert the Multimap to a flatten list so that we can randomly choose backend later
        // Backend id list which matches the location requirement, classified by location,
        // e.g. transform from {"rack:rack1": [1, 2, 3], "rack:rack2": [4, 5]} to [[1, 2, 3], [4, 5]].
        List<List<Long>> locBackendIdList = new ArrayList<>();
        int locBackendWithDiffHostLocNum = getLocationMatchedBackendIdList(
                locBackendIdList, srcNodes, locReq, systemInfoService);

        List<Long> nodeIds = Lists.newArrayList();
        // We cannot find enough nodes match the location requirement,
        // return an empty list and try to choose nodes ignoring location requirement again.
        if (locBackendWithDiffHostLocNum < nodeNum) {
            return nodeIds;
        }

        // At least we can choose `nodeNum` backends from different hosts (randomly).
        int startIdx = locBackendRandSelector.nextInt(Integer.MAX_VALUE) % locBackendIdList.size();
        Set<String> checkedHosts = Sets.newHashSet();
        while (nodeIds.size() < nodeNum) {
            List<Long> backendIdList = locBackendIdList.get(startIdx);
            if (backendIdList.isEmpty()) {
                startIdx = (startIdx + 1) % locBackendIdList.size();
                continue;
            }
            int idx = locBackendRandSelector.nextInt(Integer.MAX_VALUE) % backendIdList.size();
            Long backendId = backendIdList.get(idx);
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend != null && !nodeIds.contains(backendId) && !checkedHosts.contains(backend.getHost())) {
                nodeIds.add(backendId);
                checkedHosts.add(backend.getHost());
                backendIdList.remove(idx);
            }
            startIdx = (startIdx + 1) % locBackendIdList.size();
        }

        return nodeIds;
    }

    private List<Long> chooseNodesNoLocReq(int nodeNum, boolean isCreate, final List<ComputeNode> srcNodes) {
        long lastNodeId;

        if (isCreate) {
            lastNodeId = lastNodeIdForCreation;
        } else {
            lastNodeId = lastNodeIdForOther;
        }

        // host -> BE list
        Map<String, List<ComputeNode>> nodeMaps = Maps.newHashMap();
        for (ComputeNode node : srcNodes) {
            String host = node.getHost();

            if (!nodeMaps.containsKey(host)) {
                nodeMaps.put(host, Lists.newArrayList());
            }

            nodeMaps.get(host).add(node);
        }

        // if more than one backend exists in same host, select a backend at random
        List<ComputeNode> nodes = Lists.newArrayList();
        for (List<ComputeNode> list : nodeMaps.values()) {
            Collections.shuffle(list);
            nodes.add(list.get(0));
        }

        List<Long> nodeIds = Lists.newArrayList();
        // get last node index
        int lastNodeIndex = -1;
        int index = -1;
        for (ComputeNode node : nodes) {
            index++;
            if (node.getId() == lastNodeId) {
                lastNodeIndex = index;
                break;
            }
        }
        Iterator<ComputeNode> iterator = Iterators.cycle(nodes);
        index = -1;
        boolean failed = false;
        // 2 cycle at most
        int maxIndex = 2 * nodes.size();
        while (iterator.hasNext() && nodeIds.size() < nodeNum) {
            ComputeNode node = iterator.next();
            index++;
            if (index <= lastNodeIndex) {
                continue;
            }

            if (index > maxIndex) {
                failed = true;
                break;
            }

            long nodeId = node.getId();
            if (!nodeIds.contains(nodeId)) {
                nodeIds.add(nodeId);
                lastNodeId = nodeId;
            } else {
                failed = true;
                break;
            }
        }

        if (nodeIds.size() != nodeNum) {
            failed = true;
        }

        if (failed) {
            // debug: print backend info when the selection failed
            for (ComputeNode node : nodes) {
                LOG.debug("random select: {}", node);
            }
            return Collections.emptyList();
        }

        if (isCreate) {
            lastNodeIdForCreation = lastNodeId;
        } else {
            lastNodeIdForOther = lastNodeId;
        }
        return nodeIds;
    }

    /**
     * choose nodes by round-robin
     *
     * @param nodeNum  number of node wanted
     * @param isCreate choose nodes for creation or not
     * @param locReq   location requirement we should meet on creation,
     *                 e.g. we want to create a table with replicas for each tablet
     *                 scattered on different rack, i.e., the location property of
     *                 the creating table is "rack:*", we should meet that requirement.
     * @param srcNodes list of the candidate nodes
     * @return empty list if not enough node, otherwise return a list of node's id
     */
    public synchronized List<Long> seqChooseNodeIds(int nodeNum, boolean isCreate, Multimap<String, String> locReq,
                                                    final List<ComputeNode> srcNodes) {
        List<Long> result = null;
        if (isCreate && locReq != null) {
            result = chooseNodesMatchLocReq(nodeNum, locReq, srcNodes);
        }

        // If we cannot find nodes match location requirement, try to ignore it and return result
        if (CollectionUtils.isEmpty(result)) {
            result = chooseNodesNoLocReq(nodeNum, isCreate, srcNodes);
        }

        return result;
    }

}

