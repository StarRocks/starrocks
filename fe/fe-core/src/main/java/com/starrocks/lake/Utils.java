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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import javax.validation.constraints.NotNull;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    // Returns null if no backend available.
    public static Long chooseRandomNodeId(long workerGroupId) {
        try {
            // randomly choose one node from this workerGroup
            List<Long> nodeIds = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(workerGroupId);
            if (!nodeIds.isEmpty()) {
                int randomIndex = new Random().nextInt(nodeIds.size());
                return nodeIds.get(randomIndex);
            }
            return null;
        } catch (UserException e) {
            return null;
        }
    }

    public static Long chooseNodeId(LakeTablet tablet) {
        return chooseNodeId(tablet, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public static Long chooseNodeId(LakeTablet tablet, long workerGroupId) {
        try {
            ShardInfo shardInfo = tablet.getShardInfo();
            return chooseNodeId(shardInfo);
        } catch (Exception e) {
            LOG.error("Ignored error", e);
            return chooseRandomNodeId(workerGroupId);
        }
    }

    public static Long chooseNodeId(ShardInfo shardInfo) {
        return chooseNodeId(shardInfo, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public static Long chooseNodeId(ShardInfo shardInfo, long workerGroupId) {
        Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent().getAllBackendIdsByShard(shardInfo, true);
        if (!ids.isEmpty()) {
            return ids.iterator().next();
        }
        return chooseRandomNodeId(workerGroupId);
    }

    public static ComputeNode chooseNode(LakeTablet tablet) {
        return chooseNode(tablet, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public static ComputeNode chooseNode(LakeTablet tablet, long workerGroupId) {
        Long nodeId = chooseNodeId(tablet, workerGroupId);
        if (nodeId == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
    }

    public static ComputeNode chooseNode(ShardInfo shardInfo) {
        return chooseNode(shardInfo, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public static ComputeNode chooseNode(ShardInfo shardInfo, long workerGroupId) {
        Long nodeId = chooseNodeId(shardInfo, workerGroupId);
        if (nodeId == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
    }

    // Preconditions: Has required the database's reader lock.
    // Returns a map from backend ID to a list of tablet IDs.
    public static Map<Long, List<Long>> groupTabletID(OlapTable table, long workerGroupId)
            throws NoAliveBackendException {
        return groupTabletID(table.getPartitions(), MaterializedIndex.IndexExtState.ALL, workerGroupId);
    }

    public static Map<Long, List<Long>> groupTabletID(Collection<Partition> partitions,
                                                      MaterializedIndex.IndexExtState indexState,
                                                      long workerGroupId)
            throws NoAliveBackendException {
        Map<Long, List<Long>> groupMap = new HashMap<>();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(indexState)) {
                for (Tablet tablet : index.getTablets()) {
                    Long nodeId = chooseNodeId((LakeTablet) tablet, workerGroupId);
                    if (nodeId == null) {
                        throw new NoAliveBackendException("no alive backend");
                    }
                    groupMap.computeIfAbsent(nodeId, k -> Lists.newArrayList()).add(tablet.getId());
                }
            }
        }
        return groupMap;
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion,
                                      long commitTimeInSecond, long workerGroupId)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnId, baseVersion, newVersion, commitTimeInSecond,
                null, workerGroupId);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<Long> txnIds,
                                      long baseVersion, long newVersion, long commitTimeInSecond,
                                      Map<Long, Double> compactionScores, long workerGroupId,
                                      Map<ComputeNode, List<Long>> nodeToTablets)
            throws NoAliveBackendException, RpcException {
        if (nodeToTablets == null) {
            nodeToTablets = new HashMap<>();
        }

        for (Tablet tablet : tablets) {
            ComputeNode node = Utils.chooseNode((LakeTablet) tablet, workerGroupId);
            if (node == null) {
                throw new NoAliveBackendException("No alive node for handle publish version request");
            }
            nodeToTablets.computeIfAbsent(node, k -> Lists.newArrayList()).add(tablet.getId());
        }

        List<Future<PublishVersionResponse>> responseList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        List<ComputeNode> backendList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
            PublishVersionRequest request = new PublishVersionRequest();
            request.baseVersion = baseVersion;
            request.newVersion = newVersion;
            request.tabletIds = entry.getValue(); // todo: limit the number of Tablets sent to a single node
            request.txnIds = txnIds;
            request.commitTime = commitTimeInSecond;
            request.timeoutMs = LakeService.TIMEOUT_PUBLISH_VERSION;

            ComputeNode node = entry.getKey();
            LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
            Future<PublishVersionResponse> future = lakeService.publishVersion(request);
            responseList.add(future);
            backendList.add(node);
        }

        for (int i = 0; i < responseList.size(); i++) {
            try {
                PublishVersionResponse response = responseList.get(i).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    throw new RpcException("Fail to publish version for tablets " + response.failedTablets + ": " +
                            response.status.errorMsgs.get(0));
                }
                if (compactionScores != null && response != null && response.compactionScores != null) {
                    compactionScores.putAll(response.compactionScores);
                }
            } catch (Exception e) {
                throw new RpcException(backendList.get(i).getHost(), e.getMessage());
            }
        }
    }


    public static void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion,
                                      long commitTimeInSecond, Map<Long, Double> compactionScores, long workerGroupId)
            throws NoAliveBackendException, RpcException {
        List<Long> txnIds = Lists.newArrayList(txnId);
        publishVersionBatch(tablets, txnIds, baseVersion, newVersion, commitTimeInSecond, compactionScores, workerGroupId, null);
    }

    public static void publishLogVersion(@NotNull List<Tablet> tablets, long txnId, long version, long workerGroupId)
            throws NoAliveBackendException, RpcException {
        List<Long> txnIds = new ArrayList<>();
        txnIds.add(txnId);
        List<Long> versions = new ArrayList<>();
        versions.add(version);
        publishLogVersionBatch(tablets, txnIds, versions, workerGroupId);
    }

    public static void publishLogVersionBatch(@NotNull List<Tablet> tablets, List<Long> txnIds,
                                              List<Long> versions, long workerGroupId)
            throws NoAliveBackendException, RpcException {
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        for (Tablet tablet : tablets) {
            ComputeNode node = Utils.chooseNode((LakeTablet) tablet, workerGroupId);
            if (node == null) {
                throw new NoAliveBackendException("No alive node for handle publish version request");
            }
            nodeToTablets.computeIfAbsent(node, k -> Lists.newArrayList()).add(tablet.getId());
        }
        List<Future<PublishLogVersionResponse>> responseList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        List<ComputeNode> nodeList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
            PublishLogVersionBatchRequest request = new PublishLogVersionBatchRequest();
            request.tabletIds = entry.getValue();
            request.txnIds = txnIds;
            request.versions = versions;

            ComputeNode node = entry.getKey();
            LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
            Future<PublishLogVersionResponse> future = lakeService.publishLogVersionBatch(request);
            responseList.add(future);
            nodeList.add(node);
        }

        for (int i = 0; i < responseList.size(); i++) {
            try {
                PublishLogVersionResponse response = responseList.get(i).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    throw new RpcException(nodeList.get(i).getHost(),
                            "Fail to publish log version for tablets {}" + response.failedTablets);
                }
            } catch (Exception e) {
                throw new RpcException(nodeList.get(i).getHost(), e.getMessage());
            }
        }
    }
}
