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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import javax.validation.constraints.NotNull;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    public static Long chooseNodeId(ShardInfo shardInfo) {
        Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent().getAllNodeIdsByShard(shardInfo, true);
        if (!ids.isEmpty()) {
            return ids.iterator().next();
        }
        try {
            return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getNodeSelector().seqChooseBackendOrComputeId();
        } catch (UserException e) {
            return null;
        }
    }

    public static ComputeNode chooseNode(ShardInfo shardInfo) {
        Long nodeId = chooseNodeId(shardInfo);
        if (nodeId == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
    }

    public static Map<Long, List<Long>> groupTabletID(Collection<Partition> partitions,
                                                      MaterializedIndex.IndexExtState indexState,
                                                      long warehouseId)
            throws NoAliveBackendException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        Map<Long, List<Long>> groupMap = new HashMap<>();
        for (Partition partition : partitions) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(indexState)) {
                    for (Tablet tablet : index.getTablets()) {
                        ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(
                                warehouseId, (LakeTablet) tablet);
                        if (computeNode == null) {
                            throw new NoAliveBackendException("no alive backend");
                        }
                        groupMap.computeIfAbsent(computeNode.getId(), k -> Lists.newArrayList()).add(tablet.getId());
                    }
                }
            }
        }
        return groupMap;
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, long warehouseId)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnInfo, baseVersion, newVersion, null, warehouseId);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                           long baseVersion, long newVersion,
                                           Map<Long, Double> compactionScores,
                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                           long warehouseId)
            throws NoAliveBackendException, RpcException {
        if (nodeToTablets == null) {
            nodeToTablets = new HashMap<>();
        }

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.warehouseExists(warehouseId)) {
            LOG.warn("publish version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}", warehouseId, warehouseManager.getBackgroundWarehouse().getId());
            warehouseId = warehouseManager.getBackgroundWarehouse().getId();
        }

        List<Long> rebuildPindexTabletIds = new ArrayList<>();
        for (Tablet tablet : tablets) {
            ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) tablet);
            if (computeNode == null) {
                LOG.warn("No alive node in warehouse for handle publish version request, try to use background warehouse");
                computeNode = warehouseManager.getComputeNodeAssignedToTablet(warehouseManager.getBackgroundWarehouse().getId(),
                        (LakeTablet) tablet);
                if (computeNode == null) {
                    throw new NoAliveBackendException("No alive node for handle publish version request in background warehouse");
                }
            }
            nodeToTablets.computeIfAbsent(computeNode, k -> Lists.newArrayList()).add(tablet.getId());
            if (baseVersion == ((LakeTablet) tablet).rebuildPindexVersion() && baseVersion != 0) {
                rebuildPindexTabletIds.add(tablet.getId());
                LOG.info("lake tablet {} publish rebuild pindex version {}", tablet.getId(), baseVersion);
            }
        }

        List<Future<PublishVersionResponse>> responseList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        List<ComputeNode> nodeList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
            PublishVersionRequest request = new PublishVersionRequest();
            request.baseVersion = baseVersion;
            request.newVersion = newVersion;
            request.tabletIds = entry.getValue(); // todo: limit the number of Tablets sent to a single node
            request.timeoutMs = LakeService.TIMEOUT_PUBLISH_VERSION;
            request.txnInfos = txnInfos;
            if (!rebuildPindexTabletIds.isEmpty()) {
                request.rebuildPindexTabletIds = rebuildPindexTabletIds;
            }

            ComputeNode node = entry.getKey();
            LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
            Future<PublishVersionResponse> future = lakeService.publishVersion(request);
            responseList.add(future);
            nodeList.add(node);
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
                throw new RpcException(nodeList.get(i).getHost(), e.getMessage());
            }
        }
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, Map<Long, Double> compactionScores,
                                      long warehouseId)
            throws NoAliveBackendException, RpcException {
        List<TxnInfoPB> txnInfos = Lists.newArrayList(txnInfo);
        publishVersionBatch(tablets, txnInfos, baseVersion, newVersion, compactionScores, null, warehouseId);
    }

    public static void publishLogVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long version, long warehouseId)
            throws NoAliveBackendException, RpcException {
        List<TxnInfoPB> txnInfos = new ArrayList<>();
        txnInfos.add(txnInfo);
        List<Long> versions = new ArrayList<>();
        versions.add(version);
        publishLogVersionBatch(tablets, txnInfos, versions, warehouseId);
    }

    public static void publishLogVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txns, List<Long> versions,
                                              long warehouseId)
            throws NoAliveBackendException, RpcException {
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.warehouseExists(warehouseId)) {
            LOG.warn("publish log version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}", warehouseId, warehouseManager.getBackgroundWarehouse().getId());
            warehouseId = warehouseManager.getBackgroundWarehouse().getId();
        }

        for (Tablet tablet : tablets) {
            ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) tablet);
            if (computeNode == null) {
                LOG.warn("no alive node in warehouse for handle publish log version request, try to use background warehouse");
                computeNode = warehouseManager.getComputeNodeAssignedToTablet(warehouseManager.getBackgroundWarehouse().getId(),
                        (LakeTablet) tablet);
                if (computeNode == null) {
                    throw new NoAliveBackendException("No alive node for handle publish version request in background warehouse");
                }
            }
            nodeToTablets.computeIfAbsent(computeNode, k -> Lists.newArrayList()).add(tablet.getId());
        }
        List<Future<PublishLogVersionResponse>> responseList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        List<ComputeNode> nodeList = Lists.newArrayListWithCapacity(nodeToTablets.size());
        for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
            PublishLogVersionBatchRequest request = new PublishLogVersionBatchRequest();
            request.tabletIds = entry.getValue();
            request.txnInfos = txns;
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

    public static Optional<Long> getWarehouseIdByNodeId(SystemInfoService systemInfo, long nodeId) {
        ComputeNode node = systemInfo.getBackendOrComputeNode(nodeId);
        if (node == null) {
            LOG.warn("failed to get warehouse id by node id: {}", nodeId);
            return Optional.empty();
        }

        return Optional.of(node.getWarehouseId());
    }
}
