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
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import javax.validation.constraints.NotNull;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    // Returns null if no backend available.
    public static Long chooseBackend(LakeTablet tablet) {
        try {
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
            long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
            return tablet.getPrimaryComputeNodeId(workerGroupId);
        } catch (UserException ex) {
            LOG.info("Ignored error {}", ex.getMessage());
            try {
                return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                         .getNodeSelector().seqChooseBackendOrComputeId();
            } catch (UserException e) {
                return null;
            }
        }
    }

    public static ComputeNode chooseNode(LakeTablet tablet) {
        Long nodeId = chooseBackend(tablet);
        if (nodeId == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
    }

    // Preconditions: Has required the database's reader lock.
    // Returns a map from backend ID to a list of tablet IDs.
    public static Map<Long, List<Long>> groupTabletID(OlapTable table) throws NoAliveBackendException {
        return groupTabletID(table.getPartitions(), MaterializedIndex.IndexExtState.ALL);
    }

    public static Map<Long, List<Long>> groupTabletID(Collection<Partition> partitions,
                                                      MaterializedIndex.IndexExtState indexState)
            throws NoAliveBackendException {
        Map<Long, List<Long>> groupMap = new HashMap<>();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(indexState)) {
                for (Tablet tablet : index.getTablets()) {
                    Long beId = chooseBackend((LakeTablet) tablet);
                    if (beId == null) {
                        throw new NoAliveBackendException("no alive backend");
                    }
                    groupMap.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
                }
            }
        }
        return groupMap;
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion,
                                      long commitTimeInSecond)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnId, baseVersion, newVersion, commitTimeInSecond, null);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<Long> txnIds,
                                      long baseVersion, long newVersion, long commitTimeInSecond,
                                      Map<Long, Double> compactionScores, Map<ComputeNode, List<Long>> nodeToTablets)
            throws NoAliveBackendException, RpcException {
        if (nodeToTablets == null) {
            nodeToTablets = new HashMap<>();
        }

        for (Tablet tablet : tablets) {
            ComputeNode node = Utils.chooseNode((LakeTablet) tablet);
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
                                      long commitTimeInSecond, Map<Long, Double> compactionScores)
            throws NoAliveBackendException, RpcException {
        List<Long> txnIds = Lists.newArrayList(txnId);
        publishVersionBatch(tablets, txnIds, baseVersion, newVersion, commitTimeInSecond, compactionScores, null);
    }

    public static void publishLogVersion(@NotNull List<Tablet> tablets, long txnId, long version)
            throws NoAliveBackendException, RpcException {
        List<Long> txnIds = new ArrayList<>();
        txnIds.add(txnId);
        List<Long> versions = new ArrayList<>();
        versions.add(version);
        publishLogVersionBatch(tablets, txnIds, versions);
    }

    public static void publishLogVersionBatch(@NotNull List<Tablet> tablets, List<Long> txnIds, List<Long> versions)
            throws NoAliveBackendException, RpcException {
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        for (Tablet tablet : tablets) {
            ComputeNode node = Utils.chooseNode((LakeTablet) tablet);
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
