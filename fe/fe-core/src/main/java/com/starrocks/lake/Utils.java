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
import com.starrocks.alter.reshard.PublishTabletsInfo;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.vector.VectorIndexBuildScheduler;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getPrimaryComputeNodeIdByShard(shardInfo);
        } catch (StarRocksException e) {
            // do nothing
        }
        try {
            return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getNodeSelector().seqChooseBackendOrComputeId();
        } catch (StarRocksException e) {
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
                                                      ComputeResource computeResource)
            throws NoAliveBackendException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        Map<Long, List<Long>> groupMap = new HashMap<>();
        for (Partition partition : partitions) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(indexState)) {
                    for (Tablet tablet : index.getTablets()) {
                        ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource,
                                tablet.getId());
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
                                      long newVersion, ComputeResource computeResource, boolean useAggregatePublish)
            throws NoAliveBackendException, RpcException {
        // Collect async vector index build infos reported by BE and enqueue them into the
        // scheduler. Callers of this simplified overload (lake alter/rollup/schema-change
        // paths) would otherwise drop those infos and the newly published versions would
        // stay unbuilt until the next normal publish or leader recoveryScan.
        List<VectorIndexBuildInfoPB> vectorIndexBuildInfos = new ArrayList<>();
        publishVersion(tablets, txnInfo, baseVersion, newVersion, null, computeResource,
                null, useAggregatePublish, vectorIndexBuildInfos);
        VectorIndexBuildScheduler.onPublishComplete(vectorIndexBuildInfos, /* fromCompaction= */ false);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                           long baseVersion, long newVersion,
                                           Map<Long, Double> compactionScores,
                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                           ComputeResource computeResource,
                                           Map<Long, Long> tabletRowNum,
                                           List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        publishVersionBatch(tablets, txnInfos, baseVersion, newVersion, compactionScores, null, nodeToTablets,
                computeResource, tabletRowNum, vectorIndexBuildInfos);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                           long baseVersion, long newVersion,
                                           Map<Long, Double> compactionScores,
                                           Map<Long, TabletRange> tabletRanges,
                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                           ComputeResource computeResource,
                                           Map<Long, Long> tabletRowNum,
                                           List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.isResourceAvailable(computeResource)) {
            LOG.warn("publish version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}", computeResource,
                    warehouseManager.getBackgroundWarehouse().getId());
            computeResource = warehouseManager.getBackgroundComputeResource();
        }

        List<Long> rebuildPindexTabletIds = new ArrayList<>();
        Map<ComputeNode, PublishTabletsInfo> nodeToPublishTabletsInfo = processTablets(tablets, computeResource,
                warehouseManager, rebuildPindexTabletIds, baseVersion, newVersion);

        // Pre-compute once per batch so per-node requests can slice cheaply.
        Map<Long, Long> batchBuiltVersions = buildTabletBuiltVersionsFromTablets(tablets);

        List<Future<PublishVersionResponse>> responseList = Lists.newArrayListWithCapacity(nodeToPublishTabletsInfo.size());
        List<ComputeNode> nodeList = Lists.newArrayListWithCapacity(nodeToPublishTabletsInfo.size());
        for (Map.Entry<ComputeNode, PublishTabletsInfo> entry : nodeToPublishTabletsInfo.entrySet()) {
            ComputeNode node = entry.getKey();
            PublishTabletsInfo publishTabletInfo = entry.getValue();
            PublishVersionRequest request = new PublishVersionRequest();
            request.baseVersion = baseVersion;
            request.newVersion = newVersion;
            request.tabletIds = publishTabletInfo.getTabletIds(); // todo: limit the number of Tablets sent to a single node
            request.timeoutMs = LakeService.TIMEOUT_PUBLISH_VERSION;
            request.txnInfos = txnInfos;
            if (!rebuildPindexTabletIds.isEmpty()) {
                request.rebuildPindexTabletIds = rebuildPindexTabletIds;
            }
            request.reshardingTabletInfos = publishTabletInfo.getReshardingTablets();
            request.tabletBuiltVersions = sliceTabletBuiltVersions(batchBuiltVersions,
                    publishTabletInfo.getTabletIds());

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
                if (tabletRanges != null && response != null && response.tabletRanges != null) {
                    for (Map.Entry<Long, TabletRangePB> entry : response.tabletRanges.entrySet()) {
                        tabletRanges.put(entry.getKey(), TabletRange.fromProto(entry.getValue()));
                    }
                }
                if (baseVersion == 1 && tabletRowNum != null && response != null && response.tabletRowNums != null) {
                    tabletRowNum.putAll(response.tabletRowNums);
                }
                if (vectorIndexBuildInfos != null && response != null
                        && response.vectorIndexBuildInfos != null) {
                    vectorIndexBuildInfos.addAll(response.vectorIndexBuildInfos);
                }
            } catch (Exception e) {
                throw new RpcException(nodeList.get(i).getHost(), e.getMessage());
            }
        }

        if (nodeToTablets != null) {
            for (Map.Entry<ComputeNode, PublishTabletsInfo> entry : nodeToPublishTabletsInfo.entrySet()) {
                nodeToTablets.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                        .addAll(entry.getValue().getOldTabletIds());
            }
        }
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, Map<Long, Double> compactionScores,
                                      ComputeResource computeResource, Map<Long, Long> tabletRowNums,
                                      boolean useAggregatePublish,
                                      List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnInfo, baseVersion, newVersion, compactionScores,
                null, computeResource, tabletRowNums, useAggregatePublish, vectorIndexBuildInfos);
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, Map<Long, Double> compactionScores,
                                      Map<Long, TabletRange> tabletRanges, ComputeResource computeResource,
                                      Map<Long, Long> tabletRowNums, boolean useAggregatePublish,
                                      List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        List<TxnInfoPB> txnInfos = Lists.newArrayList(txnInfo);
        if (!useAggregatePublish) {
            publishVersionBatch(tablets, txnInfos, baseVersion, newVersion,
                    compactionScores, tabletRanges, null, computeResource, tabletRowNums,
                    vectorIndexBuildInfos);
        } else {
            aggregatePublishVersion(tablets, txnInfos, baseVersion, newVersion, compactionScores,
                    tabletRanges, null, computeResource, tabletRowNums, vectorIndexBuildInfos);
        }
    }

    public static Map<ComputeNode, PublishTabletsInfo> processTablets(List<Tablet> tablets,
                                                                     ComputeResource computeResource,
                                                                     WarehouseManager warehouseManager,
                                                                     List<Long> rebuildPindexTabletIds,
                                                                     long baseVersion, long newVersion)
            throws NoAliveBackendException {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Map<ComputeNode, PublishTabletsInfo> nodeToPublishTabletsInfo = new HashMap<>();
        for (Tablet tablet : tablets) {
            ReshardingTablet reshardingTablet = tabletReshardJobMgr.getReshardingTablet(tablet.getId(), newVersion);
            if (reshardingTablet == null) {
                ComputeNode computeNode = getComputeNode(tablet.getId(), computeResource, warehouseManager);
                nodeToPublishTabletsInfo.computeIfAbsent(computeNode, k -> new PublishTabletsInfo())
                        .addTabletId(tablet.getId());
            } else {
                ComputeNode computeNode = getComputeNode(reshardingTablet.getFirstOldTabletId(),
                        computeResource, warehouseManager);
                nodeToPublishTabletsInfo.computeIfAbsent(computeNode, k -> new PublishTabletsInfo())
                        .addReshardingTablet(reshardingTablet);
            }

            if (baseVersion == ((LakeTablet) tablet).rebuildPindexVersion() && baseVersion != 0) {
                rebuildPindexTabletIds.add(tablet.getId());
                LOG.info("lake tablet {} publish rebuild pindex version {}", tablet.getId(), baseVersion);
            }
        }

        return nodeToPublishTabletsInfo;
    }

    public static ComputeNode getComputeNode(long tabletId, ComputeResource computeResource,
                                             WarehouseManager warehouseManager)
            throws NoAliveBackendException {
        ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tabletId);
        if (computeNode == null) {
            LOG.warn("No alive node in warehouse for handle publish version request, try to use background warehouse");
            computeResource = warehouseManager.getBackgroundComputeResource();
            computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tabletId);
            if (computeNode == null) {
                throw new NoAliveBackendException("No alive node for handle publish version request in background warehouse");
            }
        }
        return computeNode;
    }

    public static void createSubRequestForAggregatePublish(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                                           long baseVersion, long newVersion,
                                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                                           ComputeResource computeResource,
                                                           AggregatePublishVersionRequest request)
            throws NoAliveBackendException, RpcException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.isResourceAvailable(computeResource)) {
            LOG.warn("publish version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}[{}]", computeResource,
                    warehouseManager.getBackgroundWarehouse().getId(),
                    warehouseManager.getBackgroundWarehouse().getName());
            computeResource = warehouseManager.getBackgroundComputeResource();
        }

        List<Long> rebuildPindexTabletIds = new ArrayList<>();
        Map<ComputeNode, PublishTabletsInfo> nodeToPublishTabletsInfo = processTablets(tablets, computeResource,
                warehouseManager, rebuildPindexTabletIds, baseVersion, newVersion);

        // Pre-compute once per batch so per-node requests can slice cheaply.
        Map<Long, Long> batchBuiltVersions = buildTabletBuiltVersionsFromTablets(tablets);

        List<ComputeNodePB> computeNodes = new ArrayList<>();
        List<PublishVersionRequest> publishReqs = new ArrayList<>();
        for (Map.Entry<ComputeNode, PublishTabletsInfo> entry : nodeToPublishTabletsInfo.entrySet()) {
            PublishTabletsInfo publishTabletInfo = entry.getValue();
            PublishVersionRequest singleReq = new PublishVersionRequest();
            singleReq.setBaseVersion(baseVersion);
            singleReq.setNewVersion(newVersion);
            singleReq.setTabletIds(publishTabletInfo.getTabletIds());
            singleReq.setTimeoutMs(LakeService.TIMEOUT_PUBLISH_VERSION);
            singleReq.setTxnInfos(txnInfos);
            singleReq.setEnableAggregatePublish(true);

            if (!rebuildPindexTabletIds.isEmpty()) {
                singleReq.setRebuildPindexTabletIds(rebuildPindexTabletIds);
            }

            singleReq.setReshardingTabletInfos(publishTabletInfo.getReshardingTablets());
            singleReq.setTabletBuiltVersions(sliceTabletBuiltVersions(batchBuiltVersions,
                    publishTabletInfo.getTabletIds()));

            ComputeNodePB computeNodePB = new ComputeNodePB();
            computeNodePB.setHost(entry.getKey().getHost());
            computeNodePB.setBrpcPort(entry.getKey().getBrpcPort());
            // Record the node id so that the aggregator-selection step later can prefer
            // an aggregator that already owns at least one tablet in the batch. Without
            // the id we cannot match compute-node PBs back to ComputeNode objects.
            computeNodePB.setId(entry.getKey().getId());

            computeNodes.add(computeNodePB);
            publishReqs.add(singleReq);
        }
        if (request.getComputeNodes() != null) {
            List<ComputeNodePB> originalComputeNodes = new ArrayList<>(request.getComputeNodes());
            computeNodes.addAll(originalComputeNodes);
        }
        if (request.getPublishReqs() != null) {
            List<PublishVersionRequest> originalPublishReqs = new ArrayList<>(request.getPublishReqs());
            publishReqs.addAll(originalPublishReqs);
        }

        request.setComputeNodes(computeNodes);
        request.setPublishReqs(publishReqs);

        if (nodeToTablets != null) {
            for (Map.Entry<ComputeNode, PublishTabletsInfo> entry : nodeToPublishTabletsInfo.entrySet()) {
                nodeToTablets.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                        .addAll(entry.getValue().getOldTabletIds());
            }
        }
    }

    public static void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                                                          long baseVersion, ComputeResource computeResource,
                                                          Map<Long, Double> compactionScores,
                                                          Map<Long, Long> tabletRowNum,
                                                          List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        sendAggregatePublishVersionRequest(request, baseVersion, computeResource, compactionScores, null,
                tabletRowNum, vectorIndexBuildInfos);
    }

    public static void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                                                          long baseVersion, ComputeResource computeResource,
                                                          Map<Long, Double> compactionScores,
                                                          Map<Long, TabletRange> tabletRanges,
                                                          Map<Long, Long> tabletRowNum,
                                                          List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (computeResource == null || !warehouseManager.isResourceAvailable(computeResource)) {
            LOG.warn("publish version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}[{}]", computeResource,
                    warehouseManager.getBackgroundWarehouse().getId(),
                    warehouseManager.getBackgroundWarehouse().getName());
            computeResource = warehouseManager.getBackgroundComputeResource();
        }

        // Prefer an aggregator that already owns at least one tablet in this batch so that
        // on the BE side the "first tablet id" used to derive bundle file paths can be
        // resolved locally via the staros worker cache (no extra get-shard-info RPC).
        // The compute-node ids are embedded in the request (see createSubRequestForAggregatePublish).
        Set<ComputeNode> candidateAggregatorNodes = collectCandidateAggregatorNodes(request);
        ComputeNode aggregatorNode = LakeAggregator.chooseAggregatorNode(computeResource, candidateAggregatorNodes);
        if (aggregatorNode == null) {
            throw new NoAliveBackendException("No alive compute node for handle aggregate publish version");
        }

        LakeService lakeService = BrpcProxy.getLakeService(aggregatorNode.getHost(), aggregatorNode.getBrpcPort());
        Future<PublishVersionResponse> future = lakeService.aggregatePublishVersion(request);

        try {
            PublishVersionResponse response = future.get();
            if (response != null) {
                TStatusCode code = TStatusCode.findByValue(response.status.statusCode);
                if (code != TStatusCode.OK) {
                    String errorMsg = "Fail to publish version for tablets:[";
                    if (response.failedTablets != null && !response.failedTablets.isEmpty()) {
                        errorMsg += response.failedTablets;
                    }
                    errorMsg += "], error msg: " + response.status.errorMsgs.get(0);
                    throw new RpcException(errorMsg);
                }
            }
            if (compactionScores != null && response != null && response.compactionScores != null) {
                compactionScores.putAll(response.compactionScores);
            }
            if (tabletRanges != null && response != null && response.tabletRanges != null) {
                for (Map.Entry<Long, TabletRangePB> entry : response.tabletRanges.entrySet()) {
                    tabletRanges.put(entry.getKey(), TabletRange.fromProto(entry.getValue()));
                }
            }
            if (baseVersion == 1 && tabletRowNum != null && response != null && response.tabletRowNums != null) {
                tabletRowNum.putAll(response.tabletRowNums);
            }
            if (vectorIndexBuildInfos != null && response != null
                    && response.vectorIndexBuildInfos != null) {
                vectorIndexBuildInfos.addAll(response.vectorIndexBuildInfos);
            }
        } catch (Exception e) {
            throw new RpcException(aggregatorNode.getHost(), e.getMessage());
        }
    }

    // Collect the ComputeNodes that own at least one tablet in the aggregate request, so
    // the aggregator picker can prefer a node whose local staros worker cache already has
    // the tablet shard info.
    private static Set<ComputeNode> collectCandidateAggregatorNodes(AggregatePublishVersionRequest request) {
        Set<ComputeNode> candidates = new HashSet<>();
        if (request == null || request.getComputeNodes() == null) {
            return candidates;
        }
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (ComputeNodePB pb : request.getComputeNodes()) {
            if (pb == null || pb.getId() == null) {
                continue;
            }
            ComputeNode node = clusterInfo.getBackendOrComputeNode(pb.getId());
            if (node != null) {
                candidates.add(node);
            }
        }
        return candidates;
    }

    public static void aggregatePublishVersion(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                               long baseVersion, long newVersion,
                                               Map<Long, Double> compactionScores,
                                               Map<ComputeNode, List<Long>> nodeToTablets,
                                               ComputeResource computeResource,
                                               Map<Long, Long> tabletRowNum,
                                               List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        aggregatePublishVersion(tablets, txnInfos, baseVersion, newVersion, compactionScores,
                null, nodeToTablets, computeResource, tabletRowNum, vectorIndexBuildInfos);
    }

    public static void aggregatePublishVersion(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                               long baseVersion, long newVersion,
                                               Map<Long, Double> compactionScores,
                                               Map<Long, TabletRange> tabletRanges,
                                               Map<ComputeNode, List<Long>> nodeToTablets,
                                               ComputeResource computeResource,
                                               Map<Long, Long> tabletRowNum,
                                               List<VectorIndexBuildInfoPB> vectorIndexBuildInfos)
            throws NoAliveBackendException, RpcException {
        AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
        try {
            createSubRequestForAggregatePublish(tablets, txnInfos, baseVersion, newVersion,
                                                nodeToTablets, computeResource, request);
            sendAggregatePublishVersionRequest(request, baseVersion, computeResource, compactionScores,
                                               tabletRanges, tabletRowNum, vectorIndexBuildInfos);
        } catch (Exception e) {
            throw e;
        }
    }

    public static void publishLogVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo,
                                         long version, ComputeResource computeResource)
            throws NoAliveBackendException, RpcException {
        List<TxnInfoPB> txnInfos = new ArrayList<>();
        txnInfos.add(txnInfo);
        List<Long> versions = new ArrayList<>();
        versions.add(version);
        publishLogVersionBatch(tablets, txnInfos, versions, computeResource);
    }

    public static void publishLogVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txns, List<Long> versions,
                                              ComputeResource computeResource)
            throws NoAliveBackendException, RpcException {
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.isResourceAvailable(computeResource)) {
            LOG.warn("publish log version operation should be successful even if the warehouse is not exist, " +
                    "and switch the warehouse id from {} to {}",
                    computeResource, warehouseManager.getBackgroundWarehouse().getId());
            computeResource = warehouseManager.getBackgroundComputeResource();
        }

        for (Tablet tablet : tablets) {
            ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tablet.getId());
            if (computeNode == null) {
                LOG.warn("no alive node in warehouse for handle publish log version request, try to use background warehouse");
                computeResource = warehouseManager.getBackgroundComputeResource();
                computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tablet.getId());
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

    // Build a per-batch map of tabletId -> vectorIndexBuiltVersion from the Tablet objects
    // already in hand. Avoids a TabletInvertedIndex + db + table + partition + index
    // traversal per tablet on the publish hot path.
    private static Map<Long, Long> buildTabletBuiltVersionsFromTablets(List<Tablet> tablets) {
        Map<Long, Long> result = null;
        for (Tablet tablet : tablets) {
            if (!(tablet instanceof LakeTablet)) {
                continue;
            }
            long bv = ((LakeTablet) tablet).getVectorIndexBuiltVersion();
            if (bv > 0) {
                if (result == null) {
                    result = new HashMap<>();
                }
                result.put(tablet.getId(), bv);
            }
        }
        return result;
    }

    // Slice a batch-wide built-versions map for a single sub-request's tablet id set.
    // Returns null when no entry matches so the request field stays unset.
    private static Map<Long, Long> sliceTabletBuiltVersions(Map<Long, Long> all, List<Long> tabletIds) {
        if (all == null || all.isEmpty()) {
            return null;
        }
        Map<Long, Long> result = null;
        for (Long tabletId : tabletIds) {
            Long bv = all.get(tabletId);
            if (bv != null) {
                if (result == null) {
                    result = new HashMap<>();
                }
                result.put(tabletId, bv);
            }
        }
        return result;
    }
}
