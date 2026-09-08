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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.proto.PublishLogVersionBatchRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.proto.TxnInfoPB;
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
        publishVersion(tablets, txnInfo, baseVersion, newVersion, null, computeResource,
                null, useAggregatePublish);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                           long baseVersion, long newVersion,
                                           Map<Long, Double> compactionScores,
                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                           ComputeResource computeResource,
                                           Map<Long, TabletStatPB> tabletStats)
            throws NoAliveBackendException, RpcException {
        publishVersionBatch(tablets, txnInfos, baseVersion, newVersion, compactionScores, null, nodeToTablets,
                computeResource, tabletStats);
    }

    public static void publishVersionBatch(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                           long baseVersion, long newVersion,
                                           Map<Long, Double> compactionScores,
                                           Map<Long, TabletRange> tabletRanges,
                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                           ComputeResource computeResource,
                                           Map<Long, TabletStatPB> tabletStats)
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
                if (tabletStats != null && response != null && response.tabletStats != null) {
                    tabletStats.putAll(response.tabletStats);
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
                                      ComputeResource computeResource,
                                      Map<Long, TabletStatPB> tabletStats, boolean useAggregatePublish)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnInfo, baseVersion, newVersion, compactionScores,
                null, computeResource, tabletStats, useAggregatePublish);
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, Map<Long, Double> compactionScores,
                                      Map<Long, TabletRange> tabletRanges, ComputeResource computeResource,
                                      Map<Long, TabletStatPB> tabletStats,
                                      boolean useAggregatePublish)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnInfo, baseVersion, newVersion, compactionScores, tabletRanges, computeResource,
                tabletStats, useAggregatePublish, false);
    }

    /**
     * @param preferSharedInitialMetadata see
     *        {@link Utils#createSubRequestForAggregatePublish}. Meaningful only on the aggregate path:
     *        the shared version-1 layout exists only for `file_bundling` tables, which always publish
     *        with useAggregatePublish set.
     */
    public static void publishVersion(@NotNull List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                      long newVersion, Map<Long, Double> compactionScores,
                                      Map<Long, TabletRange> tabletRanges, ComputeResource computeResource,
                                      Map<Long, TabletStatPB> tabletStats,
                                      boolean useAggregatePublish,
                                      boolean preferSharedInitialMetadata)
            throws NoAliveBackendException, RpcException {
        List<TxnInfoPB> txnInfos = Lists.newArrayList(txnInfo);
        if (!useAggregatePublish) {
            publishVersionBatch(tablets, txnInfos, baseVersion, newVersion,
                    compactionScores, tabletRanges, null, computeResource, tabletStats);
        } else {
            aggregatePublishVersion(tablets, txnInfos, baseVersion, newVersion, compactionScores,
                    tabletRanges, null, computeResource, tabletStats, preferSharedInitialMetadata);
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

    /**
     * Whether every tablet of {@code partition} resolves its {@code baseVersion} metadata from the
     * single partition-shared initial-metadata object (tablet id 0) instead of its own per-tablet key.
     * Sent to the BE as {@code PublishVersionRequest.prefer_shared_initial_metadata} so the
     * publish does not have to discover the layout by probing a key that was never written. The BE
     * applies it to that request's base-version reads only and caches nothing, so a wrong answer
     * costs one request rather than correctness.
     *
     * <p>Every clause is load-bearing:
     * <ul>
     * <li>Only version 1 is ever shared. DDL writes that object once at partition creation; every
     *     later version is written per tablet or into a bundle.</li>
     * <li>{@code file_bundling} is what makes DDL write it ({@code LocalMetastore#buildPartitions}),
     *     and it is the only switch this predicate keys on. A partition that has the shared layout
     *     for any other reason reports false and keeps the BE's unhinted fallback, which resolves it
     *     correctly at the cost of one probe per tablet.</li>
     * <li>A non-zero {@code metadataSwitchVersion} means the partition predates the switch to
     *     bundling, so its version 1 is per-tablet even though the table is bundling now.</li>
     * <li>The object is named after tablet id 0 with no index discriminator, and all indexes of a
     *     physical partition share one storage path, so DDL only writes it for a single-index
     *     partition and the alter jobs never write it. Counting over {@code ALL} rather than
     *     {@code VISIBLE} is deliberate: a schema-change / rollup shadow index is invisible to
     *     {@code VISIBLE} exactly while its own tablets are reading their per-tablet version-1
     *     metadata, and handing them the base index's object would return the wrong schema.</li>
     * </ul>
     */
    public static boolean preferSharedInitialMetadata(OlapTable table, PhysicalPartition partition,
                                                            long baseVersion) {
        return table != null
                && partition != null
                && baseVersion == PhysicalPartition.PARTITION_INIT_VERSION
                && table.isCloudNativeTableOrMaterializedView()
                && Boolean.TRUE.equals(table.isFileBundling())
                && partition.getMetadataSwitchVersion() == 0
                && partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size() == 1;
    }

    public static void createSubRequestForAggregatePublish(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                                           long baseVersion, long newVersion,
                                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                                           ComputeResource computeResource,
                                                           AggregatePublishVersionRequest request)
            throws NoAliveBackendException, RpcException {
        createSubRequestForAggregatePublish(tablets, txnInfos, baseVersion, newVersion, nodeToTablets, computeResource,
                request, false);
    }

    /**
     * @param preferSharedInitialMetadata see
     *        {@code PublishVersionRequest.prefer_shared_initial_metadata}. Only a publish that reads
     *        the partition's EXISTING tablets at baseVersion may pass true: the normal-load path, and
     *        tablet reshard (split / merge), which reads the old tablets. The rollup and schema-change
     *        jobs publish shadow-index tablets that keep their own per-tablet version-1 metadata and
     *        must leave it false.
     */
    public static void createSubRequestForAggregatePublish(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                                           long baseVersion, long newVersion,
                                                           Map<ComputeNode, List<Long>> nodeToTablets,
                                                           ComputeResource computeResource,
                                                           AggregatePublishVersionRequest request,
                                                           boolean preferSharedInitialMetadata)
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
            singleReq.setPreferSharedInitialMetadata(preferSharedInitialMetadata);

            if (!rebuildPindexTabletIds.isEmpty()) {
                singleReq.setRebuildPindexTabletIds(rebuildPindexTabletIds);
            }

            singleReq.setReshardingTabletInfos(publishTabletInfo.getReshardingTablets());
    
            ComputeNodePB computeNodePB = new ComputeNodePB();
            // Send the resolved IP, not the hostname: the aggregator turns each entry into a brpc
            // stub via LakeServiceBrpcStubCache::get_stub(), whose cache key is the resolved
            // EndPoint, so a hostname there costs one getaddrinfo per sub-request per publish with
            // no caching on the BE side. FE resolves through the JVM DNS cache instead. Same
            // convention as the query/load path (see ExecutionDAG#getBrpcIpAddress, PR #32062).
            // getIP() falls back to the hostname when resolution fails, so this only ever degrades
            // to the previous behavior.
            computeNodePB.setHost(entry.getKey().getIP());
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
                                                          Map<Long, TabletStatPB> tabletStats)
            throws NoAliveBackendException, RpcException {
        sendAggregatePublishVersionRequest(request, baseVersion, computeResource, compactionScores, null,
                tabletStats);
    }

    public static void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                                                          long baseVersion, ComputeResource computeResource,
                                                          Map<Long, Double> compactionScores,
                                                          Map<Long, TabletRange> tabletRanges,
                                                          Map<Long, TabletStatPB> tabletStats)
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
            if (tabletStats != null && response != null && response.tabletStats != null) {
                tabletStats.putAll(response.tabletStats);
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
                                               Map<Long, TabletStatPB> tabletStats)
            throws NoAliveBackendException, RpcException {
        aggregatePublishVersion(tablets, txnInfos, baseVersion, newVersion, compactionScores,
                null, nodeToTablets, computeResource, tabletStats);
    }

    public static void aggregatePublishVersion(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                               long baseVersion, long newVersion,
                                               Map<Long, Double> compactionScores,
                                               Map<Long, TabletRange> tabletRanges,
                                               Map<ComputeNode, List<Long>> nodeToTablets,
                                               ComputeResource computeResource,
                                               Map<Long, TabletStatPB> tabletStats)
            throws NoAliveBackendException, RpcException {
        aggregatePublishVersion(tablets, txnInfos, baseVersion, newVersion, compactionScores, tabletRanges,
                nodeToTablets, computeResource, tabletStats, false);
    }

    /**
     * @param preferSharedInitialMetadata see
     *        {@link Utils#createSubRequestForAggregatePublish}; only the normal-load and tablet-reshard
     *        publish paths may pass true.
     */
    public static void aggregatePublishVersion(@NotNull List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                                               long baseVersion, long newVersion,
                                               Map<Long, Double> compactionScores,
                                               Map<Long, TabletRange> tabletRanges,
                                               Map<ComputeNode, List<Long>> nodeToTablets,
                                               ComputeResource computeResource,
                                               Map<Long, TabletStatPB> tabletStats,
                                               boolean preferSharedInitialMetadata)
            throws NoAliveBackendException, RpcException {
        AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
        try {
            createSubRequestForAggregatePublish(tablets, txnInfos, baseVersion, newVersion,
                                                nodeToTablets, computeResource, request,
                                                preferSharedInitialMetadata);
            sendAggregatePublishVersionRequest(request, baseVersion, computeResource, compactionScores,
                                               tabletRanges, tabletStats);
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
}
