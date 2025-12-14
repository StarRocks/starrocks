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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadataRepairStatus;
import com.starrocks.proto.TabletMetadatas;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TabletRepairHelper {
    private static final Logger LOG = LogManager.getLogger(TabletRepairHelper.class);

    record PhysicalPartitionInfo(
            long physicalPartitionId,
            List<Long> allTablets, // all tablets in one physical partition
            Set<Long> leftTablets, // tablets whose valid metadata still need to be found
            Map<ComputeNode, Set<Long>> nodeToTablets,
            long maxVersion,
            long minVersion
    ) {
    }

    private static Map<Long, Map<Long, TabletMetadataPB>> getTabletMetadatas(PhysicalPartitionInfo info, long maxVersion,
                                                                             long minVersion) throws Exception {
        long physicalPartitionId = info.physicalPartitionId;
        Set<Long> leftTablets = info.leftTablets;
        Map<ComputeNode, Set<Long>> nodeToTablets = info.nodeToTablets;

        List<Future<GetTabletMetadatasResponse>> responses = Lists.newArrayList();
        List<ComputeNode> nodes = Lists.newArrayList();
        for (Map.Entry<ComputeNode, Set<Long>> entry : nodeToTablets.entrySet()) {
            ComputeNode node = entry.getKey();
            Set<Long> tabletIds = Sets.newHashSet(entry.getValue());

            tabletIds.retainAll(leftTablets);
            if (tabletIds.isEmpty()) {
                continue;
            }

            GetTabletMetadatasRequest request = new GetTabletMetadatasRequest();
            request.tabletIds = Lists.newArrayList(tabletIds);
            request.maxVersion = maxVersion;
            request.minVersion = minVersion;

            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                Future<GetTabletMetadatasResponse> future = lakeService.getTabletMetadatas(request);
                responses.add(future);
                nodes.add(node);
            } catch (RpcException e) {
                LOG.warn("Fail to send get tablet metadatas request to node {}, partition: {}, error: {}", node.getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
        for (int i = 0; i < responses.size(); ++i) {
            try {
                GetTabletMetadatasResponse response = responses.get(i).get(LakeService.TIMEOUT_GET_TABLET_STATS,
                        TimeUnit.MILLISECONDS);

                if (response == null) {
                    throw new StarRocksException("response is null");
                }

                TStatusCode statusCode = TStatusCode.findByValue(response.status.statusCode);
                if (statusCode != TStatusCode.OK) {
                    List<String> errMsgs = response.status.errorMsgs;
                    throw new StarRocksException(errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                }

                if (response.tabletMetadatas != null) {
                    for (TabletMetadatas tm : response.tabletMetadatas) {
                        long tabletId = tm.tabletId;
                        TStatusCode tabletStatusCode = TStatusCode.findByValue(tm.status.statusCode);
                        if (tabletStatusCode == TStatusCode.OK) {
                            Map<Long, TabletMetadataPB> versionMetadatas = tm.versionMetadatas;
                            tabletVersionMetadatas.put(tabletId, versionMetadatas);
                        } else if (tabletStatusCode != TStatusCode.NOT_FOUND) {
                            List<String> errMsgs = tm.status.errorMsgs;
                            throw new StarRocksException(
                                    errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    Map<Long, List<Long>> tabletVersions = Maps.newHashMap();
                    if (response.tabletMetadatas != null) {
                        for (TabletMetadatas tm : response.tabletMetadatas) {
                            TStatusCode tabletStatusCode = TStatusCode.findByValue(tm.status.statusCode);
                            if (tabletStatusCode == TStatusCode.OK) {
                                tabletVersions.put(tm.tabletId, Lists.newArrayList(tm.versionMetadatas.keySet()));
                            }
                        }
                    }
                    LOG.debug("Get {} tablet metadatas from node {}, partition: {}, version range: [{}, {}], tablet versions: {}",
                            tabletVersions.size(), nodes.get(i).getId(), physicalPartitionId, minVersion, maxVersion,
                            tabletVersions);
                }
            } catch (Exception e) {
                LOG.warn("Fail to get tablet metadatas from node {}, partition: {}, error: {}", nodes.get(i).getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        return tabletVersionMetadatas;
    }

    private static Map<Long, String> repairTabletMetadata(PhysicalPartitionInfo info, Map<Long, TabletMetadataPB> validMetadatas,
                                                          boolean isFileBundling) throws Exception {
        long physicalPartitionId = info.physicalPartitionId;
        Map<ComputeNode, Set<Long>> nodeToTablets = info.nodeToTablets;

        boolean writeBundlingFile = true;
        List<Future<RepairTabletMetadataResponse>> responses = Lists.newArrayList();
        List<ComputeNode> nodes = Lists.newArrayList();
        for (Map.Entry<ComputeNode, Set<Long>> entry : nodeToTablets.entrySet()) {
            RepairTabletMetadataRequest request = new RepairTabletMetadataRequest();
            request.enableFileBundling = isFileBundling;
            request.writeBundlingFile = isFileBundling && writeBundlingFile;

            // if enable file bundling, we only need to send the write bundling metadata request to one node.
            // other node requests are used for some cleanup work.
            Set<Long> tabletIds = request.writeBundlingFile ? Sets.newHashSet(info.allTablets) : entry.getValue();
            Preconditions.checkState(!tabletIds.isEmpty());
            if (request.writeBundlingFile) {
                writeBundlingFile = false;
            }

            List<TabletMetadataPB> newMetadatas = Lists.newArrayList();
            for (long tabletId : tabletIds) {
                TabletMetadataPB metadata = validMetadatas.get(tabletId);
                Preconditions.checkState(metadata != null);
                // set version to physical partition visible version
                metadata.version = info.maxVersion;
                newMetadatas.add(metadata);
            }

            request.tabletMetadatas = newMetadatas;

            ComputeNode node = entry.getKey();
            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                Future<RepairTabletMetadataResponse> future = lakeService.repairTabletMetadata(request);
                responses.add(future);
                nodes.add(node);
            } catch (RpcException e) {
                LOG.warn("Fail to send repair tablet metadata request to node {}, partition: {}, error: {}", node.getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        Map<Long, String> tabletErrors = Maps.newHashMap();
        for (int i = 0; i < responses.size(); i++) {
            try {
                RepairTabletMetadataResponse response = responses.get(i).get(LakeService.TIMEOUT_REPAIR_METADATA,
                        TimeUnit.MILLISECONDS);

                if (response == null) {
                    throw new StarRocksException("response is null");
                }

                TStatusCode statusCode = TStatusCode.findByValue(response.status.statusCode);
                if (statusCode != TStatusCode.OK) {
                    List<String> errMsgs = response.status.errorMsgs;
                    throw new StarRocksException(errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                }

                if (response.tabletRepairStatuses != null) {
                    for (TabletMetadataRepairStatus repairStatus : response.tabletRepairStatuses) {
                        TStatusCode tabletStatusCode = TStatusCode.findByValue(repairStatus.status.statusCode);
                        if (tabletStatusCode != TStatusCode.OK) {
                            List<String> errMsgs = repairStatus.status.errorMsgs;
                            tabletErrors.put(repairStatus.tabletId,
                                    errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Fail to repair tablet metadata from node {}, partition: {}, error: {}", nodes.get(i).getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        if (!tabletErrors.isEmpty()) {
            LOG.warn("Fail to repair tablet metadata for partition {}, failed tablets: {}", physicalPartitionId,
                    tabletErrors);
        } else {
            LOG.info("Repair tablet metadata for partition {} success", physicalPartitionId);
        }

        return tabletErrors;
    }
}
