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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadatas;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
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

                if (response.status.statusCode != 0) {
                    throw new StarRocksException(response.status.errorMsgs.get(0));
                }

                for (TabletMetadatas tm : response.tabletMetadatas) {
                    long tabletId = tm.tabletId;
                    int statusCode = tm.status.statusCode;
                    if (statusCode == 0) {
                        Map<Long, TabletMetadataPB> versionMetadatas = tm.versionMetadatas;
                        tabletVersionMetadatas.put(tabletId, versionMetadatas);
                    } else if (statusCode != 31) {
                        // status code 31 is not found
                        throw new StarRocksException(tm.status.errorMsgs.get(0));
                    }
                }

                if (LOG.isDebugEnabled()) {
                    Map<Long, List<Long>> tabletVersions = Maps.newHashMap();
                    for (TabletMetadatas tm : response.tabletMetadatas) {
                        if (tm.status.statusCode == 0) {
                            tabletVersions.put(tm.tabletId, Lists.newArrayList(tm.versionMetadatas.keySet()));
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
}
