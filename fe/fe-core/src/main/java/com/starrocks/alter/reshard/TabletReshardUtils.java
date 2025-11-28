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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.TabletRange;
import com.starrocks.common.AggregateFuture;
import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.lake.Utils;
import com.starrocks.proto.FindSplitPointRequest;
import com.starrocks.proto.FindSplitPointResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TabletReshardUtils {

    public static int calcSplitCount(long dataSize, long splitSize) {
        int splitCount = (int) (dataSize / splitSize + 1);
        return Math.min(splitCount, Config.tablet_reshard_max_split_count);
    }

    public static Future<Map<Long, List<TabletRange>>> findSplitPoint(List<SplittingTablet> splittingTablets) {
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Map<ComputeNode, List<SplittingTablet>> nodeToSplittingTablets = new HashMap<>();
            for (SplittingTablet splittingTablet : splittingTablets) {
                ComputeNode cn = Utils.getComputeNode(splittingTablet.getOldTabletId(),
                        WarehouseManager.DEFAULT_RESOURCE, warehouseManager);
                nodeToSplittingTablets.computeIfAbsent(cn, k -> new ArrayList<>()).add(splittingTablet);
            }

            List<Future<?>> futures = new ArrayList<>(nodeToSplittingTablets.size());
            for (var entry : nodeToSplittingTablets.entrySet()) {
                FindSplitPointRequest findSplitPointRequest = new FindSplitPointRequest();
                findSplitPointRequest.tabletSplitInfos = new ArrayList<>(splittingTablets.size());
                for (SplittingTablet splittingTablet : entry.getValue()) {
                    var tabletSplitInfo = new FindSplitPointRequest.TabletSplitInfo();
                    tabletSplitInfo.tabletId = splittingTablet.getOldTabletId();
                    tabletSplitInfo.splitCount = splittingTablet.getNewTabletIds().size();
                    findSplitPointRequest.tabletSplitInfos.add(tabletSplitInfo);
                }

                ComputeNode cn = entry.getKey();
                LakeService lakeService = BrpcProxy.getLakeService(cn.getHost(), cn.getBrpcPort());
                Future<FindSplitPointResponse> future = lakeService.findSplitPoint(findSplitPointRequest);
                futures.add(future);
            }

            return new AggregateFuture<Map<Long, List<TabletRange>>>(futures, responses -> {
                Map<Long, List<TabletRange>> result = new HashMap<>();
                for (Object response : responses) {
                    FindSplitPointResponse findSplitPointResponse = (FindSplitPointResponse) response;
                    Status status = new Status(findSplitPointResponse.status);
                    if (!status.ok()) {
                        throw new TabletReshardException("Failed to find split point: " + status);
                    }
                    for (var tabletSplitResult : findSplitPointResponse.tabletSplitResults) {
                        List<TabletRange> splitRanges = tabletSplitResult.splitRanges.stream()
                                .map(tabletRangePB -> TabletRange.fromProto(tabletRangePB))
                                .collect(Collectors.toList());
                        result.put(tabletSplitResult.tabletId, splitRanges);
                    }
                }
                return result;
            });
        } catch (Exception e) {
            throw new TabletReshardException("Failed to find split point", e);
        }
    }
}
