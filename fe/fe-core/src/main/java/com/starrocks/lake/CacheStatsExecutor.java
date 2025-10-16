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
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.GetTabletCacheStatsMeta;
import com.starrocks.proto.GetTabletCacheStatsRequest;
import com.starrocks.proto.GetTabletCacheStatsResponse;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.RefreshCacheStatsStatement;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class CacheStatsExecutor {
    public static ShowResultSet execute(RefreshCacheStatsStatement statement,
                                        ConnectContext connectContext) throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouseManager = globalStateMgr.getWarehouseMgr();
        Warehouse wh = warehouseManager.getWarehouse(connectContext.getCurrentWarehouseName());
        ComputeResourceProvider computeResourceProvider = warehouseManager.getComputeResourceProvider();
        List<ComputeResource> computeResources = computeResourceProvider.getComputeResources(wh);

        Map<Long, RefreshCacheStatsStatement.PartitionSnapshot> tablets = statement.prepare();
        for (ComputeResource computeResource : computeResources) {
            if (!computeResourceProvider.isResourceAvailable(computeResource)) {
                continue;
            }
            Map<Long /* beId */, List<Pair<Long /* tabletId */, Long /* version */>>> tabletsByBeMap = new HashMap<>();
            for (Map.Entry<Long, RefreshCacheStatsStatement.PartitionSnapshot> entry : tablets.entrySet()) {
                try {
                    long beId = globalStateMgr.getStarOSAgent().getPrimaryComputeNodeIdByShard(
                            entry.getKey(), computeResource.getWorkerGroupId());
                    tabletsByBeMap.computeIfAbsent(beId,
                            k -> Lists.newArrayList()).add(Pair.of(entry.getKey(), entry.getValue().visibleVersion));
                } catch (StarRocksException e) {
                    throw new DdlException(String.format("Fail to get replica for tablet %d, error: %s.",
                            entry.getKey(), e.getMessage()));
                }
            }

            Map<Long, Future<GetTabletCacheStatsResponse>> futureMap = new HashMap<>();
            for (Map.Entry<Long, List<Pair<Long, Long>>> entry : tabletsByBeMap.entrySet()) {
                long beId = entry.getKey();
                ComputeNode node = globalStateMgr.getNodeMgr().getClusterInfo()
                        .getBackendOrComputeNode(beId);
                if (node == null) {
                    throw new DdlException(String.format("Node %d not exist.", beId));
                }

                List<GetTabletCacheStatsMeta> metas = Lists.newArrayList();
                for (Pair<Long, Long> e : entry.getValue()) {
                    GetTabletCacheStatsMeta meta = new GetTabletCacheStatsMeta();
                    meta.tabletId = e.getKey();
                    meta.version = e.getValue();
                    metas.add(meta);
                }
                GetTabletCacheStatsRequest request = new GetTabletCacheStatsRequest();
                request.tablets = metas;
                try {
                    LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                    futureMap.put(beId, lakeService.getTabletCacheStats(request));
                } catch (Throwable e) {
                    throw new DdlException(String.format("Fail to send rpc request to node: %s. error: %s.",
                            node.toString(), e.getMessage()));
                }
            }

            for (Map.Entry<Long, Future<GetTabletCacheStatsResponse>> entry : futureMap.entrySet()) {
                long beId = entry.getKey();
                Future<GetTabletCacheStatsResponse> future = entry.getValue();
                GetTabletCacheStatsResponse response = null;
                try {
                    response = future.get();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new DdlException(
                            String.format("Interrupted while waiting for response from node: %d, error: %s.",
                            beId, exception.getMessage()));
                } catch (Exception e) {
                    throw new DdlException(String.format("Failed to get response from node: %d, error: %s.",
                            beId, e.getMessage()));
                }
                if (response.status != null && response.status.statusCode != 0) {
                    throw new DdlException(
                            String.format("Failed to get cache stats from node: %d, error: %s.",
                            beId, response.status.errorMsgs.get(0)));
                }
                if (response.cacheStats != null) {
                    statement.submitResult(computeResource.getWorkerGroupId(), response.cacheStats);
                }
            }
        }
        return statement.getResult();
    }
}
