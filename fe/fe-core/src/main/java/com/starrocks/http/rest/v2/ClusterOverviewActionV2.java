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

package com.starrocks.http.rest.v2;

import com.google.common.collect.ImmutableMap;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.v2.vo.ClusterOverview;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Map;

public class ClusterOverviewActionV2 extends RestBaseAction {

    public ClusterOverviewActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/cluster_overview", new ClusterOverviewActionV2(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        LocalMetastore infoService = GlobalStateMgr.getCurrentState().getLocalMetastore();

        GlobalStateMgr globalState = GlobalStateMgr.getCurrentState();
        NodeMgr nodeMgr = globalState.getNodeMgr();
        SystemInfoService clusterInfo = nodeMgr.getClusterInfo();

        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState()
                        .getNodeMgr()
                        .getClusterInfo()
                        .getIdToBackend();

        long tableCount = getTblCount(infoService);
        long dataUsedCapacity = getDataUsedCapacity(backendMap);
        long availableCapacity = getAvailableCapacity(backendMap);
        long totalCapacity = getTotalCapacity(backendMap);

        ClusterOverview clusterOverview = new ClusterOverview(
                (long) infoService.getAllDbs().size(),
                tableCount,
                dataUsedCapacity,
                availableCapacity,
                totalCapacity,
                (long) clusterInfo.getBackends().size(),
                (long) clusterInfo.getAliveBackendNumber(),
                (long) clusterInfo.getComputeNodes().size(),
                (long) clusterInfo.getAliveComputeNodeNumber(),
                (long) nodeMgr.getAllFrontends().size(),
                nodeMgr.getAliveFrontendsCnt()
        );

        sendResult(request, response, new RestBaseResultV2<>(clusterOverview));
    }

    private int getTblCount(LocalMetastore infoService) {
        Integer tableCount = infoService.getAllDbs()
                .stream()
                .map(db -> db.getTables().size())
                .reduce(Integer::sum).orElse(0);
        return tableCount;
    }

    private long getDataUsedCapacity(Map<Long, Backend> backendMap) {
        return backendMap.values()
                .stream()
                .mapToLong(Backend::getDataUsedCapacityB)
                .sum();
    }

    private long getAvailableCapacity(Map<Long, Backend> backendMap) {
        return backendMap.values()
                .stream()
                .mapToLong(Backend::getAvailableCapacityB)
                .sum();
    }

    private long getTotalCapacity(Map<Long, Backend> backendMap) {
        return backendMap.values()
                .stream()
                .mapToLong(Backend::getTotalCapacityB)
                .sum();
    }
}
