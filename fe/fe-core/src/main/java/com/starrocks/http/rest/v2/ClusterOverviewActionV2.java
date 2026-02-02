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
import com.starrocks.system.Backend;
import io.netty.handler.codec.http.HttpMethod;

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

        ClusterOverview clusterOverview = new ClusterOverview(
                (long) infoService.getAllDbs().size(),
                (long) getTblCount(infoService),
                getDataUsedCapacity(),
                getAvailableCapacity(),
                getTotalCapacity(),
                (long) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends().size(),
                (long) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber(),
                (long) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodes().size(),
                (long) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveComputeNodeNumber(),
                (long) GlobalStateMgr.getCurrentState().getNodeMgr().getAllFrontends().size(),
                GlobalStateMgr.getCurrentState().getNodeMgr().getAliveFrontendsCnt()
        );
        sendResult(request, response, new RestBaseResultV2<>(clusterOverview));
    }

    private int getTblCount(LocalMetastore infoService) {
        Integer tableCount = infoService.getAllDbs()
                .stream()
                .map(db -> db.getTables().size())
                .reduce(Integer::sum).orElse(0);
        return  tableCount;
    }

    private long getDataUsedCapacity() {
        long dataUsedCapacity = 0;
        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        for (Backend backend : backendMap.values()) {
            dataUsedCapacity += backend.getDataUsedCapacityB();
        }
        return dataUsedCapacity;
    }

    private long getAvailableCapacity() {
        long availableCapacity = 0;
        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        for (Backend backend : backendMap.values()) {
            availableCapacity += backend.getAvailableCapacityB();
        }
        return availableCapacity;
    }

    private long getTotalCapacity() {
        long totalCapacity = 0;
        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        for (Backend backend : backendMap.values()) {
            totalCapacity += backend.getTotalCapacityB();
        }
        return totalCapacity;
    }


}
