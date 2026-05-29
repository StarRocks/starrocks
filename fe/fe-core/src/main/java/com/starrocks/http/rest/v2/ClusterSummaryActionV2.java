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
import com.starrocks.http.rest.v2.vo.ClusterSummaryRestResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Map;

// This class is a RESTFUL interface to get cluster summary.
// Usage:
// wget -qO- "http://fe_host:fe_http_port/api/v2/cluster_summary"
//
//{
//   "code": 200,
//   "message": "OK",
//   "result": {
//   "dbCount": 64,
//   "tableCount": 1280,
//   "diskUsedCapacity": 549755813888,
//   "diskAvailableCapacity": 1099511627776,
//   "diskTotalCapacity": 1649267441664,
//   "totalBackendNum": 8,
//   "aliveBackendNum": 8,
//   "totalCnNum": 4,
//   "aliveCnNum": 4,
//   "totalFeNum": 3,
//   "aliveFeNum": 3
//   }
//}
public class ClusterSummaryActionV2 extends RestBaseAction {

    public ClusterSummaryActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/cluster_summary", new ClusterSummaryActionV2(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        LocalMetastore infoService = GlobalStateMgr.getCurrentState().getLocalMetastore();

        GlobalStateMgr globalState = GlobalStateMgr.getCurrentState();
        NodeMgr nodeMgr = globalState.getNodeMgr();
        SystemInfoService clusterInfo = nodeMgr.getClusterInfo();

        ImmutableMap<Long, Backend> backendMap = clusterInfo.getIdToBackend();

        long tableCount = getTblCount(infoService);
        long dataUsedCapacity = getDataUsedCapacity(backendMap);
        long availableCapacity = getAvailableCapacity(backendMap);
        long totalCapacity = getTotalCapacity(backendMap);

        ClusterSummaryRestResult clusterSummaryRestResult = new ClusterSummaryRestResult.Builder()
                .dbCount((long) infoService.getDbIds().size())
                .tableCount(tableCount)
                .diskUsedCapacity(dataUsedCapacity)
                .diskAvailableCapacity(availableCapacity)
                .diskTotalCapacity(totalCapacity)
                .totalBackendNum((long) clusterInfo.getBackends().size())
                .aliveBackendNum((long) clusterInfo.getAliveBackendNumber())
                .totalCnNum((long) clusterInfo.getComputeNodes().size())
                .aliveCnNum((long) clusterInfo.getAliveComputeNodeNumber())
                .totalFeNum((long) nodeMgr.getAllFrontends().size())
                .aliveFeNum(nodeMgr.getAliveFrontendsCnt())
                .build();

        sendResult(request, response, new RestBaseResultV2<>(clusterSummaryRestResult));
    }

    private long getTblCount(LocalMetastore infoService) {
        return infoService.getDbIds()
                .stream()
                .mapToLong(db -> (long) infoService.getTables(db).size())
                .sum();
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
