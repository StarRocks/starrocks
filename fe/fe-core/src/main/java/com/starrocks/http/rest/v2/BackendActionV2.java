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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.v2.vo.BackendInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class BackendActionV2 extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(BackendActionV2.class);

    public BackendActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/backend", new BackendActionV2(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String isRequestComputeNodeStr = request.getSingleParameter("is_request_compute_node", "false");
        boolean isRequestComputeNode = Boolean.parseBoolean(isRequestComputeNodeStr);
        List<BackendInfo> backendInfos = new ArrayList<>();

        if (isRequestComputeNode) {
            ImmutableMap<Long, ComputeNode> backendMap =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdComputeNode();
            for (ComputeNode computeNode : backendMap.values()) {
                BackendInfo backendInfo = new BackendInfo();
                InetAddress address;
                try {
                    address = InetAddress.getByName(computeNode.getHost());
                } catch (UnknownHostException e) {
                    LOG.warn("unknown host: " + computeNode.getHost(), e);
                    continue;
                }
                backendInfo.setHostName(address.getHostName());
                backendInfo.setId(computeNode.getId());
                backendInfo.setHeartPort((long) computeNode.getHeartbeatPort());
                backendInfo.setBePort(String.valueOf(computeNode.getBePort()));
                backendInfo.setHttpPort(String.valueOf(computeNode.getHttpPort()));
                backendInfo.setBrpcPort(String.valueOf(computeNode.getBrpcPort()));
                backendInfo.setState(computeNode.getBackendState().name());
                backendInfo.setAlive(computeNode.getIsAlive().get());
                backendInfo.setStartTime(TimeUtils.longToTimeString(computeNode.getLastStartTime()));
                backendInfo.setVersion(computeNode.getVersion());
                backendInfo.setLastUpdateTime(TimeUtils.longToTimeString(computeNode.getLastUpdateMs()));
                backendInfo.setMemUsed(computeNode.getMemUsedBytes());
                backendInfo.setMemLimit(computeNode.getMemLimitBytes());
                backendInfo.setCpuUsedPermille(computeNode.getCpuUsedPermille());
                backendInfo.setCpuCores(computeNode.getCpuCores());
                backendInfo.setLastMissingHeartbeatTime(computeNode.getLastMissingHeartbeatTime());
                backendInfo.setHeartbeatErrMsg(computeNode.getHeartbeatErrMsg());
                backendInfos.add(backendInfo);
            }
        } else {
            ImmutableMap<Long, Backend> backendMap =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
            for (Backend backend : backendMap.values()) {
                BackendInfo backendInfo = new BackendInfo();
                InetAddress address;
                try {
                    address = InetAddress.getByName(backend.getHost());
                } catch (UnknownHostException e) {
                    LOG.warn("unknown host: " + backend.getHost(), e);
                    continue;
                }
                backendInfo.setHostName(address.getHostName());
                backendInfo.setId(backend.getId());
                backendInfo.setHeartPort((long) backend.getHeartbeatPort());
                backendInfo.setBePort(String.valueOf(backend.getBePort()));
                backendInfo.setHttpPort(String.valueOf(backend.getHttpPort()));
                backendInfo.setBrpcPort(String.valueOf(backend.getBrpcPort()));
                backendInfo.setState(backend.getBackendState().name());
                backendInfo.setAlive(backend.getIsAlive().get());
                backendInfo.setStartTime(TimeUtils.longToTimeString(backend.getLastStartTime()));
                backendInfo.setLastReportTabletsTime(backend.getBackendStatus().lastSuccessReportTabletsTime);
                backendInfo.setVersion(backend.getVersion());
                backendInfo.setLastUpdateTime(TimeUtils.longToTimeString(backend.getLastUpdateMs()));
                backendInfo.setMemUsed(backend.getMemUsedBytes());
                backendInfo.setMemLimit(backend.getMemLimitBytes());
                backendInfo.setCpuUsedPermille(backend.getCpuUsedPermille());
                backendInfo.setCpuCores(backend.getCpuCores());
                backendInfo.setDataUsedCapacity(backend.getDataUsedCapacityB());
                backendInfo.setDataTotalCapacity(backend.getDataTotalCapacityB());
                backendInfo.setAvailableCapacity(backend.getAvailableCapacityB());
                backendInfo.setTotalCapacity(backend.getTotalCapacityB());
                backendInfo.setLastMissingHeartbeatTime(backend.getLastMissingHeartbeatTime());
                backendInfo.setHeartbeatErrMsg(backend.getHeartbeatErrMsg());
                backendInfos.add(backendInfo);
            }
        }
        sendResult(request, response, new RestBaseResultV2<>(backendInfos));
    }
}
