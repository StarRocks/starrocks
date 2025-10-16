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
import com.starrocks.http.rest.v2.vo.ComputeNodeInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ComputeNodeActionV2 extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ComputeNodeActionV2.class);

    public ComputeNodeActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/computeNode", new ComputeNodeActionV2(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        List<ComputeNodeInfo> backendNodeInfos = new ArrayList<>();
        ImmutableMap<Long, ComputeNode> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdComputeNode();
        for (ComputeNode computeNode : backendMap.values()) {
            ComputeNodeInfo computeNodeInfo = new ComputeNodeInfo(computeNode);
            backendNodeInfos.add(computeNodeInfo);
        }
        sendResult(request, response, new RestBaseResultV2<>(backendNodeInfos));
    }
}