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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/HealthAction.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http.rest;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.concurrent.atomic.AtomicBoolean;

public class HealthAction extends RestBaseAction {
    @VisibleForTesting
    static final String CHECK_NODES_PARAM = "check_nodes";

    // Set once this FE process has seen an alive BE/CN (or found none registered). Never reset.
    private static final AtomicBoolean NODES_READY = new AtomicBoolean(false);

    public HealthAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/health", new HealthAction(controller));
    }

    // Liveness probe. Historically anonymous; gated for backward compatibility so it
    // requires Basic auth (AuthN-only, no privilege check) only when the operator opts
    // in via `enable_http_auth`.
    @Override
    public boolean needAuth() {
        return Config.enable_http_auth;
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        if (GracefulExitFlag.isGracefulExit()) {
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if ("true".equalsIgnoreCase(request.getSingleParameter(CHECK_NODES_PARAM))
                && !isNodesReady(
                        clusterInfo.getTotalBackendNumber() + clusterInfo.getTotalComputeNodeNumber(),
                        clusterInfo.getAliveBackendNumber() + clusterInfo.getAliveComputeNodeNumber())) {
            sendResult(request, response, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }

        response.setContentType("application/json");

        RestResult result = new RestResult();
        result.addResultEntry("total_backend_num", clusterInfo.getTotalBackendNumber());
        result.addResultEntry("online_backend_num", clusterInfo.getAliveBackendNumber());
        sendResult(request, response, result);
    }

    @VisibleForTesting
    public static boolean isNodesReady(int totalNodes, int aliveNodes) {
        if (NODES_READY.get()) {
            return true;
        }
        if (totalNodes == 0 || aliveNodes > 0) {
            NODES_READY.set(true);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    public static void resetNodesReadyForTest() {
        NODES_READY.set(false);
    }

    @Override
    public boolean supportAsyncHandler() {
        // Health Action need to be handled synchronously
        return false;
    }
}
