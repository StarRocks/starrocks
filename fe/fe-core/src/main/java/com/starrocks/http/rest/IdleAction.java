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

package com.starrocks.http.rest;

import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.IdleStatus;
import io.netty.handler.codec.http.HttpMethod;

/**
 * API to check whether the cluster is idle
 * {
 *     "isClusterIdle": true,
 *     "clusterIdleTime": 1734113878006,
 *     "warehouses": [
 *         {
 *             "id": 0,
 *             "name": "default_warehouse",
 *             "isIdle": true,
 *             "idleTime": 1734113878006
 *         }
 *     ]
 * }
 */
public class IdleAction extends RestBaseAction {

    public IdleAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/idle_status", new IdleAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (Config.warehouse_idle_check_enable) {
            IdleStatus idleStatus = GlobalStateMgr.getCurrentState().getWarehouseIdleChecker().getIdleStatus();
            String content = GsonUtils.GSON.toJson(idleStatus);
            response.getContent().append(content);
        } else {
            response.getContent().append("warehouse idle check is not enabled");
        }
        sendResult(request, response);
    }
}
