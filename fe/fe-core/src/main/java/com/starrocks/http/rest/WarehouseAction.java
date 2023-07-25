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

import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.WarehouseInfo;
import com.starrocks.warehouse.WarehouseInfosBuilder;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class WarehouseAction extends RestBaseAction {
    public static final String URI = "/api/v1/warehouses";

    public WarehouseAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, URI, new WarehouseAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        WarehouseInfosBuilder warehouseInfoBuilder = WarehouseInfosBuilder.makeBuilderFromMetricAndMgrs();

        List<WarehouseInfo> infosFromOtherFEs = GlobalStateMgr.getCurrentState().getWarehouseInfosFromOtherFEs();
        infosFromOtherFEs.forEach(warehouseInfoBuilder::withWarehouseInfo);

        Map<String, WarehouseInfo> warehouseInfo = warehouseInfoBuilder.build();
        RestSuccessBaseResult<Result> res = new RestSuccessBaseResult<>(new Result(warehouseInfo.values()));

        response.setContentType("application/json");
        response.getContent().append(res.toJson());
        sendResult(request, response);
    }

    public static class Result {
        private Collection<WarehouseInfo> warehouses;

        public Result(Collection<WarehouseInfo> warehouses) {
            this.warehouses = warehouses;
        }

        public Collection<WarehouseInfo> getWarehouses() {
            return warehouses;
        }

        public void setWarehouses(Collection<WarehouseInfo> warehouses) {
            this.warehouses = warehouses;
        }
    }
}
