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

package com.starrocks.epack.http.rest;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.epack.warehouse.WarehouseInfo;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TGetWarehousesRequest;
import com.starrocks.thrift.TGetWarehousesResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class WarehouseAction extends RestBaseAction {
    public static final String URI = "/api/v1/warehouses";
    private static final Logger LOG = LogManager.getLogger(WarehouseAction.class);


    public WarehouseAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, URI, new WarehouseAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        WarehouseInfosBuilder warehouseInfoBuilder = WarehouseInfosBuilder.makeBuilderFromMetricAndMgrs();

        List<WarehouseInfo> infosFromOtherFEs = getWarehouseInfosFromOtherFEs();
        infosFromOtherFEs.forEach(warehouseInfoBuilder::withWarehouseInfo);

        Map<Long, WarehouseInfo> warehouseInfo = warehouseInfoBuilder.build();
        RestSuccessBaseResult<Result> res = new RestSuccessBaseResult<>(new Result(warehouseInfo.values()));

        response.setContentType("application/json");
        response.getContent().append(res.toJsonString());
        sendResult(request, response);
    }

    public List<WarehouseInfo> getWarehouseInfosFromOtherFEs() {
        List<WarehouseInfo> warehouseInfos = Lists.newArrayList();
        TGetWarehousesRequest request = new TGetWarehousesRequest();

        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getNodeMgr().getAllFrontends();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first)) {
                continue;
            }

            try {
                TGetWarehousesResponse response = FrontendServiceProxy
                        .call(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                                Config.thrift_rpc_timeout_ms,
                                Config.thrift_rpc_retry_times,
                                client -> client.getWarehouses(request));
                if (response.getStatus().getStatus_code() != TStatusCode.OK) {
                    LOG.warn("getWarehouseInfos to remote fe: {} failed", fe.getHost());
                } else if (response.isSetWarehouse_infos()) {
                    response.getWarehouse_infos().stream()
                            .map(WarehouseInfo::fromThrift)
                            .forEach(warehouseInfos::add);
                }
            } catch (Exception e) {
                LOG.warn("getWarehouseInfos to remote fe: {} failed", fe.getHost(), e);
            }
        }

        return warehouseInfos;
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
