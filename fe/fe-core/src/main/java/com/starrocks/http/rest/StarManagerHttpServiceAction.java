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

import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.staros.StarMgrServer;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * The function is used to retrieve the information of a specific shard by its service info and shard id.
 * eg:
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/shard/<id>
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/shardgroup/<id>
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/listshardgroup
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/shardgroup/removereplicas
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/worker/<id>
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/workergroup/<id>
 * fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/listworkergroup
 */
public class StarManagerHttpServiceAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(StarManagerHttpServiceAction.class);
    protected static final String SERVICE = "service";
    private static final String ID = "id";

    public StarManagerHttpServiceAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        StarManagerHttpServiceAction action = new StarManagerHttpServiceAction(controller);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/shard/{" + ID + "}", action);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/shardgroup/{" + ID + "}", action);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/listshardgroup", action);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/worker/{" + ID + "}", action);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/workergroup/{" + ID + "}", action);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/listworkergroup", action);

        controller.registerHandler(HttpMethod.POST,
                "/api/v1/starmgr/service/{" + SERVICE + "}/shardgroup/{" + ID + "}/removereplicas", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);
        HttpResponse httpResponse = StarMgrServer.getCurrentState().getHttpService().starmgrHttpService(request.getRequest());
        if (httpResponse instanceof FullHttpResponse) {
            FullHttpResponse fullResponse = (FullHttpResponse) httpResponse;
            response.getContent().append(fullResponse.content().toString(CharsetUtil.UTF_8));
            response.setContentType(httpResponse.headers().get(HttpHeaderNames.CONTENT_TYPE));
            writeResponse(request, response, httpResponse.status());
        } else {
            throw new DdlException("Internal Error");
        }
    }
}