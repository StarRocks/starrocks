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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.staros.client.StarClientException;
import com.staros.proto.WorkerGroupDetailInfo;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * The function is used to retrieve the information of a specific worker group by by its service info.
 * eg:
 *  fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/listworkergroups
 */
public class ListWorkerGroupsAction extends RestBaseAction {
    protected static final String SERVICE = "service";
    private static final Logger LOG = LogManager.getLogger(ListWorkerGroupsAction.class);

    public ListWorkerGroupsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ListWorkerGroupsAction action = new ListWorkerGroupsAction(controller);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/listworkergroups", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String service = request.getSingleParameter(SERVICE);
        if (Strings.isNullOrEmpty(service)) {
            response.getContent().append("Missing params, Need service info");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        List<WorkerGroupDetailInfo> workerGroupInfosList;
        try {
            workerGroupInfosList = GlobalStateMgr.getCurrentStarOSAgent().listWorkerGroupInfo(service);
        } catch (StarClientException e) {
            response.getContent().append(
                    "Service info : " + service + ", Failed to list worker groups, Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Map<String, List<String>> results = Maps.newHashMap();
        if (workerGroupInfosList.size() != 0) {
            results.put("ServiceId", Collections.singletonList(workerGroupInfosList.get(0).getServiceId()));
            results.put("WorkerGroupIds",
                    workerGroupInfosList.stream()
                            .map(workGroupInfo -> Long.toString(workGroupInfo.getGroupId()))
                            .collect(Collectors.toList()));
        }

        // to json response
        sendResultByJson(request, response, results);
    }
}