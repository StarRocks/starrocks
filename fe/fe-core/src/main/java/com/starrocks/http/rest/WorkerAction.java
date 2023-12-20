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
import com.staros.proto.WorkerInfo;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * The function is used to retrieve the information of a specific worker by its service info and worker id.
 * eg:
 *  fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/worker/<id>
 */
public class WorkerAction extends RestBaseAction {
    protected static final String SERVICE = "service";
    protected static final String WORKER_ID = "id";
    private static final Logger LOG = LogManager.getLogger(WorkerAction.class);

    public WorkerAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        WorkerAction action = new WorkerAction(controller);
        controller.registerHandler(
                HttpMethod.GET, "/api/v1/starmgr/service/{" + SERVICE + "}/worker/{" + WORKER_ID + "}", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String service = request.getSingleParameter(SERVICE);
        String workerId = request.getSingleParameter(WORKER_ID);
        if (Strings.isNullOrEmpty(service) || Strings.isNullOrEmpty(workerId)) {
            response.getContent().append("Missing params, Need service info and worker id");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Long id;
        try {
            id = Long.valueOf(workerId);
        } catch (NumberFormatException e) {
            response.getContent().append("Worker id is not a long type number. Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        WorkerInfo workerInfo;
        try {
            workerInfo = GlobalStateMgr.getCurrentStarOSAgent().getWorkerInfo(service, id);
        } catch (StarClientException e) {
            response.getContent().append("Service info: " + service + ", Worker id: " + id
                    + ", Failed to get worker info, Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("ServiceId", Collections.singletonList(workerInfo.getServiceId()));
        results.put("GroupId", Collections.singletonList(Long.toString(workerInfo.getGroupId())));
        results.put("WorkerId", Collections.singletonList(Long.toString(workerInfo.getWorkerId())));
        results.put("IpPort", Collections.singletonList(workerInfo.getIpPort()));
        switch (workerInfo.getWorkerState()) {
            case ON:
                results.put("WorkerState", Collections.singletonList("ON"));
                break;
            case OFF:
                results.put("WorkerState", Collections.singletonList("OFF"));
                break;
            case DOWN:
                results.put("WorkerState", Collections.singletonList("DOWN"));
                break;
            default:
                results.put("WorkerState", Collections.singletonList("UNKNOWN"));
                break;
        }
        List<String> propertiesList = new ArrayList<>();
        Map<String, String> propertiesMap = workerInfo.getWorkerPropertiesMap();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            propertiesList = propertiesMap.entrySet()
                                     .stream()
                                     .map(entry -> entry.getKey() + " = " + entry.getValue())
                                     .collect(Collectors.toList());
        }
        results.put("WorkerProperties", propertiesList);
        results.put("StartTime", Collections.singletonList(Long.toString(workerInfo.getStartTime())));

        // to json response
        sendResultByJson(request, response, results);
    }
}