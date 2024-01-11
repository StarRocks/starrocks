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
 * The function is used to retrieve the information of a specific worker group by by its service info and group id.
 * eg:
 *  fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/workergroup/<id>
 */
public class WorkerGroupAction extends RestBaseAction {
    protected static final String SERVICE = "service";
    private static final String WORKER_GROUP_ID = "id";
    private static final Logger LOG = LogManager.getLogger(WorkerGroupAction.class);

    public WorkerGroupAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        WorkerGroupAction action = new WorkerGroupAction(controller);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/workergroup/{" + WORKER_GROUP_ID + "}", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String service = request.getSingleParameter(SERVICE);
        String workerId = request.getSingleParameter(WORKER_GROUP_ID);
        if (Strings.isNullOrEmpty(service) || Strings.isNullOrEmpty(workerId)) {
            response.getContent().append("Missing params, Need service info and worker group id");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Long id;
        try {
            id = Long.valueOf(workerId);
        } catch (NumberFormatException e) {
            response.getContent().append("Worker group id is not a long type number. Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        WorkerGroupDetailInfo workerGroupInfo;
        try {
            workerGroupInfo = GlobalStateMgr.getCurrentStarOSAgent().getWorkerGroupInfo(service, id);
        } catch (StarClientException e) {
            response.getContent().append("Service info : " + service + ", Worker group id: " + id
                    + ", Failed to get worker group info, Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("ServiceId", Collections.singletonList(workerGroupInfo.getServiceId()));
        results.put("GroupId", Collections.singletonList(Long.toString(workerGroupInfo.getGroupId())));
        results.put("GroupSize", Collections.singletonList(workerGroupInfo.getSpec().getSize()));
        results.put("Owner", Collections.singletonList(workerGroupInfo.getOwner()));
        switch (workerGroupInfo.getState()) {
            case PENDING:
                results.put("WorkerGroupState", Collections.singletonList("PENDING"));
                break;
            case READY:
                results.put("WorkerGroupState", Collections.singletonList("READY"));
                break;
            case DELETING:
                results.put("WorkerGroupState", Collections.singletonList("DELETING"));
                break;
            default:
                results.put("WorkerGroupState", Collections.singletonList("UNKNOWN"));
                break;
        }
        List<String> propertiesList = new ArrayList<>();
        Map<String, String> propertiesMap = workerGroupInfo.getPropertiesMap();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            propertiesList = propertiesMap.entrySet()
                                     .stream()
                                     .map(entry -> entry.getKey() + " = " + entry.getValue())
                                     .collect(Collectors.toList());
        }
        results.put("Properties", propertiesList);
        List<String> labelsList = new ArrayList<>();
        Map<String, String> labelsMap = workerGroupInfo.getLabelsMap();
        if (labelsMap != null && !labelsMap.isEmpty()) {
            labelsList = labelsMap.entrySet()
                                 .stream()
                                 .map(entry -> entry.getKey() + " = " + entry.getValue())
                                 .collect(Collectors.toList());
        }
        results.put("Labels", labelsList);
        List<WorkerInfo> workerInfosList = workerGroupInfo.getWorkersInfoList();
        List<String> workerIdsList = new ArrayList<>();
        if (workerInfosList != null && !workerInfosList.isEmpty()) {
            workerIdsList = workerInfosList.stream()
                                    .map(workerInfo -> Long.toString(workerInfo.getWorkerId()))
                                    .collect(Collectors.toList());
        }
        results.put("WorkerIds", workerIdsList);

        // to json response
        sendResultByJson(request, response, results);
    }
}