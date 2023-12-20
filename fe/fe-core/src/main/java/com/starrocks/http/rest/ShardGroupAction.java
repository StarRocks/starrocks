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
import com.staros.proto.ShardGroupInfo;
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
 * The function is used to retrieve the information of a specific shard group by its service info and its group id.
 * eg:
 *  fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/shardgroup/<id>
 */
public class ShardGroupAction extends RestBaseAction {
    protected static final String SERVICE = "service";
    private static final String SHARD_GROUP_ID = "id";
    private static final Logger LOG = LogManager.getLogger(ShardGroupAction.class);

    public ShardGroupAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ShardGroupAction action = new ShardGroupAction(controller);
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/starmgr/service/{" + SERVICE + "}/shardgroup/{" + SHARD_GROUP_ID + "}", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String service = request.getSingleParameter(SERVICE);
        String shardGroupId = request.getSingleParameter(SHARD_GROUP_ID);
        if (Strings.isNullOrEmpty(service) || Strings.isNullOrEmpty(shardGroupId)) {
            response.getContent().append("Missing params, Need service info and shard group id");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Long id;
        try {
            id = Long.valueOf(shardGroupId);
        } catch (NumberFormatException e) {
            response.getContent().append("Shard group id is not a long type number. Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        ShardGroupInfo shardGroupInfo;
        try {
            shardGroupInfo = GlobalStateMgr.getCurrentStarOSAgent().getShardGroupInfo(service, id);
        } catch (StarClientException e) {
            response.getContent().append("Service info : " + service + ", Shard group id: " + id
                    + ", Failed to get shard group info, Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("ServiceId", Collections.singletonList(shardGroupInfo.getServiceId()));
        results.put("GroupId", Collections.singletonList(Long.toString(shardGroupInfo.getGroupId())));
        List<Long> shardIdsNumberList = shardGroupInfo.getShardIdsList();
        List<String> shardIdsStringList = new ArrayList<>();
        if (shardIdsNumberList != null && !shardIdsNumberList.isEmpty()) {
            shardIdsStringList = shardIdsNumberList.stream().map(String::valueOf).collect(Collectors.toList());
        }
        results.put("ShardIds", shardIdsStringList);
        switch (shardGroupInfo.getPolicy()) {
            case NONE:
                results.put("PlacementPolicy", Collections.singletonList("NONE"));
                break;
            case RANDOM:
                results.put("PlacementPolicy", Collections.singletonList("RANDOM"));
                break;
            case SPREAD:
                results.put("PlacementPolicy", Collections.singletonList("SPREAD"));
                break;
            case PACK:
                results.put("PlacementPolicy", Collections.singletonList("PACK"));
                break;
            case EXCLUDE:
                results.put("PlacementPolicy", Collections.singletonList("EXCLUDE"));
                break;
            default:
                results.put("PlacementPolicy", Collections.singletonList("UNKNOWN"));
                break;
        }
        results.put("Anonymous", Collections.singletonList(Boolean.toString(shardGroupInfo.getAnonymous())));
        results.put("MetaGroupId", Collections.singletonList(Long.toString(shardGroupInfo.getMetaGroupId())));
        List<String> propertiesList = new ArrayList<>();
        Map<String, String> propertiesMap = shardGroupInfo.getPropertiesMap();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            propertiesList = propertiesMap.entrySet()
                                     .stream()
                                     .map(entry -> entry.getKey() + " = " + entry.getValue())
                                     .collect(Collectors.toList());
        }
        results.put("Properties", propertiesList);
        List<String> labelsList = new ArrayList<>();
        Map<String, String> labelsMap = shardGroupInfo.getLabelsMap();
        if (labelsMap != null && !labelsMap.isEmpty()) {
            labelsList = labelsMap.entrySet()
                                 .stream()
                                 .map(entry -> entry.getKey() + " = " + entry.getValue())
                                 .collect(Collectors.toList());
        }
        results.put("Labels", labelsList);

        // to json response
        sendResultByJson(request, response, results);
    }
}