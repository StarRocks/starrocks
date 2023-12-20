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
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardState;
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
 * The function is used to retrieve the information of a specific shard by its service info and shard id.
 * eg:
 *  fe_host:http_port/api/v1/starmgr/service/<serviceid|servicename>/shard/<id>
 */
public class ShardAction extends RestBaseAction {
    protected static final String SERVICE = "service";
    private static final String SHARD_ID = "id";
    private static final Logger LOG = LogManager.getLogger(ShardAction.class);

    public ShardAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ShardAction action = new ShardAction(controller);
        controller.registerHandler(
                HttpMethod.GET, "/api/v1/starmgr/service/{" + SERVICE + "}/shard/{" + SHARD_ID + "}", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String service = request.getSingleParameter(SERVICE);
        String shardId = request.getSingleParameter(SHARD_ID);
        if (Strings.isNullOrEmpty(service) || Strings.isNullOrEmpty(shardId)) {
            response.getContent().append("Missing params, Need service info and shard id");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Long id;
        try {
            id = Long.valueOf(shardId);
        } catch (NumberFormatException e) {
            response.getContent().append("Shard id is not a long type number. Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        ShardInfo shardInfo;
        try {
            shardInfo = GlobalStateMgr.getCurrentStarOSAgent().getShardInfo(service, id);
        } catch (StarClientException e) {
            response.getContent().append("Service info : " + service + ", Shard id: " + id
                    + ", Failed to get shard info, Error: " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("ServiceId", Collections.singletonList(shardInfo.getServiceId()));
        List<Long> groupIdsNumberList = shardInfo.getGroupIdsList();
        List<String> groupIdsStringList = new ArrayList<>();
        if (groupIdsNumberList != null && !groupIdsNumberList.isEmpty()) {
            groupIdsStringList = groupIdsStringList.stream().map(String::valueOf).collect(Collectors.toList());
        }
        results.put("GroupIds", groupIdsStringList.stream().map(String::valueOf).collect(Collectors.toList()));
        results.put("ShardId", Collections.singletonList(Long.toString(shardInfo.getShardId())));
        results.put("ShardState",
                Collections.singletonList(shardInfo.getShardState() == ShardState.NORMAL ? "Normal" : "Abnormal"));
        results.put("FileStoreInfoVersion",
                Collections.singletonList(Long.toString(shardInfo.getFilePath().getFsInfo().getVersion())));
        results.put("FileStoreInfoFullPath", Collections.singletonList(shardInfo.getFilePath().getFullPath()));
        results.put("FilsCacheEnable",
                Collections.singletonList(Boolean.toString(shardInfo.getFileCache().getEnableCache())));
        results.put(
                "ExpectedReplicaNum", Collections.singletonList(Integer.toString(shardInfo.getExpectedReplicaNum())));
        List<String> propertiesList = new ArrayList<>();
        Map<String, String> propertiesMap = shardInfo.getShardPropertiesMap();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            propertiesList = propertiesMap.entrySet()
                                     .stream()
                                     .map(entry -> entry.getKey() + " = " + entry.getValue())
                                     .collect(Collectors.toList());
        }
        results.put("ShardProperties", propertiesList);

        // to json response
        sendResultByJson(request, response, results);
    }
}