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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ProfileAction.java

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

package com.starrocks.http.rest.v2;

import com.starrocks.common.util.ProfileManager;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;

// This class is a RESTFUL interface to get query profile from all frontend nodes.
// Usage:
//   wget http://fe_host:fe_http_port/api/v2/profile?query_id=123456&is_request_all_frontend=true;
public class ProfileActionV2 extends RestBaseAction {

    private static final String QUERY_PLAN_URI = "api/profile?query_id=%s";

    public ProfileActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/profile", new ProfileActionV2(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String authorization = request.getAuthorizationHeader();
        String queryId = request.getSingleParameter("query_id");
        String isRequestAllStr = request.getSingleParameter("is_request_all_frontend", "false");
        boolean isRequestAll = Boolean.parseBoolean(isRequestAllStr);;

        if (queryId == null || queryId.isEmpty()) {
            sendErrorResponse(response,
                    "Invalid parameter: query_id",
                    HttpResponseStatus.BAD_REQUEST,
                    request);
            return;
        }

        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);

        if (queryProfileStr != null) {
            sendSuccessResponse(response, queryProfileStr, request);
            return;
        }

        if (isRequestAll) {
            // If the query profile is not found in the local node,
            // we will query other frontend nodes to get the query profile.
            String queryPlainUrl = String.format(QUERY_PLAN_URI, queryId);
            List<String> profileList = queryOtherFrontendNodes(queryPlainUrl, authorization, HttpMethod.GET);
            for (String profile : profileList) {
                if (profile != null) {
                    sendSuccessResponse(response, profile, request);
                    return;
                }
            }
        }

        sendErrorResponse(response,
                String.format("Query id %s not found.", queryId),
                HttpResponseStatus.NOT_FOUND,
                request);

    }
}
