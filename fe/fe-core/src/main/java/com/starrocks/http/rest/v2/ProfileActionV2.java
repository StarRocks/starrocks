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
//   wget http://fe_host:fe_http_port/api/v2/profile?query_id=123456
public class ProfileActionV2 extends RestBaseAction {

    private static final String QUERY_PLAN_URI = "api/profile?query_id=%s";

    public ProfileActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/profile", new ProfileActionV2(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String authorization = request.getAuthorizationHeader();
        String queryId = request.getSingleParameter("query_id");
        if (queryId == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        String relativeUrl = String.format(QUERY_PLAN_URI, queryId);
        List<String> dataList = queryAllFrontendNodes(relativeUrl, authorization, HttpMethod.GET);
        for (String data : dataList) {
            if (data != null) {
                response.getContent().append(data);
                sendResult(request, response);
                return;
            }
        }
        response.getContent().append("query id " + queryId + " not found.");
        sendResult(request, response, HttpResponseStatus.NOT_FOUND);
    }
}
