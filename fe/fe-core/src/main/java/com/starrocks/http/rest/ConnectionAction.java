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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ConnectionAction.java

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

package com.starrocks.http.rest;

import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

// This class is used to get current query_id of connection_id.
// Every connection holds at most one query at every point.
// Some we can get query_id firstly, and get query by query_id.
public class ConnectionAction extends RestBaseAction {
    public ConnectionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/connection", new ConnectionAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String connStr = request.getSingleParameter("connection_id");
        if (connStr == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        long connectionId = Long.valueOf(connStr.trim());
        ConnectContext context = ExecuteEnv.getInstance().getScheduler().getContext(connectionId);
        if (context == null || context.getExecutionId() == null) {
            response.getContent().append("connection id " + connectionId + " not found.");
            sendResult(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }
        String queryId = DebugUtil.printId(context.getExecutionId());
        response.setContentType("application/json");
        response.getContent().append("{\"query_id\" : \"" + queryId + "\"}");
        sendResult(request, response);
    }
}
