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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/QueryDetailAction.java

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

import com.google.gson.Gson;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;

public class QueryDetailAction extends RestBaseAction {

    public QueryDetailAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/query_detail", new QueryDetailAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String eventTimeStr = request.getSingleParameter("event_time");
        if (eventTimeStr == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        long eventTime = Long.parseLong(eventTimeStr.trim());
        List<QueryDetail> queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(eventTime);
        Gson gson = new Gson();
        String jsonString = gson.toJson(queryDetails);
        response.getContent().append(jsonString);
        sendResult(request, response);
    }
}
