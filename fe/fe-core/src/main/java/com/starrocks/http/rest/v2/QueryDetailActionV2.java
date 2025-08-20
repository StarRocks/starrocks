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

package com.starrocks.http.rest.v2;

import com.google.gson.Gson;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Arrays;
import java.util.List;

// This class is a RESTFUL interface to get query profile from all frontend nodes.
// Usage:
//   wget http://fe_host:fe_http_port/api/v2/query_detail?user=root&event_time=1753671088&is_request_all_frontend=true;
public class QueryDetailActionV2 extends RestBaseAction {

    private static final String QUERY_PLAN_URI = "/api/v2/query_detail";

    public QueryDetailActionV2(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, QUERY_PLAN_URI, new QueryDetailActionV2(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String authorization = request.getAuthorizationHeader();
        String eventTimeStr = request.getSingleParameter("event_time");
        String user = request.getSingleParameter("user");
        String isRequestAllStr = request.getSingleParameter("is_request_all_frontend", "false");
        boolean isRequestAll = Boolean.parseBoolean(isRequestAllStr);

        if (eventTimeStr == null || eventTimeStr.isEmpty()) {
            sendErrorResponse(response,
                    "not valid parameter",
                    HttpResponseStatus.BAD_REQUEST,
                    request);
            return;
        }

        long eventTime = Long.parseLong(eventTimeStr.trim());

        List<QueryDetail> queryDetails;
        if (user != null && !user.isEmpty()) {
            // If user is specified, we will filter the query details by user.
            queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(eventTime, user);
        } else {
            // If user is not specified, we will get all query details after the specified event time
            queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(eventTime);
        }

        if (isRequestAll) {
            // If the query profile is not found in the local fe's ProfileManager,
            // we will query other frontend nodes to get the query profile.
            String queryPath = QUERY_PLAN_URI + "?event_time=" + eventTimeStr;
            if (user != null && !user.isEmpty()) {
                queryPath += "&user=" + user;
            }
            List<String> dataList = fetchResultFromOtherFrontendNodes(queryPath, authorization, HttpMethod.GET, true);
            if (dataList != null) {
                for (String data : dataList) {
                    if (data != null) {
                        Gson gson = new Gson();
                        QueryDetail[] queryDetailArray = gson.fromJson(data, QueryDetail[].class);
                        queryDetails.addAll(Arrays.asList(queryDetailArray));
                    }
                }
            }
        }

        Gson gson = new Gson();
        String jsonString = gson.toJson(queryDetails);
        response.getContent().append(jsonString);
        sendResult(request, response);
    }
}
