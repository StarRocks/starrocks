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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.QueryDetailQueue;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.util.List;

public class QueryDetailProfileAction extends RestBaseAction {

    public QueryDetailProfileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/query_detail_profile",
                new QueryDetailProfileAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String jsonStr = request.getContent();

        Gson gson = new Gson();
        ProfileRequest profileRequest;

        try {
            Type type = new TypeToken<ProfileRequest>() {
            }.getType();
            profileRequest = gson.fromJson(jsonStr, type);

            if (profileRequest == null || profileRequest.profileIds == null || profileRequest.profileIds.isEmpty()) {
                response.getContent().append("profileIds cannot be empty");
                sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        } catch (Exception e) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        List<String> profileIds = profileRequest.profileIds;
        List<String> profiles = QueryDetailQueue.getQueryProfilesByQueryIds(profileIds);

        String jsonString = gson.toJson(profiles);
        response.getContent().append(jsonString);
        sendResult(request, response);
    }

    private static class ProfileRequest {
        List<String> profileIds;
    }
}