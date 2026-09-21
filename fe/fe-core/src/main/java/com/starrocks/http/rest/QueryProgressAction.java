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

package com.starrocks.http.rest;

import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.QueryProgressUtils;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

// This class is a RESTFUL interface to get query progress.
// It will be used in query monitor.
// Usage:
//   http://fe_host:fe_http_port/api/query/progress?query_id=123456
public class QueryProgressAction extends RestBaseAction {

    public QueryProgressAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/query/progress", new QueryProgressAction(controller));
    }

<<<<<<< HEAD
    @Override
    public void execute(BaseRequest request, BaseResponse response) {
=======
    // Historically anonymous; gated for backward compatibility until enable_http_auth flips on. The profile
    // access check needs a caller identity, so turning it on requires authentication here as well -- an
    // anonymous poller of this endpoint starts getting 401 the moment an operator enables that check, which is
    // called out in the config docs as an upgrade step.
    @Override
    public boolean needAuth() {
        return Config.enable_http_auth || Config.authorization_enable_query_profile_access_check;
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws AccessDeniedException {
        requireOperateIfHttpAuthEnabled();

>>>>>>> d0bbc92 ([BugFix] Add RBAC check for reading query profiles (#79375))
        String queryId = request.getSingleParameter("query_id");
        if (queryId == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        if (profileElement != null) {
            // The progress view is ANALYZE PROFILE's output for the query, so it follows the same access rule.
            Authorizer.checkQueryProfileAccess(ConnectContext.get(), profileElement);
            response.getContent().append(QueryProgressUtils.getQueryProgress(queryId, profileElement));
            sendResult(request, response);
        } else {
            response.getContent().append("query id " + queryId + " not found.");
            sendResult(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }
}
