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

import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfileParser;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.sql.ExplainAnalyzer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// This class is a RESTFUL interface to get query progress.
// It will be used in query monitor.
// Usage:
//   http://fe_host:fe_http_port/api/query/progress?query_id=123456
public class QueryProgressAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(QueryProgressAction.class);

    public QueryProgressAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/query/progress", new QueryProgressAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String queryId = request.getSingleParameter("query_id");
        if (queryId == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        if (profileElement != null) {
            String result = "";
            //For short circuit query, 'ProfileElement#plan' is null
            if (profileElement.plan == null &&
                    profileElement.infoStrings.get(ProfileManager.QUERY_TYPE) != null &&
                    !profileElement.infoStrings.get(ProfileManager.QUERY_TYPE).equals("Load")) {
                result = "short circuit point query doesn't suppot get query progress, " +
                        "you can set it off by using set enable_short_circuit=false";
            } else {
                try {
                    result = ExplainAnalyzer.analyze(profileElement.plan,
                            RuntimeProfileParser.parseFrom(
                                    CompressionUtils.gzipDecompressString(profileElement.profileContent)));
                } catch (Exception e) {
                    result = "Failed to get query progress, query_id:" + queryId;
                    LOG.warn(result, e);
                }
            }
            response.getContent().append(result);
            sendResult(request, response);
        } else {
            response.getContent().append("query id " + queryId + " not found.");
            sendResult(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }
}
