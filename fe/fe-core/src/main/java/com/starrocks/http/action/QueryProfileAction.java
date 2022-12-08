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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/action/QueryProfileAction.java

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

package com.starrocks.http.action;

import com.google.common.base.Strings;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryProfileAction extends WebBaseAction {

    public QueryProfileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/query_profile", new QueryProfileAction(controller));
    }

    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        String queryId = request.getSingleParameter("query_id");
        if (Strings.isNullOrEmpty(queryId)) {
            response.appendContent("");
            response.appendContent("<p class=\"text-error\"> Must specify a query_id[]</p>");
        }

        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr != null) {
            appendCopyButton(response.getContent());
            appendQueryProfile(response.getContent(), queryProfileStr);
            getPageFooter(response.getContent());
            writeResponse(request, response);
        } else {
            appendQueryProfile(response.getContent(), "query id " + queryId + " not found.");
            getPageFooter(response.getContent());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }

    private void appendQueryProfile(StringBuilder buffer, String queryProfileStr) {
        buffer.append("<pre id='profile'>");
        buffer.append(queryProfileStr);
        buffer.append("</pre>");
    }

    private void appendCopyButton(StringBuilder buffer) {
        buffer.append("<script type=\"text/javascript\">\n" +
                "function copyProfile(){\n" +
                "  v = $('#profile').html()\n" +
                "  const t = document.createElement('textarea')\n" +
                "  t.style.cssText = 'position: absolute;top:0;left:0;opacity:0'\n" +
                "  document.body.appendChild(t)\n" +
                "  t.value = v\n" +
                "  t.select()\n" +
                "  document.execCommand('copy')\n" +
                "  document.body.removeChild(t)\n" +
                "}\n" +
                "</script>");
        buffer.append("<input type=\"button\" onclick=\"copyProfile();\" value=\"Copy Profile\"></input>");
    }
}
