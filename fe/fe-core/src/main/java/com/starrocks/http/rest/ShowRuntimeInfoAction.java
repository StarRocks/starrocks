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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ShowRuntimeInfoAction.java

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
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class ShowRuntimeInfoAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ShowRuntimeInfoAction.class);

    public ShowRuntimeInfoAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_runtime_info",
                new ShowRuntimeInfoAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        HashMap<String, String> feInfo = new HashMap<String, String>();

        // Get memory info
        Runtime r = Runtime.getRuntime();
        feInfo.put("free_mem", String.valueOf(r.freeMemory()));
        feInfo.put("total_mem", String.valueOf(r.totalMemory()));
        feInfo.put("max_mem", String.valueOf(r.maxMemory()));

        // Get thread count
        ThreadGroup parentThread = Thread.currentThread().getThreadGroup();
        while (parentThread.getParent() != null) {
            parentThread = parentThread.getParent();
        }

        feInfo.put("thread_cnt", String.valueOf(parentThread.activeCount()));

        Gson gson = new Gson();
        response.setContentType("application/json");
        response.getContent().append(gson.toJson(feInfo));

        sendResult(request, response);
    }
}
