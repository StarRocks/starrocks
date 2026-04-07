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

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.memory.MemoryStat;
import com.starrocks.memory.MemoryUsageTracker;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

public class MemoryUsageAction extends RestBaseAction {

    public MemoryUsageAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/memory_usage",
                new MemoryUsageAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (!"127.0.0.1".equals(request.getHostString())) {
            response.appendContent("only allow access from 127.0.0.1");
            writeResponse(request, response, HttpResponseStatus.FORBIDDEN);
            return;
        }

        Map<String, Object> result = Maps.newLinkedHashMap();

        result.put("jvm", MemoryUsageTracker.getJVMMemoryMap());

        Map<String, Map<String, MemoryStat>> memoryUsage = MemoryUsageTracker.collectMemoryUsage();

        Map<String, Object> modules = Maps.newLinkedHashMap();
        for (Map.Entry<String, Map<String, MemoryStat>> moduleEntry : memoryUsage.entrySet()) {
            Map<String, Object> classMap = Maps.newLinkedHashMap();
            for (Map.Entry<String, MemoryStat> classEntry : moduleEntry.getValue().entrySet()) {
                MemoryStat stat = classEntry.getValue();
                Map<String, Object> statMap = Maps.newLinkedHashMap();
                statMap.put("current_consumption", stat.getCurrentConsumption());
                statMap.put("peak_consumption", stat.getPeakConsumption());
                statMap.put("counter", stat.getCounterMap());
                classMap.put(classEntry.getKey(), statMap);
            }
            modules.put(moduleEntry.getKey(), classMap);
        }
        result.put("modules", modules);

        Gson gson = new Gson();
        response.setContentType("application/json");
        response.getContent().append(gson.toJson(result));

        sendResult(request, response);
    }
}
