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

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.memory.ProcProfileCollector;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ProcProfileCreateAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ProcProfileCreateAction.class);

    public ProcProfileCreateAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/v1/proc_profile/create",
                new ProcProfileCreateAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String profileType = request.getSingleParameter("type");

        if (Strings.isNullOrEmpty(profileType)) {
            response.appendContent(new RestBaseResult("Missing 'type' parameter").toJson());
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        if (!profileType.equalsIgnoreCase("cpu") && !profileType.equalsIgnoreCase("memory")) {
            response.appendContent(new RestBaseResult("Invalid 'type' parameter. Must be 'cpu' or 'memory'").toJson());
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        try {
            // Create a new ProcProfileCollector instance for manual collection
            ProcProfileCollector collector = new ProcProfileCollector();

            // Trigger profile collection asynchronously
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    String fileName;
                    if (profileType.equalsIgnoreCase("cpu")) {
                        fileName = collector.collectCPUProfile();
                    } else {
                        fileName = collector.collectMemProfile();
                    }
                    return fileName;
                } catch (Exception e) {
                    LOG.error("Failed to collect {} profile", profileType, e);
                    throw new RuntimeException("Profile collection failed: " + e.getMessage());
                }
            });

            // Wait for completion with timeout
            String fileName = future.get(Config.proc_profile_collect_time_s + 10, TimeUnit.SECONDS);
            response.appendContent(fileName);
            writeResponse(request, response, HttpResponseStatus.OK);

        } catch (Exception e) {
            LOG.error("Error creating {} profile", profileType, e);
            response.appendContent(new RestBaseResult("Failed to create profile: " + e.getMessage()).toJson());
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
