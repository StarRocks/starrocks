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

package com.starrocks.http.action;

import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpUtils;
import com.starrocks.http.IllegalArgException;
import com.starrocks.memory.ProcProfileCollector;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcProfileCollectAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(ProcProfileCollectAction.class);
    private static final SimpleDateFormat PROFILE_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

    public ProcProfileCollectAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/proc_profile/collect", new ProcProfileCollectAction(controller));
    }

    @Override
    public void executePost(BaseRequest request, BaseResponse response) {
        String nodeParam = request.getSingleParameter("node");
        if (nodeParam == null || nodeParam.isEmpty()) {
            nodeParam = "FE";
        }

        String secondsStr = request.getSingleParameter("seconds");
        long seconds = 10; // default 10 seconds
        if (!StringUtils.isEmpty(secondsStr)) {
            try {
                seconds = Long.parseLong(secondsStr);
                if (seconds <= 0 || seconds > 3600) {
                    seconds = 10; // reset to default if invalid
                }
            } catch (NumberFormatException e) {
                LOG.warn("Invalid seconds parameter: {}", secondsStr);
            }
        }

        String typeParam = request.getSingleParameter("type");
        String profileType = "both"; // default to both
        if (!StringUtils.isEmpty(typeParam)) {
            if ("cpu".equalsIgnoreCase(typeParam) || "mem".equalsIgnoreCase(typeParam) || "both".equalsIgnoreCase(typeParam)) {
                profileType = typeParam.toLowerCase();
            }
        }

        JsonObject result = new JsonObject();

        if ("FE".equals(nodeParam)) {
            // Collect profile on FE
            try {
                collectFEProfile(seconds, profileType);
                result.addProperty("status", "success");
                result.addProperty("message", "Profile collection started for " + seconds + " seconds");
                result.addProperty("node", "FE");
                result.addProperty("type", profileType);
            } catch (Exception e) {
                LOG.error("Failed to collect FE profile", e);
                result.addProperty("status", "error");
                result.addProperty("message", "Failed to collect profile: " + e.getMessage());
            }
        } else if (nodeParam.startsWith("BE:")) {
            // Collect profile on BE
            try {
                String beResult = collectBEProfile(nodeParam, seconds, profileType);
                result.addProperty("status", "success");
                result.addProperty("message", beResult);
                result.addProperty("node", nodeParam);
                result.addProperty("type", profileType);
            } catch (Exception e) {
                LOG.error("Failed to collect BE profile", e);
                result.addProperty("status", "error");
                result.addProperty("message", "Failed to collect profile: " + e.getMessage());
            }
        } else {
            result.addProperty("status", "error");
            result.addProperty("message", "Invalid node parameter: " + nodeParam);
        }

        response.setContentType("application/json");
        response.appendContent(result.toString());
        writeResponse(request, response);
    }

    private void collectFEProfile(long seconds, String profileType) throws Exception {
        ProcProfileCollector collector = GlobalStateMgr.getCurrentState().getProcProfileCollector();
        if (collector == null) {
            throw new RuntimeException("ProcProfileCollector is not initialized");
        }

        // Use AsyncProfiler directly to collect profile for specified duration
        one.profiler.AsyncProfiler profiler = one.profiler.AsyncProfiler.getInstance();
        String profileLogDir = collector.getProfileLogDir();
        String timestamp = PROFILE_TIME_FORMAT.format(new Date(System.currentTimeMillis()));

        if ("cpu".equals(profileType) || "both".equals(profileType)) {
            String cpuFileName = "cpu-profile-" + timestamp + ".html";
            try {
                profiler.execute(String.format("start,quiet,event=cpu,cstack=vm,jstackdepth=%d,file=%s",
                        Config.proc_profile_jstack_depth, profileLogDir + "/" + cpuFileName));
                Thread.sleep(seconds * 1000L);
                profiler.execute(String.format("stop,file=%s", profileLogDir + "/" + cpuFileName));
                collector.compressFile(cpuFileName);
                LOG.info("Collected CPU profile for {} seconds: {}", seconds, cpuFileName);
            } catch (Exception e) {
                LOG.error("Failed to collect CPU profile", e);
                throw e;
            }
        }

        if ("mem".equals(profileType) || "both".equals(profileType)) {
            String memFileName = "mem-profile-" + timestamp + ".html";
            try {
                profiler.execute(String.format("start,quiet,event=alloc,alloc=2m,cstack=vm,jstackdepth=%d,file=%s",
                        Config.proc_profile_jstack_depth, profileLogDir + "/" + memFileName));
                Thread.sleep(seconds * 1000L);
                profiler.execute(String.format("stop,file=%s", profileLogDir + "/" + memFileName));
                collector.compressFile(memFileName);
                LOG.info("Collected memory profile for {} seconds: {}", seconds, memFileName);
            } catch (Exception e) {
                LOG.error("Failed to collect memory profile", e);
                throw e;
            }
        }
    }

    private String collectBEProfile(String beNodeId, long seconds, String profileType) throws Exception {
        // Parse BE node: "BE:host:port"
        String[] parts = beNodeId.substring(3).split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid BE node format: " + beNodeId);
        }

        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid BE port: " + parts[1]);
        }

        StringBuilder result = new StringBuilder();
        
        // Use the new BE API endpoint for collecting profiles
        String collectUrl = "http://" + host + ":" + port + "/api/proc_profile?action=collect";
        try {
            // Build POST request with form parameters
            String typeParam = profileType.equals("mem") ? "contention" : profileType;
            String formData = "seconds=" + java.net.URLEncoder.encode(String.valueOf(seconds), "UTF-8") +
                    "&type=" + java.net.URLEncoder.encode(typeParam, "UTF-8");
            
            org.apache.http.entity.StringEntity entity = new org.apache.http.entity.StringEntity(
                    formData, org.apache.http.entity.ContentType.APPLICATION_FORM_URLENCODED);
            
            String response = HttpUtils.post(collectUrl, entity, null);
            // Parse JSON response
            com.google.gson.JsonObject jsonResponse = com.google.gson.JsonParser.parseString(response).getAsJsonObject();
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").getAsString())) {
                result.append(jsonResponse.get("message").getAsString());
            } else {
                String errorMsg = jsonResponse.has("message") ? jsonResponse.get("message").getAsString() : "Unknown error";
                throw new RuntimeException("BE profile collection failed: " + errorMsg);
            }
        } catch (Exception e) {
            LOG.error("Failed to collect profile from BE {}:{}", host, port, e);
            throw new RuntimeException("Failed to collect profile: " + e.getMessage(), e);
        }

        return result.toString().trim();
    }
}
