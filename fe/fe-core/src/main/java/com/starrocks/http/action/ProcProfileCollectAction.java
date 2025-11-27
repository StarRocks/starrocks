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
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpUtils;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcProfileCollectAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(ProcProfileCollectAction.class);

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
            JsonObject result = new JsonObject();
            result.addProperty("status", "error");
            result.addProperty("message", "Missing node parameter. Please specify a BE node.");
            response.setContentType("application/json");
            response.appendContent(result.toString());
            writeResponse(request, response);
            return;
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
            // For BE, we use "cpu", "contention", or "both" (mem maps to contention)
            if ("cpu".equalsIgnoreCase(typeParam) || "contention".equalsIgnoreCase(typeParam) || 
                "both".equalsIgnoreCase(typeParam) || "mem".equalsIgnoreCase(typeParam)) {
                profileType = typeParam.toLowerCase();
                // Map "mem" to "contention" for BE
                if ("mem".equals(profileType)) {
                    profileType = "contention";
                }
            }
        }

        JsonObject result = new JsonObject();

        if ("FE".equals(nodeParam)) {
            // FE profiling is not supported - only BE profiling is available
            result.addProperty("status", "error");
            result.addProperty("message", "FE profiling is not supported. Please use BE profiling instead.");
        } else if (nodeParam != null && nodeParam.startsWith("BE:")) {
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
        java.text.SimpleDateFormat timestampFormat = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss");
        String timestamp = timestampFormat.format(new Date(System.currentTimeMillis()));
        
        // Collect CPU profile using existing /pprof/profile endpoint
        if ("cpu".equals(profileType) || "both".equals(profileType)) {
            try {
                String pprofUrl = "http://" + host + ":" + port + "/pprof/profile?seconds=" + seconds;
                String profileData = HttpUtils.get(pprofUrl, null);
                
                // Compress the profile data
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                try (java.util.zip.GZIPOutputStream gzos = new java.util.zip.GZIPOutputStream(baos)) {
                    gzos.write(profileData.getBytes("UTF-8"));
                }
                byte[] compressedData = baos.toByteArray();
                
                // Save to BE's proc_profile directory
                String filename = "cpu-profile-" + timestamp + "-pprof.gz";
                String saveUrl = "http://" + host + ":" + port + "/api/proc_profile?action=save&filename=" + 
                        java.net.URLEncoder.encode(filename, "UTF-8");
                
                org.apache.http.entity.ByteArrayEntity entity = new org.apache.http.entity.ByteArrayEntity(
                        compressedData, org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM);
                
                String saveResponse = HttpUtils.post(saveUrl, entity, null);
                com.google.gson.JsonObject saveJson = com.google.gson.JsonParser.parseString(saveResponse).getAsJsonObject();
                if (saveJson.has("status") && "success".equals(saveJson.get("status").getAsString())) {
                    result.append("CPU profile collected and saved. ");
                } else {
                    throw new RuntimeException("Failed to save CPU profile: " + 
                            (saveJson.has("message") ? saveJson.get("message").getAsString() : "Unknown error"));
                }
            } catch (Exception e) {
                LOG.error("Failed to collect CPU profile from BE {}:{}", host, port, e);
                throw new RuntimeException("Failed to collect CPU profile: " + e.getMessage(), e);
            }
        }

        // Collect contention profile
        if ("contention".equals(profileType) || "both".equals(profileType)) {
            try {
                String contentionUrl = "http://" + host + ":" + port + "/pprof/contention?seconds=" + seconds;
                String profileData = HttpUtils.get(contentionUrl, null);
                
                // Compress the profile data
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                try (java.util.zip.GZIPOutputStream gzos = new java.util.zip.GZIPOutputStream(baos)) {
                    gzos.write(profileData.getBytes("UTF-8"));
                }
                byte[] compressedData = baos.toByteArray();
                
                // Save to BE's proc_profile directory
                String filename = "contention-profile-" + timestamp + "-pprof.gz";
                String saveUrl = "http://" + host + ":" + port + "/api/proc_profile?action=save&filename=" + 
                        java.net.URLEncoder.encode(filename, "UTF-8");
                
                org.apache.http.entity.ByteArrayEntity entity = new org.apache.http.entity.ByteArrayEntity(
                        compressedData, org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM);
                
                String saveResponse = HttpUtils.post(saveUrl, entity, null);
                com.google.gson.JsonObject saveJson = com.google.gson.JsonParser.parseString(saveResponse).getAsJsonObject();
                if (saveJson.has("status") && "success".equals(saveJson.get("status").getAsString())) {
                    result.append("Contention profile collected and saved. ");
                } else {
                    throw new RuntimeException("Failed to save contention profile: " + 
                            (saveJson.has("message") ? saveJson.get("message").getAsString() : "Unknown error"));
                }
            } catch (Exception e) {
                LOG.error("Failed to collect contention profile from BE {}:{}", host, port, e);
                // Don't throw for contention if it's not critical
                result.append("Contention profile collection failed: ").append(e.getMessage()).append(". ");
            }
        }

        return result.toString().trim();
    }
}
