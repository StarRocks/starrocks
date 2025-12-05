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

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpUtils;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ProcProfileAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(ProcProfileAction.class);

    private static final String CPU_FILE_NAME_PREFIX = "cpu-profile-";
    private static final String MEM_FILE_NAME_PREFIX = "mem-profile-";
    private static final SimpleDateFormat PROFILE_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

    public ProcProfileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/proc_profile", new ProcProfileAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        // Add proc profile tabs CSS link in the head section
        // Insert before the closing </head> tag by replacing the body tag opening
        String content = response.getContent().toString();
        int headEndIndex = content.lastIndexOf("</head>");
        if (headEndIndex > 0) {
            response.getContent().setLength(0);
            response.getContent().append(content.substring(0, headEndIndex));
            response.getContent()
                    .append("  <link href=\"/static/css?res=proc-profile-tabs.css\" ")
                    .append("rel=\"stylesheet\" media=\"screen\"/>\n");
            response.getContent().append(content.substring(headEndIndex));
        }

        String nodeParam = request.getSingleParameter("node");
        if (nodeParam == null || nodeParam.isEmpty()) {
            nodeParam = "FE";
        }

        addTabNavigation(response.getContent(), nodeParam);
        addProfileListInfo(response.getContent(), nodeParam);

        getPageFooter(response.getContent());

        writeResponse(request, response);
    }

    private void addTabNavigation(StringBuilder buffer, String selectedNode) {
        buffer.append("<ul class=\"nav nav-tabs\" style=\"margin-bottom: 20px;\">");

        // FE tab
        String feClass = "FE".equals(selectedNode) ? "active" : "";
        buffer.append("<li class=\"").append(feClass).append("\">");
        buffer.append("<a href=\"/proc_profile?node=FE\">FE</a>");
        buffer.append("</li>");

        // BE tabs
        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        for (Backend backend : backendMap.values()) {
            if (!backend.isAlive()) {
                continue;
            }
            String beNodeId = "BE:" + backend.getHost() + ":" + backend.getHttpPort();
            String beClass = beNodeId.equals(selectedNode) ? "active" : "";
            buffer.append("<li class=\"").append(beClass).append("\">");
            buffer.append("<a href=\"/proc_profile?node=").append(beNodeId).append("\">");
            buffer.append("BE: ").append(backend.getHost()).append(":").append(backend.getHttpPort());
            buffer.append("</a>");
            buffer.append("</li>");
        }

        buffer.append("</ul>");
    }

    private void addProfileListInfo(StringBuilder buffer, String nodeParam) {
        buffer.append("<h2>Process Profiles</h2>");
        buffer.append("<p>This table lists all available CPU and memory profile files</p>");

        List<ProfileFileInfo> profileFiles;
        if ("FE".equals(nodeParam)) {
            profileFiles = getProfileFiles();
        } else if (nodeParam.startsWith("BE:")) {
            profileFiles = getProfileFilesFromBE(nodeParam);
        } else {
            buffer.append("<p>Invalid node parameter: ").append(nodeParam).append("</p>");
            return;
        }

        if (profileFiles.isEmpty()) {
            if ("FE".equals(nodeParam)) {
                buffer.append("<p>No profile files found in directory: ")
                        .append(Config.sys_log_dir)
                        .append("/proc_profile</p>");
            } else {
                buffer.append("<p>No profile files found or node unavailable</p>");
            }
            return;
        }

        appendProfileTableHeader(buffer);
        appendProfileTableBody(buffer, profileFiles, nodeParam);
        appendTableFooter(buffer);
    }

    private List<ProfileFileInfo> getProfileFiles() {
        List<ProfileFileInfo> profileFiles = new ArrayList<>();
        String profileLogDir = Config.sys_log_dir + "/proc_profile";
        File dir = new File(profileLogDir);

        if (!dir.exists() || !dir.isDirectory()) {
            LOG.warn("Profile directory does not exist: {}", profileLogDir);
            return profileFiles;
        }

        File[] files = dir.listFiles();
        assert files != null;
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".tar.gz")) {
                String fileName = file.getName();
                String profileType = getProfileType(fileName);
                if (profileType != null) {
                    String timePart = getTimePart(fileName);
                    if (timePart != null) {
                        try {
                            Date timestamp = PROFILE_TIME_FORMAT.parse(timePart);
                            profileFiles.add(new ProfileFileInfo(
                                    profileType,
                                    timestamp,
                                    file.length(),
                                    fileName
                            ));
                        } catch (Exception e) {
                            LOG.warn("Failed to parse timestamp from filename: {}", fileName, e);
                        }
                    }
                }
            }
        }

        // Sort by timestamp descending (newest first)
        profileFiles.sort((f1, f2) -> f2.timestamp.compareTo(f1.timestamp));

        return profileFiles;
    }

    private String getProfileType(String fileName) {
        if (fileName.startsWith(CPU_FILE_NAME_PREFIX)) {
            return "CPU";
        } else if (fileName.startsWith(MEM_FILE_NAME_PREFIX)) {
            return "Memory";
        }
        return null;
    }

    private String getTimePart(String fileName) {
        String prefix = fileName.startsWith(CPU_FILE_NAME_PREFIX) ? CPU_FILE_NAME_PREFIX : MEM_FILE_NAME_PREFIX;
        int startIndex = prefix.length();
        int endIndex = fileName.indexOf(".html.tar.gz");
        if (endIndex > startIndex) {
            return fileName.substring(startIndex, endIndex);
        }
        return null;
    }

    private void appendProfileTableHeader(StringBuilder buffer) {
        buffer.append(
                "<table id=\"table_id\" class=\"table table-hover table-bordered table-striped table-condensed\">");
        buffer.append("<thead><tr>");
        buffer.append("<th>Type</th>");
        buffer.append("<th>Timestamp</th>");
        buffer.append("<th>File Size</th>");
        buffer.append("<th>View</th>");
        buffer.append("</tr></thead>");
    }

    private List<ProfileFileInfo> getProfileFilesFromBE(String beNodeId) {
        List<ProfileFileInfo> profileFiles = new ArrayList<>();

        // Parse BE node: "BE:host:port"
        String[] parts = beNodeId.substring(3).split(":");
        if (parts.length != 2) {
            LOG.warn("Invalid BE node format: {}", beNodeId);
            return profileFiles;
        }

        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid BE port: {}", parts[1]);
            return profileFiles;
        }

        try {
            String url = "http://" + host + ":" + port + "/api/proc_profile/list";
            String jsonResponse = HttpUtils.get(url, null);

            JsonElement jsonElement = JsonParser.parseString(jsonResponse);
            JsonObject jsonObject = jsonElement.getAsJsonObject();

            if (jsonObject.has("error")) {
                LOG.warn("Error from BE {}:{}: {}", host, port, jsonObject.get("error").getAsString());
                return profileFiles;
            }

            JsonArray profilesArray = jsonObject.getAsJsonArray("profiles");
            if (profilesArray == null) {
                return profileFiles;
            }

            SimpleDateFormat parseFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");

            for (JsonElement element : profilesArray) {
                JsonObject profileObj = element.getAsJsonObject();
                String filename = profileObj.get("filename").getAsString();
                String type = profileObj.get("type").getAsString();
                String timestampStr = profileObj.get("timestamp").getAsString();
                long size = profileObj.get("size").getAsLong();

                try {
                    Date timestamp = parseFormat.parse(timestampStr);
                    profileFiles.add(new ProfileFileInfo(type, timestamp, size, filename));
                } catch (Exception e) {
                    LOG.warn("Failed to parse timestamp: {}", timestampStr, e);
                }
            }

            // Sort by timestamp descending (newest first)
            profileFiles.sort((f1, f2) -> f2.timestamp.compareTo(f1.timestamp));

        } catch (Exception e) {
            LOG.warn("Failed to fetch proc profiles from BE {}:{}", host, port, e);
        }

        return profileFiles;
    }

    private void appendProfileTableBody(StringBuilder buffer, List<ProfileFileInfo> profileFiles, String nodeParam) {
        buffer.append("<tbody>");
        SimpleDateFormat displayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (ProfileFileInfo profile : profileFiles) {
            buffer.append("<tr>");
            buffer.append("<td>").append(profile.type).append("</td>");
            buffer.append("<td>").append(displayFormat.format(profile.timestamp)).append("</td>");
            buffer.append("<td>").append(DebugUtil.getPrettyStringBytes(profile.fileSize)).append("</td>");
            buffer.append("<td>");
            if ("FE".equals(nodeParam)) {
                buffer.append("<a href=\"/proc_profile/file?filename=")
                        .append(profile.fileName)
                        .append("\" target=\"_blank\">View</a>");
            } else {
                buffer.append("<a href=\"/proc_profile/file?node=")
                        .append(nodeParam)
                        .append("&filename=")
                        .append(profile.fileName)
                        .append("\" target=\"_blank\">View</a>");
            }
            buffer.append("</td>");
            buffer.append("</tr>");
        }
        buffer.append("</tbody>");
    }

    private static class ProfileFileInfo {
        final String type;
        final Date timestamp;
        final long fileSize;
        final String fileName;

        ProfileFileInfo(String type, Date timestamp, long fileSize, String fileName) {
            this.type = type;
            this.timestamp = timestamp;
            this.fileSize = fileSize;
            this.fileName = fileName;
        }
    }
}