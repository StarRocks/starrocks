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

import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
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

        addProfileListInfo(response.getContent());

        getPageFooter(response.getContent());

        writeResponse(request, response);
    }

    private void addProfileListInfo(StringBuilder buffer) {
        buffer.append("<h2>Process Profiles</h2>");
        buffer.append("<p>This table lists all available CPU and memory profile files</p>");

        List<ProfileFileInfo> profileFiles = getProfileFiles();

        if (profileFiles.isEmpty()) {
            buffer.append("<p>No profile files found in directory: ").append(Config.sys_log_dir)
                    .append("/proc_profile</p>");
            return;
        }

        appendProfileTableHeader(buffer);
        appendProfileTableBody(buffer, profileFiles);
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

    private void appendProfileTableBody(StringBuilder buffer, List<ProfileFileInfo> profileFiles) {
        buffer.append("<tbody>");
        SimpleDateFormat displayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (ProfileFileInfo profile : profileFiles) {
            buffer.append("<tr>");
            buffer.append("<td>").append(profile.type).append("</td>");
            buffer.append("<td>").append(displayFormat.format(profile.timestamp)).append("</td>");
            buffer.append("<td>").append(DebugUtil.getPrettyStringBytes(profile.fileSize)).append("</td>");
            buffer.append("<td>");
            buffer.append("<a href=\"/proc_profile/file?filename=")
                    .append(profile.fileName)
                    .append("\" target=\"_blank\">View</a>");
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