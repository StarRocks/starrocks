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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/action/SystemAction.java

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.LeaderOpExecutor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class SystemAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(SystemAction.class);

    public SystemAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/system", new SystemAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        String currentPath = request.getSingleParameter("path");
        if (Strings.isNullOrEmpty(currentPath)) {
            currentPath = "/";
        }
        appendSystemInfo(response.getContent(), currentPath, currentPath);

        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendSystemInfo(StringBuilder buffer, String procPath, String path) {
        buffer.append("<h2>System Info</h2>");
        buffer.append("<p>This page lists the system info, like /proc in Linux.</p>");
        buffer.append("<p class=\"text-info\"> Current path: " + path + "<a href=\"?path=" + getParentPath(path)
                + "\" class=\"btn btn-primary\" style=\"float: right;\">"
                + "Parent Dir</a></p><br/>");

        ProcNodeInterface procNode = getProcNode(procPath);
        if (procNode == null) {
            buffer.append("<p class=\"text-error\"> No such proc path[" + path + "]</p>");
            return;
        }
        boolean isDir = (procNode instanceof ProcDirInterface);

        List<String> columnNames = null;
        List<List<String>> rows = null;
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            // forward to master
            String showProcStmt = "SHOW PROC \"" + procPath + "\"";
            LeaderOpExecutor leaderOpExecutor = new LeaderOpExecutor(new OriginStatement(showProcStmt, 0),
                    ConnectContext.get(), RedirectStatus.FORWARD_NO_SYNC);
            try {
                leaderOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("Fail to forward. ", e);
                buffer.append("<p class=\"text-error\"> Failed to forward request to master</p>");
                return;
            }

            ShowResultSet resultSet = leaderOpExecutor.getProxyResultSet();
            if (resultSet == null) {
                buffer.append("<p class=\"text-error\"> Failed to get result from master</p>");
                return;
            }

            columnNames = resultSet.getMetaData().getColumns().stream().map(c -> c.getName()).collect(
                    Collectors.toList());
            rows = resultSet.getResultRows();
        } else {
            ProcResult result;
            try {
                result = procNode.fetchResult();
            } catch (AnalysisException e) {
                buffer.append("<p class=\"text-error\"> The result is null, "
                        + "maybe haven't be implemented completely[" + e.getMessage() + "], please check.</p>");
                buffer.append("<p class=\"text-info\"> "
                        + "INFO: ProcNode type is [" + procNode.getClass().getName()
                        + "]</p>");
                return;
            }

            columnNames = result.getColumnNames();
            rows = result.getRows();
        }

        Preconditions.checkNotNull(columnNames);
        Preconditions.checkNotNull(rows);

        appendTableHeader(buffer, columnNames);
        appendSystemTableBody(buffer, rows, isDir, path);
        appendTableFooter(buffer);
    }

    private void appendSystemTableBody(StringBuilder buffer, List<List<String>> rows, boolean isDir, String path) {
        UrlValidator validator = new UrlValidator();
        for (List<String> strList : rows) {
            buffer.append("<tr>");
            int columnIndex = 1;
            for (String str : strList) {
                buffer.append("<td>");
                if (isDir && columnIndex == 1) {
                    String escapeStr = str.replace("%", "%25");
                    buffer.append("<a href=\"?path=" + path + "/" + escapeStr + "\">");
                    buffer.append(str);
                    buffer.append("</a>");
                } else if (validator.isValid(str)) {
                    buffer.append("<a href=\"" + str + "\">");
                    buffer.append("URL");
                    buffer.append("</a>");
                } else {
                    buffer.append(str != null ? str.replaceAll("\\n", "<br/>") : "");
                }
                buffer.append("</td>");
                ++columnIndex;
            }
            buffer.append("</tr>");
        }
    }

    // some expamle:
    //   '/'            => '/'
    //   '///aaa'       => '///'
    //   '/aaa/bbb///'  => '/aaa'
    //   '/aaa/bbb/ccc' => '/aaa/bbb'
    // ATTN: the root path's parent is itself.
    private String getParentPath(String path) {
        int lastSlashIndex = path.length() - 1;
        while (lastSlashIndex > 0) {
            int tempIndex = path.lastIndexOf('/', lastSlashIndex);
            if (tempIndex > 0) {
                if (tempIndex == lastSlashIndex) {
                    lastSlashIndex = tempIndex - 1;
                    continue;
                } else if (tempIndex < lastSlashIndex) { // '//aaa/bbb'
                    lastSlashIndex = tempIndex;
                    return path.substring(0, lastSlashIndex);
                }
            } else {
                // exist the loop
                break;
            }
        }
        return "/";
    }
}
