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

import com.github.vertical_blank.sqlformatter.SqlFormatter;
import com.google.common.base.Strings;
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
import org.owasp.encoder.Encode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

import static com.github.vertical_blank.sqlformatter.languages.Dialect.MySql;
import static com.github.vertical_blank.sqlformatter.languages.Dialect.StandardSql;

public class QueryProfileAction extends WebBaseAction {

    private static final Logger LOG = LogManager.getLogger(QueryProfileAction.class);
    private static final String ANSI_REGEX = "\\u001B\\[[;?0-9]*[a-zA-Z]";
    private static final SqlFormatter.Formatter MYSQL_FORMATTER = SqlFormatter.of(MySql);
    private static final SqlFormatter.Formatter STANDARDSQL_FORMATTER = SqlFormatter.of(StandardSql);

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
            getPageFooter(response.getContent());
            writeResponse(request, response);
            return;
        }

        ProfileManager.ProfileElement queryProfile = ProfileManager.getInstance().getProfileElement(queryId);
        if (queryProfile != null) {
            String content;
            String contentType = request.getSingleParameter("content_type");
            if ("sql".equalsIgnoreCase(contentType)) {
                content = getFormattedSql(queryProfile);
            } else if ("analyze".equalsIgnoreCase(contentType)) {
                content = getAnalyzeProfileResult(queryProfile);
            } else {
                // Profile String
                content = ProfileManager.getInstance().getProfile(queryId);
                if (content == null) {
                    content = String.format("Failed to decompress profile content of %s\n", queryId);
                }
            }
            appendButtons(response.getContent());
            appendContent(response.getContent(), content);
            getPageFooter(response.getContent());
            writeResponse(request, response);
        } else {
            // HTML encode the queryId to prevent XSS
            String encodedQueryId = Encode.forHtml(queryId);
            appendContent(response.getContent(), "query id " + encodedQueryId + " not found.");
            getPageFooter(response.getContent());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }

    // com.github.vertical_blank.sqlformatter provides best-effort formatting. It doesn't  throw exceptions for malformed or
    // invalid SQL syntax.
    private String getFormattedSql(ProfileManager.ProfileElement queryProfile) {
        String sqlStatement = queryProfile.infoStrings.getOrDefault(ProfileManager.SQL_STATEMENT, "");
        String formattedSql;
        if ("StarRocks".equalsIgnoreCase(queryProfile.infoStrings.get(ProfileManager.SQL_DIALECT))) {
            formattedSql = MYSQL_FORMATTER.format(sqlStatement);
        } else {
            formattedSql = STANDARDSQL_FORMATTER.format(sqlStatement);
        }
        return formattedSql;
    }

    private String getAnalyzeProfileResult(ProfileManager.ProfileElement queryProfile) {
        String analysis;
        try {
            analysis = ExplainAnalyzer.analyze(queryProfile.plan, RuntimeProfileParser.parseFrom(
                    CompressionUtils.gzipDecompressString(queryProfile.profileContent)), Collections.emptyList(), false);
            analysis = analysis.replaceAll(ANSI_REGEX, "");
        } catch (Exception e) {
            String queryId = queryProfile.infoStrings.getOrDefault(ProfileManager.QUERY_ID, "unknown");
            LOG.warn("Failed to analyze profile of {}", queryId, e);
            analysis = String.format("Failed to analyze profile of %s\n%s\n", queryId, e);
        }
        return analysis;
    }

    private void appendContent(StringBuilder buffer, String content) {
        buffer.append("<pre id='profile'>");

        BufferedReader reader = new BufferedReader(new StringReader(content));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.contains(ProfileManager.SQL_STATEMENT)) {
                    buffer.append(escapeHtmlInPreTag(line)).append("\n");
                } else {
                    buffer.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            LOG.warn("transform profile content error", e);
        }

        buffer.append("</pre>");
    }

    private void appendButtons(StringBuilder buffer) {
        buffer.append("<script type=\"text/javascript\">\n" +
                "function viewProfile() {\n" +
                "  const params = new URLSearchParams(window.location.search);\n" +
                "  const query_id = params.get('query_id');\n" +
                "  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=profile';\n" +
                "}" + "</script>");
        buffer.append("<script type=\"text/javascript\">\n" +
                "function formattedSql() {\n" +
                "  const params = new URLSearchParams(window.location.search);\n" +
                "  const query_id = params.get('query_id');\n" +
                "  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=sql';\n" +
                "}" + "</script>");
        buffer.append("<script type=\"text/javascript\">\n" +
                "function analyzeProfile() {\n" +
                "  const params = new URLSearchParams(window.location.search);\n" +
                "  const query_id = params.get('query_id');\n" +
                "  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=analyze';\n" +
                "}" + "</script>");
        buffer.append("<script type=\"text/javascript\">\n" +
                "function copy(){\n" +
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
        buffer.append("<script type=\"text/javascript\">\n" +
                "function download() {\n" +
                "  content = $('#profile').html()\n" +
                "  const file = new Blob([content], { type: \"text/plain\" });\n" +
                "  const params = new URLSearchParams(window.location.search);\n" +
                "  const query_id = params.get('query_id');\n" +
                "  const a = document.createElement('a');\n" +
                "  a.href = URL.createObjectURL(file);\n" +
                "  a.download = query_id + \"profile.txt\";\n" +
                "  a.click();\n" +
                "\n" +
                "  URL.revokeObjectURL(a.href);\n" +
                "}" + "</script>");
        buffer.append("<input type=\"button\" onclick=\"viewProfile();\" value=\"View Profile\"></input>");
        buffer.append("<input type=\"button\" onclick=\"formattedSql();\" value=\"Formatted SQL\"></input>");
        buffer.append("<input type=\"button\" onclick=\"analyzeProfile();\" value=\"Analyze Profile\"></input>");
        buffer.append("<input type=\"button\" onclick=\"copy();\" value=\"Copy\"></input>");
        buffer.append("<input type=\"button\" onclick=\"download();\" value=\"Download\"></input>");
    }
}
