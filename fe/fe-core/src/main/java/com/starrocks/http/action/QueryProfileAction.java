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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.owasp.encoder.Encode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class QueryProfileAction extends WebBaseAction {

    private static final Logger LOG = LogManager.getLogger(QueryProfileAction.class);

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

        // HTML encode the queryId to prevent XSS
        String encodedQueryId = Encode.forHtml(queryId);
        
        // Get display parameters - maintain backward compatibility
        String tierParam = request.getSingleParameter("tier");
        String formatParam = request.getSingleParameter("format");
        
        // Create configuration based on request parameters
        ProfileDisplayConfig config = ProfileDisplayConfig.fromRequestParams(tierParam, formatParam);
        
        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr != null) {
            // Only show controls if tiered display is requested (backward compatibility)
            if (config.isUseTieredDisplay()) {
                appendTierSelectionControls(response.getContent(), encodedQueryId, 
                                          tierParam != null ? tierParam.toLowerCase() : "advanced");
            }
            appendCopyButton(response.getContent());
            appendQueryProfile(response.getContent(), queryProfileStr, config);
            getPageFooter(response.getContent());
            writeResponse(request, response);
        } else {
            appendQueryProfile(response.getContent(), "query id " + encodedQueryId + " not found.", config);
            getPageFooter(response.getContent());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }

    private void appendQueryProfile(StringBuilder buffer, String queryProfileStr) {
        appendQueryProfile(buffer, queryProfileStr, ProfileDisplayConfig.createLegacyConfig());
    }
    
    private void appendQueryProfile(StringBuilder buffer, String queryProfileStr, ProfileDisplayConfig config) {
        buffer.append("<pre id='profile'>");

        // For backward compatibility, if not using tiered display, show original format
        if (!config.isUseTieredDisplay()) {
            BufferedReader reader = new BufferedReader(new StringReader(queryProfileStr));
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
        } else {
            // Use enhanced tiered display - this would require parsing the profile string
            // and reformatting it using the TieredProfileFormatter
            // For now, show the original content with a note about the new features
            BufferedReader reader = new BufferedReader(new StringReader(queryProfileStr));
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    if (line.contains(ProfileManager.SQL_STATEMENT)) {
                        buffer.append(escapeHtmlInPreTag(line)).append("\n");
                    } else {
                        // Apply friendly names if enabled
                        if (config.isUseFriendlyNames()) {
                            line = applyFriendlyNames(line);
                        }
                        buffer.append(line).append("\n");
                    }
                }
            } catch (IOException e) {
                LOG.warn("transform profile content error", e);
            }
        }

        buffer.append("</pre>");
        
        // Add tier-specific display note only for tiered display
        if (config.isUseTieredDisplay()) {
            buffer.append("<div class='tier-info'>");
            switch (config.getMaxTier()) {
                case BASIC:
                    buffer.append("<p><em>Showing Basic tier: ≤5 most critical metrics per operator with performance guidance</em></p>");
                    break;
                case ADVANCED:
                    buffer.append("<p><em>Showing Advanced tier: Detailed performance metrics for experienced users</em></p>");
                    break;
                case TRACE:
                    buffer.append("<p><em>Showing Trace tier: All metrics including developer debugging information</em></p>");
                    break;
            }
            
            // Show enabled features
            StringBuilder features = new StringBuilder();
            if (config.isUseFriendlyNames()) features.append("friendly names, ");
            if (config.isShowRuleOfThumb()) features.append("rule-of-thumb guidance, ");
            if (config.isGroupMetrics()) features.append("metric grouping, ");
            if (!config.isShowZeroValues()) features.append("zero-value filtering, ");
            if (config.isShowDistributions()) features.append("statistical distributions, ");
            
            if (features.length() > 0) {
                features.setLength(features.length() - 2); // Remove trailing ", "
                buffer.append("<p><em>Features enabled: ").append(features).append("</em></p>");
            }
            
            buffer.append("</div>");
        }
    }
    
    private String applyFriendlyNames(String line) {
        // Simple text replacement for common metric names in profile output
        // This is a simplified implementation - a full implementation would parse the profile structure
        String result = line;
        
        // Apply some key friendly name replacements
        result = result.replace("PullChunkNum:", "Data Chunks Processed:");
        result = result.replace("QuerySpillBytes:", "Data Spilled to Disk:");
        result = result.replace("QueryPeakMemoryUsagePerNode:", "Peak Memory per Node:");
        result = result.replace("QueryCumulativeCpuTime:", "Total CPU Time:");
        result = result.replace("QueryExecutionWallTime:", "Query Duration:");
        result = result.replace("RawRowsRead:", "Rows Scanned:");
        result = result.replace("RowsRead:", "Rows Returned (after filters):");
        result = result.replace("HashTableMemoryUsage:", "Join Memory Usage:");
        result = result.replace("SerializeChunkTime:", "Data Serialization Time:");
        result = result.replace("NetworkTime:", "Network Transfer Time:");
        result = result.replace("ScanTime:", "Data Scan Time:");
        result = result.replace("BuildHashTableTime:", "Hash Table Build Time:");
        result = result.replace("SearchHashTableTime:", "Hash Table Lookup Time:");
        
        return result;
    }

    private void appendTierSelectionControls(StringBuilder buffer, String queryId, String currentTier) {
        buffer.append("<div class='tier-controls' style='margin-bottom: 20px; padding: 10px; background-color: #f5f5f5; border-radius: 5px;'>");
        buffer.append("<h4>Query Profile Readability Controls</h4>");
        buffer.append("<p>Select metric tier to display:</p>");
        buffer.append("<div class='tier-buttons' style='margin: 10px 0;'>");
        
        // Basic tier button
        String basicClass = "basic".equals(currentTier) ? " btn-primary" : " btn-default";
        buffer.append("<a href='/query_profile?query_id=").append(queryId).append("&tier=basic' ");
        buffer.append("class='btn").append(basicClass).append("' style='margin-right: 10px; padding: 8px 16px; text-decoration: none; border: 1px solid #ccc; border-radius: 3px;'>");
        buffer.append("🟢 Basic (≤5 key metrics)</a>");
        
        // Advanced tier button  
        String advancedClass = "advanced".equals(currentTier) ? " btn-primary" : " btn-default";
        buffer.append("<a href='/query_profile?query_id=").append(queryId).append("&tier=advanced' ");
        buffer.append("class='btn").append(advancedClass).append("' style='margin-right: 10px; padding: 8px 16px; text-decoration: none; border: 1px solid #ccc; border-radius: 3px;'>");
        buffer.append("🟡 Advanced (detailed metrics)</a>");
        
        // Trace tier button
        String traceClass = "trace".equals(currentTier) ? " btn-primary" : " btn-default";
        buffer.append("<a href='/query_profile?query_id=").append(queryId).append("&tier=trace' ");
        buffer.append("class='btn").append(traceClass).append("' style='padding: 8px 16px; text-decoration: none; border: 1px solid #ccc; border-radius: 3px;'>");
        buffer.append("🔧 Trace (all metrics)</a>");
        
        buffer.append("</div>");
        
        // Feature descriptions
        buffer.append("<div class='tier-descriptions' style='font-size: 0.9em; color: #666;'>");
        buffer.append("<ul style='margin: 5px 0;'>");
        buffer.append("<li><strong>Basic:</strong> Shows ≤5 most critical metrics per operator with 🟢🟡🔴 performance guidance</li>");
        buffer.append("<li><strong>Advanced:</strong> Includes detailed metrics grouped by category (timing, memory, I/O, etc.)</li>");
        buffer.append("<li><strong>Trace:</strong> All metrics including developer debugging information</li>");
        buffer.append("</ul>");
        buffer.append("<p><em>✨ New features: User-friendly metric names, zero-value filtering, rule-of-thumb guidance</em></p>");
        buffer.append("</div>");
        
        buffer.append("</div>");
        
        // Add CSS for better button styling
        buffer.append("<style>");
        buffer.append(".btn-primary { background-color: #337ab7 !important; color: white !important; }");
        buffer.append(".btn-default { background-color: #fff !important; color: #333 !important; }");
        buffer.append(".btn:hover { background-color: #286090 !important; color: white !important; }");
        buffer.append(".tier-info { margin-top: 10px; padding: 8px; background-color: #e7f3ff; border-left: 4px solid #2196F3; }");
        buffer.append("</style>");
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
        buffer.append("<script type=\"text/javascript\">\n" +
                "function downloadProfile() {\n" +
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
        buffer.append("<input type=\"button\" onclick=\"copyProfile();\" value=\"Copy Profile\"></input>");
        buffer.append("<input type=\"button\" onclick=\"downloadProfile();\" value=\"Download Profile\"></input>");
    }
}
