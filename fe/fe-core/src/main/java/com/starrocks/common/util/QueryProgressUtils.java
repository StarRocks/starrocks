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

package com.starrocks.common.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.starrocks.sql.ExplainAnalyzer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class QueryProgressUtils {
    private static final Logger LOG = LogManager.getLogger(QueryProgressUtils.class);

    private QueryProgressUtils() {
    }

    public static String getProgressPercent(String queryId) {
        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        if (profileElement == null) {
            return "";
        }

        String queryProgress = getQueryProgress(queryId, profileElement);
        if (queryProgress == null || queryProgress.isEmpty() || queryProgress.charAt(0) != '{') {
            return "";
        }

        try {
            JsonObject jsonObject = JsonParser.parseString(queryProgress).getAsJsonObject();
            JsonObject progressInfo = jsonObject.getAsJsonObject("progress_info");
            if (progressInfo == null || !progressInfo.has("progress_percent")) {
                return "";
            }
            return progressInfo.get("progress_percent").getAsString();
        } catch (JsonSyntaxException | IllegalStateException e) {
            LOG.warn("failed to parse query progress, query_id: {}, msg: {}", queryId, queryProgress);
            return "";
        }
    }

    public static String getQueryProgress(String queryId) {
        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        if (profileElement == null) {
            return "query id " + queryId + " not found.";
        }
        return getQueryProgress(queryId, profileElement);
    }

    public static String getQueryProgress(String queryId, ProfileManager.ProfileElement profileElement) {
        if (profileElement.plan == null &&
                profileElement.infoStrings.get(ProfileManager.QUERY_TYPE) != null &&
                !profileElement.infoStrings.get(ProfileManager.QUERY_TYPE).equals("Load")) {
            return "short circuit point query doesn't suppot get query progress, "
                    + "you can set it off by using set enable_short_circuit=false";
        }

        try {
            return ExplainAnalyzer.analyze(profileElement.plan, profileElement.getRuntimeProfile());
        } catch (Exception e) {
            String result = "Failed to get query progress, query_id:" + queryId;
            LOG.warn(result, e);
            return result;
        }
    }
}
