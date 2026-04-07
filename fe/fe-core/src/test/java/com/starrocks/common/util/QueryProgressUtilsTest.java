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
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.ExplainAnalyzer;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryProgressUtilsTest {
    private static final String QUERY_ID = "123";

    @Test
    public void testGetQueryProgressAndPercent() throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        try {
            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    JsonObject progressInfo = new JsonObject();
                    progressInfo.addProperty("total_operator_num", 5);
                    progressInfo.addProperty("finished_operator_num", 3);
                    progressInfo.addProperty("progress_percent", "60.00%");

                    JsonObject result = new JsonObject();
                    result.addProperty("query_id", QUERY_ID);
                    result.addProperty("state", "Running");
                    result.add("progress_info", progressInfo);
                    return result.toString();
                }
            };

            manager.pushProfile(new ProfilingExecPlan(), createProfile(QUERY_ID, "Query", "Running"));
            String queryProgress = QueryProgressUtils.getQueryProgress(QUERY_ID);

            Assertions.assertTrue(queryProgress.contains("\"progress_percent\":\"60.00%\""));
            Assertions.assertEquals("60.00%", QueryProgressUtils.getProgressPercent(QUERY_ID));
        } finally {
            manager.clearProfiles();
        }
    }

    @Test
    public void testGetQueryProgressNotFound() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        Assertions.assertEquals("query id 123 not found.", QueryProgressUtils.getQueryProgress(QUERY_ID));
        Assertions.assertEquals("", QueryProgressUtils.getProgressPercent(QUERY_ID));
    }

    @Test
    public void testGetQueryProgressShortCircuit() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        try {
            manager.pushProfile(null, createProfile(QUERY_ID, "Query", "Running"));
            Assertions.assertEquals("short circuit point query doesn't suppot get query progress, "
                            + "you can set it off by using set enable_short_circuit=false",
                    QueryProgressUtils.getQueryProgress(QUERY_ID));
            Assertions.assertEquals("", QueryProgressUtils.getProgressPercent(QUERY_ID));
        } finally {
            manager.clearProfiles();
        }
    }

    @Test
    public void testGetProgressPercentHandlesInvalidPayloadAndAnalysisFailure() throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        try {
            manager.pushProfile(new ProfilingExecPlan(), createProfile(QUERY_ID, "Query", "Running"));

            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    return "not-json";
                }
            };
            Assertions.assertEquals("", QueryProgressUtils.getProgressPercent(QUERY_ID));

            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    throw new StarRocksException("mock failure");
                }
            };
            Assertions.assertEquals("Failed to get query progress, query_id:123",
                    QueryProgressUtils.getQueryProgress(QUERY_ID));
            Assertions.assertEquals("", QueryProgressUtils.getProgressPercent(QUERY_ID));
        } finally {
            manager.clearProfiles();
        }
    }

    private RuntimeProfile createProfile(String queryId, String queryType, String queryState) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, queryState);
        profile.addChild(summaryProfile);
        return profile;
    }
}
