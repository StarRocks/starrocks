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

package com.starrocks.qe;

import com.google.gson.JsonObject;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.sql.ExplainAnalyzer;
import com.starrocks.thrift.TQueryStatisticsInfo;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.starrocks.common.proc.CurrentGlobalQueryStatisticsProcDirTest.QUERY_ONE_LOCAL;

public class QueryStatisticsInfoTest {
    QueryStatisticsInfo firstQuery = QUERY_ONE_LOCAL;

    @Test
    public void testEquality() {
        QueryStatisticsInfo otherQuery = new QueryStatisticsInfo(
                firstQuery.getQueryStartTime(),
                firstQuery.getFeIp(),
                firstQuery.getQueryId(),
                firstQuery.getConnId(),
                firstQuery.getDb(),
                firstQuery.getUser(),
                firstQuery.getCpuCostNs(),
                firstQuery.getScanBytes(),
                firstQuery.getScanRows(),
                firstQuery.getMemUsageBytes(),
                firstQuery.getSpillBytes(),
                firstQuery.getExecTime(),
                firstQuery.getExecProgress(),
                firstQuery.getExecState(),
                firstQuery.getWareHouseName(),
                firstQuery.getCustomQueryId(),
                firstQuery.getResourceGroupName()
        );
        Assertions.assertEquals(firstQuery, otherQuery);
        Assertions.assertEquals(firstQuery.hashCode(), otherQuery.hashCode());
    }

    @Test
    public void testThrift() {
        TQueryStatisticsInfo firstQueryThrift = firstQuery.toThrift();
        QueryStatisticsInfo firstQueryTest = QueryStatisticsInfo.fromThrift(firstQueryThrift);
        Assertions.assertEquals(firstQuery, firstQueryTest);
    }

    @Test
    public void testGetExecProgress() throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, "123");
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Running");
        profile.addChild(summaryProfile);

        try {
            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    JsonObject progressInfo = new JsonObject();
                    progressInfo.addProperty("total_operator_num", 5);
                    progressInfo.addProperty("finished_operator_num", 3);
                    progressInfo.addProperty("progress_percent", "60.00%");

                    JsonObject result = new JsonObject();
                    result.addProperty("query_id", "123");
                    result.addProperty("state", "Running");
                    result.add("progress_info", progressInfo);
                    return result.toString();
                }
            };

            manager.pushProfile(new ProfilingExecPlan(), profile);
            Assertions.assertEquals("60.00%", QueryStatisticsInfo.getExecProgress("123"));

            manager.clearProfiles();
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));

            manager.pushProfile(null, profile);
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));

            manager.clearProfiles();
            manager.pushProfile(new ProfilingExecPlan(), profile);
            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    throw new StarRocksException("mock failure");
                }
            };
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));
        } finally {
            manager.clearProfiles();
        }
    }
}
