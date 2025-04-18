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

package com.starrocks.http;

import com.google.gson.JsonObject;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.sql.ExplainAnalyzer;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class QueryProgressActionTest extends StarRocksHttpTestCase {

    static RuntimeProfile profile;
    static RuntimeProfile summaryProfile;
    static ProfileManager manager;
    static ProfilingExecPlan plan = new ProfilingExecPlan();
    //init profile manager, only contains a query: 123
    static {
        profile = new RuntimeProfile("");
        summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, "123");
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Running");
        profile.addChild(summaryProfile);

        manager = ProfileManager.getInstance();
        manager.pushProfile(plan, profile);
    }

    private String sendHttp(String queryId) throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/query/progress?query_id=" + queryId)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        return respStr;
    }

    @Test
    public void testQueryProgressAction() throws IOException {
        //note: in the ExplainAnalyzer, the internal class NodeInfo and NodeState is private,
        //      here can't mockup parseProfile function, so mockup getQueryProgress function.
        new MockUp<ExplainAnalyzer>() {
            //@Mock
            //public void parseProfile() {
            //    ExplainAnalyzer.NodeInfo nodeInfo1 = new ExplainAnalyzer.NodeInfo(true, 1, true, null, null, null);
            //    ExplainAnalyzer.NodeInfo nodeInfo2 = new ExplainAnalyzer.NodeInfo(true, 2, true, null, null, null);
            //    ExplainAnalyzer.NodeInfo nodeInfo3 = new ExplainAnalyzer.NodeInfo(true, 3, true, null, null, null);
            //    ExplainAnalyzer.NodeInfo nodeInfo4 = new ExplainAnalyzer.NodeInfo(true, 4, true, null, null, null);
            //    ExplainAnalyzer.NodeInfo nodeInfo5 = new ExplainAnalyzer.NodeInfo(true, 5, true, null, null, null);
            //
            //    nodeInfo1.setState(NodeState.FINISHED);
            //    nodeInfo2.setState(NodeState.FINISHED);
            //    nodeInfo3.setState(NodeState.FINISHED);
            //    nodeInfo4.setState(NodeState.RUNNING);
            //    nodeInfo5.setState(NodeState.RUNNING);
            //
            //    Map<Integer, ExplainAnalyzer.NodeInfo> allNodeInfos = Maps.newHashMap();
            //    allNodeInfos.put(nodeInfo1);
            //    allNodeInfos.put(nodeInfo2);
            //    allNodeInfos.put(nodeInfo3);
            //    allNodeInfos.put(nodeInfo4);
            //    allNodeInfos.put(nodeInfo5);
            //}
            //};
            @Mock
            public String getQueryProgress() throws StarRocksException {
                try {
                    JsonObject progressInfo = new JsonObject();
                    progressInfo.addProperty("total_operator_num", 5);
                    progressInfo.addProperty("finished_operator_num", 3);
                    progressInfo.addProperty("progress_percent", "60.00%");

                    JsonObject result = new JsonObject();
                    result.addProperty("query_id", summaryProfile.getInfoString(manager.QUERY_ID));
                    result.addProperty("state", summaryProfile.getInfoString(manager.QUERY_STATE));
                    result.add("progress_info", progressInfo);
                    return result.toString();
                } catch (Exception e) {
                    throw new StarRocksException("Failed to get query progress.");
                }
            }
        };
        new MockUp<ProfileManager>() {
            @Mock
            public ProfileManager.ProfileElement getProfileElement(String queryId) {
                //return summaryProfile.getInfoString(ProfileManager.QUERY_ID).equals(queryId)
                //                      ? manager.getAllProfileElements().get(0) : null;
                return manager.hasProfile(queryId) ? manager.getAllProfileElements().get(0) : null;
            }
        };

        //1.check query progress result
        String existQueryId = "123";
        String respStr1 = sendHttp(existQueryId);
        String expResult = "{\"query_id\":\"123\",\"state\":\"Running\",\"progress_info\"" +
                ":{\"total_operator_num\":5,\"finished_operator_num\":3,\"progress_percent\":\"60.00%\"}}";
        Assert.assertEquals(respStr1, expResult);

        //2.check query id not found
        String notExistQueryId = "456";
        String respStr2 = sendHttp(notExistQueryId);
        Assert.assertTrue(respStr2.contains("query id 456 not found"));

        //3.check not valid parameter
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/query/progress?query_id_x=" + existQueryId)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr3 = response.body().string();
        Assert.assertTrue(respStr3.contains("not valid parameter"));

        //4.check short circuit point query
        //for short circuit query, 'ProfileElement#plan' is null
        manager.pushProfile(null, profile);
        String respStr4 = sendHttp(existQueryId);
        Assert.assertTrue(respStr4.contains("short circuit point query doesn't suppot get query progress"));
        manager.pushProfile(plan, profile);

        //5.check abnormal case, eg.summaryProfile is null
        //manager.pushProfile(null, profile);
        //manager.getProfileElement(existQueryId).infoStrings.put(ProfileManager.QUERY_TYPE, "Load");
        //String queryType = manager.getProfileElement(existQueryId).infoStrings.get(ProfileManager.QUERY_TYPE);
        summaryProfile = null;
        String respStr5 = sendHttp(existQueryId);
        Assert.assertTrue(respStr5.contains("Failed to get query progress"));
    }
}
