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

package com.starrocks.http.rest.v2;

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProfileActionV2Test extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/api/v2/profile";
    private static final String QUERY_ID = "eaff21d2-3734-11ee-909f-8e20563011de";

    // A profile run by root, so the root caller reads it as its owner.
    private static ProfileManager.ProfileElement rootProfileElement() {
        RuntimeProfile profile = new RuntimeProfile("Query");
        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString(ProfileManager.QUERY_ID, QUERY_ID);
        summary.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summary.addInfoString(ProfileManager.USER, "root");
        summary.addInfoString(ProfileManager.SQL_STATEMENT, "select count(*) from lineorder");
        profile.addChild(summary);
        return ProfileManager.getInstance().createElement(summary, profile);
    }

    @Test
    public void testQueryProfile() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?query_id=eaff21d2-3734-11ee-909f-8e20563011de")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assertions.assertEquals(response.code(), 404);
        Assertions.assertTrue(respStr.contains("Query id eaff21d2-3734-11ee-909f-8e20563011de not found."));
    }

    @Test
    public void testQueryProfileFromLeaderFront() throws Exception {

        new MockUp<ProfileManager>() {
            @Mock
            public ProfileManager.ProfileElement getProfileElement(String queryId) {
                if (queryId.equalsIgnoreCase(QUERY_ID)) {
                    return rootProfileElement();
                }
                return null;
            }
        };

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?query_id=eaff21d2-3734-11ee-909f-8e20563011de"
                 + "&is_request_all_frontend=true")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assertions.assertEquals(response.code(), 200);
        Assertions.assertTrue(respStr.contains("Query ID: eaff21d2-3734-11ee-909f-8e20563011de"));
    }

    @Test
    public void testQueryProfileFromFronts() throws Exception {

        Frontend frontend = new Frontend(0, FrontendNodeType.LEADER, "", "localhost", 0);

        new Expectations(GlobalStateMgr.getCurrentState().getNodeMgr()) {
            {

                GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode();
                minTimes = 1;
                result = new Pair<>(frontend.getHost(), HTTP_PORT);
            }
        };

        new MockUp<RestBaseAction>() {
            @Mock
            public static List<Pair<String, Integer>> getOtherAliveFe() {
                Pair<String, Integer> frontNode = GlobalStateMgr.getCurrentState()
                        .getNodeMgr()
                        .getSelfNode();
                List<Pair<String, Integer>> frontNodes = new ArrayList<>();
                frontNodes.add(frontNode);
                return frontNodes;
            }
        };

        new MockUp<ProfileManager>() {
            int callCount = 0;
            @Mock
            public ProfileManager.ProfileElement getProfileElement(String queryId) {
                if (callCount <= 0) {
                    callCount++;
                    // Simulate that the profile is not found in the local ProfileManager
                    return null;
                }
                return rootProfileElement();
            }
        };

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?query_id=eaff21d2-3734-11ee-909f-8e20563011de"
                        + "&is_request_all_frontend=true")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        RestBaseResultV2<String> queryProfileResult = GsonUtils.GSON.fromJson(
                respStr,
                new TypeToken<RestBaseResultV2<String>>() {
                }.getType());
        Assertions.assertEquals(queryProfileResult.getStatus(), ActionStatus.OK);
        Assertions.assertTrue(queryProfileResult.getResult().contains("Query ID: eaff21d2-3734-11ee-909f-8e20563011de"));
        Assertions.assertThrows(
                JsonSyntaxException.class,
                () -> GsonUtils.GSON.fromJson(queryProfileResult.getResult(), JsonElement.class),
                "Query profile should be plain text, not a JSON object");
    }
}
