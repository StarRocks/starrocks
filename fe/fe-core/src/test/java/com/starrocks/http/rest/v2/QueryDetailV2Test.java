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

import com.google.gson.reflect.TypeToken;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
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
import java.util.Objects;

public class QueryDetailV2Test extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/api/v2/query_detail";

    @Test
    public void testQueryDetail() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?event_time=1753671088")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assertions.assertEquals(response.code(), 200);
        Assertions.assertNotNull(respStr);
    }

    @Test
    public void testQueryDetailFromLeaderFront() throws IOException {

        QueryDetail startQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", false, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        startQueryDetail.setScanRows(100);
        startQueryDetail.setScanBytes(10001);
        startQueryDetail.setReturnRows(1);
        startQueryDetail.setCpuCostNs(1002);
        startQueryDetail.setMemCostBytes(100003);
        QueryDetailQueue.addQueryDetail(startQueryDetail);

        long eventTime  = startQueryDetail.getEventTime() - 1;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?event_time=" + eventTime)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assertions.assertEquals(response.code(), 200);
        Assertions.assertTrue(respStr.contains("219a2d5443c542d4-8fc938db37c892e3"));
    }

    @Test
    public void testQueryDetailFromFronts() throws IOException {

        Frontend frontend = new Frontend(0, FrontendNodeType.LEADER, "", "localhost", 0);

        new Expectations(GlobalStateMgr.getCurrentState().getNodeMgr()) {
            {

                GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode();
                minTimes = 1;
                result = new Pair<>(frontend.getHost(), HTTP_PORT);
            }
        };

        QueryDetail startQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", false, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        startQueryDetail.setScanRows(100);
        startQueryDetail.setScanBytes(10001);
        startQueryDetail.setReturnRows(1);
        startQueryDetail.setCpuCostNs(1002);
        startQueryDetail.setMemCostBytes(100003);
        QueryDetailQueue.addQueryDetail(startQueryDetail);


        new MockUp<RestBaseAction>() {
            @Mock
            public static List<Pair<String, Integer>> getOtherAliveFe() {

                QueryDetail startQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e5", false, 1, "127.0.0.1",
                        System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                        "testDb", "select * from table2 limit 1",
                        "root", "", "default_catalog");
                startQueryDetail.setScanRows(150);
                startQueryDetail.setScanBytes(10005);
                startQueryDetail.setReturnRows(1);
                startQueryDetail.setCpuCostNs(1005);
                startQueryDetail.setMemCostBytes(100005);
                QueryDetailQueue.addQueryDetail(startQueryDetail);

                Pair<String, Integer> frontNode = GlobalStateMgr.getCurrentState()
                        .getNodeMgr()
                        .getSelfNode();
                List<Pair<String, Integer>> frontNodes = new ArrayList<>();
                frontNodes.add(frontNode);
                return frontNodes;
            }
        };

        long eventTime  = startQueryDetail.getEventTime() - 1;
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI +
                      "?event_time=" + eventTime +
                      "&user=root" +
                      "&is_request_all_frontend=true")
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = Objects.requireNonNull(response.body()).string();
        RestBaseResultV2<List<QueryDetail>> queryDetails = GsonUtils.GSON.fromJson(
                respStr,
                new TypeToken<RestBaseResultV2<List<QueryDetail>>>() {
                }.getType());
        Assertions.assertEquals(response.code(), 200);
        Assertions.assertEquals(3, queryDetails.getResult().size());
    }
}
