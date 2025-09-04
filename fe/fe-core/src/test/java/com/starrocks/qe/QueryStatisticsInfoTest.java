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

import com.starrocks.thrift.TQueryStatisticsInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
        HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
        HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
        HttpRequest mockRequest = Mockito.mock(HttpRequest.class);
        Mockito.when(mockRequest.uri()).thenReturn(URI.create("http://localhost:8030/api/query/progress?query_id=123"));
        Mockito.when(mockHttpClient.send(Mockito.any(HttpRequest.class), Mockito.any())).thenReturn(mockResponse);
        QueryStatisticsInfo info = new QueryStatisticsInfo();

        //1.check query progress result    
        String response1 = "{\"query_id\":\"123\",\"state\":\"Running\",\"progress_info\"" +
                ":{\"total_operator_num\":5,\"finished_operator_num\":3,\"progress_percent\":\"60.00%\"}}";
        Mockito.when(mockResponse.statusCode()).thenReturn(200);
        Mockito.when(mockResponse.body()).thenReturn(response1);

        String result1 = info.getExecProgress("localhost", "123", mockHttpClient);
        Assertions.assertEquals("60.00%", result1);

        //2.check query id not found, like set enable_profile=false
        String response2 = "query id 123 not found.";
        Mockito.when(mockResponse.statusCode()).thenReturn(404);
        Mockito.when(mockResponse.body()).thenReturn(response2);

        String result2 = info.getExecProgress("localhost", "123", mockHttpClient);
        Assertions.assertEquals(result2, "");

        //3.check short circuit query
        String response3 = "short circuit point query doesn't suppot get query progress, " +
                             "you can set it off by using set enable_short_circuit=false";
        Mockito.when(mockResponse.statusCode()).thenReturn(200);
        Mockito.when(mockResponse.body()).thenReturn(response3);

        String result3 = info.getExecProgress("localhost", "123", mockHttpClient);
        Assertions.assertEquals(result3, "");

        //4.check special case, like fe_ip:http_port is unreachable
        Mockito.doThrow(new IOException("Network is unreachable")).when(mockHttpClient)
                               .send(Mockito.any(HttpRequest.class), Mockito.any());
        String result4 = info.getExecProgress("localhost", "123", mockHttpClient);
        Assertions.assertEquals(result4, "");
    }
}
