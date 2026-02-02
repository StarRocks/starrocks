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
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.v2.vo.ClusterOverview;
import com.starrocks.persist.gson.GsonUtils;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.fail;

public class ClusterOverviewActionV2Test extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/api/v2/cluster_overview";

    @Test
    public void testGetBackend() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + QUERY_PLAN_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        RestBaseResultV2<ClusterOverview> resp = parseResponseBody(respStr);
        ClusterOverview clusterOverview = resp.getResult();
        Assertions.assertEquals(200, response.code());
        Assertions.assertEquals(2, clusterOverview.getDbCount());
        Assertions.assertEquals(64, clusterOverview.getTableCount());
        Assertions.assertEquals(3, clusterOverview.getTotalBackendNum());
    }


    private static RestBaseResultV2<ClusterOverview> parseResponseBody(String body) {
        try {
            return GsonUtils.GSON.fromJson(
                    body,
                    new TypeToken<RestBaseResultV2<ClusterOverview>>() {
                    }.getType());
        } catch (Exception e) {
            fail(e.getMessage() + ", resp: " + body);
            throw new IllegalStateException(e);
        }
    }
}
