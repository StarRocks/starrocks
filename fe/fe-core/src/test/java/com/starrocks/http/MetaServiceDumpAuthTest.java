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

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * /dump and /dump_starmgr are called by operators, not by peer FEs, so they skip the client-is-FE check and must
 * require an authenticated admin instead.
 */
public class MetaServiceDumpAuthTest extends StarRocksHttpTestCase {

    private final AtomicInteger dumpCount = new AtomicInteger();

    @BeforeEach
    public void mockDumpImage() {
        dumpCount.set(0);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public String dumpImage() {
                dumpCount.incrementAndGet();
                return "/mock/image.1";
            }
        };
    }

    private Response get(String path, String authorization) throws IOException {
        Request.Builder builder = new Request.Builder().get().url(BASE_URL + path);
        if (authorization != null) {
            builder.addHeader("Authorization", authorization);
        }
        return networkClient.newCall(builder.build()).execute();
    }

    @Test
    public void testDumpRequiresAuthentication() throws IOException {
        try (Response response = get("/dump", null)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/dump", Credentials.basic("root", "wrong_password"))) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        Assertions.assertEquals(0, dumpCount.get());

        try (Response response = get("/dump", rootAuth)) {
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            Assertions.assertTrue(body.contains("dump finished. /mock/image.1"), body);
        }
        Assertions.assertEquals(1, dumpCount.get());
    }

    @Test
    public void testDumpStarMgrRequiresAuthentication() throws IOException {
        try (Response response = get("/dump_starmgr", null)) {
            Assertions.assertEquals(401, response.code(), response.body().string());
        }
        try (Response response = get("/dump_starmgr", rootAuth)) {
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            // The UT cluster runs in shared-nothing mode.
            Assertions.assertTrue(body.contains("does not support dump starmgr meta"), body);
        }
    }
}
