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

package com.starrocks.httpv2.rest;

import com.starrocks.common.Log4jConfig;
import com.starrocks.httpv2.StarRocksHttpV2TestCase;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoginApiTest extends StarRocksHttpV2TestCase {

    private static final String LOGIN_URI = "/rest/v2/login";


    private void sendHttp() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "");

        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + LOGIN_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        assertTrue(response.isSuccessful());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
    }

    @Test
    public void testLogin() throws IOException {
        Log4jConfig.initLogging();
        sendHttp();
    }
}

