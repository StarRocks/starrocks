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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class StreamLoadActionTest extends StarRocksHttpTestCase {
    private static final String STREAM_LOAD_URI_FORMAT = "http://%s:%s/api/%s/%s/_stream_load";
    private static final String COLUMNS = "column_1,column_2";
    private String streamLoadURL;
    private OkHttpClient httpClient;
    private String beStreamLoadURL;

    @Override
    public void doSetUp() {
        streamLoadURL = String.format(STREAM_LOAD_URI_FORMAT, "localhost", HTTP_PORT, DB_NAME, TABLE_NAME);
        // Add an alive backend
        Backend aliveBE = new Backend(1234, "192.168.1.1", 8040);
        aliveBE.setBePort(9300);
        aliveBE.setAlive(true);
        aliveBE.setHttpPort(9737);
        aliveBE.setDisks(new ImmutableMap.Builder<String, DiskInfo>().put("1", new DiskInfo("")).build());
        GlobalStateMgr.getCurrentSystemInfo().addBackend(aliveBE);
        beStreamLoadURL = String.format(STREAM_LOAD_URI_FORMAT,
                aliveBE.getHost(), aliveBE.getHttpPort(), DB_NAME, TABLE_NAME);

        // Disable following redirection
        httpClient = new OkHttpClient.Builder()
                .readTimeout(100, TimeUnit.SECONDS)
                .followRedirects(false)
                .build();
    }

    @Test
    public void testStreamLoadActionRedirectToInternalAddress() throws IOException {
        try (Response response = httpClient.newCall(createRequest()).execute()) {
            Assert.assertTrue(response.isRedirect());
            Assert.assertEquals(307, response.code());
            // Location: http://192.168.1.1:9373/api/testDb/testTbl/_stream_load
            Assert.assertEquals(beStreamLoadURL, response.header("Location"));
        }
    }

    @Test
    public void testStreamLoadActionRedirectToExternalAddress() throws IOException {
        try {
            Config.stream_load_use_be_external_address = true;
            String externalAddr = "test-be-external-address.com";
            int externalPort = 13579;
            Config.stream_load_be_external_address = String.format("%s:%d", externalAddr, externalPort);
            String externalBeURL = String.format(STREAM_LOAD_URI_FORMAT, externalAddr, externalPort, DB_NAME, TABLE_NAME);
            try (Response response = httpClient.newCall(createRequest()).execute()) {
                Assert.assertTrue(response.isRedirect());
                Assert.assertEquals(307, response.code());
                // expect to get the configured external address
                // Location: http://test-be-external-address.com:13579/api/testDb/testTbl/_stream_load
                Assert.assertEquals(externalBeURL, response.header("Location"));
            }
        } finally {
            // reset to default value
            Config.stream_load_use_be_external_address = false;
            Config.stream_load_be_external_address = "";
        }
    }

    @NotNull
    private Request createRequest() {
        RequestBody body = RequestBody.create("{}", JSON);
        Request.Builder requestBuilder = new Request.Builder()
                .url(streamLoadURL)
                .put(body)
                .addHeader("Authorization", rootAuth)
                .addHeader("columns", COLUMNS)
                .addHeader("label", UUID.randomUUID().toString())
                .addHeader("Expect", "100-continue");
        return requestBuilder.build();
    }
}
