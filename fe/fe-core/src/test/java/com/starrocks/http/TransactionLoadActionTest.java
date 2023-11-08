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
import com.starrocks.http.rest.TransactionLoadAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TransactionLoadActionTest extends StarRocksHttpTestCase {

    private static HttpServer beServer;

    @Override
    @Before
    public void setUp() {
        Backend backend4 = new Backend(1234, "localhost", 8040);
        backend4.setBePort(9300);
        backend4.setAlive(true);
        backend4.setHttpPort(9737);
        backend4.setDisks(new ImmutableMap.Builder<String, DiskInfo>().put("1", new DiskInfo("")).build());
        GlobalStateMgr.getCurrentSystemInfo().addBackend(backend4);
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };
    }

    /**
     * we need close be server after junit test
     */
    @AfterClass
    public static void close() {
        GlobalStateMgr.getCurrentSystemInfo().dropBackend(new Backend(1234, "localhost", HTTP_PORT));
        beServer.shutDown();
    }

    @BeforeClass
    public static void initBeServer() throws IllegalArgException, InterruptedException {
        beServer = new HttpServer(9737);
        BaseAction ac = new BaseAction(beServer.getController()) {

            @Override
            public void execute(BaseRequest request, BaseResponse response) {
                TransactionResult resp = new TransactionResult();
                response.appendContent(resp.toJson());
                writeResponse(request, response, HttpResponseStatus.OK);
            }
        };
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/begin", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/prepare", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/commit", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/rollback", ac);
        beServer.start();
        // must ensure the http server started before any unit test
        while (!beServer.isStarted()) {
            Thread.sleep(500);
        }
    }

    @Test
    @Ignore("test whether this case affect cases in TableQueryPlanActionTest")
    public void beginTransactionTimes() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/begin";

        for (int i = 0; i < 4096; i++) {
            Request request = new Request.Builder()
                    .get()
                    .addHeader("Authorization", rootAuth)
                    .addHeader("db", "testDb")
                    .addHeader("label", String.valueOf(i))
                    .url(pathUri)
                    .method("POST", new RequestBody() {

                        @Override
                        public MediaType contentType() {
                            return null;
                        }

                        @Override
                        public void writeTo(@NotNull BufferedSink arg0) {
                        }

                    })
                    .build();
            try (Response response = networkClient.newCall(request).execute()) {
                ResponseBody responseBody = response.body();
                Assert.assertNotNull(responseBody);
                String res = responseBody.string();
                Assert.assertTrue(res.contains("OK"));
            }

            Assert.assertTrue(TransactionLoadAction.getAction().txnNodeMapSize() <= 2048);
        }
    }

    @Test
    public void beginTransaction() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/begin";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("db", "abc")
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("db", "abc")
                .addHeader("label", "abcdbcef")
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertTrue(res.contains("OK"));
        }
    }

    @Test
    public void commitTransaction() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/commit";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("db", "abc")
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

    }

    @Test
    public void rollbackTransaction() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/rollback";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("db", "abc")
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }

        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("db", "testDb")
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }
    }

    @Test
    public void prepareTransaction() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/prepare";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(pathUri)
                .method("POST", new RequestBody() {

                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(@NotNull BufferedSink arg0) {
                    }

                })
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertNotNull(responseBody);
            String res = responseBody.string();
            Assert.assertFalse(res.contains("OK"));
        }
    }
}
