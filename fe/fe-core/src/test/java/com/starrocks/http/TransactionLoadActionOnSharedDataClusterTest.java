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

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.common.DdlException;
import com.starrocks.http.rest.TransactionLoadAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
<<<<<<< HEAD
=======
import com.starrocks.thrift.TNetworkAddress;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
<<<<<<< HEAD
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TransactionLoadActionOnSharedDataClusterTest extends StarRocksHttpTestCase {
=======
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public class TransactionLoadActionOnSharedDataClusterTest extends TransactionLoadActionTest {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    private static HttpServer beServer;
    private static int TEST_HTTP_PORT = 0;

    @Override
<<<<<<< HEAD
    @Before
    public void setUp() {
=======
    protected void doSetUp() {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

<<<<<<< HEAD


=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        ComputeNode computeNode = new ComputeNode(1234, "localhost", 8040);
        computeNode.setBePort(9300);
        computeNode.setAlive(true);
        computeNode.setHttpPort(TEST_HTTP_PORT);
<<<<<<< HEAD
        GlobalStateMgr.getCurrentSystemInfo().addComputeNode(computeNode);
=======
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(computeNode);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };
<<<<<<< HEAD
    }

    @After
    public void tearDown() {
        GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(new ComputeNode(1234, "localhost", HTTP_PORT));
    }

    @BeforeClass
    public static void initBeServer() throws IllegalArgException, InterruptedException {
=======

        new MockUp<TransactionLoadAction>() {

            @Mock
            public void redirectTo(BaseRequest request,
                                   BaseResponse response,
                                   TNetworkAddress addr) throws DdlException {
                TransactionResult result = new TransactionResult();
                result.setOKMsg("mock redirect to BE");
                response.setContentType(JSON.toString());
                response.appendContent(result.toJson());
                writeResponse(request, response);
            }

        };
    }

    /**
     * we need close be server after junit test
     */
    @AfterClass
    public static void close() {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .dropComputeNode(new ComputeNode(1234, "localhost", HTTP_PORT));
        beServer.shutDown();
    }

    @BeforeClass
    public static void initBeServer() throws Exception {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        TEST_HTTP_PORT = detectUsableSocketPort();
        beServer = new HttpServer(TEST_HTTP_PORT);
        BaseAction ac = new BaseAction(beServer.getController()) {

            @Override
<<<<<<< HEAD
            public void execute(BaseRequest request, BaseResponse response) throws DdlException {
=======
            public void execute(BaseRequest request, BaseResponse response) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                TransactionResult resp = new TransactionResult();
                response.appendContent(resp.toJson());
                writeResponse(request, response, HttpResponseStatus.OK);
            }
        };
<<<<<<< HEAD
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/begin", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/prepare", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/commit", ac);
        beServer.getController().registerHandler(HttpMethod.POST, "/api/transaction/rollback", ac);
        beServer.start();
        // must ensure the http server started before any unit test
        while (!beServer.isStarted()) {
            Thread.sleep(500);
        }
<<<<<<< HEAD
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
                        public void writeTo(BufferedSink arg0) throws IOException {
                        }

                    })
                    .build();
            Response response = networkClient.newCall(request).execute();
            Assert.assertEquals(true, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        Response response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        response = networkClient.newCall(request).execute();

        Assert.assertEquals(true, response.body().string().contains("OK"));
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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        Response response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        Response response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        response = networkClient.newCall(request).execute();

        Assert.assertEquals(false, response.body().string().contains("OK"));

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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        response = networkClient.newCall(request).execute();
        String res = response.body().string();

        Assert.assertEquals(false, res.contains("OK"));
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
                    public void writeTo(BufferedSink arg0) throws IOException {
                    }

                })
                .build();

        Response response = networkClient.newCall(request).execute();
        String res = response.body().string();
        System.out.println(res);

        Assert.assertEquals(false, res.contains("OK"));
    }

=======
        assertTrue(beServer.isStarted());
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}
