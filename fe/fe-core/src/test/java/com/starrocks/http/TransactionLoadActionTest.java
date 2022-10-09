// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.http;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;

import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.TransactionLoadAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;

import mockit.MockUp;
import mockit.Mock;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

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
    
    @After
    public void tearDown() {
        GlobalStateMgr.getCurrentSystemInfo().dropBackend(new Backend(1234, "localhost", HTTP_PORT));
    }

    @BeforeClass
    public static void initBeServer() throws IllegalArgException, InterruptedException {
        beServer = new HttpServer(9737);
        BaseAction ac = new BaseAction(beServer.getController()) {

            @Override
            public void execute(BaseRequest request, BaseResponse response) throws DdlException {
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
    public void beginTransactionTimes() throws IOException {
        String PATH_URI = "http://localhost:" + HTTP_PORT + "/api/transaction/begin";

        for (int i = 0; i < 4096; i ++) {
            Request request = new Request.Builder()
                    .get()
                    .addHeader("Authorization", rootAuth)
                    .addHeader("db", "testDb")
                    .addHeader("label", String.valueOf(i))
                    .url(PATH_URI)
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

            Assert.assertTrue(TransactionLoadAction.getAction().txnBackendMapSize() <= 2048);
        }
    }

    @Test
    public void beginTransaction() throws IOException {
        String PATH_URI = "http://localhost:" + HTTP_PORT + "/api/transaction/begin";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(PATH_URI)
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
                .url(PATH_URI)
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
                .url(PATH_URI)
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
        String PATH_URI = "http://localhost:" + HTTP_PORT + "/api/transaction/commit";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(PATH_URI)
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
                .url(PATH_URI)
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
        String PATH_URI = "http://localhost:" + HTTP_PORT + "/api/transaction/rollback";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(PATH_URI)
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
                .url(PATH_URI)
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
                .url(PATH_URI)
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
        String PATH_URI = "http://localhost:" + HTTP_PORT + "/api/transaction/prepare";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(PATH_URI)
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
}
