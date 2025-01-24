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
import com.starrocks.common.DdlException;
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
import okio.BufferedSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TransactionLoadActionTest extends StarRocksHttpTestCase {

    private static HttpServer beServer;
    private static int TEST_HTTP_PORT = 0;

    @Override
    @Before
    public void setUp() {
        Backend backend4 = new Backend(1234, "localhost", 8040);
        backend4.setBePort(9300);
        backend4.setAlive(true);
        backend4.setHttpPort(TEST_HTTP_PORT);
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
        TEST_HTTP_PORT = detectUsableSocketPort();

        beServer = new HttpServer(TEST_HTTP_PORT);
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
    @Ignore("test whether this case affect cases in TableQueryPlanActionTest")
    public void beginTransactionTimes() throws IOException {
        String pathUri = "http://localhost:" + HTTP_PORT + "/api/transaction/begin";

        for (int i = 0; i < 4096; i++) {
<<<<<<< HEAD
            Request request = new Request.Builder()
                    .get()
                    .addHeader("Authorization", rootAuth)
                    .addHeader("db", "testDb")
                    .addHeader("label", String.valueOf(i))
                    .url(pathUri)
                    .method("POST", new RequestBody() {

                        @Override
                        public MediaType contentType() {
=======
            final String label = Objects.toString(i);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
            }

            assertTrue(TransactionLoadAction.getAction().txnNodeMapSize() <= 2048);
        }
    }

    @Test
    public void operateTransactionWithBadRequestTest() throws Exception {
        {
            Request request = newRequest(TransactionOperation.TXN_BEGIN);
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("No database selected."));
            }
        }

        {
            Request request = newRequest(
                    TransactionOperation.TXN_BEGIN,
                    (uriBuilder, reqBuilder) -> reqBuilder.addHeader(DB_KEY, DB_NAME));
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Empty label."));
            }
        }

        {
            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "8");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "5");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Channel ID should be between"));
            }
        }

        {
            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            }, RequestBody.create("not json", JSON));
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Malformed json tablets"));
            }
        }
    }

    @Test
    public void beginTransactionWithChannelInfoTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.beginLoadTaskFromFrontend(
                            anyString, anyString, anyString, anyString, anyString,
                            anyLong, anyInt, anyInt, (TransactionResult) any, anyLong);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void beginLoadTaskFromFrontend(String dbName,
                                                  String tableName,
                                                  String label,
                                                  String user,
                                                  String clientIp,
                                                  long timeoutMillis,
                                                  int channelNum,
                                                  int channelId,
                                                  TransactionResult resp,
                                                  long warehouseId) {
                            resp.addResultEntry(TransactionResult.LABEL_KEY, label);
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "0");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "2");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, Objects.toString(body.get(TransactionResult.LABEL_KEY)));
            }
        }

        {
            new Expectations() {
                {
                    streamLoadMgr.beginLoadTaskFromFrontend(
                            anyString, anyString, anyString, anyString, anyString,
                            anyLong, anyInt, anyInt, (TransactionResult) any, anyLong);
                    times = 1;
                    result = new StarRocksException("begin load task error");
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "0");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "2");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("begin load task error"));
            }
        }
    }

    @Test
    public void beginTransactionWithoutChannelInfoTest() throws Exception {
        String label = RandomStringUtils.randomAlphanumeric(32);
        Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
            reqBuilder.addHeader(DB_KEY, DB_NAME);
            reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
            reqBuilder.addHeader(LABEL_KEY, label);
        });
        try (Response response = networkClient.newCall(request).execute()) {
            Map<String, Object> body = parseResponseBody(response);
            assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
            assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("mock redirect to BE"));
        }
    }

    @Test
    public void beginTransactionWithWarehouseTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.beginLoadTaskFromFrontend(
                            anyString, anyString, anyString, anyString, anyString,
                            anyLong, anyInt, anyInt, (TransactionResult) any, anyLong);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void beginLoadTaskFromFrontend(String dbName,
                                                              String tableName,
                                                              String label,
                                                              String user,
                                                              String clientIp,
                                                              long timeoutMillis,
                                                              int channelNum,
                                                              int channelId,
                                                              TransactionResult resp,
                                                              long warehouseId) {
                            resp.addResultEntry(TransactionResult.LABEL_KEY, label);
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "0");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "2");
                // no warehouse set here
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, Objects.toString(body.get(TransactionResult.LABEL_KEY)));
            }
        }
    }

    @Test
    public void beginTransactionWithNonExistentWarehouseTest() throws Exception {
        {
            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "0");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "2");
                reqBuilder.addHeader(WAREHOUSE_KEY, "non_exist_warehouse");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY))
                        .contains("Warehouse name: non_exist_warehouse not exist"));
            }
        }
    }

    @Test
    public void beginTransactionForBypassWriteTest() throws Exception {
        {
            new Expectations() {
                {
                    globalTransactionMgr.beginTransaction(
                            anyLong,
                            (List<Long>) any,
                            anyString,
                            (TxnCoordinator) any,
                            LoadJobSourceType.BYPASS_WRITE,
                            anyLong);
                    times = 1;
                    result = new BeginTransactionException("begin transaction error");
                }
            };
            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);

                uriBuilder.addParameter(SOURCE_TYPE, Objects.toString(LoadJobSourceType.BYPASS_WRITE.getFlag()));
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("begin transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(0, Integer.MAX_VALUE);
            new Expectations() {
                {
                    globalTransactionMgr.beginTransaction(
                            anyLong,
                            (List<Long>) any,
                            anyString,
                            (TxnCoordinator) any,
                            LoadJobSourceType.BYPASS_WRITE,
                            anyLong);
                    times = 1;
                    result = txnId;
                }
            };
            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);

                uriBuilder.addParameter(SOURCE_TYPE, Objects.toString(LoadJobSourceType.BYPASS_WRITE.getFlag()));
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void prepareTransactionWithChannelInfoTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.prepareLoadTask(anyString, anyInt, (HttpHeaders) any, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void prepareLoadTask(String label,
                                                    int channelId,
                                                    HttpHeaders headers,
                                                    TransactionResult resp) throws StarRocksException {
                            resp.setErrorMsg("prepare load task error");
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("prepare load task error"));
            }
        }

        {
            new Expectations() {
                {
                    streamLoadMgr.prepareLoadTask(anyString, anyInt, (HttpHeaders) any, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void prepareLoadTask(String label,
                                                    int channelId,
                                                    HttpHeaders headers,
                                                    TransactionResult resp) throws StarRocksException {
                            resp.setOKMsg("");
                        }

                    };

                    streamLoadMgr.tryPrepareLoadTaskTxn(anyString, (TransactionResult) any);
                    times = 1;
                    result = new StarRocksException("try prepare load task txn error");
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("try prepare load task txn error"));
            }
        }

        {
            new Expectations() {
                {
                    streamLoadMgr.prepareLoadTask(anyString, anyInt, (HttpHeaders) any, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void prepareLoadTask(String label,
                                                    int channelId,
                                                    HttpHeaders headers,
                                                    TransactionResult resp) throws StarRocksException {
                            resp.setOKMsg("");
                        }

                    };

                    streamLoadMgr.tryPrepareLoadTaskTxn(anyString, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void tryPrepareLoadTaskTxn(String label, TransactionResult resp) throws
                                StarRocksException {
                            resp.addResultEntry(TransactionResult.LABEL_KEY, label);
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
            }
        }
    }

    @Test
    public void prepareTransactionWithoutChannelInfoTest() throws Exception {
        String label = RandomStringUtils.randomAlphanumeric(32);
        setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
            private static final long serialVersionUID = -4276328107866085321L;

            {
                put(label, 1234L);
            }
        });

        Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
            reqBuilder.addHeader(DB_KEY, DB_NAME);
            reqBuilder.addHeader(LABEL_KEY, label);
        });
        try (Response response = networkClient.newCall(request).execute()) {
            Map<String, Object> body = parseResponseBody(response);
            assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
            assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("mock redirect to BE"));
        }
    }

    @Test
    public void prepareTransactionForBypassWriteTest() throws Exception {
        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.UNKNOWN),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.UNKNOWN)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Can not prepare"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("has already COMMITTED"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE)
                    );

                    globalTransactionMgr.prepareTransaction(
                            anyLong, anyLong,
                            (List<TabletCommitInfo>) any,
                            (List<TabletFailInfo>) any,
                            (TxnCommitAttachment) any, anyLong);
                    times = 1;
                    result = new StarRocksException("prepare transaction error");

                }
            };

            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("prepare transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE)
                    );

                    globalTransactionMgr.prepareTransaction(
                            anyLong, anyLong,
                            (List<TabletCommitInfo>) any,
                            (List<TabletFailInfo>) any,
                            (TxnCommitAttachment) any, anyLong);
                    times = 1;
                }
            };

            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            }, RequestBody.create(
                    objectMapper.writeValueAsString(
                            new Body(Lists.newArrayList(new TabletCommitInfo(400L, 1234L)), new ArrayList<>(0))),
                    JSON
            ));
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void commitTransactionWithChannelInfoTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.commitLoadTask(anyString, (TransactionResult) any);
                    times = 1;
                    result = new StarRocksException("commit load task error");
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("commit load task error"));
            }
        }

        {
            new Expectations() {
                {
                    streamLoadMgr.commitLoadTask(anyString, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void commitLoadTask(String label, TransactionResult resp) throws StarRocksException {
                            resp.addResultEntry(TransactionResult.LABEL_KEY, label);
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
            }
        }
    }

    @Test
    public void commitTransactionWithoutChannelInfoTest() throws Exception {
        {
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = null;
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 5890524883711716645L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("No transaction found by label"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.UNKNOWN);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = -4276328107866085321L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("mock redirect to BE"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.ABORTED);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 8612091611347668755L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Can not commit"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.COMMITTED);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 3214813746415023231L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("has already committed"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.PREPARED);

                    globalTransactionMgr.commitPreparedTransaction(anyLong, anyLong, anyLong);
                    times = 1;
                    result = new StarRocksException("commit prepared transaction error");
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 6893430743492341004L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(
                        Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("commit prepared transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.PREPARED);

                    globalTransactionMgr.commitPreparedTransaction(anyLong, anyLong, anyLong);
                    times = 1;
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 8165080593735535441L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void commitTransactionForBypassWriteTest() throws Exception {
        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            null
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("No transaction found by label"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Can not commit"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("has already committed"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED)
                    );

                    globalTransactionMgr.commitPreparedTransaction(anyLong, anyLong, anyLong);
                    times = 1;
                    result = new StarRocksException("commit prepared transaction error");
                }
            };

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(
                        Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("commit prepared transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED)
                    );

                    globalTransactionMgr.commitPreparedTransaction(anyLong, anyLong, anyLong);
                    times = 1;
                }
            };

            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void commitTransactionForBypassWriteWithLifeCycleTest() throws Exception {
        long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
        String label = RandomStringUtils.randomAlphanumeric(32);
        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                times = 4;
                returns(
                        newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE),
                        newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARE),
                        newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                        newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED)
                );

                globalTransactionMgr.beginTransaction(
                        anyLong,
                        (List<Long>) any,
                        anyString,
                        (TxnCoordinator) any,
                        LoadJobSourceType.BYPASS_WRITE,
                        anyLong);
                times = 1;
                result = txnId;

                globalTransactionMgr.prepareTransaction(
                        anyLong, anyLong,
                        (List<TabletCommitInfo>) any,
                        (List<TabletFailInfo>) any,
                        (TxnCommitAttachment) any, anyLong);
                times = 1;

                globalTransactionMgr.commitPreparedTransaction(anyLong, anyLong, anyLong);
                times = 1;
            }
        };

        {
            Request request = newRequest(TransactionOperation.TXN_BEGIN, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(TABLE_KEY, TABLE_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);

                uriBuilder.addParameter(SOURCE_TYPE, Objects.toString(LoadJobSourceType.BYPASS_WRITE.getFlag()));
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }

        {
            Request request = newRequest(TransactionOperation.TXN_PREPARE, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            }, RequestBody.create(
                    objectMapper.writeValueAsString(
                            new Body(Lists.newArrayList(new TabletCommitInfo(400L, 1234L)), new ArrayList<>(0))),
                    JSON
            ));
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }

        {
            Request request = newRequest(TransactionOperation.TXN_COMMIT, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void rollbackTransactionWithChannelInfoTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.rollbackLoadTask(anyString, (TransactionResult) any);
                    times = 1;
                    result = new StarRocksException("rollback load task error");
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("rollback load task error"));
            }
        }

        {
            new Expectations() {
                {
                    streamLoadMgr.rollbackLoadTask(anyString, (TransactionResult) any);
                    times = 1;
                    result = new Delegate<Void>() {

                        public void rollbackLoadTask(String label, TransactionResult resp) throws StarRocksException {
                            resp.addResultEntry(TransactionResult.LABEL_KEY, label);
                        }

                    };
                }
            };

            String label = RandomStringUtils.randomAlphanumeric(32);
            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
                reqBuilder.addHeader(CHANNEL_ID_STR, "1");
                reqBuilder.addHeader(CHANNEL_NUM_STR, "3");
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
            }
        }
    }

    @Test
    public void rollbackTransactionWithoutChannelInfoTest() throws Exception {
        {
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = null;
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 5890524883711716645L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("No transaction found by label"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.UNKNOWN);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = -4276328107866085321L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("mock redirect to BE"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.COMMITTED);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = -5731416357248595041L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Can not abort"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.ABORTED);
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = -6655156575562250213L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("has already aborted"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.PREPARED);

                    globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString);
                    times = 1;
                    result = new StarRocksException("abort transaction error");
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = -891006164191904128L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("abort transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 1;
                    result = newTxnState(txnId, label, LoadJobSourceType.FRONTEND_STREAMING, TransactionStatus.PREPARED);

                    globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString);
                    times = 1;
                }
            };

            setField(TransactionLoadAction.getAction(), "txnNodeMap", new LinkedHashMap<String, Long>() {
                private static final long serialVersionUID = 4824168412840558066L;

                {
                    put(label, 1234L);
                }
            });

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void rollbackTransactionForBypassWriteTest() throws Exception {
        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            null
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("No transaction found by label"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.COMMITTED)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("Can not abort"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.ABORTED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.ABORTED)
                    );
                }
            };

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("has already aborted"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED)
                    );

                    globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString, (List<TabletFailInfo>) any);
                    times = 1;
                    result = new StarRocksException("abort transaction error");
                }
            };

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(FAILED, body.get(TransactionResult.STATUS_KEY));
                assertTrue(Objects.toString(body.get(TransactionResult.MESSAGE_KEY)).contains("abort transaction error"));
            }
        }

        {
            long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
            String label = RandomStringUtils.randomAlphanumeric(32);
            new Expectations() {
                {
                    globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                    times = 2;
                    returns(
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED),
                            newTxnState(txnId, label, LoadJobSourceType.BYPASS_WRITE, TransactionStatus.PREPARED)
                    );

                    globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString, (List<TabletFailInfo>) any);
                    times = 1;
                }
            };

            Request request = newRequest(TransactionOperation.TXN_ROLLBACK, (uriBuilder, reqBuilder) -> {
                reqBuilder.addHeader(DB_KEY, DB_NAME);
                reqBuilder.addHeader(LABEL_KEY, label);
            });
            try (Response response = networkClient.newCall(request).execute()) {
                Map<String, Object> body = parseResponseBody(response);
                assertEquals(OK, body.get(TransactionResult.STATUS_KEY));
                assertEquals(label, body.get(TransactionResult.LABEL_KEY));
                assertEquals(txnId, Long.parseLong(Objects.toString(body.get(TransactionResult.TXN_ID_KEY))));
            }
        }
    }

    @Test
    public void loadTransactionWithChannelInfoTest() throws Exception {
        {
            new Expectations() {
                {
                    streamLoadMgr.executeLoadTask(
                            anyString, anyInt, (HttpHeaders) any, (TransactionResult) any, anyString, anyString);
                    times = 1;
                    result = new Delegate<TNetworkAddress>() {

                        public TNetworkAddress executeLoadTask(
                                String label,
                                int channelId,
                                HttpHeaders headers,
                                TransactionResult resp,
                                String dbName,
                                String tableName)
                                throws StarRocksException {
                            resp.setErrorMsg("execute load task error");
>>>>>>> 7c98728a86 ([BugFix] Fix concurrent issue in olap table listener (#54051))
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
}
