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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/http/StarRocksHttpTestCase.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker.ThrowingRunnable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.load.Load;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionStatus;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import junit.framework.AssertionFailedError;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public abstract class StarRocksHttpTestCase {

    public OkHttpClient networkClient = new OkHttpClient.Builder()
            .readTimeout(100, TimeUnit.SECONDS)
            .build();

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private static HttpServer httpServer;

    public static final String DB_NAME = "testDb";
    public static final String TABLE_NAME = "testTbl";

    protected static final String ES_TABLE_NAME = "es_table";

    protected static long testBackendId1 = 1000;
    protected static long testBackendId2 = 1001;
    protected static long testBackendId3 = 1002;

    protected static long testReplicaId1 = 2000;
    protected static long testReplicaId2 = 2001;
    protected static long testReplicaId3 = 2002;

    protected static long testDbId = 100L;
    protected static long testTableId = 200L;
    protected static long testPartitionId = 201L;
    protected static long testPhysicalPartitionId = 202L;
    public static long testIndexId = testTableId; // the base indexid == tableid
    protected static long tabletId = 400L;

    public static long testStartVersion = 12;
    public static int testSchemaHash = 93423942;

    public static int HTTP_PORT;

    protected static String URI;
    protected static String BASE_URL;
    protected static final String AUTH_KEY = "Authorization";
    protected String rootAuth = Credentials.basic("root", "");

    protected static ObjectMapper objectMapper = new ObjectMapper();

    @Mocked
    private static EditLog editLog;

    public static int detectUsableSocketPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Could not find a free TCP/IP port");
        }
    }

    public static OlapTable newEmptyTable(String name) {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();
        Column k1 = new Column("k1", Type.BIGINT);
        Column k2 = new Column("k2", Type.DOUBLE);
        Column k3 = new Column("k3", Type.DATETIME);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);
        columns.add(k3);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);

        // partition
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        Partition partition = new Partition(testPartitionId, testPhysicalPartitionId,
                "testPartition", baseIndex, distributionInfo);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(testStartVersion);
        partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId, (short) 3);
        partitionInfo.setIsInMemory(testPartitionId, false);
        OlapTable table = new OlapTable(testTableId, name, columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(testIndexId, "testIndex", columns, 0, testSchemaHash, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexId(testIndexId);
        return table;
    }

    public static OlapTable newTable(String name) {
        return newTable(name, 1024000L);
    }

    public static OlapTable newTable(String name, long replicaDataSize) {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();
        Column k1 = new Column("k1", Type.BIGINT);
        Column k2 = new Column("k2", Type.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);

        Replica replica1 =
                new Replica(testReplicaId1, testBackendId1, testStartVersion, testSchemaHash,
                        replicaDataSize, 2000L,
                        Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica2 =
                new Replica(testReplicaId2, testBackendId2, testStartVersion, testSchemaHash,
                        replicaDataSize, 2000L,
                        Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica3 =
                new Replica(testReplicaId3, testBackendId3, testStartVersion, testSchemaHash,
                        replicaDataSize, 2000L,
                        Replica.ReplicaState.NORMAL, -1, 0);

        // tablet
        LocalTablet tablet = new LocalTablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta =
                new TabletMeta(testDbId, testTableId, testPartitionId, testIndexId, testSchemaHash, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        Partition partition = new Partition(testPartitionId, testPhysicalPartitionId,
                "testPartition", baseIndex, distributionInfo);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(testStartVersion);
        partition.getDefaultPhysicalPartition().setNextVersion(testStartVersion + 1);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId, (short) 3);
        partitionInfo.setIsInMemory(testPartitionId, false);
        OlapTable table = new OlapTable(testTableId, name, columns, KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(testIndexId, "testIndex", columns, 0, testSchemaHash, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexId(testIndexId);
        return table;
    }

    private static EsTable newEsTable(String name) {
        Column k1 = new Column("k1", Type.BIGINT);
        Column k2 = new Column("k2", Type.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId + 100, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId + 100, (short) 3);
        EsTable table = null;
        Map<String, String> props = new HashMap<>();
        props.put(EsTable.KEY_HOSTS, "http://node-1:8080");
        props.put(EsTable.KEY_USER, "root");
        props.put(EsTable.KEY_PASSWORD, "root");
        props.put(EsTable.KEY_INDEX, "test");
        props.put(EsTable.KEY_TYPE, "doc");
        try {
            table = new EsTable(testTableId + 1, name, columns, props, partitionInfo);
        } catch (DdlException e) {
            e.printStackTrace();
        }
        return table;
    }

    private static GlobalStateMgr newDelegateCatalog() {
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        //EasyMock.expect(globalStateMgr.getAuth()).andReturn(starrocksAuth).anyTimes();
        Database db = new Database(testDbId, "testDb");
        OlapTable table = newTable(TABLE_NAME);
        db.registerTableUnlocked(table);
        OlapTable table1 = newTable(TABLE_NAME + 1);
        db.registerTableUnlocked(table1);
        EsTable esTable = newEsTable(ES_TABLE_NAME);
        db.registerTableUnlocked(esTable);
        OlapTable newEmptyTable = newEmptyTable("test_empty_table");
        db.registerTableUnlocked(newEmptyTable);
        ConcurrentHashMap<String, Database> nameToDb = new ConcurrentHashMap<>();
        nameToDb.put(db.getFullName(), db);
        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.isLeader();
                minTimes = 0;
                result = true;

                globalStateMgr.getLocalMetastore().getDb("emptyDb");
                minTimes = 0;
                result = null;

                globalStateMgr.getLocalMetastore().getDb(anyString);
                minTimes = 0;
                result = new Database();

                globalStateMgr.getLoadInstance();
                minTimes = 0;
                result = new Load();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getDb("testDb");
                minTimes = 0;
                result = db;

                localMetastore.getDb(testDbId);
                minTimes = 0;
                result = db;
            }
        };

        return globalStateMgr;
    }

    private static GlobalStateMgr newDelegateGlobalStateMgr() {
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        //EasyMock.expect(globalStateMgr.getAuth()).andReturn(starrocksAuth).anyTimes();
        Database db = new Database(testDbId, "testDb");
        OlapTable table = newTable(TABLE_NAME);
        table.setTableProperty(new TableProperty(ImmutableMap.of(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1")));
        db.registerTableUnlocked(table);
        OlapTable table1 = newTable(TABLE_NAME + 1);
        db.registerTableUnlocked(table1);
        EsTable esTable = newEsTable(ES_TABLE_NAME);
        db.registerTableUnlocked(esTable);
        OlapTable newEmptyTable = newEmptyTable("test_empty_table");
        db.registerTableUnlocked(newEmptyTable);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, new TemporaryTableMgr(), null, null);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.isLeader();
                minTimes = 0;
                result = true;

                globalStateMgr.getLoadInstance();
                minTimes = 0;
                result = new Load();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getMetadataMgr();
                minTimes = 0;
                result = metadataMgr;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
            }
        };

        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb((ConnectContext) any, "default_catalog", "testDb");
                minTimes = 0;
                result = db;

                metadataMgr.getTable((ConnectContext) any, "default_catalog", "testDb", "testTbl");
                minTimes = 0;
                result = table;

                metadataMgr.getTable((ConnectContext) any, "default_catalog", "testDb", "test_empty_table");
                minTimes = 0;
                result = newEmptyTable;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getDb("testDb");
                minTimes = 0;
                result = db;

                localMetastore.getDb(testDbId);
                minTimes = 0;
                result = db;
            }
        };

        return globalStateMgr;
    }

    private static void assignBackends() {
        Backend backend1 = new Backend(testBackendId1, "node-1", 9308);
        backend1.setBePort(9300);
        Backend backend2 = new Backend(testBackendId2, "node-2", 9308);
        backend2.setBePort(9300);
        Backend backend3 = new Backend(testBackendId3, "node-3", 9308);
        backend3.setBePort(9300);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend1);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend3);
    }

    @BeforeClass
    public static void initHttpServer() throws IllegalArgException, InterruptedException {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            HTTP_PORT = socket.getLocalPort();
            BASE_URL = "http://localhost:" + HTTP_PORT;
            URI = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/" + TABLE_NAME;
        } catch (Exception e) {
            throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                }
            }
        }

        httpServer = new HttpServer(HTTP_PORT);
        httpServer.setup();
        httpServer.start();
        // must ensure the http server started before any unit test
        while (!httpServer.isStarted()) {
            Thread.sleep(500);
        }
    }

    @Before
    public void setUp() throws Exception {
        GlobalStateMgr globalStateMgr = newDelegateCatalog();
        setUpWithGlobalStateMgr(globalStateMgr);
    }

    public void setUpWithCatalog() throws Exception {
        GlobalStateMgr globalStateMgr = newDelegateGlobalStateMgr();
        setUpWithGlobalStateMgr(globalStateMgr);

        new MockUp<GlobalStateMgr>() {
            @Mock
            SchemaChangeHandler getSchemaChangeHandler() {
                return new SchemaChangeHandler();
            }

            @Mock
            MaterializedViewHandler getRollupHandler() {
                return new MaterializedViewHandler();
            }

            @Mock
            GlobalTransactionMgr getGlobalTransactionMgr() {
                new MockUp<GlobalTransactionMgr>() {
                    @Mock
                    TransactionStatus getLabelState(long dbId, String label) {
                        if (label == "a") {
                            return TransactionStatus.PREPARED;
                        } else {
                            return TransactionStatus.PREPARE;
                        }
                    }
                };

                return new GlobalTransactionMgr(null);
            }
        };
    }

    private void setUpWithGlobalStateMgr(GlobalStateMgr globalStateMgr) throws Exception {
        SystemInfoService systemInfoService = new SystemInfoService();
        TabletInvertedIndex tabletInvertedIndex = new TabletInvertedIndex();
        NodeMgr nodeMgr = new NodeMgr();

        new MockUp<GlobalStateMgr>() {
            @Mock
            SchemaChangeHandler getSchemaChangeHandler() {
                return new SchemaChangeHandler();
            }

            @Mock
            MaterializedViewHandler getRollupHandler() {
                return new MaterializedViewHandler();
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getTabletInvertedIndex();
                minTimes = 0;
                result = tabletInvertedIndex;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = true;

                globalStateMgr.isSafeMode();
                minTimes = 0;
                result = true;
            }
        };

        Frontend frontend = new Frontend(0, FrontendNodeType.LEADER, "", "", 0);
        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;

                nodeMgr.getMySelf();
                minTimes = 0;
                result = frontend;
            }
        };

        // init default warehouse
        globalStateMgr.getWarehouseMgr().initDefaultWarehouse();
        assignBackends();
        doSetUp();
    }

    @After
    public void tearDown() {
    }

    @AfterClass
    public static void closeHttpServer() {
        httpServer.shutDown();
    }

    protected void doSetUp() throws Exception {

    }

    public void expectThrowsNoException(ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            throw new AssertionFailedError(e.getMessage());
        }
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        return expectThrows(expectedType,
                "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", runnable);
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, String noExceptionMessage,
                                                       ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                return expectedType.cast(e);
            }
            AssertionFailedError assertion = new AssertionFailedError(
                    "Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError(noExceptionMessage);
    }

    protected static void writeResponse(BaseRequest request, BaseResponse response) {
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    protected static void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
        FullHttpResponse responseObj = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(response.getContent().toString().getBytes(StandardCharsets.UTF_8)));

        HttpMethod method = request.getRequest().method();
        if (!method.equals(HttpMethod.HEAD)) {
            response.updateHeader(
                    HttpHeaderNames.CONTENT_LENGTH.toString(),
                    String.valueOf(responseObj.content().readableBytes())
            );
        }

        request.getContext().write(responseObj).addListener(ChannelFutureListener.CLOSE);
    }

    protected static Map<String, Object> parseResponseBody(Response response) throws IOException {
        assertNotNull(response);
        ResponseBody body = response.body();
        assertNotNull(body);
        String bodyStr = body.string();
        return objectMapper.readValue(bodyStr, new TypeReference<>() {});
    }
}
