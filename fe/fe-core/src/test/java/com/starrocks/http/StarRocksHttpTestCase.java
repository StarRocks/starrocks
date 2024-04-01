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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker.ThrowingRunnable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.Load;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
        Partition partition = new Partition(testPartitionId, "testPartition", baseIndex, distributionInfo);
        partition.updateVisibleVersion(testStartVersion);
        partition.setNextVersion(testStartVersion + 1);

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
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();
        Column k1 = new Column("k1", Type.BIGINT);
        Column k2 = new Column("k2", Type.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);

        Replica replica1 =
                new Replica(testReplicaId1, testBackendId1, testStartVersion, testSchemaHash,
                        1024000L, 2000L,
                        Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica2 =
                new Replica(testReplicaId2, testBackendId2, testStartVersion, testSchemaHash,
                        1024000L, 2000L,
                        Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica3 =
                new Replica(testReplicaId3, testBackendId3, testStartVersion, testSchemaHash,
                        1024000L, 2000L,
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
        Partition partition = new Partition(testPartitionId, "testPartition", baseIndex, distributionInfo);
        partition.updateVisibleVersion(testStartVersion);
        partition.setNextVersion(testStartVersion + 1);

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
                globalStateMgr.getDb(db.getId());
                minTimes = 0;
                result = db;

                globalStateMgr.getDb(DB_NAME);
                minTimes = 0;
                result = db;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = true;

                globalStateMgr.getDb("emptyDb");
                minTimes = 0;
                result = null;

                globalStateMgr.getDb(anyString);
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
                localMetastore.listDbNames();
                minTimes = 0;
                result = Lists.newArrayList("testDb");

                localMetastore.getFullNameToDb();
                minTimes = 0;
                result = nameToDb;
            }
        };

        return globalStateMgr;
    }

    private static GlobalStateMgr newDelegateGlobalStateMgr() {
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

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, null, null);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDb(db.getId());
                minTimes = 0;
                result = db;

                globalStateMgr.getDb(DB_NAME);
                minTimes = 0;
                result = db;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = true;

                globalStateMgr.getDb("emptyDb");
                minTimes = 0;
                result = null;

                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = new Database();

                globalStateMgr.getLoadInstance();
                minTimes = 0;
                result = new Load();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getMetadataMgr();
                minTimes = 0;
                result = metadataMgr;
            }
        };

        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("default_catalog", "testDb");
                minTimes = 0;
                result = db;

                metadataMgr.getTable("default_catalog", "testDb", "testTbl");
                minTimes = 0;
                result = table;

                metadataMgr.getTable("default_catalog", "testDb", "test_empty_table");
                minTimes = 0;
                result = newEmptyTable;
            }
        };
        ;

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
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        assignBackends();
        doSetUp();
    }

    public void setUpWithCatalog() throws Exception {
        GlobalStateMgr globalStateMgr = newDelegateGlobalStateMgr();
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
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

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
}
