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

package com.starrocks.connector.kudu;

import com.google.common.collect.Lists;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.RpcRemoteException;
import org.apache.kudu.client.Status;
import org.apache.kudu.rpc.RpcHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.starrocks.catalog.KuduTableTest.genColumnSchema;
import static com.starrocks.type.TypeFactory.CATALOG_MAX_VARCHAR_LENGTH;

public class KuduMetadataTest {
    @Mocked
    KuduClient client;
    @Mocked
    KuduScanToken token;
    public static final String KUDU_MASTER = "localhost:7051";
    public static final String KUDU_CATALOG = "test_kudu_catalog";
    public static final String SCHEMA_EMULATION_PREFIX = "impala::";
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            genColumnSchema("f0", org.apache.kudu.Type.INT32),
            genColumnSchema("f1", org.apache.kudu.Type.STRING)
    ));
    private static final PartitionSchema EMPTY_PARTITION_SCHEMA = new PartitionSchema(
            new PartitionSchema.RangeSchema(Lists.newArrayList()),
            Lists.newArrayList(),
            new Schema(Lists.newArrayList())
    );

    private final List<KuduScanToken> tokens = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        this.tokens.add(token);
    }

    @AfterEach
    public void setDown() throws KuduException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testGetTable(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(),
                KuduConnector.DEFAULT_KUDU_CLIENT_OPERATION_TIMEOUT_MS,
                KuduConnector.DEFAULT_KUDU_CLIENT_ADMIN_OPERATION_TIMEOUT_MS);
        new Expectations() {
            {
                client.tableExists(anyString);
                result = true;
                client.openTable(anyString);
                result = mockedTable;
                mockedTable.getSchema();
                result = SCHEMA;
                mockedTable.getPartitionSchema();
                result = EMPTY_PARTITION_SCHEMA;
            }
        };
        Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        KuduTable kuduTable = (KuduTable) table;
        Assertions.assertEquals("test_kudu_catalog", kuduTable.getCatalogName());
        Assertions.assertEquals("db1", kuduTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", kuduTable.getCatalogTableName());
        Assertions.assertEquals(2, kuduTable.getColumns().size());
        Assertions.assertEquals(0, kuduTable.getPartitionColumnNames().size());
        Assertions.assertEquals(IntegerType.INT, kuduTable.getColumns().get(0).getType());
        Assertions.assertTrue(kuduTable.getBaseSchema().get(0).isAllowNull());
        Assertions.assertEquals(TypeFactory.createVarcharType(CATALOG_MAX_VARCHAR_LENGTH),
                kuduTable.getBaseSchema().get(1).getType());
        Assertions.assertTrue(kuduTable.getBaseSchema().get(1).isAllowNull());
    }

    @Test
    public void testListTableNames() throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER,
                true, SCHEMA_EMULATION_PREFIX, Optional.empty(),
                KuduConnector.DEFAULT_KUDU_CLIENT_OPERATION_TIMEOUT_MS,
                KuduConnector.DEFAULT_KUDU_CLIENT_ADMIN_OPERATION_TIMEOUT_MS);
        List<String> tableNames = Lists.newArrayList("impala::db1.tbl1", "db1.tbl1");
        new Expectations() {
            {
                client.getTablesList().getTablesList();
                result = tableNames;
            }
        };
        List<String> tables = metadata.listTableNames(new ConnectContext(), "db1");
        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals("tbl1", tables.get(0));
    }

    @Test
    public void testGetRemoteFiles(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(),
                KuduConnector.DEFAULT_KUDU_CLIENT_OPERATION_TIMEOUT_MS,
                KuduConnector.DEFAULT_KUDU_CLIENT_ADMIN_OPERATION_TIMEOUT_MS);
        List<String> requiredNames = Lists.newArrayList("f2", "dt");
        new Expectations() {
            {
                client.tableExists(anyString);
                result = true;
                client.openTable(anyString);
                result = mockedTable;
                client.newScanTokenBuilder((org.apache.kudu.client.KuduTable) any)
                        .setProjectedColumnNames(requiredNames)
                        .build();
                result = tokens;
            }
        };
        Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        KuduTable kuduTable = (KuduTable) table;
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(requiredNames).build();
        List<RemoteFileInfo> remoteFileInfos = metadata.getRemoteFiles(kuduTable, params);
        Assertions.assertEquals(1, remoteFileInfos.size());
        Assertions.assertEquals(1, remoteFileInfos.get(0).getFiles().size());
        KuduRemoteFileDesc desc = (KuduRemoteFileDesc) remoteFileInfos.get(0).getFiles().get(0);
        Assertions.assertEquals(1, desc.getKuduScanTokens().size());
    }

    @Test
    public void testUnsupportedGetTableStatistics(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws Exception {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(),
                KuduConnector.DEFAULT_KUDU_CLIENT_OPERATION_TIMEOUT_MS,
                KuduConnector.DEFAULT_KUDU_CLIENT_ADMIN_OPERATION_TIMEOUT_MS);
        RpcRemoteException exception = createRpcRemoteException("server sent error Invalid argument: " +
                "Call on service kudu.master.MasterService received at " +
                "0.0.0.0:7051 from 0.0.0.0:43670 with an invalid method name: GetTableStatistics");
        new Expectations() {
            {
                client.tableExists(anyString);
                result = true;
                client.openTable(anyString);
                result = mockedTable;
                mockedTable.getTableStatistics();
                result = exception;
            }
        };
        Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        KuduTable kuduTable = (KuduTable) table;
        Statistics statistics = metadata.getTableStatistics(
                null, kuduTable, Collections.emptyMap(), Collections.emptyList(), null, -1,
                TvrTableSnapshot.empty());
        Assertions.assertEquals(1D, statistics.getOutputRowCount(), 0.01);
    }

    @Test
    public void testKuduClientCacheKeyIncludesTimeouts() {
        // Use a master address distinct from other tests so the static client cache is not polluted
        // by entries created elsewhere in this test class.
        String master = "localhost:17051";
        long opTimeoutSmall = 60_000L;
        long opTimeoutLarge = 90_000L;
        long adminTimeout = 45_000L;

        // First catalog with the smaller op timeout populates the cache.
        new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), master, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(), opTimeoutSmall, adminTimeout);
        Assertions.assertTrue(
                KuduMetadata.clientCacheContains(master, opTimeoutSmall, adminTimeout),
                "Cache should contain the first catalog's (master, opTimeout, adminTimeout) key");

        // A second catalog pointing to the same master but with a different op timeout must not be
        // collapsed onto the first entry — the cache key has to honour the timeout configuration.
        new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), master, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(), opTimeoutLarge, adminTimeout);
        Assertions.assertTrue(
                KuduMetadata.clientCacheContains(master, opTimeoutLarge, adminTimeout),
                "Second catalog with a larger op timeout should produce a distinct client cache entry");

        // The first entry must remain reachable (i.e. the second catalog did not evict it).
        Assertions.assertTrue(
                KuduMetadata.clientCacheContains(master, opTimeoutSmall, adminTimeout),
                "First catalog's cache entry must still be present after the second catalog is created");

        // Distinct admin timeout must also produce a distinct cache entry.
        long adminTimeoutLarger = 90_000L;
        new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), master, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty(), opTimeoutSmall, adminTimeoutLarger);
        Assertions.assertTrue(
                KuduMetadata.clientCacheContains(master, opTimeoutSmall, adminTimeoutLarger),
                "Catalog with a different admin timeout should produce a distinct client cache entry");
    }

    private RpcRemoteException createRpcRemoteException(String message) throws Exception {
        Status status = Status.IllegalState(message);
        RpcHeader.ErrorStatusPB errPb = RpcHeader.ErrorStatusPB.getDefaultInstance();

        Constructor<RpcRemoteException>
                constructor = RpcRemoteException.class.getDeclaredConstructor(
                Status.class, RpcHeader.ErrorStatusPB.class);
        constructor.setAccessible(true);
        return constructor.newInstance(status, errPb);
    }
}
