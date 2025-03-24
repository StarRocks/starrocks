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
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.Statistics;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.starrocks.catalog.KuduTableTest.genColumnSchema;
import static com.starrocks.catalog.ScalarType.CATALOG_MAX_VARCHAR_LENGTH;

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

    @Before
    public void setUp() {
        this.tokens.add(token);
    }

    @After
    public void setDown() throws KuduException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testGetTable(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty());
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
        Assert.assertEquals("test_kudu_catalog", kuduTable.getCatalogName());
        Assert.assertEquals("db1", kuduTable.getCatalogDBName());
        Assert.assertEquals("tbl1", kuduTable.getCatalogTableName());
        Assert.assertEquals(2, kuduTable.getColumns().size());
        Assert.assertEquals(0, kuduTable.getPartitionColumnNames().size());
        Assert.assertEquals(ScalarType.INT, kuduTable.getColumns().get(0).getType());
        Assert.assertTrue(kuduTable.getBaseSchema().get(0).isAllowNull());
        Assert.assertEquals(ScalarType.createVarcharType(CATALOG_MAX_VARCHAR_LENGTH),
                kuduTable.getBaseSchema().get(1).getType());
        Assert.assertTrue(kuduTable.getBaseSchema().get(1).isAllowNull());
    }

    @Test
    public void testListTableNames() throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER,
                true, SCHEMA_EMULATION_PREFIX, Optional.empty());
        List<String> tableNames = Lists.newArrayList("impala::db1.tbl1", "db1.tbl1");
        new Expectations() {
            {
                client.getTablesList().getTablesList();
                result = tableNames;
            }
        };
        List<String> tables = metadata.listTableNames(new ConnectContext(), "db1");
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("tbl1", tables.get(0));
    }

    @Test
    public void testGetRemoteFiles(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws KuduException {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty());
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
        Assert.assertEquals(1, remoteFileInfos.size());
        Assert.assertEquals(1, remoteFileInfos.get(0).getFiles().size());
        KuduRemoteFileDesc desc = (KuduRemoteFileDesc) remoteFileInfos.get(0).getFiles().get(0);
        Assert.assertEquals(1, desc.getKuduScanTokens().size());
    }

    @Test
    public void testUnsupportedGetTableStatistics(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws Exception {
        KuduMetadata metadata = new KuduMetadata(KUDU_CATALOG, new HdfsEnvironment(), KUDU_MASTER, true,
                SCHEMA_EMULATION_PREFIX, Optional.empty());
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
                null, kuduTable, Collections.emptyMap(), Collections.emptyList(), null, -1, TableVersionRange.empty());
        Assert.assertEquals(1D, statistics.getOutputRowCount(), 0.01);
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
