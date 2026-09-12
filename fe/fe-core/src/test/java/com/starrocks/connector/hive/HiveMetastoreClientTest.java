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

package com.starrocks.connector.hive;

import com.starrocks.common.ExceptionChecker;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.util.List;

public class HiveMetastoreClientTest {
    @Test
    public void testGetTableError(@Mocked ThriftHiveMetastore.Iface client) throws TException {
        new MockUp<TSocket>() {
            @Mock
            public boolean isOpen() {
                return true;
            }
        };

        new MockUp<ThriftHiveMetastore.Client>() {
            @Mock
            Table get_table(String dbName, String tblName) throws NoSuchObjectException {
                throw new NoSuchObjectException("Table not found");
            }
        };

        Configuration configuration = new HiveConf();
        configuration.set("metastore.thrift.uris", "thrift://127.0.0.1:1234");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(configuration);
        ExceptionChecker.expectThrowsWithMsg(NoSuchObjectException.class,
                "Table not found",
                () -> metaStoreClient.getTable("db", "table"));
    }

    @Test
    public void testReconnectBeforeGetTableReqFallback(@Mocked ThriftHiveMetastore.Iface client) throws TException {
        new MockUp<TSocket>() {
            @Mock
            public boolean isOpen() {
                return true;
            }
        };

        boolean[] reconnected = {false};
        boolean[] reqRanAfterReconnect = {false};

        new MockUp<ThriftHiveMetastore.Client>() {
            @Mock
            Table get_table(String dbName, String tblName) throws TException {
                // get_table() read times out, leaving the connection in an unknown state.
                throw new TTransportException(new SocketTimeoutException("Read timed out"));
            }

            @Mock
            GetTableResult get_table_req(GetTableRequest req) {
                reqRanAfterReconnect[0] = reconnected[0];
                GetTableResult result = new GetTableResult();
                result.setTable(new Table());
                return result;
            }
        };

        new MockUp<HiveMetaStoreClient>() {
            @Mock
            public void reconnect() {
                reconnected[0] = true;
            }
        };

        Configuration configuration = new HiveConf();
        configuration.set("metastore.thrift.uris", "thrift://127.0.0.1:1234");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(configuration);

        Table table = metaStoreClient.getTable("db", "table");
        Assertions.assertNotNull(table);
        // The fallback must reconnect before issuing get_table_req on a fresh connection;
        // otherwise get_table_req reads get_table's stale response and the Thrift seqid desyncs.
        Assertions.assertTrue(reconnected[0], "reconnect() should be invoked before the get_table_req fallback");
        Assertions.assertTrue(reqRanAfterReconnect[0], "get_table_req() must run after reconnect()");
    }

    @Test
    public void testListPartitionsByFilter(@Mocked ThriftHiveMetastore.Iface client) throws Exception {
        new MockUp<TSocket>() {
            @Mock
            public boolean isOpen() {
                return true;
            }
        };

        new MockUp<ThriftHiveMetastore.Client>() {
            @Mock
            public List<Partition> get_partitions_by_filter(
                    String dbName, String tableName, String filter, short maxParts) {
                Partition partition = new Partition();
                partition.setValues(List.of("2026-05-25", "3091", "app_create"));
                return List.of(partition);
            }
        };

        Configuration configuration = new HiveConf();
        configuration.set("metastore.thrift.uris", "thrift://127.0.0.1:1234");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(configuration);
        List<Partition> partitions = metaStoreClient.listPartitionsByFilter(
                "dw", "dw_log_511", "s_date >= \"2026-05-25\"", (short) -1);

        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("2026-05-25", partitions.get(0).getValues().get(0));
    }
}
