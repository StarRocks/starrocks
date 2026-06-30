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

import com.google.common.collect.Lists;
import com.starrocks.common.ExceptionChecker;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testListPartitionsByFilter(@Mocked ThriftHiveMetastore.Iface client) throws TException {
        new MockUp<TSocket>() {
            @Mock
            public boolean isOpen() {
                return true;
            }
        };

        Partition partition = new Partition();
        partition.setValues(Lists.newArrayList("20260601"));

        new MockUp<ThriftHiveMetastore.Client>() {
            @Mock
            List<Partition> get_partitions_by_filter(String dbName, String tblName, String filter, short maxParts)
                    throws MetaException, NoSuchObjectException, TException {
                Assertions.assertEquals("db", dbName);
                Assertions.assertEquals("tbl", tblName);
                Assertions.assertEquals("dt >= '20260601' AND dt <= '20260606'", filter);
                Assertions.assertEquals((short) -1, maxParts);
                return Lists.newArrayList(partition);
            }
        };

        Configuration configuration = new HiveConf();
        configuration.set("metastore.thrift.uris", "thrift://127.0.0.1:1234");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(configuration);
        List<Partition> partitions = metaStoreClient.listPartitionsByFilter(
                "db", "tbl", "dt >= '20260601' AND dt <= '20260606'", (short) -1);
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("20260601", partitions.get(0).getValues().get(0));
    }

    @Test
    public void testListPartitionsByFilterPropagatesNoSuchObject(@Mocked ThriftHiveMetastore.Iface client)
            throws TException {
        new MockUp<TSocket>() {
            @Mock
            public boolean isOpen() {
                return true;
            }
        };

        new MockUp<ThriftHiveMetastore.Client>() {
            @Mock
            List<Partition> get_partitions_by_filter(String dbName, String tblName, String filter, short maxParts)
                    throws MetaException, NoSuchObjectException, TException {
                throw new NoSuchObjectException("Table not found");
            }
        };

        Configuration configuration = new HiveConf();
        configuration.set("metastore.thrift.uris", "thrift://127.0.0.1:1234");
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(configuration);
        ExceptionChecker.expectThrowsWithMsg(NoSuchObjectException.class,
                "Table not found",
                () -> metaStoreClient.listPartitionsByFilter("db", "missing_tbl", "dt = '20260601'", (short) -1));
    }
}
