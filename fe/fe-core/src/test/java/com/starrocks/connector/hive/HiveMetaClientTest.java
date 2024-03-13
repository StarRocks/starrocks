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

import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HiveMetaClientTest {
    @Test
    public void testClientPool(@Mocked HiveMetaStoreClient metaStoreClient) throws Exception {
        new Expectations() {
            {
                metaStoreClient.getTable(anyString, anyString);
                result = new Table();
                minTimes = 0;
            }
        };

        final int[] clientNum = {0};

        new MockUp<RetryingMetaStoreClient>() {
            @Mock
            public IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
                                             ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName,
                                             boolean allowEmbedded) throws MetaException {
                clientNum[0]++;
                return metaStoreClient;
            }
        };

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:9030");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        // NOTE: this is HiveMetaClient.MAX_HMS_CONNECTION_POOL_SIZE
        int poolSize = 32;

        // call client method concurrently,
        // and make sure the number of hive clients will not exceed poolSize
        for (int i = 0; i < 10; i++) {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int j = 0; j < poolSize; j++) {
                es.execute(() -> {
                    try {
                        client.getTable("db", "tbl");
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail(e.getMessage());
                    }
                });
            }
            es.shutdown();
            es.awaitTermination(1, TimeUnit.HOURS);
        }
        System.out.println("called times is " + clientNum[0]);

        Assert.assertTrue(
                clientNum[0] >= 1 && clientNum[0] <= poolSize);
    }

    @Test
    public void testGetHiveClient() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:90303");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        try {
            client.getAllDatabaseNames();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to instantiate"));
        }
    }

    @Test
    public void testRecyclableClient(@Mocked HiveMetaStoreClient metaStoreClient) throws TException {
        new Expectations() {
            {
                metaStoreClient.getTable(anyString, anyString);
                result = new Exception("get table failed");
                minTimes = 0;
            }
        };

        new MockUp<RetryingMetaStoreClient>() {
            @Mock
            public IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
                                             ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName,
                                             boolean allowEmbedded) throws MetaException {
                return metaStoreClient;
            }
        };

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:90300");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        try {
            client.getTable("db", "tbl");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get table"));
            Assert.assertEquals(0, client.getClientSize());
        }

        new Expectations() {
            {
                metaStoreClient.getTable(anyString, anyString);
                result = new Table();
                minTimes = 0;
            }
        };

        client.getTable("db", "tbl");
        Assert.assertEquals(1, client.getClientSize());

        client.getTable("db", "tbl");
        Assert.assertEquals(1, client.getClientSize());

    }

    @Test
    public void testGetTextFileFormatDesc() {
        // Check is using default delimiter
        TextFileFormatDesc emptyDesc = HiveMetastoreApiConverter.toTextFileFormatDesc(new HashMap<>());
        Assert.assertNull(emptyDesc.getFieldDelim());
        Assert.assertNull(emptyDesc.getLineDelim());
        Assert.assertNull(emptyDesc.getCollectionDelim());
        Assert.assertNull(emptyDesc.getMapkeyDelim());

        // Check blank delimiter
        Map<String, String> blankParameters = new HashMap<>();
        blankParameters.put("field.delim", "");
        blankParameters.put("line.delim", "");
        blankParameters.put("collection.delim", "");
        blankParameters.put("mapkey.delim", "");
        TextFileFormatDesc blankDesc = HiveMetastoreApiConverter.toTextFileFormatDesc(blankParameters);
        Assert.assertNull(blankDesc.getFieldDelim());
        Assert.assertNull(blankDesc.getLineDelim());
        Assert.assertNull(blankDesc.getCollectionDelim());
        Assert.assertNull(blankDesc.getMapkeyDelim());

        // Check is using OpenCSVSerde
        Map<String, String> openCSVParameters = new HashMap<>();
        openCSVParameters.put("separatorChar", ",");
        TextFileFormatDesc openCSVDesc = HiveMetastoreApiConverter.toTextFileFormatDesc(openCSVParameters);
        Assert.assertEquals(",", openCSVDesc.getFieldDelim());
        Assert.assertNull(openCSVDesc.getLineDelim());
        Assert.assertNull(openCSVDesc.getCollectionDelim());
        Assert.assertNull(openCSVDesc.getMapkeyDelim());

        // Check is using custom delimiter
        Map<String, String> parameters = new HashMap<>();
        parameters.put("field.delim", ",");
        parameters.put("line.delim", "\004");
        parameters.put("collection.delim", "\006");
        parameters.put("mapkey.delim", ":");
        TextFileFormatDesc customDesc = HiveMetastoreApiConverter.toTextFileFormatDesc(parameters);
        Assert.assertEquals(",", customDesc.getFieldDelim());
        Assert.assertEquals("\004", customDesc.getLineDelim());
        Assert.assertEquals("\006", customDesc.getCollectionDelim());
        Assert.assertEquals(":", customDesc.getMapkeyDelim());
    }

    @Test
    public void testDropTable(@Mocked HiveMetaStoreClient metaStoreClient) throws TException {
        new Expectations() {
            {
                metaStoreClient.dropTable("hive_db", "hive_table", anyBoolean, anyBoolean);
                result = any;
            }
        };

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:90300");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        client.dropTable("hive_db", "hive_table");
    }

    @Test
    public void testTableExists(@Mocked HiveMetaStoreClient metaStoreClient) throws TException {
        new Expectations() {
            {
                metaStoreClient.tableExists("hive_db", "hive_table");
                result = true;
            }
        };
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:90300");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        Assert.assertTrue(client.tableExists("hive_db", "hive_table"));
    }

    @Test
    public void testForCoverage(@Mocked HiveMetaStoreClient metaStoreClient) throws TException {
        Partition partition = new Partition();
        String dbName = "hive_db";
        String tblName = "hive_table";

        new Expectations() {
            {
                metaStoreClient.alter_table(dbName, tblName, null);
                result = any;

                metaStoreClient.alter_partition(dbName, tblName, partition);
                result = any;

                metaStoreClient.listPartitionNames(dbName, tblName, (short) -1);
                result = any;

                metaStoreClient.listPartitionNames(dbName, tblName, new ArrayList<String>(), (short) -1);
                result = any;

                metaStoreClient.getPartitionsByNames(dbName, tblName, new ArrayList<>());
                result = new TException("something wrong");

                metaStoreClient.getPartitionsByNames(dbName, tblName, Arrays.asList("retry"));
                result = new TTransportException("something wrong");

                metaStoreClient.getTableColumnStatistics(dbName, tblName, new ArrayList<>());
                result = any;

                metaStoreClient.getPartitionColumnStatistics(dbName, tblName, new ArrayList<>(), new ArrayList<>());
                result = any;

                metaStoreClient.getNextNotification(0, 0, null);
                result = any;
            }
        };
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "thrift://127.0.0.1:90300");
        HiveMetaClient client = new HiveMetaClient(hiveConf);
        client.alterTable(dbName, tblName, null);
        client.alterPartition("hive_db", "hive_table", partition);
        client.getPartitionKeys(dbName, tblName);
        client.getPartitionKeysByValue(dbName, tblName, new ArrayList<String>());

        Assert.assertThrows(StarRocksConnectorException.class,
                () -> client.getPartitionsByNames(dbName, tblName, new ArrayList<>()));
        Assert.assertThrows(StarRocksConnectorException.class,
                () -> client.getPartitionsByNames(dbName, tblName, Arrays.asList("retry")));

        client.getTableColumnStats(dbName, tblName, new ArrayList<>());
        client.getPartitionColumnStats(dbName, tblName, new ArrayList<>(), new ArrayList<>());
        client.getNextNotification(0, 0, null);

    }
}

