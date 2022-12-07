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


package com.starrocks.connector.iceberg.hive;

import com.starrocks.connector.hive.HiveMetaStoreThriftClient;
import com.starrocks.connector.iceberg.hive.HiveClientPool;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HiveClientPoolTest {

    private static final String HIVE_SITE_CONTENT = "<?xml version=\"1.0\"?>\n" +
            "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
            "<configuration>\n" +
            "  <property>\n" +
            "    <name>hive.metastore.sasl.enabled</name>\n" +
            "    <value>true</value>\n" +
            "  </property>\n" +
            "</configuration>\n";

    HiveClientPool clients;

    @Before
    public void before() {
        clients = new HiveClientPool(2, new Configuration());
    }

    @After
    public void after() {
        clients.close();
        clients = null;
    }

    @Test
    public void testConf() {
        HiveConf conf = createHiveConf();
        conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:/mywarehouse/");

        HiveClientPool clientPool = new HiveClientPool(10, conf);
        HiveConf clientConf = clientPool.hiveConf();

        Assert.assertEquals(conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
        Assert.assertEquals(10, clientPool.poolSize());

        // 'hive.metastore.sasl.enabled' should be 'true' as defined in xml
        Assert.assertEquals(conf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname),
                clientConf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));
        Assert.assertTrue(clientConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL));
    }

    private HiveConf createHiveConf() {
        HiveConf hiveConf = new HiveConf();
        try (InputStream inputStream = new ByteArrayInputStream(HIVE_SITE_CONTENT.getBytes(StandardCharsets.UTF_8))) {
            hiveConf.addResource(inputStream, "for_test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hiveConf;
    }

    @Test
    public void testNewClientFailure() {
        new MockUp<HiveClientPool>() {
            @Mock
            protected IMetaStoreClient newClient() {
                throw new RuntimeException("Connection exception");
            }
        };
        Assert.assertThrows("Connection exception", RuntimeException.class,
                () -> clients.run(Object::toString));
    }

    @Test
    public void testGetTablesFailsForNonReconnectableException(@Mocked HiveMetaStoreThriftClient hmsClient)
            throws Exception {
        new MockUp<HiveClientPool>() {
            @Mock
            protected IMetaStoreClient newClient() {
                return hmsClient;
            }
        };

        new Expectations() {
            {
                hmsClient.getTables(anyString, anyString);
                result = new MetaException("Another meta exception");
            }
        };

        Assert.assertThrows("Another meta exception", MetaException.class,
                () -> clients.run(client -> client.getTables("default", "t")));
    }

    @Test
    public void testConnectionFailureRestoreForMetaException(
            @Mocked HiveMetaStoreThriftClient hmsClient,
            @Mocked HiveMetaStoreThriftClient newClient) throws Exception {
        new MockUp<HiveClientPool>() {
            @Mock
            protected IMetaStoreClient newClient() {
                return hmsClient;
            }

            @Mock
            protected IMetaStoreClient reconnect(IMetaStoreClient client) {
                return newClient;
            }
        };

        List<String> databases = Lists.newArrayList("db1", "db2");
        new Expectations() {
            {
                hmsClient.getAllDatabases();
                result = new MetaException("Got exception: org.apache.thrift.transport.TTransportException");

                minTimes = 0;
                newClient.getAllDatabases();
                result = databases;
            }
        };

        Assert.assertEquals(databases, clients.run(client -> client.getAllDatabases(), true));
    }
}
