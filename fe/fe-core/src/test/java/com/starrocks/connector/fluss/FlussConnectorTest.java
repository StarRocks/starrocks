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

package com.starrocks.connector.fluss;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FlussConnectorTest {
    private static final String CATALOG = "fluss_catalog";
    private static final String BOOTSTRAP_SERVERS = "localhost:9123";

    @Mocked
    private Connection connection;
    @Mocked
    private Admin admin;
    private Configuration capturedClientConf;

    @Test
    public void testMissingBootstrapServers() {
        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new FlussConnector(new ConnectorContext(CATALOG, "fluss", Map.of())));

        Assertions.assertEquals("The property bootstrap.servers must be set.", exception.getMessage());
    }

    @Test
    public void testPropertiesAndMetadata() {
        Map<String, String> properties = validProperties();
        FlussConnector connector = createConnector(properties);

        Map<String, String> clientConf = capturedClientConf.toMap();
        Assertions.assertEquals(BOOTSTRAP_SERVERS, clientConf.get(FlussConnector.BOOTSTRAP_SERVERS));
        Assertions.assertEquals("64mb", clientConf.get("client.writer.buffer-size"));
        Assertions.assertFalse(clientConf.containsKey("table.datalake.paimon.metastore"));
        Assertions.assertFalse(clientConf.containsKey("unrelated.option"));

        FlussMetadata metadata = Assertions.assertInstanceOf(FlussMetadata.class, connector.getMetadata());
        Configuration catalogConf = Deencapsulation.getField(metadata, "catalogConf");
        Assertions.assertEquals("filesystem", catalogConf.toMap().get("table.datalake.paimon.metastore"));
        Assertions.assertEquals("/tmp/fluss-warehouse",
                catalogConf.toMap().get("table.datalake.paimon.warehouse"));
        Assertions.assertFalse(catalogConf.containsKey("unrelated.option"));
    }

    @Test
    public void testShutdown() throws Exception {
        FlussConnector connector = createConnector(validProperties());

        connector.shutdown();

        new Verifications() {
            {
                admin.close();
                times = 1;
                connection.close();
                times = 1;
            }
        };
    }

    @Test
    public void testShutdownIgnoresCloseException() throws Exception {
        FlussConnector connector = createConnector(validProperties());
        new Expectations() {
            {
                admin.close();
                result = new RuntimeException("failed to close admin");
            }
        };

        Assertions.assertDoesNotThrow(connector::shutdown);
    }

    private FlussConnector createConnector(Map<String, String> properties) {
        Connection mockedConnection = connection;
        new MockUp<ConnectionFactory>() {
            @Mock
            public Connection createConnection(Configuration configuration) {
                capturedClientConf = new Configuration(configuration);
                return mockedConnection;
            }
        };
        new Expectations() {
            {
                connection.getAdmin();
                result = admin;
            }
        };

        return new FlussConnector(new ConnectorContext(CATALOG, "fluss", properties));
    }

    private static Map<String, String> validProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FlussConnector.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        properties.put("fluss.option.client.writer.buffer-size", "64mb");
        properties.put("table.datalake.paimon.metastore", "filesystem");
        properties.put("table.datalake.paimon.warehouse", "/tmp/fluss-warehouse");
        properties.put("unrelated.option", "ignored");
        return properties;
    }
}
