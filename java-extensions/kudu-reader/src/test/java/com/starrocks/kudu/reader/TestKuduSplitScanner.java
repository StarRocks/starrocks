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

package com.starrocks.kudu.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.starrocks.jni.connector.OffHeapTable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import static java.lang.String.format;

@Disabled("Unusable ut, it required docker env")
public class TestKuduSplitScanner {
    private TestingKuduServer testingKuduServer;
    private String masterAddress;
    private KuduClient client;
    private String kuduScanToken;
    private static final String TABLE_NAME = "test";
    private static final Schema SCHEMA = new Schema(Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("f0", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("f1", Type.STRING).key(true).build()
    ));

    @BeforeEach
    public void setUp() throws IOException {
        testingKuduServer = new TestingKuduServer();
        masterAddress = testingKuduServer.getMasterAddress();
        client = new KuduClientBuilder(masterAddress).build();
        mockTestingData(client);
        kuduScanToken = new String(Base64.getUrlEncoder().withoutPadding().encode(client.newScanTokenBuilder(
            client.openTable(TABLE_NAME)).build().get(0).serialize()), java.nio.charset.StandardCharsets.UTF_8);
    }

    @AfterEach
    public void setDown() {
        if (testingKuduServer != null) {
            testingKuduServer.close();
        }
    }

    @Test
    public void testScan() throws IOException {
        String requiredFields = "f0,f1";
        int fetchSize = 4096;

        HashMap<String, String> params = new HashMap<>(10);
        params.put("kudu_master", masterAddress);
        params.put("kudu_scan_token", kuduScanToken);
        params.put("required_fields", requiredFields);

        KuduSplitScanner scanner = new KuduSplitScanner(fetchSize, params);
        scanner.open();
        while (true) {
            scanner.getNextOffHeapChunk();
            OffHeapTable table = scanner.getOffHeapTable();
            if (table.getNumRows() == 0) {
                break;
            }
            table.show(10);
            table.checkTableMeta(true);
            table.close();
        }
        scanner.close();
    }

    public void mockTestingData(KuduClient client) {
        KuduSession session = null;
        try {
            CreateTableOptions createTableOptions = new CreateTableOptions()
                    .setNumReplicas(1)
                    .addHashPartitions(Collections.singletonList("f0"), 2);
            client.createTable(TABLE_NAME, SCHEMA, createTableOptions);
            session = client.newSession();
            KuduTable kuduTable = client.openTable(TABLE_NAME);
            for (int i = 0; i < 10; i++) {
                Insert insert = kuduTable.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, String.valueOf(i));
                session.apply(insert);
            }
            session.flush();
            session.close();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (KuduException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class TestingKuduServer implements Closeable  {
        private static final String KUDU_IMAGE = "apache/kudu";
        public static final String TAG = "1.15.0";
        private static final Integer KUDU_MASTER_PORT = 7051;
        private static final Integer KUDU_TSERVER_PORT = 7050;
        private static final Integer NUMBER_OF_REPLICA = 1;
        private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.4.0";
        private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
        private static final String KUDU_MASTER_HOST = "locahost";
        private final Network network;
        private final ToxiproxyContainer toxiProxy;
        private final GenericContainer<?> master;
        private final List<GenericContainer<?>> tServers;

        public TestingKuduServer() {
            network = Network.newNetwork();
            ImmutableList.Builder<GenericContainer<?>> tServersBuilder = ImmutableList.builder();

            String hostIP = getHostIPAddress();

            String masterContainerAlias = "kudu-master";
            this.master = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, TAG))
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withNetwork(network)
                .withNetworkAliases(masterContainerAlias);

            toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
            toxiProxy.start();

            for (int instance = 0; instance < NUMBER_OF_REPLICA; instance++) {
                String instanceName = "kudu-tserver-" + instance;
                ToxiproxyContainer.ContainerProxy proxy = toxiProxy.getProxy(instanceName, KUDU_TSERVER_PORT);
                GenericContainer<?> tableServer = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, TAG))
                        .withExposedPorts(KUDU_TSERVER_PORT)
                        .withCommand("tserver")
                        .withEnv("KUDU_MASTERS", format("%s:%s", masterContainerAlias, KUDU_MASTER_PORT))
                        .withEnv("TSERVER_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr " +
                            "--use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s",
                            instanceName, KUDU_TSERVER_PORT, hostIP, proxy.getProxyPort()))
                        .withNetwork(network)
                        .withNetworkAliases(instanceName)
                        .dependsOn(master);

                tServersBuilder.add(tableServer);
            }
            this.tServers = tServersBuilder.build();
            master.start();

            tServers.forEach(GenericContainer::start);
        }

        private static String getHostIPAddress() {
            try {
                Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
                while (networkInterfaceEnumeration.hasMoreElements()) {
                    for (InterfaceAddress interfaceAddress : networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
                        if (interfaceAddress.getAddress().isSiteLocalAddress() && interfaceAddress.getAddress()
                                instanceof Inet4Address) {
                            return interfaceAddress.getAddress().getHostAddress();
                        }
                    }
                }
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("Could not find site local ipv4 address, failed to launch kudu");
        }

        public String getMasterAddress() {
            return KUDU_MASTER_HOST + ":" + master.getMappedPort(KUDU_MASTER_PORT);
        }

        @Override
        public void close() {
            try (Closer closer = Closer.create()) {
                closer.register(master::stop);
                tServers.forEach(tabletServer -> closer.register(tabletServer::stop));
                closer.register(toxiProxy::stop);
                closer.register(network::close);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
