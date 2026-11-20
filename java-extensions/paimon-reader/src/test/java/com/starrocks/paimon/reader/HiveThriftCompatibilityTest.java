// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.paimon.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.TFilterTransport;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HiveThriftCompatibilityTest {
    @TempDir
    Path warehouse;

    @Test
    void hiveClientConstructorsCanBeResolved() {
        assertTrue(HiveMetaStoreClient.class.getDeclaredConstructors().length > 0);
    }

    @Test
    void filterTransportDelegatesThriftConfiguration() throws Exception {
        TMemoryBuffer wrapped = new TMemoryBuffer(128);
        TFilterTransport filter = new TFilterTransport(wrapped);
        assertSame(wrapped.getConfiguration(), filter.getConfiguration());
        filter.updateKnownMessageSize(32);
        filter.checkReadBytesAvailable(16);
    }

    @Test
    void hiveClientConnectsWithRandomUriSelection() throws Exception {
        checkHiveClient(false);
        checkHiveClient(true);
    }

    private void checkHiveClient(boolean framed) throws Exception {
        ThriftHiveMetastore.Iface metastore = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {ThriftHiveMetastore.Iface.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get_all_databases")) {
                        return List.of("test_db");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        TServerSocket socket = new TServerSocket(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()));
        TServer.Args serverArgs = new TServer.Args(socket).processor(new ThriftHiveMetastore.Processor<>(metastore));
        if (framed) {
            serverArgs.transportFactory(new TFramedTransport.Factory());
        }
        TServer server = new TSimpleServer(serverArgs);
        AtomicReference<TTransport> activeTransport = new AtomicReference<>();
        server.setServerEventHandler(new TServerEventHandler() {
            @Override
            public void preServe() {
            }

            @Override
            public ServerContext createContext(TProtocol input, TProtocol output) {
                activeTransport.set(input.getTransport());
                return new ServerContext() {
                    @Override
                    public <T> T unwrap(Class<T> type) {
                        return null;
                    }

                    @Override
                    public boolean isWrapperFor(Class<?> type) {
                        return false;
                    }
                };
            }

            @Override
            public void deleteContext(ServerContext context, TProtocol input, TProtocol output) {
            }

            @Override
            public void processContext(ServerContext context, TTransport input, TTransport output) {
            }
        });
        Thread serverThread = new Thread(server::serve, "test-hive-metastore");
        serverThread.setDaemon(true);
        serverThread.start();
        try {
            Configuration conf = new Configuration(false);
            MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS,
                    "thrift://localhost:" + socket.getServerSocket().getLocalPort());
            MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URI_SELECTION, "RANDOM");
            MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
            MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_FRAMED_TRANSPORT, framed);
            try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
                assertEquals(List.of("test_db"), client.getAllDatabases());
            }
            Options options = new Options();
            options.set("metastore", "hive");
            options.set("uri", "thrift://localhost:" + socket.getServerSocket().getLocalPort());
            options.set("warehouse", warehouse.toUri().toString());
            try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options, conf))) {
                assertEquals(List.of("test_db"), catalog.listDatabases());
            }
        } finally {
            server.stop();
            TTransport transport = activeTransport.get();
            if (transport != null) {
                transport.close();
            }
            socket.close();
            serverThread.join(5000);
        }
    }
}
