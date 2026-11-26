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

package com.starrocks.rpc;

import com.starrocks.common.Config;
import com.starrocks.common.ThriftServer;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TBinaryLiteral;
import com.starrocks.thrift.TGetTabletsInfoRequest;
import com.starrocks.thrift.TGetTabletsInfoResult;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class ConfigurableTSocketTest {
    private int savedMaxMessageSize;

    @BeforeEach
    void setUp() {
        savedMaxMessageSize = Config.thrift_max_message_size;
    }

    @AfterEach
    void tearDown() {
        Config.thrift_max_message_size = savedMaxMessageSize;
    }

    private static Map<Long, TTablet> buildTabletsMap(int count) {
        Map<Long, TTablet> map = new HashMap<>(count * 2);
        for (int i = 0; i < count; i++) {
            long tabletId = 800_000_000L + i;
            TTabletInfo info = new TTabletInfo(tabletId, 10001, 70L, 0L, 100L, 1024L);
            map.put(tabletId, new TTablet(Collections.singletonList(info)));
        }
        return map;
    }

    private static Map<Long, TTabletStat> buildTabletStatsMap(int count) {
        Map<Long, TTabletStat> map = new HashMap<>(count * 2);
        for (int i = 0; i < count; i++) {
            long tabletId = 800_000_000L + i;
            TTabletStat stat = new TTabletStat(tabletId);
            stat.setData_size(4096L);
            stat.setRow_num(128L);
            stat.setVersion_count(2L);
            map.put(tabletId, stat);
        }
        return map;
    }

    @Test
    void testLargeBudgetAndUnlimitedConfig() throws Exception {
        long largeTabletMapMinBytes = 16_663_758L * 9L; // ~143 MiB (> 100 MiB default)
        try (ServerSocket ss = new ServerSocket(0)) {
            Thread acceptThread = new Thread(() -> {
                try (Socket ignored = ss.accept()) {
                    Thread.sleep(200);
                } catch (Exception ignored) {
                }
            });
            acceptThread.setDaemon(true);
            acceptThread.start();

            Config.thrift_max_message_size = 1024 * 1024 * 1024;
            try (ConfigurableTSocket sock = new ConfigurableTSocket("127.0.0.1", ss.getLocalPort(), 2000)) {
                sock.open();
                Assertions.assertDoesNotThrow(() -> sock.checkReadBytesAvailable(largeTabletMapMinBytes));

                Config.thrift_max_message_size = 100 * 1024 * 1024;
                Assertions.assertThrows(TTransportException.class,
                        () -> sock.checkReadBytesAvailable(largeTabletMapMinBytes));

                Config.thrift_max_message_size = -1;
                long beyondIntMax = 300_000_000L * 9L;
                Assertions.assertDoesNotThrow(() -> sock.checkReadBytesAvailable(beyondIntMax));
            }
        }
    }

    @Test
    void testBackendPoolHonorsDynamicMaxMessageSize() throws Exception {
        Map<Long, TTablet> tablets = buildTabletsMap(2_000);
        Map<Long, TTabletStat> stats = buildTabletStatsMap(2_000);

        BackendService.Iface handler = (BackendService.Iface) Proxy.newProxyInstance(
                BackendService.Iface.class.getClassLoader(),
                new Class<?>[] {BackendService.Iface.class},
                (proxy, method, args) -> {
                    if ("get_tablets_info".equals(method.getName())) {
                        TGetTabletsInfoResult res = new TGetTabletsInfoResult(new TStatus(TStatusCode.OK));
                        res.setReport_version(1L);
                        res.setTablets(tablets);
                        return res;
                    }
                    if ("get_tablet_stat".equals(method.getName())) {
                        TTabletStatResult res = new TTabletStatResult();
                        res.setTablets_stats(stats);
                        return res;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        TProcessor processor = new BackendService.Processor<>(handler);
        TServerSocket serverTransport = new TServerSocket(0);
        int port = serverTransport.getServerSocket().getLocalPort();
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                .processor(processor)
                .protocolFactory(new TBinaryProtocol.Factory()));
        Thread serverThread = new Thread(server::serve, "test-be-thrift");
        serverThread.setDaemon(true);
        serverThread.start();

        TNetworkAddress addr = new TNetworkAddress("127.0.0.1", port);
        try {
            Config.thrift_max_message_size = 8192;
            Assertions.assertThrows(TTransportException.class, () ->
                    ThriftRPCRequestExecutor.callNoRetry(ThriftConnectionPool.backendPool, addr,
                            c -> c.get_tablets_info(new TGetTabletsInfoRequest())));
            Assertions.assertThrows(TTransportException.class, () ->
                    ThriftRPCRequestExecutor.callNoRetry(ThriftConnectionPool.backendPool, addr,
                            BackendService.Client::get_tablet_stat));

            Config.thrift_max_message_size = 1024 * 1024;
            TGetTabletsInfoResult infoRes = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.backendPool, addr,
                    c -> c.get_tablets_info(new TGetTabletsInfoRequest()));
            Assertions.assertEquals(2_000, infoRes.getTabletsSize());

            TTabletStatResult statRes = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.backendPool, addr,
                    BackendService.Client::get_tablet_stat);
            Assertions.assertEquals(2_000, statRes.getTablets_statsSize());

            // Lower limit on reused pooled connection without clearing pool.
            Config.thrift_max_message_size = 8192;
            Assertions.assertThrows(TTransportException.class, () ->
                    ThriftRPCRequestExecutor.callNoRetry(ThriftConnectionPool.backendPool, addr,
                            c -> c.get_tablets_info(new TGetTabletsInfoRequest())));

            Config.thrift_max_message_size = -1;
            TGetTabletsInfoResult unlimitedRes = ThriftRPCRequestExecutor.callNoRetry(
                    ThriftConnectionPool.backendPool, addr,
                    c -> c.get_tablets_info(new TGetTabletsInfoRequest()));
            Assertions.assertEquals(2_000, unlimitedRes.getTabletsSize());
        } finally {
            ThriftConnectionPool.backendPool.clearPool(addr);
            server.stop();
        }
    }

    @Test
    void testThriftServerHonorsDynamicMaxMessageSize() throws Exception {
        AtomicInteger receivedTablets = new AtomicInteger();
        FrontendService.Iface feHandler = (FrontendService.Iface) Proxy.newProxyInstance(
                FrontendService.Iface.class.getClassLoader(),
                new Class<?>[] {FrontendService.Iface.class},
                (proxy, method, args) -> {
                    if ("report".equals(method.getName())) {
                        TReportRequest req = (TReportRequest) args[0];
                        receivedTablets.set(req.getTabletsSize());
                        return new TMasterResult(new TStatus(TStatusCode.OK));
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }

        ThriftServer feServer = new ThriftServer(port, new FrontendService.Processor<>(feHandler));
        feServer.start();
        Thread.sleep(100);

        TReportRequest reportReq = new TReportRequest();
        reportReq.setBackend(new TBackend("127.0.0.1", 9060, 8040));
        reportReq.setTablets(buildTabletsMap(2_000));

        try {
            Config.thrift_max_message_size = 8192;
            try (TSocket clientSock = new ConfigurableTSocket("127.0.0.1", port, 5000)) {
                clientSock.open();
                FrontendService.Client client = new FrontendService.Client(new TBinaryProtocol(clientSock));
                Assertions.assertThrows(TException.class, () -> client.report(reportReq));
            }

            Config.thrift_max_message_size = 1024 * 1024;
            try (TSocket clientSock = new ConfigurableTSocket("127.0.0.1", port, 5000)) {
                clientSock.open();
                FrontendService.Client client = new FrontendService.Client(new TBinaryProtocol(clientSock));
                TMasterResult res = client.report(reportReq);
                Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
                Assertions.assertEquals(2_000, receivedTablets.get());

                // Lower limit while the server thread is waiting on the persistent connection.
                Config.thrift_max_message_size = 8192;
                Assertions.assertThrows(TException.class, () -> client.report(reportReq));
            }

            Config.thrift_max_message_size = -1;
            receivedTablets.set(0);
            try (TSocket clientSock = new ConfigurableTSocket("127.0.0.1", port, 5000)) {
                clientSock.open();
                FrontendService.Client client = new FrontendService.Client(new TBinaryProtocol(clientSock));
                TMasterResult res = client.report(reportReq);
                Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatus_code());
                Assertions.assertEquals(2_000, receivedTablets.get());
            }
        } finally {
            feServer.stop();
        }
    }

    @Test
    void testUnlimitedSerDes() throws Exception {
        byte[] bytes = new byte[2_000_000];
        TBinaryLiteral literal = new TBinaryLiteral(ByteBuffer.wrap(bytes));
        Config.thrift_max_message_size = -1;
        AttachmentRequest request = new AttachmentRequest();
        request.setRequest(literal);
        TBinaryLiteral out = new TBinaryLiteral();
        request.getRequest(out);
        Assertions.assertEquals(2_000_000, out.value.remaining());
    }
}
