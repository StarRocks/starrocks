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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.common.Config;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArrowFlightSqlServiceTest {
    @TempDir
    Path tempDir;

    @Test
    public void testDisable() {
        ArrowFlightSqlService service = new ArrowFlightSqlService(-1);
        service.start();
        service.stop();
    }

    /**
     * Mock {@link FlightServer.Builder#build()}.
     */
    @Test
    public void testEnable(@Mocked FlightServer server) throws IOException, InterruptedException {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        new Expectations() {
            {
                server.start();
                result = server;
                times = 1;
            }

            {
                server.shutdown();
                times = 1;
            }

            {
                server.awaitTermination();
                times = 1;
            }

            {
                server.awaitTermination(anyLong, TimeUnit.SECONDS);
                result = true;
                times = 1;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);
        service.start();
        service.stop();
    }

    /**
     * The header limit is what decides whether a JWT listing many groups gets through at all: gRPC's
     * own default is 8 KB and applies before any authenticator runs. Asserts the configured value is
     * what reaches the server builder, both the default and an override.
     */
    @Test
    public void testHeaderLimitComesFromTheConfig() {
        List<Integer> passed = new ArrayList<>();
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer.Builder setMaxHeaderListSize(Invocation invocation, int size) {
                passed.add(size);
                return invocation.getInvokedInstance();
            }

            // A real builder, so the call above is intercepted on the instance the service
            // actually configures; nothing is started, so no server object is needed.
            @Mock
            public FlightServer build() {
                return null;
            }
        };

        int saved = Config.arrow_flight_max_header_size;
        try {
            new ArrowFlightSqlService(1234);
            Config.arrow_flight_max_header_size = 65536;
            new ArrowFlightSqlService(1234);
        } finally {
            Config.arrow_flight_max_header_size = saved;
        }
        Assertions.assertEquals(List.of(32768, 65536), passed);
    }

    // A TLS setup that can't be used must stop the FE from starting, not leave it serving plaintext.
    @Test
    public void testTlsWithoutCertificateFails() {
        Config.arrow_flight_ssl_enable = true;
        try {
            assertThrows(IllegalArgumentException.class, () -> new ArrowFlightSqlService(1234));
        } finally {
            Config.arrow_flight_ssl_enable = false;
        }
    }

    @Test
    public void testTls() throws Exception {
        ArrowFlightTestSupport.PemFiles pem = ArrowFlightTestSupport.generateSelfSignedCertificate(tempDir, "fe");
        Config.arrow_flight_ssl_enable = true;
        Config.arrow_flight_ssl_cert_file = pem.cert.toString();
        Config.arrow_flight_ssl_key_file = pem.key.toString();
        Config.arrow_flight_ssl_ca_cert_file = pem.cert.toString();
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        ArrowFlightSqlService service = new ArrowFlightSqlService(port);
        Thread serverThread = new Thread(service::start);
        serverThread.start();
        try (BufferAllocator allocator = new RootAllocator()) {
            long deadline = System.currentTimeMillis() + 30_000;
            while (!service.running && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(service.running);

            // Over TLS the call reaches the application, which wants a login
            try (FlightClient client = ArrowFlightSqlTlsConfig.fromConfig().newClient(allocator, "localhost", port)) {
                FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                        () -> client.listActions().forEach(action -> { }));
                assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
            }
            // A plaintext client doesn't get that far
            try (FlightClient plaintext = FlightClient.builder(allocator,
                    Location.forGrpcInsecure("localhost", port)).build()) {
                FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                        () -> plaintext.listActions().forEach(action -> { }));
                assertNotEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
            }
        } finally {
            service.stop();
            serverThread.join(10_000);
            Config.arrow_flight_ssl_enable = false;
            Config.arrow_flight_ssl_cert_file = "";
            Config.arrow_flight_ssl_key_file = "";
            Config.arrow_flight_ssl_ca_cert_file = "";
        }
    }
}
