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

import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static com.starrocks.service.arrow.flight.sql.ArrowFlightTestSupport.ping;
import static com.starrocks.service.arrow.flight.sql.ArrowFlightTestSupport.startPingServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * The TLS settings of the FE apply to the connections it opens to other FEs, and not to the ones to a BE, whose
 * Arrow Flight server doesn't speak TLS.
 */
public class ArrowFlightSqlServiceImplTlsTest {

    @TempDir
    Path tempDir;

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void testOnlyFePeersUseTls() throws Exception {
        ArrowFlightTestSupport.PemFiles pem = ArrowFlightTestSupport.generateSelfSignedCertificate(tempDir, "fe");
        ArrowFlightSqlTlsConfig tls = new ArrowFlightSqlTlsConfig(
                pem.cert.toString(), pem.key.toString(), pem.cert.toString(), false);
        ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(
                mock(ArrowFlightSqlSessionManager.class), Location.forGrpcTls("localhost", 1234), tls);

        try (FlightServer fe = startPingServer(allocator, tls);
                FlightServer be = startPingServer(allocator, null)) {
            try (FlightClient toFe = service.createClient("localhost", fe.getPort(), true)) {
                assertEquals("pong", ping(toFe));
            }
            try (FlightClient toBe = service.createClient("localhost", be.getPort(), false)) {
                assertEquals("pong", ping(toBe));
            }
            // a BE is never reached over TLS, even if TLS is enabled
            try (FlightClient plaintext = service.createClient("localhost", fe.getPort(), false)) {
                assertThrows(FlightRuntimeException.class, () -> ping(plaintext));
            }
        } finally {
            service.close();
        }
    }

    @Test
    public void testPeersArePlaintextWithoutTls() throws Exception {
        ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(
                mock(ArrowFlightSqlSessionManager.class), Location.forGrpcInsecure("localhost", 1234));

        try (FlightServer fe = startPingServer(allocator, null);
                FlightClient toFe = service.createClient("localhost", fe.getPort(), true)) {
            assertEquals("pong", ping(toFe));
        } finally {
            service.close();
        }
    }
}
