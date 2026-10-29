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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.starrocks.service.arrow.flight.sql.ArrowFlightTestSupport.ping;
import static com.starrocks.service.arrow.flight.sql.ArrowFlightTestSupport.startPingServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArrowFlightSqlTlsConfigTest {

    @TempDir
    Path tempDir;

    private BufferAllocator allocator;
    private ArrowFlightTestSupport.PemFiles pem;
    private String cert;
    private String key;

    @BeforeEach
    public void setUp() throws Exception {
        allocator = new RootAllocator();
        pem = ArrowFlightTestSupport.generateSelfSignedCertificate(tempDir, "server");
        cert = pem.cert.toString();
        key = pem.key.toString();
    }

    @AfterEach
    public void tearDown() {
        Config.arrow_flight_ssl_enable = false;
        Config.arrow_flight_ssl_cert_file = "";
        Config.arrow_flight_ssl_key_file = "";
        Config.arrow_flight_ssl_ca_cert_file = "";
        Config.arrow_flight_ssl_require_client_auth = false;
        allocator.close();
    }

    @Test
    public void testFromConfigIsNullWhenDisabled() {
        Config.arrow_flight_ssl_enable = false;
        Config.arrow_flight_ssl_cert_file = "/does/not/matter";

        assertNull(ArrowFlightSqlTlsConfig.fromConfig());
    }

    @Test
    public void testFromConfigWhenEnabled() {
        Config.arrow_flight_ssl_enable = true;
        Config.arrow_flight_ssl_cert_file = cert;
        Config.arrow_flight_ssl_key_file = key;

        assertNotNull(ArrowFlightSqlTlsConfig.fromConfig());

        // enabling TLS without a certificate must not fall back to plaintext
        Config.arrow_flight_ssl_cert_file = "";
        assertThrows(IllegalArgumentException.class, ArrowFlightSqlTlsConfig::fromConfig);
    }

    @Test
    public void testCertificateAndKeyAreRequired() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig("", key, "", false));
        assertTrue(e.getMessage().contains("arrow_flight_ssl_cert_file must be set"), e.getMessage());

        e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig(cert, null, "", false));
        assertTrue(e.getMessage().contains("arrow_flight_ssl_key_file must be set"), e.getMessage());

        String missing = tempDir.resolve("missing.key").toString();
        e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig(cert, missing, "", false));
        assertTrue(e.getMessage().contains("arrow_flight_ssl_key_file '" + missing + "' is not a readable file"),
                e.getMessage());

        e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig(cert, key, missing, false));
        assertTrue(e.getMessage().contains("arrow_flight_ssl_ca_cert_file '" + missing + "' is not a readable file"),
                e.getMessage());
    }

    @Test
    public void testKeyMustBePkcs8() throws IOException {
        Path pkcs1 = tempDir.resolve("pkcs1.key");
        Files.writeString(pkcs1, "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig(cert, pkcs1.toString(), "", false));
        assertTrue(e.getMessage().contains("PKCS#8"), e.getMessage());
        assertTrue(e.getMessage().contains("openssl pkcs8 -topk8 -nocrypt"), e.getMessage());
    }

    @Test
    public void testClientAuthNeedsCaFile() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowFlightSqlTlsConfig(cert, key, "", true));
        assertTrue(e.getMessage().contains("arrow_flight_ssl_ca_cert_file must be set"), e.getMessage());
    }

    @Test
    public void testTls() throws Exception {
        // the self-signed certificate is its own CA
        ArrowFlightSqlTlsConfig tls = new ArrowFlightSqlTlsConfig(cert, key, cert, false);

        try (FlightServer server = startPingServer(allocator, tls);
                FlightClient client = tls.newClient(allocator, "localhost", server.getPort())) {
            assertEquals("pong", ping(client));

            // a plaintext client can't talk to a TLS server
            try (FlightClient plaintext = FlightClient.builder(allocator,
                    Location.forGrpcInsecure("localhost", server.getPort())).build()) {
                assertThrows(FlightRuntimeException.class, () -> ping(plaintext));
            }

            // a client that doesn't trust the certificate is refused
            try (FlightClient untrusting = FlightClient.builder(allocator,
                    Location.forGrpcTls("localhost", server.getPort())).build()) {
                assertThrows(FlightRuntimeException.class, () -> ping(untrusting));
            }
        }
    }

    @Test
    public void testMutualTls() throws Exception {
        ArrowFlightSqlTlsConfig mutual = new ArrowFlightSqlTlsConfig(cert, key, cert, true);

        try (FlightServer server = startPingServer(allocator, mutual);
                FlightClient client = mutual.newClient(allocator, "localhost", server.getPort())) {
            // the client presents the certificate of this FE
            assertEquals("pong", ping(client));

            // a client that trusts the server but presents no certificate is refused
            ArrowFlightSqlTlsConfig withoutClientCert = new ArrowFlightSqlTlsConfig(cert, key, cert, false);
            try (FlightClient anonymous = withoutClientCert.newClient(allocator, "localhost", server.getPort())) {
                assertThrows(FlightRuntimeException.class, () -> ping(anonymous));
            }
        }
    }

    // The files are read once. The server and the clients to other FEs keep using what was read, even if the files are
    // replaced or removed later: they all change together when the FE restarts.
    @Test
    public void testFilesAreReadOnce() throws Exception {
        ArrowFlightSqlTlsConfig mutual = new ArrowFlightSqlTlsConfig(cert, key, cert, true);
        Files.delete(pem.cert);
        Files.delete(pem.key);

        try (FlightServer server = startPingServer(allocator, mutual);
                FlightClient client = mutual.newClient(allocator, "localhost", server.getPort())) {
            assertEquals("pong", ping(client));
        }
    }
}
