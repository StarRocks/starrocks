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
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * TLS settings of the Arrow Flight SQL server of the FE, taken from the {@code arrow_flight_ssl_*} configs.
 *
 * <p>They secure the FE's own server and the clients the FE opens to other FEs when it forwards requests.
 * The Arrow Flight server of a BE is not covered, so the connections from the FE to a BE stay plaintext.
 *
 * <p>The certificate, key and CA files are read once, when the settings are created. The server and every client to
 * another FE use what was read then, until the FE restarts, even if the files are replaced or removed in between.
 */
public final class ArrowFlightSqlTlsConfig {
    private static final String PKCS8_PEM_HEADER = "-----BEGIN PRIVATE KEY-----";

    // The paths, for messages and the log, and the contents of the files
    private final String certFile;
    private final byte[] certPem;
    private final String keyFile;
    private final byte[] keyPem;
    // null if arrow_flight_ssl_ca_cert_file is not set
    private final String caCertFile;
    private final byte[] caCertPem;
    private final boolean requireClientAuth;

    /**
     * @throws IllegalArgumentException if a required setting is missing or a file can't be read
     */
    public ArrowFlightSqlTlsConfig(String certFile, String keyFile, String caCertFile, boolean requireClientAuth) {
        this.certFile = certFile;
        this.certPem = readPem("arrow_flight_ssl_cert_file", certFile);
        this.keyFile = keyFile;
        this.keyPem = readPem("arrow_flight_ssl_key_file", keyFile);
        checkPkcs8(keyFile, keyPem);
        if (caCertFile == null || caCertFile.isEmpty()) {
            if (requireClientAuth) {
                throw new IllegalArgumentException(
                        "arrow_flight_ssl_ca_cert_file must be set when arrow_flight_ssl_require_client_auth is true");
            }
            this.caCertFile = null;
            this.caCertPem = null;
        } else {
            this.caCertFile = caCertFile;
            this.caCertPem = readPem("arrow_flight_ssl_ca_cert_file", caCertFile);
        }
        this.requireClientAuth = requireClientAuth;
    }

    /**
     * @return the settings of the {@code arrow_flight_ssl_*} configs, or null if {@code arrow_flight_ssl_enable} is false
     * @throws IllegalArgumentException if TLS is enabled but the settings are incomplete or a file can't be read
     */
    public static ArrowFlightSqlTlsConfig fromConfig() {
        if (!Config.arrow_flight_ssl_enable) {
            return null;
        }
        return new ArrowFlightSqlTlsConfig(Config.arrow_flight_ssl_cert_file, Config.arrow_flight_ssl_key_file,
                Config.arrow_flight_ssl_ca_cert_file, Config.arrow_flight_ssl_require_client_auth);
    }

    /**
     * Makes the server use TLS, and verify client certificates against the CA certificates if client authentication is
     * required. The location of the server has to be a {@link Location#forGrpcTls} one.
     */
    public FlightServer.Builder configure(FlightServer.Builder builder) throws IOException {
        builder.useTls(new ByteArrayInputStream(certPem), new ByteArrayInputStream(keyPem));
        if (requireClientAuth) {
            builder.useMTlsClientVerification(new ByteArrayInputStream(caCertPem));
        }
        return builder;
    }

    /**
     * Opens a connection to the Arrow Flight SQL server of another FE. The server's certificate is verified against the
     * CA certificates, or the default trust store of the JVM if there are none. If client authentication is required,
     * this FE presents its own certificate.
     */
    public FlightClient newClient(BufferAllocator allocator, String host, int port) {
        FlightClient.Builder builder = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcTls(host, port));
        if (caCertPem != null) {
            builder.trustedCertificates(new ByteArrayInputStream(caCertPem));
        }
        if (requireClientAuth) {
            builder.clientCertificate(new ByteArrayInputStream(certPem), new ByteArrayInputStream(keyPem));
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "certFile=" + certFile + ", keyFile=" + keyFile + ", caCertFile=" + caCertFile
                + ", requireClientAuth=" + requireClientAuth;
    }

    private static byte[] readPem(String config, String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException(config + " must be set when arrow_flight_ssl_enable is true");
        }
        File file = new File(path);
        if (!file.isFile() || !file.canRead()) {
            throw new IllegalArgumentException(config + " '" + path + "' is not a readable file");
        }
        try {
            return Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't read " + config + " '" + path + "': " + e.getMessage(), e);
        }
    }

    // The gRPC server only reads PKCS#8 keys, and complains about anything else with an error that doesn't say why.
    private static void checkPkcs8(String keyFile, byte[] keyPem) {
        if (!new String(keyPem, StandardCharsets.ISO_8859_1).contains(PKCS8_PEM_HEADER)) {
            throw new IllegalArgumentException("arrow_flight_ssl_key_file '" + keyFile
                    + "' is not an unencrypted PKCS#8 PEM private key (\"BEGIN PRIVATE KEY\"). Convert it with: "
                    + "openssl pkcs8 -topk8 -nocrypt -in <key file> -out <pkcs8 key file>");
        }
    }
}
