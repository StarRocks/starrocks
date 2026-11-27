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

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helpers for the tests that talk TLS to a real in-process Arrow Flight server. They don't touch Arrow buffers, which the
 * test JVM can't allocate.
 */
final class ArrowFlightTestSupport {
    private static final String STORE_PASSWORD = "changeit";

    private ArrowFlightTestSupport() {
    }

    /** A certificate and its private key, as PEM files. */
    static final class PemFiles {
        final Path cert;
        final Path key;

        PemFiles(Path cert, Path key) {
            this.cert = cert;
            this.key = key;
        }
    }

    /**
     * Creates a self-signed certificate for localhost and 127.0.0.1 with the JDK's keytool, and writes it and its key
     * (PKCS#8) as PEM files, so that no private key has to be checked in. Being self-signed, it is its own CA.
     */
    static PemFiles generateSelfSignedCertificate(Path dir, String name) throws Exception {
        Path keystore = dir.resolve(name + ".p12");
        String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "flight", "-keyalg", "RSA",
                "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", keystore.toString(), "-storepass", STORE_PASSWORD)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "keytool failed: " + output);

        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            store.load(in, STORE_PASSWORD.toCharArray());
        }
        Path cert = dir.resolve(name + ".crt");
        Path key = dir.resolve(name + ".key");
        Files.writeString(cert, pem("CERTIFICATE", store.getCertificate("flight").getEncoded()));
        Files.writeString(key, pem("PRIVATE KEY", store.getKey("flight", STORE_PASSWORD.toCharArray()).getEncoded()));
        return new PemFiles(cert, key);
    }

    /** Starts a server on a free port of localhost that answers the action "ping" with "pong". */
    static FlightServer startPingServer(BufferAllocator allocator, ArrowFlightSqlTlsConfig tls) throws IOException {
        NoOpFlightProducer producer = new NoOpFlightProducer() {
            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                listener.onNext(new Result("pong".getBytes(StandardCharsets.UTF_8)));
                listener.onCompleted();
            }
        };
        if (tls == null) {
            return FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), producer).build().start();
        }
        return tls.configure(FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0), producer))
                .build().start();
    }

    /** Sends "ping" and returns the answer. Throws a FlightRuntimeException if the connection can't be made. */
    static String ping(FlightClient client) {
        Result result = client.doAction(new Action("ping")).next();
        return new String(result.getBody(), StandardCharsets.UTF_8);
    }

    /**
     * A stand-in for another FE. It only speaks TLS, records every call it gets (the RPC and the Authorization header),
     * and answers just enough for the forwarding code of {@link ArrowFlightSqlServiceImpl} to succeed. A result stream is
     * answered with an error, because sending one needs Arrow buffers.
     */
    static final class FakeFe implements AutoCloseable {
        static final String NO_SUCH_RESULT = "no such result on the fake FE";

        private static final FlightServerMiddleware RECORDED = new FlightServerMiddleware() {
            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
            }

            @Override
            public void onCallCompleted(CallStatus status) {
            }

            @Override
            public void onCallErrored(Throwable err) {
            }
        };

        /** The calls received, in order, such as "DO_ACTION Bearer token". */
        final List<String> calls = new CopyOnWriteArrayList<>();
        /** The types of the actions received. */
        final List<String> actions = new CopyOnWriteArrayList<>();
        private final FlightServer server;

        FakeFe(BufferAllocator allocator, ArrowFlightSqlTlsConfig tls) throws IOException {
            NoOpFlightProducer producer = new NoOpFlightProducer() {
                @Override
                public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                    return new FlightInfo(new Schema(Collections.emptyList()), descriptor, Collections.emptyList(), -1, -1);
                }

                @Override
                public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                    listener.error(CallStatus.NOT_FOUND.withDescription(NO_SUCH_RESULT).toRuntimeException());
                }

                @Override
                public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                    actions.add(action.getType());
                    byte[] body;
                    if (FlightConstants.CLOSE_SESSION.getType().equals(action.getType())) {
                        body = remaining(new CloseSessionResult(CloseSessionResult.Status.CLOSED).serialize());
                    } else if (FlightConstants.SET_SESSION_OPTIONS.getType().equals(action.getType())) {
                        body = remaining(new SetSessionOptionsResult(Collections.emptyMap()).serialize());
                    } else {
                        body = "ok".getBytes(StandardCharsets.UTF_8);
                    }
                    listener.onNext(new Result(body));
                    listener.onCompleted();
                }
            };
            FlightServer.Builder builder = FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0), producer)
                    .middleware(FlightServerMiddleware.Key.<FlightServerMiddleware>of("recorder"),
                            (info, headers, context) -> {
                                calls.add(info.method() + " " + headers.get("authorization"));
                                return RECORDED;
                            });
            server = tls.configure(builder).build().start();
        }

        int getPort() {
            return server.getPort();
        }

        @Override
        public void close() throws Exception {
            server.close();
        }
    }

    private static byte[] remaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
