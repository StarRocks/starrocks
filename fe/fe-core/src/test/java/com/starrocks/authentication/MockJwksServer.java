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

package com.starrocks.authentication;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal in-process HTTP endpoint that serves a JWKS document, used to exercise the real {@code
 * JWKSourceBuilder} remote-fetch + cache path in {@link JwkMgr#getJwkSource(String)}.
 *
 * <p>The existing {@code MockTokenUtils.MockJwkMgr} only redirects {@code getJwkSet} to a local
 * file, so it never goes through the Nimbus cache. JWKS cache behaviour (TTL / refresh-ahead /
 * outage tolerance / rate limiting, §5.8) can only be tested by pointing a {@code jwks_url} at a
 * real {@code http://} endpoint — that is what this server provides.
 *
 * <p>Capabilities used by the cache tests:
 * <ul>
 *   <li>{@link #fetchCount()} — number of times the JWKS body was actually served (200), to assert
 *       that N authentications only trigger one fetch (cache hit).</li>
 *   <li>{@link #setBody(String)} — hot-swap the served JWKS, to simulate key rotation.</li>
 *   <li>{@link #setMode(Mode)} — make the endpoint fail (503) or drop the connection, to simulate
 *       an IdP outage and verify outage-tolerant serving from the stale cache.</li>
 * </ul>
 *
 * <p>Binds to {@code 127.0.0.1} on an ephemeral port so it never collides on a shared box.
 */
public class MockJwksServer implements AutoCloseable {

    public enum Mode {
        /** Serve the current body with 200. */
        SERVE,
        /** Respond 503 Service Unavailable (simulate IdP down). */
        FAIL_503,
        /** Close the connection without a response (simulate a hard network failure). */
        DROP
    }

    private final HttpServer server;
    private final int port;
    private final AtomicInteger fetchCount = new AtomicInteger(0);
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private volatile String body;
    private volatile Mode mode = Mode.SERVE;

    public MockJwksServer(String initialBody) throws IOException {
        this.body = initialBody;
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.port = server.getAddress().getPort();
        server.createContext("/jwks", exchange -> {
            requestCount.incrementAndGet();
            try {
                Mode current = mode;
                if (current == Mode.DROP) {
                    exchange.close();
                    return;
                }
                if (current == Mode.FAIL_503) {
                    byte[] msg = "service unavailable".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(503, msg.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(msg);
                    }
                    return;
                }
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                fetchCount.incrementAndGet();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(payload);
                }
            } catch (IOException ignored) {
                // best-effort test server; nothing to recover
            } finally {
                exchange.close();
            }
        });
        server.setExecutor(null);
        server.start();
    }

    /** The JWKS URL to configure as a SI {@code jwks_url}. */
    public String url() {
        return "http://127.0.0.1:" + port + "/jwks";
    }

    /** Number of times the JWKS body was successfully served (200). */
    public int fetchCount() {
        return fetchCount.get();
    }

    /** Number of requests that reached the handler, including failed ones. */
    public int requestCount() {
        return requestCount.get();
    }

    /** Replace the served JWKS document (simulate key rotation). */
    public void setBody(String newBody) {
        this.body = newBody;
    }

    /** Switch the failure mode (simulate IdP outage). */
    public void setMode(Mode newMode) {
        this.mode = newMode;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
