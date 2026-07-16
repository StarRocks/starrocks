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

package com.starrocks.connector.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives the real RESTSessionCatalog (no iceberg mocks) against an in-process REST catalog server
 * to verify the OAuth2 auth-session recovery end to end: the server starts rejecting the issued
 * bearer token with 401, and the catalog must transparently re-authenticate with the client
 * credential and answer the request.
 */
public class IcebergRESTCatalogAuthRecoveryHttpTest {

    private HttpServer server;
    private ConnectContext connectContext;

    private final AtomicInteger tokenSequence = new AtomicInteger();
    private final AtomicInteger tokenFetches = new AtomicInteger();
    private volatile String validToken;

    @BeforeEach
    public void setUp() throws IOException {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
            }
        };
        connectContext = new ConnectContext();

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/oauth/tokens", exchange -> {
            String token = "token-" + tokenSequence.incrementAndGet();
            validToken = token;
            tokenFetches.incrementAndGet();
            respond(exchange, 200,
                    "{\"access_token\":\"" + token + "\",\"token_type\":\"bearer\",\"expires_in\":3600}");
        });
        server.createContext("/v1/config", exchange -> respond(exchange, 200, "{\"defaults\":{},\"overrides\":{}}"));
        server.createContext("/v1/namespaces", exchange -> {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            String expected = validToken == null ? null : "Bearer " + validToken;
            if (expected == null || !expected.equals(auth)) {
                respond(exchange, 401,
                        "{\"error\":{\"message\":\"Not authorized\",\"type\":\"NotAuthorizedException\",\"code\":401}}");
            } else {
                respond(exchange, 200, "{\"namespaces\":[[\"db1\"]]}");
            }
        });
        server.start();
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Test
    public void testRecoversThroughRealClientWhenServerRejectsToken() {
        String uri = "http://127.0.0.1:" + server.getAddress().getPort();
        IcebergRESTCatalog catalog = new IcebergRESTCatalog("rest_catalog", new Configuration(),
                ImmutableMap.of(
                        "iceberg.catalog.uri", uri,
                        "iceberg.catalog.security", "oauth2",
                        "iceberg.catalog.oauth2.credential", "id:secret"));

        Assertions.assertEquals(List.of("db1"), catalog.listAllDatabases(connectContext));

        // the server rotates its signing state: every token issued so far is now rejected with 401,
        // while the client credential can still mint a fresh one
        validToken = null;

        Assertions.assertEquals(List.of("db1"), catalog.listAllDatabases(connectContext));
        Assertions.assertTrue(tokenFetches.get() >= 2,
                "recovery must have re-authenticated with the client credential, fetches=" + tokenFetches.get());
    }
}
