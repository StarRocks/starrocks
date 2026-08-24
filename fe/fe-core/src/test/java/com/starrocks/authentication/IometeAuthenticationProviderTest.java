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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.analyzer.SemanticException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class IometeAuthenticationProviderTest {
    private HttpServer server;

    @BeforeEach
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testSuccessfulAuthenticationSendsUserAndApiTokenToCore() {
        AtomicReference<String> requestMethod = new AtomicReference<>();
        AtomicReference<String> requestUser = new AtomicReference<>();
        AtomicReference<String> requestToken = new AtomicReference<>();
        server.createContext("/api/v1/authz/domains/hands-on/lakehouses/basic", exchange -> {
            requestMethod.set(exchange.getRequestMethod());
            requestUser.set(exchange.getRequestHeaders().getFirst("x-user-id"));
            requestToken.set(exchange.getRequestHeaders().getFirst("x-api-token"));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();

        IometeAuthenticationProvider provider = new IometeAuthenticationProvider(
                "http://127.0.0.1:" + server.getAddress().getPort(), "hands-on", "basic",
                java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(2));

        Assertions.assertDoesNotThrow(() -> provider.authenticate(
                new AccessControlContext(),
                new UserIdentity("alice", "%"),
                "api-token\u0000".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals("GET", requestMethod.get());
        Assertions.assertEquals("alice", requestUser.get());
        Assertions.assertEquals("api-token", requestToken.get());
    }

    @Test
    public void testNonSuccessfulAuthenticationIsRejected() {
        server.createContext("/api/v1/authz/domains/hands-on/lakehouses/basic", exchange -> {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
        });
        server.start();

        IometeAuthenticationProvider provider = new IometeAuthenticationProvider(
                "http://127.0.0.1:" + server.getAddress().getPort(), "hands-on", "basic",
                java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(2));

        Assertions.assertThrows(AuthenticationException.class, () -> provider.authenticate(
                new AccessControlContext(),
                new UserIdentity("alice", "%"),
                "wrong\u0000".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testSecurityIntegrationAndClientMapping() {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_IOMETE.name());
        properties.put(IometeSecurityIntegration.SERVER_URL, "https://core.example.com");
        properties.put(IometeSecurityIntegration.DOMAIN, "hands-on");
        properties.put(IometeSecurityIntegration.LAKEHOUSE, "basic");

        SecurityIntegration integration = SecurityIntegrationFactory.createSecurityIntegration("iomete", properties);
        Assertions.assertInstanceOf(IometeSecurityIntegration.class, integration);
        Assertions.assertEquals(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(),
                AuthPlugin.covertFromServerToClient(AuthPlugin.Server.AUTHENTICATION_IOMETE.toString()));
    }

    @Test
    public void testMissingServerUrlIsRejected() {
        IometeSecurityIntegration integration = new IometeSecurityIntegration("iomete", Map.of());
        Assertions.assertThrows(SemanticException.class, integration::checkProperty);
    }

    @Test
    public void testMissingDomainIsRejected() {
        IometeSecurityIntegration integration = new IometeSecurityIntegration("iomete", Map.of(
                IometeSecurityIntegration.SERVER_URL, "https://core.example.com",
                IometeSecurityIntegration.LAKEHOUSE, "basic"));
        Assertions.assertThrows(SemanticException.class, integration::checkProperty);
    }

    @Test
    public void testMissingLakehouseIsRejected() {
        IometeSecurityIntegration integration = new IometeSecurityIntegration("iomete", Map.of(
                IometeSecurityIntegration.SERVER_URL, "https://core.example.com",
                IometeSecurityIntegration.DOMAIN, "hands-on"));
        Assertions.assertThrows(SemanticException.class, integration::checkProperty);
    }
}
