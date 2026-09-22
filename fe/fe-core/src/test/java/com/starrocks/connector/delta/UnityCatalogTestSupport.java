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

package com.starrocks.connector.delta;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

final class UnityCatalogTestSupport {
    static final String TOKEN = "synthetic.header.full-signature";
    static final String STORAGE = "abfss://container@account.dfs.core.windows.net";
    static final String SAS_KEY = "fs.azure.sas.fixed.token.account.dfs.core.windows.net";

    private UnityCatalogTestSupport() {
    }

    static String sas(String signature) {
        return sas(signature, STORAGE + "/alpha");
    }

    static String sas(String signature, String location) {
        int depth = URI.create(location).getPath().substring(1).split("/").length;
        return "sv=2023-11-03&sp=rl&sr=d&sdd=" + depth + "&spr=https&se=2099-01-01T00%3A00%3A00Z&sig=" + signature;
    }

    static JsonObject table(String name, String location) {
        JsonObject table = new JsonObject();
        table.addProperty("name", name);
        table.addProperty("schema_name", "schema");
        table.addProperty("catalog_name", "main");
        table.addProperty("full_name", "main.schema." + name);
        table.addProperty("table_id", "id-" + name);
        table.addProperty("data_source_format", "DELTA");
        table.addProperty("storage_location", location);
        table.addProperty("created_at", 1700000000000L);
        return table;
    }

    static JsonObject credentials(String name, String location, String sas) {
        JsonObject credentials = new JsonObject();
        credentials.addProperty("table_id", "id-" + name);
        credentials.addProperty("url", location);
        credentials.addProperty("expiration_time", Instant.parse("2099-01-01T00:00:00Z").toEpochMilli());
        JsonObject azure = new JsonObject();
        azure.addProperty("sas_token", sas);
        credentials.add("azure_user_delegation_sas", azure);
        return credentials;
    }

    static final class FakeCatalog implements AutoCloseable {
        final List<Request> requests = new CopyOnWriteArrayList<>();
        private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
        private final HttpServer server;

        FakeCatalog() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/2.1/unity-catalog", this::handle);
            server.start();
        }

        Map<String, String> properties() {
            return Map.of("hive.metastore.type", "unity",
                    "unity.catalog.uri", "http://127.0.0.1:" + server.getAddress().getPort() + "/api/2.1/unity-catalog",
                    "unity.catalog.name", "main");
        }

        void enqueue(int status, String body) {
            replies.add(new Reply(status, body));
        }

        void enqueueTable(String name, String location, String sas) {
            enqueue(200, table(name, location).toString());
            enqueue(200, credentials(name, location, sas).toString());
        }

        List<String> authorizations() {
            List<String> authorizations = new ArrayList<>();
            requests.forEach(request -> authorizations.add(request.authorization()));
            return authorizations;
        }

        private void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                Reply reply = replies.poll();
                if (reply == null) {
                    reply = new Reply(500, "Unexpected Catalog request");
                }
                if (reply.status() == 302) {
                    exchange.getResponseHeaders().add("Location", properties().get("unity.catalog.uri") + "/redirected");
                }
                byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), body.length);
                exchange.getResponseBody().write(body);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    record Request(String method, URI uri, String authorization, String body) {
    }

    private record Reply(int status, String body) {
    }
}
