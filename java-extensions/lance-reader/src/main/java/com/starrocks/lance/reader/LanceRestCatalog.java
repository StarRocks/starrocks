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

package com.starrocks.lance.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.Timeout;
import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.NamespaceApi;
import org.lance.namespace.client.apache.api.TableApi;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Standard Lance Namespace REST APIs, isolated from FE's Arrow and HTTP dependencies. */
public final class LanceRestCatalog implements LanceNamespace, Closeable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DELIMITER = "\u001f";
    private static final int MAX_PAGES = 10000;
    private final CloseableHttpClient http;
    private final NamespaceApi namespaces;
    private final TableApi tables;
    private final String tokenFile;
    private final List<String> tableId;
    private final String namespaceId = "starrocks-rest-" + UUID.randomUUID();
    private volatile String expectedLocation;

    public LanceRestCatalog(String baseUri, String tokenFile, List<String> tableId, String expectedLocation) {
        try {
            URI uri = URI.create(baseUri);
            if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                    || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Lance REST catalog requires an HTTP(S) URL without credentials");
        }
        this.tokenFile = tokenFile;
        this.tableId = List.copyOf(tableId);
        this.expectedLocation = expectedLocation;
        http = HttpClients.custom().disableRedirectHandling().disableAutomaticRetries()
                .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(Timeout.ofSeconds(10))
                        .setResponseTimeout(Timeout.ofSeconds(30)).build()).build();
        ApiClient api = new ApiClient(http).setBasePath(baseUri.replaceAll("/+$", ""));
        namespaces = new NamespaceApi(api);
        tables = new TableApi(api);
    }

    private Map<String, String> headers() {
        if (tokenFile == null || tokenFile.isEmpty()) {
            return Map.of();
        }
        try {
            // Read on every request, including credential refresh, so mounted tokens can rotate.
            String token = Files.readString(Path.of(tokenFile)).trim();
            if (token.isEmpty() || token.contains("\n") || token.contains("\r")) {
                throw new IOException();
            }
            return Map.of("Authorization", "Bearer " + token);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read the Lance REST bearer token file");
        }
    }

    private static String identifier(List<String> id) {
        if (id.stream().anyMatch(part -> part == null || part.isEmpty() || part.contains(DELIMITER))) {
            throw new IllegalArgumentException("Invalid Lance namespace identifier");
        }
        return id.isEmpty() ? DELIMITER : String.join(DELIMITER, id);
    }

    public static List<String> parseIdentifier(String json) {
        try {
            var node = JSON.readTree(json);
            if (!node.isArray()) {
                throw new IllegalArgumentException();
            }
            List<String> result = new ArrayList<>();
            for (var part : node) {
                if (!part.isTextual()) {
                    throw new IllegalArgumentException();
                }
                result.add(part.textValue());
            }
            identifier(result);
            return List.copyOf(result);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Lance namespace identifier");
        }
    }

    private List<String> listChildren(List<String> id, boolean listTables) {
        Set<String> result = new LinkedHashSet<>();
        Set<String> tokens = new HashSet<>();
        String token = null;
        for (int page = 0; page < MAX_PAGES; page++) {
            Set<String> names;
            try {
                if (listTables) {
                    ListTablesResponse response = namespaces.listTables(
                            identifier(id), DELIMITER, token, 1000, false, headers());
                    names = response.getTables();
                    token = response.getPageToken();
                } else {
                    ListNamespacesResponse response = namespaces.listNamespaces(
                            identifier(id), DELIMITER, token, 1000, headers());
                    names = response.getNamespaces();
                    token = response.getPageToken();
                }
            } catch (ApiException e) {
                throw requestFailed(e);
            }
            if (names == null) {
                throw new IllegalStateException("Lance REST catalog returned an invalid listing");
            }
            for (String name : names) {
                identifier(List.of(name));
                result.add(name);
            }
            if (token == null || token.isEmpty()) {
                return List.copyOf(result);
            }
            if (!tokens.add(token)) {
                throw new IllegalStateException("Lance REST catalog repeated a pagination token");
            }
        }
        throw new IllegalStateException("Lance REST catalog exceeded the pagination limit");
    }

    /** Include the root and empty namespaces, then traverse each namespace's child list. */
    public static String listNamespaces(String uri, String tokenFile) throws IOException {
        try (LanceRestCatalog client = new LanceRestCatalog(uri, tokenFile, List.of(), null)) {
            List<List<String>> result = new ArrayList<>();
            result.add(List.of());
            for (int i = 0; i < result.size(); i++) {
                List<String> parent = result.get(i);
                for (String child : client.listChildren(parent, false)) {
                    if (parent.size() >= 64 || result.size() >= 10000) {
                        throw new IllegalStateException("Lance REST namespace traversal exceeded its limit");
                    }
                    List<String> id = new ArrayList<>(parent);
                    id.add(child);
                    result.add(List.copyOf(id));
                }
            }
            return JSON.writeValueAsString(result);
        }
    }

    public static String listTables(String uri, String tokenFile, String namespaceJson) throws IOException {
        try (LanceRestCatalog client = new LanceRestCatalog(uri, tokenFile, List.of(), null)) {
            return JSON.writeValueAsString(client.listChildren(parseIdentifier(namespaceJson), true));
        }
    }

    public static String loadTable(String uri, String tokenFile, String tableIdJson) {
        List<String> id = parseIdentifier(tableIdJson);
        try (LanceRestCatalog client = new LanceRestCatalog(uri, tokenFile, id, null);
                Dataset dataset = Dataset.open().namespaceClient(client).tableId(id).build()) {
            return JSON.writeValueAsString(Map.of("location", client.expectedLocation,
                    "schema", dataset.getSchema().toJson(), "version", dataset.version()));
        } catch (Exception e) {
            // Neither storage credentials nor native signed-URL errors may cross the FE bridge.
            throw new IllegalStateException("Failed to read Lance schema from the REST catalog dataset");
        }
    }

    @Override
    public synchronized DescribeTableResponse describeTable(DescribeTableRequest request) {
        if (tableId.isEmpty() || !tableId.equals(request.getId())) {
            throw new IllegalArgumentException("Unexpected Lance table credential request");
        }
        try {
            DescribeTableResponse response = tables.describeTable(identifier(tableId),
                    new DescribeTableRequest().id(tableId).vendCredentials(true), DELIMITER, false, false, false, headers());
            String location = response.getLocation();
            if (location == null || location.isBlank()) {
                throw new IllegalStateException("Lance REST catalog returned no table location");
            }
            try {
                URI locationUri = URI.create(location);
                // In ADLS URIs the user-info component is the container, not a credential.
                boolean adls = "abfs".equalsIgnoreCase(locationUri.getScheme())
                        || "abfss".equalsIgnoreCase(locationUri.getScheme());
                boolean credentialUserInfo = locationUri.getUserInfo() != null
                        && (!adls || locationUri.getUserInfo().contains(":"));
                if (credentialUserInfo || locationUri.getRawQuery() != null
                        || locationUri.getRawFragment() != null) {
                    throw new IllegalArgumentException();
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Lance table location must not contain credentials or query parameters");
            }
            if ((response.getTable() != null && !response.getTable().equals(tableId.get(tableId.size() - 1)))
                    || (response.getNamespace() != null && !response.getNamespace().isEmpty()
                    && !response.getNamespace().equals(tableId.subList(0, tableId.size() - 1)))) {
                throw new IllegalStateException("Lance REST catalog returned a different table identifier");
            }
            if (expectedLocation != null && !expectedLocation.equals(location)) {
                throw new IllegalStateException("Lance table location changed; replan the query");
            }
            if (Boolean.TRUE.equals(response.getManagedVersioning())) {
                throw new IllegalStateException("Lance managed versioning is not supported by this connector");
            }
            expectedLocation = location;
            // Pass all standard storage_options through. Lance handles expiry and refresh callbacks.
            return response;
        } catch (ApiException e) {
            throw requestFailed(e);
        }
    }

    private static IllegalStateException requestFailed(ApiException e) {
        return new IllegalStateException("Lance REST catalog request failed (HTTP " + e.getCode() + ")");
    }

    @Override
    public String namespaceId() {
        return namespaceId;
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
        throw new UnsupportedOperationException("Use the configured Lance REST catalog constructor");
    }

    @Override
    public void close() throws IOException {
        http.close();
    }
}
