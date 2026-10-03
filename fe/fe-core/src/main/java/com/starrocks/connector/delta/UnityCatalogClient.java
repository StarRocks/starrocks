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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.catalog.Database;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN;

/** A query-owned client. The token is captured after native authentication and is never refreshed here. */
public final class UnityCatalogClient implements IMetastore {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).followRedirects(HttpClient.Redirect.NEVER).build();

    private final UnityCatalogProperties profile;
    private final String authToken;
    private final HttpClient httpClient;

    public UnityCatalogClient(Map<String, String> properties, String authToken) {
        this(properties, authToken, HTTP_CLIENT);
    }

    public UnityCatalogClient(Map<String, String> properties, String authToken, HttpClient httpClient) {
        this.profile = new UnityCatalogProperties(properties);
        if (authToken == null || authToken.isBlank() || authToken.chars().anyMatch(c -> c <= 32 || c >= 127)) {
            throw new StarRocksConnectorException("Unity Catalog requires a native authenticated session auth token");
        }
        if (httpClient.followRedirects() != HttpClient.Redirect.NEVER) {
            throw new StarRocksConnectorException("Unity Catalog HTTP redirects must be disabled");
        }
        this.authToken = authToken;
        this.httpClient = httpClient;
    }

    @Override
    public List<String> getAllDatabaseNames() {
        return list("schemas", "catalog_name=" + encode(profile.getCatalogName()), null);
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        UnityCatalogProperties.validateName(dbName);
        return list("tables", "catalog_name=" + encode(profile.getCatalogName()) + "&schema_name=" + encode(dbName), dbName);
    }

    private List<String> list(String resource, String query, String dbName) {
        List<String> names = new ArrayList<>();
        Set<String> pages = new HashSet<>();
        String page = "";
        do {
            JsonNode response = request("GET", resource + "?" + query
                    + (page.isEmpty() ? "" : "&page_token=" + encode(page)), null, false);
            JsonNode entries = response.path(resource);
            if (!entries.isArray()) {
                throw invalidResponse();
            }
            for (JsonNode entry : entries) {
                requireEqual(entry, "catalog_name", profile.getCatalogName());
                if (dbName != null) {
                    requireEqual(entry, "schema_name", dbName);
                    if (!"DELTA".equals(entry.path("data_source_format").asText())) {
                        continue;
                    }
                }
                String name = required(entry, "name");
                UnityCatalogProperties.validateName(name);
                names.add(name);
            }
            JsonNode next = response.path("next_page_token");
            if (!next.isMissingNode() && !next.isNull() && !next.isTextual()) {
                throw invalidResponse();
            }
            page = next.asText("");
            if (!page.isEmpty() && (!pages.add(page) || pages.size() > 10_000)) {
                throw new StarRocksConnectorException("Unity Catalog pagination did not terminate");
            }
        } while (!page.isEmpty());
        return List.copyOf(names);
    }

    @Override
    public Database getDb(String dbName) {
        UnityCatalogProperties.validateName(dbName);
        JsonNode response = request("GET", "schemas/" + encode(profile.getCatalogName() + "." + dbName), null, true);
        if (response == null) {
            return null;
        }
        requireEqual(response, "catalog_name", profile.getCatalogName());
        requireEqual(response, "name", dbName);
        return new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName);
    }

    private JsonNode loadTable(String dbName, String tableName) {
        UnityCatalogProperties.validateName(dbName);
        UnityCatalogProperties.validateName(tableName);
        String fullName = profile.getCatalogName() + "." + dbName + "." + tableName;
        JsonNode response = request("GET", "tables/" + encode(fullName), null, true);
        if (response != null) {
            requireEqual(response, "catalog_name", profile.getCatalogName());
            requireEqual(response, "schema_name", dbName);
            requireEqual(response, "name", tableName);
            requireEqual(response, "data_source_format", "DELTA");
            if (response.has("full_name")) {
                requireEqual(response, "full_name", fullName);
            }
        }
        return response;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return loadTable(dbName, tableName) != null;
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        JsonNode table = loadTable(dbName, tableName);
        if (table == null) {
            return null;
        }
        String tableId = required(table, "table_id");
        URI location = UnityCatalogProperties.validateStorageLocation(required(table, "storage_location"));
        if (location.getPath().equals("/")) {
            throw new StarRocksConnectorException("Unity Catalog Delta tables require an ADLS directory location");
        }
        String body;
        try {
            body = MAPPER.writeValueAsString(Map.of("table_id", tableId, "operation", "READ"));
        } catch (IOException e) {
            throw new StarRocksConnectorException("Unable to construct Unity Catalog READ request");
        }
        JsonNode credentials = request("POST", "temporary-table-credentials", body, false);
        if (!location.equals(UnityCatalogProperties.validateStorageLocation(required(credentials, "url")))) {
            throw new StarRocksConnectorException("Unity Catalog READ credentials have a different storage location");
        }
        if (credentials.has("table_id")) {
            requireEqual(credentials, "table_id", tableId);
        }
        String sas = readSas(credentials, location);
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(
                Map.of(AZURE_ADLS2_ENDPOINT, location.getHost(), AZURE_ADLS2_SAS_TOKEN, sas));
        return new MetastoreTable(dbName, tableName, location.toASCIIString(), table.path("created_at").asLong(0) / 1000,
                cloudConfiguration);
    }

    private static String readSas(JsonNode response, URI location) {
        String sas = required(response.path("azure_user_delegation_sas"), "sas_token");
        if (sas.startsWith("?")) {
            sas = sas.substring(1);
        }
        try {
            long now = System.currentTimeMillis();
            JsonNode expiration = response.path("expiration_time");
            if (sas.chars().anyMatch(c -> c <= 32 || c >= 127 || c == '#')
                    || !expiration.isIntegralNumber() || !expiration.canConvertToLong() || expiration.longValue() <= now) {
                throw new IllegalArgumentException();
            }
            Map<String, String> fields = new HashMap<>();
            for (String pair : sas.split("&")) {
                String[] parts = pair.split("=", 2);
                if (parts.length != 2 || fields.put(URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(parts[1], StandardCharsets.UTF_8)) != null) {
                    throw new IllegalArgumentException();
                }
            }
            // Azure must enforce the table root for FE checkpoint reads as well as BE data reads.
            if (fields.getOrDefault("sig", "").isEmpty() || !Set.of("r", "rl").contains(fields.getOrDefault("sp", ""))
                    || !"d".equals(fields.get("sr"))
                    || Integer.parseInt(fields.getOrDefault("sdd", "0")) != location.getPath().substring(1).split("/").length
                    || !"https".equals(fields.getOrDefault("spr", "https"))
                    || !fields.containsKey("se") || Instant.parse(fields.get("se")).toEpochMilli() < expiration.longValue()
                    || (fields.containsKey("ske") && Instant.parse(fields.get("ske")).toEpochMilli() < expiration.longValue())
                    || (fields.containsKey("st") && Instant.parse(fields.get("st")).toEpochMilli() > now)) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException | DateTimeParseException | ArithmeticException e) {
            throw new StarRocksConnectorException("Unity Catalog returned invalid or expired READ SAS credentials");
        }
        return sas;
    }

    private JsonNode request(String method, String resource, String body, boolean allowMissing) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(profile.getUri() + "/" + resource))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Bearer " + authToken)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "UnityCatalog.request")) {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (allowMissing && response.statusCode() == 404) {
                return null;
            }
            if (response.statusCode() != 200) {
                throw new StarRocksConnectorException("Unity Catalog request failed (HTTP %s)", response.statusCode());
            }
            JsonNode result = MAPPER.readTree(response.body());
            if (result == null || !result.isObject()) {
                throw invalidResponse();
            }
            return result;
        } catch (IOException e) {
            // HTTP and JSON parser exceptions can contain the bearer token or SAS response body.
            throw new StarRocksConnectorException("Unity Catalog request or JSON response failed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException("Unity Catalog request interrupted");
        }
    }

    private static String required(JsonNode node, String key) {
        JsonNode value = node.path(key);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw invalidResponse();
        }
        return value.textValue();
    }

    private static void requireEqual(JsonNode node, String key, String expected) {
        if (!expected.equals(required(node, key))) {
            throw invalidResponse();
        }
    }

    private static StarRocksConnectorException invalidResponse() {
        return new StarRocksConnectorException("Unity Catalog returned invalid or unsupported metadata");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
