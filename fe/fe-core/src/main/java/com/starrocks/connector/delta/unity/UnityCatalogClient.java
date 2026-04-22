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

package com.starrocks.connector.delta.unity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.exception.StarRocksConnectorException;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thin REST client for the Databricks Unity Catalog control plane.
 *
 * <p>Authentication is limited to Personal Access Tokens in v1: every request is sent with
 * {@code Authorization: Bearer <pat>}. The client paginates list endpoints transparently and
 * retries on 429 / 5xx with exponential backoff.</p>
 */
public class UnityCatalogClient {
    private static final Logger LOG = LogManager.getLogger(UnityCatalogClient.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PATH_SCHEMAS = "/api/2.1/unity-catalog/schemas";
    private static final String PATH_TABLES = "/api/2.1/unity-catalog/tables";
    private static final String PATH_TEMP_CREDS = "/api/2.1/unity-catalog/temporary-table-credentials";
    private static final int PAGE_SIZE = 500;

    private final String baseUrl;
    private final String bearerToken;
    private final OkHttpClient httpClient;
    private final int maxRetries;

    public UnityCatalogClient(UnityCatalogProperties properties) {
        this(properties.getHost(), properties.getToken(),
                defaultHttpClient(properties.getRequestTimeoutMs()),
                properties.getMaxRetries());
    }

    // Visible for testing.
    public UnityCatalogClient(String baseUrl, String bearerToken, OkHttpClient httpClient, int maxRetries) {
        this.baseUrl = baseUrl;
        this.bearerToken = bearerToken;
        this.httpClient = httpClient;
        this.maxRetries = Math.max(0, maxRetries);
    }

    private static OkHttpClient defaultHttpClient(long timeoutMs) {
        return new OkHttpClient.Builder()
                .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .writeTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .build();
    }

    public List<UnityCatalogTypes.Schema> listSchemas(String ucCatalog) {
        List<UnityCatalogTypes.Schema> result = new ArrayList<>();
        String pageToken = null;
        do {
            HttpUrl.Builder urlBuilder = resolve(PATH_SCHEMAS).newBuilder()
                    .addQueryParameter("catalog_name", ucCatalog)
                    .addQueryParameter("max_results", Integer.toString(PAGE_SIZE));
            if (pageToken != null) {
                urlBuilder.addQueryParameter("page_token", pageToken);
            }
            UnityCatalogTypes.ListSchemasResponse resp = executeGet(urlBuilder.build(),
                    UnityCatalogTypes.ListSchemasResponse.class);
            if (resp.schemas != null) {
                result.addAll(resp.schemas);
            }
            pageToken = emptyToNull(resp.nextPageToken);
        } while (pageToken != null);
        return result;
    }

    public List<UnityCatalogTypes.TableSummary> listTables(String ucCatalog, String schemaName) {
        List<UnityCatalogTypes.TableSummary> result = new ArrayList<>();
        String pageToken = null;
        do {
            HttpUrl.Builder urlBuilder = resolve(PATH_TABLES).newBuilder()
                    .addQueryParameter("catalog_name", ucCatalog)
                    .addQueryParameter("schema_name", schemaName)
                    .addQueryParameter("max_results", Integer.toString(PAGE_SIZE));
            if (pageToken != null) {
                urlBuilder.addQueryParameter("page_token", pageToken);
            }
            UnityCatalogTypes.ListTablesResponse resp = executeGet(urlBuilder.build(),
                    UnityCatalogTypes.ListTablesResponse.class);
            if (resp.tables != null) {
                result.addAll(resp.tables);
            }
            pageToken = emptyToNull(resp.nextPageToken);
        } while (pageToken != null);
        return result;
    }

    public UnityCatalogTypes.TableInfo getTable(String fullName) {
        HttpUrl url = resolve(PATH_TABLES + "/" + encodePathSegment(fullName));
        return executeGet(url, UnityCatalogTypes.TableInfo.class);
    }

    public boolean tableExists(String fullName) {
        HttpUrl url = resolve(PATH_TABLES + "/" + encodePathSegment(fullName));
        Request request = baseRequest(url).get().build();
        try (Response response = executeWithRetry(request)) {
            if (response.code() == 404) {
                return false;
            }
            if (!response.isSuccessful()) {
                throw new StarRocksConnectorException("Unity Catalog GET %s failed with status %d: %s",
                        url, response.code(), safeReadBody(response));
            }
            return true;
        } catch (IOException e) {
            throw new StarRocksConnectorException("Unity Catalog GET %s failed: %s", url, e.getMessage());
        }
    }

    public UnityCatalogTypes.TemporaryTableCredentials getTemporaryTableCredentials(String tableId, String operation) {
        HttpUrl url = resolve(PATH_TEMP_CREDS);
        UnityCatalogTypes.TemporaryTableCredentialsRequest payload =
                new UnityCatalogTypes.TemporaryTableCredentialsRequest(tableId, operation);
        String body;
        try {
            body = MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new StarRocksConnectorException("Failed to serialize temporary credentials request: %s",
                    e.getMessage());
        }
        Request request = baseRequest(url)
                .post(RequestBody.create(body, JSON))
                .build();
        try (Response response = executeWithRetry(request)) {
            if (!response.isSuccessful()) {
                throw new StarRocksConnectorException(
                        "Unity Catalog POST %s for table_id=%s failed with status %d: %s",
                        url, tableId, response.code(), safeReadBody(response));
            }
            return parseBody(response, UnityCatalogTypes.TemporaryTableCredentials.class);
        } catch (IOException e) {
            throw new StarRocksConnectorException("Unity Catalog POST %s failed: %s", url, e.getMessage());
        }
    }

    private <T> T executeGet(HttpUrl url, Class<T> type) {
        Request request = baseRequest(url).get().build();
        try (Response response = executeWithRetry(request)) {
            if (!response.isSuccessful()) {
                throw new StarRocksConnectorException("Unity Catalog GET %s failed with status %d: %s",
                        url, response.code(), safeReadBody(response));
            }
            return parseBody(response, type);
        } catch (IOException e) {
            throw new StarRocksConnectorException("Unity Catalog GET %s failed: %s", url, e.getMessage());
        }
    }

    private Response executeWithRetry(Request request) throws IOException {
        IOException lastIo = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                try {
                    long backoffMs = Math.min(2000L, (1L << (attempt - 1)) * 200L);
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying Unity Catalog request", ie);
                }
            }
            Response response;
            try {
                response = httpClient.newCall(request).execute();
            } catch (IOException io) {
                lastIo = io;
                LOG.warn("Unity Catalog request failed (attempt {} of {}): {}", attempt + 1, maxRetries + 1,
                        io.getMessage());
                continue;
            }
            if (isRetryable(response.code()) && attempt < maxRetries) {
                LOG.warn("Unity Catalog request returned status {} (attempt {} of {}), retrying",
                        response.code(), attempt + 1, maxRetries + 1);
                response.close();
                continue;
            }
            return response;
        }
        if (lastIo != null) {
            throw lastIo;
        }
        throw new IOException("Unity Catalog request exhausted retries without a response");
    }

    private Request.Builder baseRequest(HttpUrl url) {
        return new Request.Builder()
                .url(url)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .header(HttpHeaders.ACCEPT, "application/json")
                .header(HttpHeaders.USER_AGENT, "StarRocks-DeltaLake-Unity-Connector");
    }

    private HttpUrl resolve(String path) {
        HttpUrl parsed = HttpUrl.parse(baseUrl + path);
        if (parsed == null) {
            throw new StarRocksConnectorException("Invalid Unity Catalog URL: %s%s", baseUrl, path);
        }
        return parsed;
    }

    private static String encodePathSegment(String segment) {
        // HttpUrl.Builder.addPathSegments splits on '/', which would mangle the 3-level full name;
        // encode manually so "catalog.schema.table" is one segment.
        return segment.replace(" ", "%20");
    }

    private static <T> T parseBody(Response response, Class<T> type) throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            throw new StarRocksConnectorException("Unity Catalog response body was null for %s",
                    response.request().url());
        }
        String raw = body.string();
        try {
            return MAPPER.readValue(raw, type);
        } catch (JsonProcessingException e) {
            throw new StarRocksConnectorException("Failed to parse Unity Catalog response from %s: %s",
                    response.request().url(), e.getMessage());
        }
    }

    private static String safeReadBody(Response response) {
        try (ResponseBody body = response.body()) {
            if (body == null) {
                return "";
            }
            String raw = body.string();
            return raw.length() > 512 ? raw.substring(0, 512) + "..." : raw;
        } catch (IOException e) {
            return "<unreadable body: " + e.getMessage() + ">";
        }
    }

    private static boolean isRetryable(int statusCode) {
        return statusCode == 429 || (statusCode >= 500 && statusCode < 600);
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    // Visible for testing.
    public static List<String> supportedReadOperations() {
        return ImmutableList.of("READ", "READ_WRITE");
    }
}
