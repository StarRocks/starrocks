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

package com.starrocks.connector.starrocks;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.http.HttpUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStarRocksScanTransport;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Client for the remote StarRocks FE catalog control plane, served over HTTP/JSON.
 *
 * <p>The control plane (capabilities, list databases/tables, get table, prepare /
 * start remote scan, batch session cleanup) used to ride the FE thrift port, which
 * is internal-facing and typically not exposed across cluster boundaries. It now
 * speaks HTTP — or HTTPS, per the configured URL scheme, for remote FEs published
 * behind a TLS proxy — to the remote FE http port, the same surface the Spark
 * connector and the catalog statistics endpoints already use. The data plane
 * (BE-to-BE chunk fetch over BRPC / Arrow Flight) is unchanged.
 *
 * <p>Requests and responses are the plain JSON wire DTOs ({@link StarRocksRemoteScanWire})
 * — there is deliberately no client-side mirror of them. Callers that need thrift or
 * catalog representations convert at their own use sites with the converter functions
 * next to the DTOs ({@code toDto}/{@code toThrift}/{@code toDomain}/{@link #toColumns}).
 */
public class StarRocksFeClient {
    private static final Logger LOG = LogManager.getLogger(StarRocksFeClient.class);
    private static final Gson GSON = new Gson();

    private static final String CONTROL_PREFIX = "/api/_starrocks_remote";

    private final String feHttpUrl;
    // Normalized "scheme://host:port" base URLs, parsed (and validated) once at construction.
    private final List<String> feEndpoints;
    private final String scanTransport;
    private final String feUser;
    private final String fePassword;
    private final int httpTimeoutMs;
    private final int httpRetryTimes;

    public StarRocksFeClient(StarRocksConnectorConfig config) {
        this(config.getFeHttpUrl(), config.getScanTransport(), config.getFeUser(), config.getFePassword(),
                config.getFeHttpTimeoutMs(), config.getFeHttpRetryTimes());
    }

    /**
     * Build a client from the named catalog's CURRENT properties. ALTER CATALOG updates the
     * Catalog config in place, so resolving by name at use time always sees the latest
     * settings — this is why connection settings deliberately do not live on table objects.
     */
    public static StarRocksFeClient fromCatalog(String catalogName) {
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        if (catalog == null) {
            throw new StarRocksConnectorException(
                    "starrocks catalog '%s' does not exist or has been dropped", catalogName);
        }
        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(catalog.getConfig());
        return new StarRocksFeClient(config);
    }

    public StarRocksFeClient(String feHttpUrl, String scanTransport) {
        this(feHttpUrl, scanTransport, "", "");
    }

    public StarRocksFeClient(String feHttpUrl, String scanTransport, String feUser, String fePassword) {
        this(feHttpUrl, scanTransport, feUser, fePassword, 10000, 3);
    }

    public StarRocksFeClient(String feHttpUrl, String scanTransport, String feUser, String fePassword,
                             long httpTimeoutMs, int httpRetryTimes) {
        this.feHttpUrl = feHttpUrl.endsWith("/") ? feHttpUrl.substring(0, feHttpUrl.length() - 1) : feHttpUrl;
        this.scanTransport = scanTransport;
        this.feUser = feUser == null ? "" : feUser;
        this.fePassword = fePassword == null ? "" : fePassword;
        this.httpTimeoutMs = Math.toIntExact(Math.max(1, Math.min(Integer.MAX_VALUE, httpTimeoutMs)));
        this.httpRetryTimes = Math.max(0, httpRetryTimes);
        // Parse (and validate) the endpoint list once up front: a misconfigured URL fails
        // fast, and every request reuses the parsed endpoints instead of re-splitting the URL.
        this.feEndpoints = ImmutableList.copyOf(parseFeAddresses(this.feHttpUrl));
    }

    public String getFeHttpUrl() {
        return feHttpUrl;
    }

    public String getScanTransport() {
        return scanTransport;
    }

    public String getFeUser() {
        return feUser;
    }

    public String getFePassword() {
        return fePassword;
    }

    public long getFeHttpTimeoutMs() {
        return httpTimeoutMs;
    }

    public int getFeHttpRetryTimes() {
        return httpRetryTimes;
    }

    public StarRocksRemoteScanWire.CapabilitiesResponse getCapabilities() {
        String body = httpGet(CONTROL_PREFIX + "/capabilities");
        StarRocksRemoteScanWire.CapabilitiesResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.CapabilitiesResponse.class);
        ensureOk(response, "remote StarRocks FE failed to get capabilities");
        return response;
    }

    /**
     * Endpoint A: table statistics snapshot with epoch-gated conditional fetch.
     * Returns null when the remote does not serve the endpoint (degradation).
     */
    public StarRocksRemoteTableStats.Snapshot fetchTableStatsSnapshot(
            String dbName, String tableName, StarRocksRemoteTableStats.Epochs cachedEpochs) {
        StringBuilder query = new StringBuilder();
        if (cachedEpochs != null) {
            appendQueryParam(query, "cached_list_epoch", cachedEpochs.list);
            appendQueryParam(query, "cached_data_epoch", cachedEpochs.data);
            appendQueryParam(query, "cached_analyze_epoch", cachedEpochs.analyze);
        }
        String path = "/api/" + urlEncode(dbName) + "/" + urlEncode(tableName) + "/_sr_catalog_stats_snapshot"
                + (query.length() == 0 ? "" : "?" + query);
        String body;
        try {
            body = httpGet(path);
        } catch (Exception e) {
            // Statistics are best-effort: an unreachable endpoint degrades to no stats.
            return null;
        }
        StarRocksRemoteTableStats.Snapshot snapshot =
                GSON.fromJson(body, StarRocksRemoteTableStats.Snapshot.class);
        if (snapshot == null || snapshot.status != 200 || snapshot.epochs == null) {
            LOG.warn("remote FE returned invalid stats snapshot for {}.{}: {}", dbName, tableName,
                    snapshot == null ? "null" : snapshot.exception);
            return null;
        }
        return snapshot;
    }

    /**
     * Endpoint B: batch per-partition column statistics. Returns null when the
     * remote does not serve the endpoint (degradation).
     */
    public StarRocksRemoteTableStats.PartitionStatsResponse fetchPartitionColumnStats(
            String dbName, String tableName, List<Long> partitionIds, List<String> columns) {
        StarRocksRemoteTableStats.PartitionStatsRequest request =
                new StarRocksRemoteTableStats.PartitionStatsRequest();
        request.partitionIds = partitionIds;
        request.columns = columns;
        String path = "/api/" + urlEncode(dbName) + "/" + urlEncode(tableName) + "/_sr_catalog_partition_stats";
        String body;
        try {
            body = httpPost(path, GSON.toJson(request));
        } catch (Exception e) {
            // Statistics are best-effort: an unreachable endpoint degrades to no stats.
            return null;
        }
        StarRocksRemoteTableStats.PartitionStatsResponse response =
                GSON.fromJson(body, StarRocksRemoteTableStats.PartitionStatsResponse.class);
        if (response == null || response.status != 200) {
            LOG.warn("remote FE returned invalid partition stats for {}.{}: {}", dbName, tableName,
                    response == null ? "null" : response.exception);
            return null;
        }
        return response;
    }

    public List<String> listDbNames() {
        String body = httpGet(CONTROL_PREFIX + "/databases");
        StarRocksRemoteScanWire.ListDatabasesResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.ListDatabasesResponse.class);
        ensureOk(response, "remote StarRocks FE failed to list databases");
        return response.databases == null ? Collections.emptyList() : response.databases;
    }

    public List<String> listTableNames(String dbName) {
        String body = httpGet(CONTROL_PREFIX + "/tables?db=" + urlEncode(dbName));
        StarRocksRemoteScanWire.ListTablesResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.ListTablesResponse.class);
        ensureOk(response, "remote StarRocks FE failed to list tables in database: " + dbName);
        return response.tables == null ? Collections.emptyList() : response.tables;
    }

    public StarRocksRemoteScanWire.Table getTable(String dbName, String tableName) {
        String body = httpGet(CONTROL_PREFIX + "/table?db=" + urlEncode(dbName) + "&table=" + urlEncode(tableName));
        StarRocksRemoteScanWire.GetTableResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.GetTableResponse.class);
        if (response == null) {
            throw new StarRocksConnectorException("remote StarRocks FE returned empty get table response");
        }
        if (response.status == 404) {
            return null;
        }
        ensureOk(response, "remote StarRocks FE failed to get table: " + dbName + "." + tableName);
        return response.table;
    }

    public StarRocksRemoteScanWire.PrepareScanResponse prepareRemoteScan(
            StarRocksRemoteScanWire.PrepareScanRequest request) {
        // The transport is a client (catalog) setting, not something callers choose per scan.
        request.transport = scanTransport;
        if (request.sessionVars != null && request.sessionVars.isEmpty()) {
            request.sessionVars = null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("prepareRemoteScan session={} db={} table={} pushdown_sql={}",
                    request.sessionId, request.db, request.table, request.pushdownPredicateSql);
        }
        String body = httpPost(CONTROL_PREFIX + "/prepare_scan", GSON.toJson(request));
        StarRocksRemoteScanWire.PrepareScanResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.PrepareScanResponse.class);
        ensureOk(response, "remote StarRocks FE failed to prepare remote scan");
        // Empty (or absent) streams is a legitimate response shape: the remote optimizer
        // collapsed the scan to EMPTYSET (e.g., empty source table or contradictory
        // predicate). The caller treats zero streams as zero scan ranges and the local BE
        // emits EOS without issuing start_scan or fetch_remote_scan_chunk.
        if (response.streams == null) {
            response.streams = Collections.emptyList();
        }
        if (response.streams.stream().anyMatch(stream ->
                Strings.isNullOrEmpty(stream.scanToken) || stream.remoteBe == null)) {
            throw new StarRocksConnectorException("remote StarRocks FE returned invalid prepare remote scan response");
        }
        return response;
    }

    public void startScanSession(String sessionId) {
        String errorMessage = "remote StarRocks FE failed to start remote scan session: " + sessionId;
        StarRocksRemoteScanWire.ScanControlRequest request = new StarRocksRemoteScanWire.ScanControlRequest();
        request.sessionId = sessionId;
        // Remote scan sessions live in the memory of the one remote FE that served
        // prepare_scan. The remote FEs are often behind a load balancer that routes each
        // request to a random FE, so session-addressed calls ask the receiving FE to look
        // the session up on its peer FEs when it is not local (forward_request=true; the
        // forwarded calls themselves carry forward_request=false — a single hop).
        String body = httpPost(CONTROL_PREFIX + "/start_scan?forward_request=true", GSON.toJson(request));
        StarRocksRemoteScanWire.SimpleResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.SimpleResponse.class);
        ensureOk(response, errorMessage);
    }

    public void batchCleanupScanSessions(List<StarRocksRemoteScanWire.CleanupItem> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        StarRocksRemoteScanWire.BatchCleanupRequest request = new StarRocksRemoteScanWire.BatchCleanupRequest();
        request.items = new ArrayList<>(items);
        // Session-addressed like start_scan: forward_request=true lets the receiving FE
        // broadcast the batch to its peer FEs for the sessions that are not local to it.
        String body = httpPost(CONTROL_PREFIX + "/cleanup_sessions?forward_request=true", GSON.toJson(request));
        StarRocksRemoteScanWire.SimpleResponse response =
                GSON.fromJson(body, StarRocksRemoteScanWire.SimpleResponse.class);
        ensureOk(response, "remote StarRocks FE failed to batch cleanup scan sessions");
    }

    public static Map<String, String> buildRemoteSessionVars(SessionVariable sessionVariable) {
        if (sessionVariable == null) {
            return Collections.emptyMap();
        }
        Map<String, String> variables = new LinkedHashMap<>();
        variables.put(SessionVariable.QUERY_TIMEOUT, String.valueOf(sessionVariable.getQueryTimeoutS()));
        variables.put(SessionVariable.TIME_ZONE, sessionVariable.getTimeZone());
        variables.put(SessionVariable.ENABLE_STRICT_TYPE, String.valueOf(sessionVariable.isEnableStrictType()));
        return variables;
    }

    public TStarRocksScanTransport toThriftTransport() {
        if (StarRocksConnectorConfig.TRANSPORT_BRPC_CHUNK.equals(scanTransport)) {
            return TStarRocksScanTransport.STARROCKS_BRPC_CHUNK;
        }
        return TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT;
    }

    // ---- HTTP plumbing -----------------------------------------------------

    /**
     * A multi-endpoint list is its own retry budget — every request already sweeps
     * each FE once, and a second sweep would hit the same dead endpoints again.
     * {@code retry.times} therefore only applies to the single-endpoint deployment
     * (typically a load balancer), where one sweep is a single attempt.
     */
    private int httpAttemptCount() {
        return feEndpoints.size() == 1 ? 1 + httpRetryTimes : 1;
    }

    private String httpGet(String path) {
        Map<String, String> headers = buildHttpAuthHeaders();
        Exception lastError = null;
        for (int attempt = 0; attempt < httpAttemptCount(); attempt++) {
            for (String endpoint : feEndpoints) {
                String uri = endpoint + path;
                try {
                    return HttpUtils.get(uri, headers, httpTimeoutMs);
                } catch (Exception e) {
                    lastError = e;
                    LOG.warn("remote StarRocks control-plane GET failed on {}, retrying", uri, e);
                }
            }
        }
        throw new StarRocksConnectorException(
                "remote StarRocks control-plane GET failed on all remote FEs: " + path
                        + ", last error: " + lastError, lastError);
    }

    private String httpPost(String path, String jsonBody) {
        Map<String, String> headers = buildHttpAuthHeaders();
        Exception lastError = null;
        for (int attempt = 0; attempt < httpAttemptCount(); attempt++) {
            for (String endpoint : feEndpoints) {
                String uri = endpoint + path;
                try {
                    StringEntity entity = new StringEntity(jsonBody, StandardCharsets.UTF_8);
                    entity.setContentType("application/json");
                    HttpPost httpPost = new HttpPost(uri);
                    // HttpUtils' shared RequestConfig enables Expect: 100-continue,
                    // which the FE Netty HTTP server never acknowledges — the client
                    // would stall for the full 3s expect-continue timeout on every
                    // POST. Build the request config ourselves with it disabled.
                    httpPost.setConfig(RequestConfig.custom()
                            .setExpectContinueEnabled(false)
                            .setConnectTimeout(httpTimeoutMs)
                            .setSocketTimeout(httpTimeoutMs)
                            .setConnectionRequestTimeout(httpTimeoutMs)
                            .build());
                    httpPost.setEntity(entity);
                    headers.forEach(httpPost::addHeader);
                    try (CloseableHttpResponse response = HttpUtils.getInstance().execute(httpPost)) {
                        int code = response.getStatusLine().getStatusCode();
                        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                        // The control plane carries its real status inside the JSON envelope; a
                        // non-200 transport status that still has a body is surfaced to the caller
                        // so it can read the envelope (e.g. a 404 get-table). Empty-body failures
                        // fall through to endpoint failover.
                        if (code != 200 && Strings.isNullOrEmpty(responseBody)) {
                            throw new StarRocksConnectorException("http status " + code);
                        }
                        return responseBody;
                    }
                } catch (Exception e) {
                    lastError = e;
                    LOG.warn("remote StarRocks control-plane POST failed on {}, retrying", uri, e);
                }
            }
        }
        throw new StarRocksConnectorException(
                "remote StarRocks control-plane POST failed on all remote FEs: " + path
                        + ", last error: " + lastError, lastError);
    }

    private Map<String, String> buildHttpAuthHeaders() {
        String user = feUser;
        int atIndex = feUser.indexOf('@');
        if (atIndex >= 0) {
            user = feUser.substring(0, atIndex);
        }
        String token = Base64.getEncoder().encodeToString(
                (user + ":" + (fePassword == null ? "" : fePassword)).getBytes(StandardCharsets.UTF_8));
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Basic " + token);
        headers.put("Accept", "application/json");
        return headers;
    }

    private static void ensureOk(StarRocksRemoteScanWire.StatusEnvelope response, String errorMessage) {
        if (response == null) {
            throw new StarRocksConnectorException(errorMessage + ": empty response");
        }
        if (response.status != 200) {
            throw new StarRocksConnectorException(errorMessage + ": status " + response.status
                    + (Strings.isNullOrEmpty(response.exception) ? "" : " " + response.exception));
        }
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }

    private static void appendQueryParam(StringBuilder query, String key, String value) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }
        if (query.length() > 0) {
            query.append('&');
        }
        query.append(key).append('=').append(urlEncode(value));
    }

    /**
     * Parses and validates the comma-separated FE endpoint list into normalized
     * {@code scheme://host:port} base URLs; called once at construction, requests iterate
     * the stored result. Each endpoint may be {@code http://host:port},
     * {@code https://host:port} (e.g. a public nginx proxy in front of the remote FEs),
     * or bare {@code host:port}, which defaults to http.
     */
    static List<String> parseFeAddresses(String feUrl) {
        if (Strings.isNullOrEmpty(feUrl)) {
            throw new StarRocksConnectorException("remote StarRocks FE url is empty");
        }
        List<String> addresses = new ArrayList<>();
        for (String endpoint : feUrl.split(",")) {
            String trimmed = endpoint.trim();
            if (!trimmed.isEmpty()) {
                addresses.add(parseFeAddress(trimmed));
            }
        }
        if (addresses.isEmpty()) {
            throw new StarRocksConnectorException("invalid remote StarRocks FE address: " + feUrl);
        }
        return addresses;
    }

    static String parseFeAddress(String feUrl) {
        String scheme = "http";
        String hostPort = feUrl;
        if (feUrl.regionMatches(true, 0, "http://", 0, 7)) {
            hostPort = feUrl.substring(7);
        } else if (feUrl.regionMatches(true, 0, "https://", 0, 8)) {
            scheme = "https";
            hostPort = feUrl.substring(8);
        } else if (feUrl.contains("://")) {
            throw new StarRocksConnectorException(
                    "remote StarRocks FE url scheme must be http or https: " + feUrl);
        }
        if (hostPort.endsWith("/")) {
            hostPort = hostPort.substring(0, hostPort.length() - 1);
        }
        try {
            String[] parts = hostPort.split(":", 2);
            if (parts.length != 2 || Strings.isNullOrEmpty(parts[0])) {
                throw new StarRocksConnectorException("invalid remote StarRocks FE address: " + feUrl);
            }
            return scheme + "://" + parts[0] + ":" + Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException("invalid remote StarRocks FE address: " + feUrl, e);
        }
    }

    public static Type parseType(String typeSql) {
        if (typeSql.contains("<")) {
            return SqlParser.parseType(typeSql, new SessionVariable());
        }

        String normalized = typeSql.trim().toLowerCase(Locale.ROOT);
        String base = normalized;
        int leftParen = normalized.indexOf('(');
        if (leftParen >= 0) {
            base = normalized.substring(0, leftParen).trim();
        }

        switch (base) {
            case "string":
                return TypeFactory.createDefaultCatalogString();
            case "varchar":
                return TypeFactory.createVarcharType(parseLength(normalized, TypeFactory.getOlapMaxVarcharLength()));
            case "char":
                return TypeFactory.createCharType(parseLength(normalized, TypeFactory.getOlapMaxVarcharLength()));
            case "varbinary":
                return TypeFactory.createVarbinary(parseLength(normalized, TypeFactory.getOlapMaxVarcharLength()));
            case "decimal":
            case "decimalv2": {
                int[] precisionScale = parsePrecisionScale(normalized, 10, 0);
                return TypeFactory.createUnifiedDecimalType(precisionScale[0], precisionScale[1]);
            }
            default:
                try {
                    return TypeFactory.createType(PrimitiveType.valueOf(base.toUpperCase(Locale.ROOT)));
                } catch (IllegalArgumentException e) {
                    throw new StarRocksConnectorException("unsupported remote StarRocks column type: " + typeSql, e);
                }
        }
    }

    private static int parseLength(String text, int defaultLength) {
        int[] values = parseParenthesizedInts(text);
        return values.length == 0 ? defaultLength : values[0];
    }

    private static int[] parsePrecisionScale(String text, int defaultPrecision, int defaultScale) {
        int[] values = parseParenthesizedInts(text);
        if (values.length == 0) {
            return new int[] {defaultPrecision, defaultScale};
        }
        if (values.length == 1) {
            return new int[] {values[0], defaultScale};
        }
        return new int[] {values[0], values[1]};
    }

    private static int[] parseParenthesizedInts(String text) {
        int left = text.indexOf('(');
        int right = text.indexOf(')');
        if (left < 0 || right <= left) {
            return new int[0];
        }
        String[] items = text.substring(left + 1, right).split(",");
        int[] values = new int[items.length];
        for (int i = 0; i < items.length; i++) {
            values[i] = Integer.parseInt(items[i].trim());
        }
        return values;
    }

    /** Converts a wire table's columns to catalog columns (types parsed from their SQL text). */
    public static List<Column> toColumns(StarRocksRemoteScanWire.Table table) {
        if (table.columns == null) {
            return Collections.emptyList();
        }
        return table.columns.stream()
                .map(column -> new Column(column.name,
                        parseType(Strings.isNullOrEmpty(column.type) ? "unknown" : column.type),
                        column.nullable))
                .collect(Collectors.toList());
    }
}
