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

package com.starrocks.authorization.opa;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class OpaHttpClient implements OpaPolicyClient {
    private static final Logger LOG = LogManager.getLogger(OpaHttpClient.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final Gson gson = new Gson();
    private final OkHttpClient httpClient;
    private final String policyUrl;
    private final String rowFiltersUrl;
    private final String columnMaskingUrl;
    private final String batchColumnMaskingUrl;
    private final boolean logRequests;
    private final boolean logResponses;

    public OpaHttpClient() {
        this(Config.opa_policy_url, Config.opa_row_filters_url, Config.opa_column_masking_url,
                Config.opa_batch_column_masking_url, Config.opa_connect_timeout_ms, Config.opa_read_timeout_ms,
                Config.opa_log_requests, Config.opa_log_responses);
    }

    OpaHttpClient(String policyUrl, String rowFiltersUrl, String columnMaskingUrl, String batchColumnMaskingUrl,
                  int connectTimeoutMs, int readTimeoutMs, boolean logRequests, boolean logResponses) {
        if (Strings.isNullOrEmpty(policyUrl)) {
            throw new IllegalArgumentException("opa_policy_url must be set when OPA access control is enabled");
        }
        this.policyUrl = policyUrl;
        this.rowFiltersUrl = emptyToNull(rowFiltersUrl);
        this.columnMaskingUrl = emptyToNull(columnMaskingUrl);
        this.batchColumnMaskingUrl = emptyToNull(batchColumnMaskingUrl);
        this.logRequests = logRequests;
        this.logResponses = logResponses;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public boolean checkPermission(OpaRequest request) {
        try {
            return parseBooleanResult(post(policyUrl, request));
        } catch (OpaQueryException e) {
            LOG.warn("OPA authorization request failed: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<String> getRowFilters(OpaRequest request) {
        if (rowFiltersUrl == null) {
            return List.of();
        }
        return parseRowFilters(post(rowFiltersUrl, request));
    }

    @Override
    public Optional<String> getColumnMask(OpaRequest request) {
        if (columnMaskingUrl == null) {
            return Optional.empty();
        }
        return parseColumnMask(post(columnMaskingUrl, request));
    }

    @Override
    public Map<String, String> getBatchColumnMasks(OpaRequest request, List<String> columnNames) {
        if (batchColumnMaskingUrl == null) {
            return Map.of();
        }
        return parseBatchColumnMasks(post(batchColumnMaskingUrl, request), columnNames);
    }

    @Override
    public boolean supportsBatchColumnMasks() {
        return batchColumnMaskingUrl != null;
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private JsonObject post(String url, OpaRequest request) {
        String requestBody = gson.toJson(new OpaEnvelope(request));
        if (logRequests) {
            LOG.debug("Sending OPA request to {}: {}", url, requestBody);
        }
        Request httpRequest = new Request.Builder()
                .url(url)
                .post(RequestBody.create(JSON, requestBody))
                .build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            ResponseBody body = response.body();
            String responseBody = body == null ? "" : body.string();
            if (logResponses) {
                LOG.debug("Received OPA response from {} with status {}: {}", url, response.code(), responseBody);
            }
            if (!response.isSuccessful()) {
                throw new OpaQueryException(
                        "OPA server returned HTTP " + response.code() + " for " + url);
            }
            JsonElement responseJson = JsonParser.parseString(responseBody);
            if (!responseJson.isJsonObject()) {
                throw new OpaQueryException("OPA response is not a JSON object");
            }
            return responseJson.getAsJsonObject();
        } catch (OpaQueryException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new OpaQueryException("failed to query OPA at " + url, e);
        }
    }

    private boolean parseBooleanResult(JsonObject response) {
        JsonElement result = response.get("result");
        return result != null && result.isJsonPrimitive() && result.getAsJsonPrimitive().isBoolean()
                && result.getAsBoolean();
    }

    private List<String> parseRowFilters(JsonObject response) {
        JsonElement result = response.get("result");
        if (result == null || result.isJsonNull()) {
            return List.of();
        }
        if (!result.isJsonArray()) {
            throw new OpaQueryException("OPA row filters result must be an array");
        }
        ImmutableList.Builder<String> filters = ImmutableList.builder();
        for (JsonElement item : result.getAsJsonArray()) {
            parseExpression(item).ifPresent(filters::add);
        }
        return filters.build();
    }

    private Optional<String> parseColumnMask(JsonObject response) {
        JsonElement result = response.get("result");
        if (result == null || result.isJsonNull()) {
            return Optional.empty();
        }
        return parseExpression(result);
    }

    private Map<String, String> parseBatchColumnMasks(JsonObject response, List<String> columnNames) {
        JsonElement result = response.get("result");
        if (result == null || result.isJsonNull()) {
            return Map.of();
        }
        if (!result.isJsonArray()) {
            throw new OpaQueryException("OPA batch column masking result must be an array");
        }
        Map<String, String> columnToMask = Maps.newHashMap();
        for (JsonElement item : result.getAsJsonArray()) {
            if (!item.isJsonObject()) {
                throw new OpaQueryException("OPA batch column masking item must be an object");
            }
            JsonObject itemObject = item.getAsJsonObject();
            Optional<String> expression = parseExpression(itemObject);
            if (expression.isEmpty()) {
                continue;
            }
            JsonElement columnElement = itemObject.get("column");
            if (columnElement != null && columnElement.isJsonPrimitive()) {
                columnToMask.put(columnElement.getAsString(), expression.get());
                continue;
            }
            JsonElement indexElement = itemObject.get("index");
            if (indexElement != null && indexElement.isJsonPrimitive() && indexElement.getAsJsonPrimitive().isNumber()) {
                int index = indexElement.getAsInt();
                if (index >= 0 && index < columnNames.size()) {
                    columnToMask.put(columnNames.get(index), expression.get());
                    continue;
                }
            }
            throw new OpaQueryException("OPA batch column masking item must contain column or index");
        }
        return columnToMask;
    }

    private Optional<String> parseExpression(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return Optional.empty();
        }
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
            return Optional.of(element.getAsString());
        }
        if (element.isJsonObject()) {
            JsonElement expression = element.getAsJsonObject().get("expression");
            if (expression == null || expression.isJsonNull()) {
                return Optional.empty();
            }
            if (expression.isJsonPrimitive() && expression.getAsJsonPrimitive().isString()) {
                return Optional.of(expression.getAsString());
            }
        }
        throw new OpaQueryException("OPA result expression must be a string");
    }

    private static String emptyToNull(String value) {
        return Strings.isNullOrEmpty(value) ? null : value;
    }

    private static class OpaEnvelope {
        @SerializedName("input")
        private final OpaRequest input;

        private OpaEnvelope(OpaRequest input) {
            this.input = input;
        }
    }
}
