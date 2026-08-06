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
import okhttp3.HttpUrl;
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

class OpaHttpClient implements OpaPolicyClient {
    private static final Logger LOG = LogManager.getLogger(OpaHttpClient.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final Gson gson = new Gson();
    private final OkHttpClient httpClient;
    private final String policyUrl;
    private final String rowFiltersUrl;
    private final String columnMaskingUrl;
    private final String batchColumnMaskingUrl;

    OpaHttpClient() {
        this(Config.opa_policy_url, Config.opa_row_filters_url, Config.opa_column_masking_url,
                Config.opa_batch_column_masking_url, Config.opa_connect_timeout_ms, Config.opa_read_timeout_ms);
    }

    OpaHttpClient(String policyUrl, String rowFiltersUrl, String columnMaskingUrl, String batchColumnMaskingUrl,
                  int connectTimeoutMs, int readTimeoutMs) {
        this.policyUrl = validateUrl("opa_policy_url", policyUrl, true);
        this.rowFiltersUrl = validateUrl("opa_row_filters_url", rowFiltersUrl, false);
        this.columnMaskingUrl = validateUrl("opa_column_masking_url", columnMaskingUrl, false);
        this.batchColumnMaskingUrl = validateUrl("opa_batch_column_masking_url", batchColumnMaskingUrl, false);
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
        Request httpRequest = new Request.Builder()
                .url(url)
                .post(RequestBody.create(requestBody, JSON))
                .build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            ResponseBody body = response.body();
            String responseBody = body == null ? "" : body.string();
            if (!response.isSuccessful()) {
                throw new OpaQueryException("OPA server returned HTTP " + response.code());
            }
            JsonElement responseJson = JsonParser.parseString(responseBody);
            if (!responseJson.isJsonObject()) {
                throw new OpaQueryException("OPA response is not a JSON object");
            }
            return responseJson.getAsJsonObject();
        } catch (OpaQueryException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new OpaQueryException("failed to query OPA: " + e.getClass().getSimpleName());
        }
    }

    private boolean parseBooleanResult(JsonObject response) {
        JsonElement result = response.get("result");
        return result != null && result.isJsonPrimitive() && result.getAsJsonPrimitive().isBoolean()
                && result.getAsBoolean();
    }

    private List<String> parseRowFilters(JsonObject response) {
        JsonElement result = requireResult(response, "row filters");
        if (result.isJsonNull()) {
            return List.of();
        }
        if (!result.isJsonArray()) {
            throw new OpaQueryException("OPA row filters result must be an array");
        }
        ImmutableList.Builder<String> filters = ImmutableList.builder();
        for (JsonElement item : result.getAsJsonArray()) {
            filters.add(parseExpression(item));
        }
        return filters.build();
    }

    private Optional<String> parseColumnMask(JsonObject response) {
        JsonElement result = requireResult(response, "column masking");
        if (result.isJsonNull()) {
            return Optional.empty();
        }
        return Optional.of(parseExpression(result));
    }

    private Map<String, String> parseBatchColumnMasks(JsonObject response, List<String> columnNames) {
        JsonElement result = requireResult(response, "batch column masking");
        if (result.isJsonNull()) {
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
            String expression = parseExpression(itemObject);
            JsonElement columnElement = itemObject.get("column");
            if (columnElement != null && columnElement.isJsonPrimitive()) {
                columnToMask.put(columnElement.getAsString(), expression);
                continue;
            }
            JsonElement indexElement = itemObject.get("index");
            if (indexElement != null && indexElement.isJsonPrimitive() && indexElement.getAsJsonPrimitive().isNumber()) {
                int index = indexElement.getAsInt();
                if (index >= 0 && index < columnNames.size()) {
                    columnToMask.put(columnNames.get(index), expression);
                    continue;
                }
            }
            throw new OpaQueryException("OPA batch column masking item must contain column or index");
        }
        return columnToMask;
    }

    private String parseExpression(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            throw new OpaQueryException("OPA result expression must be a non-blank string");
        }
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
            return requireNonBlankExpression(element.getAsString());
        }
        if (element.isJsonObject()) {
            JsonElement expression = element.getAsJsonObject().get("expression");
            if (expression == null || expression.isJsonNull()) {
                throw new OpaQueryException("OPA result expression must be a non-blank string");
            }
            if (expression.isJsonPrimitive() && expression.getAsJsonPrimitive().isString()) {
                return requireNonBlankExpression(expression.getAsString());
            }
        }
        throw new OpaQueryException("OPA result expression must be a non-blank string");
    }

    private String requireNonBlankExpression(String expression) {
        if (expression.isBlank()) {
            throw new OpaQueryException("OPA result expression must be a non-blank string");
        }
        return expression;
    }

    private static JsonElement requireResult(JsonObject response, String policyType) {
        if (!response.has("result")) {
            throw new OpaQueryException("OPA " + policyType + " response must contain result");
        }
        return response.get("result");
    }

    private static String validateUrl(String configName, String value, boolean required) {
        if (Strings.isNullOrEmpty(value)) {
            if (required) {
                throw new IllegalArgumentException(configName + " must be set when OPA access control is enabled");
            }
            return null;
        }
        if (value.isBlank() || HttpUrl.parse(value) == null) {
            throw new IllegalArgumentException(configName + " must be a valid HTTP or HTTPS URL");
        }
        return value;
    }

    private static class OpaEnvelope {
        @SerializedName("input")
        private final OpaRequest input;

        private OpaEnvelope(OpaRequest input) {
            this.input = input;
        }
    }
}
