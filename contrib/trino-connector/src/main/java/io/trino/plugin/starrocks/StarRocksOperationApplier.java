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

package io.trino.plugin.starrocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamTableRegion;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.http.StreamLoadEntity;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.data.load.stream.StreamLoadUtils.getSendUrl;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class StarRocksOperationApplier
        implements AutoCloseable
{
    private static final Logger log = Logger.get(StarRocksOperationApplier.class);
    private final StreamTableRegion region;
    private final StreamLoadProperties properties;
    private final long maxCacheBytes;
    private Header[] defaultHeaders;
    private final HttpClientBuilder clientBuilder;
    private volatile long availableHostPos;
    private final ObjectMapper objectMapper;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong totalFlushRows = new AtomicLong(0L);

    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    private final AtomicLong numberLoadRows = new AtomicLong(0L);

    public StarRocksOperationApplier(String database, String table, Optional<String> temporaryTableName, List<String> columns, Boolean isPkTable, StreamLoadProperties properties, HttpClientBuilder clientBuilder)
    {
        String uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        StreamLoadTableProperties tableProperties = properties.getTableProperties();
        this.region = new StreamTableRegion(uniqueKey, database, table, temporaryTableName, properties.getLabelPrefix(), this, tableProperties);
        this.properties = properties;
        this.maxCacheBytes = properties.getMaxCacheBytes();
        initDefaultHeaders(isPkTable, columns, properties);
        this.clientBuilder = clientBuilder;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Not thread safe
     * Applies an operation without waiting for it to be flushed, operations are flushed in the background
     *
     * @param row row data
     */
    public void applyOperationAsync(String row)
    {
        int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
        if (currentCacheBytes.addAndGet(bytes) >= maxCacheBytes) {
            try {
                if (region.commit()) {
                    this.send();
                }
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
    }

    protected void initDefaultHeaders(Boolean isPkTable, List<String> columns, StreamLoadProperties properties)
    {
        Map<String, String> headers = new HashMap<>(properties.getHeaders());
        if (!headers.containsKey("timeout")) {
            headers.put("timeout", "120");
        }
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        headers.put(HttpHeaders.EXPECT, "100-continue");
        headers.put("ignore_json_size", "true");
        if (isPkTable) {
            headers.put("columns", String.join(",", columns));
            headers.put("partial_update", "true");
        }
        this.defaultHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }

    public StreamLoadResponse send()
    {
        StreamLoadTableProperties tableProperties = properties.getTableProperties();
        try {
            StreamLoadDataFormat dataFormat = tableProperties.getDataFormat();
            String host = getAvailableHost();
            String table = region.getTemporaryTableName().orElseGet(region::getTable);
            String sendUrl = getSendUrl(host, region.getDatabase(), table);
            String label = region.getLabel();

            log.info("Stream loading, label : %s, region : %s", label, region.getUniqueKey());

            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());
            httpPut.setEntity(new StreamLoadEntity(region, dataFormat, region.getEntityMeta()));

            httpPut.setHeaders(defaultHeaders);

            for (Map.Entry<String, String> entry : tableProperties.getProperties().entrySet()) {
                httpPut.removeHeaders(entry.getKey());
                httpPut.addHeader(entry.getKey(), entry.getValue());
            }

            httpPut.addHeader("label", label);

            try (CloseableHttpClient client = clientBuilder.build()) {
                log.info("Stream loading, label : %s, request : %s", label, httpPut);
                long startNanoTime = System.currentTimeMillis();
                CloseableHttpResponse response = client.execute(httpPut);
                String responseBody = EntityUtils.toString(response.getEntity());

                log.info("Stream load completed, label : %s, database : %s, table : %s, body : %s",
                        label, region.getDatabase(), table, responseBody);

                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody = objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);

                String status = streamLoadBody.getStatus();

                if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_OK.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    region.complete(streamLoadResponse);
                }
                else if (StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED.equals(status)) {
                    boolean succeed = checkLabelState(host, region.getDatabase(), label);
                    if (!succeed) {
                        throw new StreamLoadFailException("Stream load failed");
                    }
                }
                else {
                    throw new StreamLoadFailException(responseBody, streamLoadBody);
                }
                return streamLoadResponse;
            }
            catch (Exception e) {
                log.error("Stream load failed unknown, label : " + label, e);
                throw e;
            }
        }
        catch (Exception e) {
            log.error("Stream load failed, thread : " + Thread.currentThread().getName(), e);
            region.callback(e);
        }
        return null;
    }

    protected String getAvailableHost()
    {
        String[] hosts = properties.getLoadUrls();
        int size = hosts.length;
        long pos = availableHostPos;
        while (pos < pos + size) {
            String host = "http://" + hosts[(int) (pos % size)];
            if (testHttpConnection(host)) {
                pos++;
                availableHostPos = pos;
                return host;
            }
        }

        return null;
    }

    private boolean testHttpConnection(String host)
    {
        try {
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(properties.getConnectTimeout());
            connection.connect();
            connection.disconnect();
            return true;
        }
        catch (Exception e) {
            log.warn("Failed to connect to address: %s", host, e);
            return false;
        }
    }

    protected boolean checkLabelState(String host, String database, String label)
            throws Exception
    {
        int idx = 0;
        for (;; ) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                String url = host + "/api/" + database + "/get_load_state?label=" + label;
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
                httpGet.setHeader("Connection", "close");
                try (CloseableHttpResponse response = client.execute(httpGet)) {
                    String entityContent = EntityUtils.toString(response.getEntity());

                    if (response.getStatusLine().getStatusCode() != 200) {
                        throw new StreamLoadFailException("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label : `" + label + "`, body : " + entityContent);
                    }

                    log.info("Label `%s` check, body : %s", label, entityContent);

                    StreamLoadResponse.StreamLoadResponseBody responseBody =
                            objectMapper.readValue(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                    String state = responseBody.getState();
                    if (state == null) {
                        log.error("Get label state failed, body : %s", objectMapper.writeValueAsString(responseBody));
                        throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s]. response[%s]\n", label, entityContent));
                    }
                    switch (state) {
                        case StreamLoadConstants.LABEL_STATE_VISIBLE:
                        case StreamLoadConstants.LABEL_STATE_PREPARED:
                        case StreamLoadConstants.LABEL_STATE_COMMITTED:
                            return true;
                        case StreamLoadConstants.LABEL_STATE_PREPARE:
                            continue;
                        case StreamLoadConstants.LABEL_STATE_ABORTED:
                            return false;
                        case StreamLoadConstants.LABEL_STATE_UNKNOWN:
                        default:
                            throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error " +
                                    "label[%s] state[%s]\n", label, state));
                    }
                }
            }
        }
    }

    public void callback(StreamLoadResponse response)
    {
        long currentBytes = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        log.info("pre bytes : %s, current bytes : %s, totalFlushRows : %s", currentBytes, currentCacheBytes.get(), totalFlushRows.get());
        if (response.getBody() != null) {
            if (response.getBody().getNumberTotalRows() != null) {
                numberTotalRows.addAndGet(response.getBody().getNumberTotalRows());
            }
            if (response.getBody().getNumberLoadedRows() != null) {
                numberLoadRows.addAndGet(response.getBody().getNumberLoadedRows());
            }
        }
    }

    public void callback(Throwable e)
    {
        log.error("Stream load failed", e);
        throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
    }

    @Override
    public void close()
    {
        if (region.commit()) {
            this.send();
            log.info("Operation applier close, currentBytes : %s, flushRows : %s" +
                            ", numberTotalRows : %s, numberLoadRows : %s",
                    currentCacheBytes.get(), totalFlushRows.get(), numberTotalRows.get(), numberLoadRows.get());
        }
    }
}
