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
package com.starrocks.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

/**
 * Http client utils
 * Created by andrewcheng
 */
public class RestClient {
    private static final Logger LOG = LogManager.getLogger(RestClient.class);
    private final CloseableHttpClient client;
    public RestClient(Integer timeOut) {
        client = getHttpClient(timeOut);
    }

    private static CloseableHttpClient getHttpClient(Integer timeOut) {
        final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(200);
        connManager.setDefaultMaxPerRoute(20);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(timeOut)
                .setConnectTimeout(timeOut)
                .setSocketTimeout(timeOut)
                .build();
        HttpRequestRetryHandler retry = (exception, executionCount, context) -> {
            if (executionCount >= 3) {
                // retried over 3 times, give up
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {
                // connection refused
                return false;
            }
            if (exception instanceof NoHttpResponseException) {
                // If the server drops the connection, try again
                return true;
            }
            if (exception instanceof SSLHandshakeException) {
                // Do not retry SSL handshake exception
                return false;
            }
            if (exception instanceof InterruptedIOException) {
                // time out
                return true;
            }
            if (exception instanceof UnknownHostException) {
                // The target server is unreachable
                return false;
            }
            if (exception instanceof SSLException) {
                // ssl handshake exception
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // If the request is idempotent, try again
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        };
        return HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setRetryHandler(retry)
                .setConnectionManager(connManager)
                .build();
    }

    public String httpGet(String url) throws IOException {
        String content = "";
        CloseableHttpResponse response = null;
        HttpGet request = new HttpGet(url);
        try {
            response = client.execute(request);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new IOException("Failed to get http response, due to http code: "
                        + response.getStatusLine().getStatusCode() + ", response: " + content);
            }
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity, "UTF-8");
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (Exception ex) {
                    LOG.warn("Error during HTTP connection cleanup", ex);
                }
            }
            request.releaseConnection();
        }
        return content;
    }
}
