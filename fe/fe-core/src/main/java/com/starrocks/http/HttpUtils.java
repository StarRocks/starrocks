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

package com.starrocks.http;


import org.apache.http.HttpStatus;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLContext;

public class HttpUtils {

    public static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    private static volatile CloseableHttpClient httpClient;

    private static PoolingHttpClientConnectionManager clientConnectionManager;

    public static CloseableHttpClient getInstance() {
        if (httpClient == null) {
            synchronized (HttpUtils.class) {
                if (httpClient == null) {
                    httpClient = getHttpClient();
                }
            }
        }
        return httpClient;
    }

    public static CloseableHttpClient getHttpClient() {

        RequestConfig  requestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                .setExpectContinueEnabled(Boolean.TRUE)
                .setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST, AuthSchemes.SPNEGO))
                .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC, AuthSchemes.SPNEGO))
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .setConnectionRequestTimeout(5000)
                .setRedirectsEnabled(true)
                .build();

        return HttpClients.custom()
                .setConnectionManager(clientConnectionManager)
                .setDefaultRequestConfig(requestConfig)
                .setRetryHandler(new DefaultHttpRequestRetryHandler(5, false))
                .build();
    }

    static {
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLContext sslContext = builder.build();
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("http", PlainConnectionSocketFactory.getSocketFactory())
                            .register("https", socketFactory)
                            .build();
            clientConnectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            clientConnectionManager.setDefaultMaxPerRoute(50);
            clientConnectionManager.setMaxTotal(100);

        } catch (NoSuchAlgorithmException e) {
            LOG.error("Got NoSuchAlgorithmException when SSLContext init", e);
        } catch (KeyManagementException e) {
            LOG.error("Got KeyManagementException when SSLContext init", e);
        } catch (KeyStoreException e) {
            LOG.error("Got KeyStoreException when SSLContext init", e);
        }

        httpClient = getHttpClient();

        LOG.info(" initial http client successfully");

    }

    public static String get(String uri, Map<String, String> header) throws Exception {
        HttpGet httpGet = new HttpGet(uri);
        addHeaders(httpGet, header);
        return executeRequest(uri, httpGet, null);
    }

    public static String post(String uri, AbstractHttpEntity entity, Map<String, String> header) throws Exception {
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(entity);
        addHeaders(httpPost, header);
        return executeRequest(uri, httpPost, entity);
    }

    private static void addHeaders(HttpRequestBase request, Map<String, String> headers) {
        if (headers != null && !headers.isEmpty()) {
            headers.forEach(request::addHeader);
        }
    }

    private static String executeRequest(String uri,
                                         HttpUriRequest request,
                                         AbstractHttpEntity entity) throws HttpRequestException {
        CloseableHttpClient httpClient = getInstance();
        if (Objects.isNull(httpClient)) {
            LOG.error("HttpClient is null for uri: {}", uri);
            throw new HttpRequestException("HttpClient is null for uri: " + uri);
        }
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                if (entity != null) {
                    String msg = String.format("Http request failed, url=%s, code=%d, body=%s, response=%s",
                            uri, code, entity, result);
                    LOG.error(msg);
                    throw new HttpRequestException(msg);
                } else {
                    String msg = String.format("Http request failed, url=%s, code=%d, response=%s",
                            uri, code, result);
                    LOG.error(msg);
                    throw new HttpRequestException(msg);
                }
            }
        } catch (IOException e) {
            String msg = "Http request exception, uri=" + uri;
            LOG.error(msg, e);
            throw new HttpRequestException(msg, e);
        }
    }

}
