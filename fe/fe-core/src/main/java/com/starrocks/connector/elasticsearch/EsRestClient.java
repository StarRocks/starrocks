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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsRestClient.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.elasticsearch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.exception.StarRocksConnectorException;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static com.starrocks.connector.elasticsearch.EsUtil.getFromJSONArray;
import static com.starrocks.connector.elasticsearch.EsUtil.readTree;

public class EsRestClient {

    private static final Logger LOG = LogManager.getLogger(EsRestClient.class);
    private final ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
        mapper.configure(MapperFeature.USE_ANNOTATIONS, false);
    }

    private static final OkHttpClient NETWORK_CLIENT = new OkHttpClient.Builder()
            .readTimeout(10, TimeUnit.SECONDS)
            .build();

    private static OkHttpClient sslNetworkClient;

    private final Request.Builder builder;
    private final String[] nodes;
    private String currentNode;
    private int currentNodeIndex = 0;

    private boolean sslEnabled;

    public EsRestClient(String[] nodes, String authUser, String authPassword, boolean sslEnabled) {
        this(nodes, authUser, authPassword);
        this.sslEnabled = sslEnabled;
    }

    public EsRestClient(String[] nodes, String authUser, String authPassword) {
        this.nodes = nodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION,
                    Credentials.basic(authUser, authPassword));
        }
        this.currentNode = nodes[currentNodeIndex];
    }

    private void selectNextNode() {
        currentNodeIndex++;
        // reroute, because the previously failed node may have already been restored
        if (currentNodeIndex >= nodes.length) {
            currentNodeIndex = 0;
        }
        currentNode = nodes[currentNodeIndex];
    }

    public Map<String, EsNodeInfo> getHttpNodes() throws StarRocksConnectorException {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        if (nodesData == null) {
            return Collections.emptyMap();
        }
        Map<String, EsNodeInfo> nodesMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue());
            if (node.hasHttp()) {
                nodesMap.put(node.getId(), node);
            }
        }
        return nodesMap;
    }

    /**
     * Get remote ES Cluster version
     *
     * @return
     * @throws Exception
     */
    public EsMajorVersion version() throws StarRocksConnectorException {
        Map<String, Object> result = get("/", null);
        if (result == null) {
            throw new StarRocksConnectorException("Unable to retrieve ES main cluster info.");
        }
        Map<String, String> versionBody = (Map<String, String>) result.get("version");
        return EsMajorVersion.parse(versionBody.get("number"));
    }

    /**
     * Get mapping for indexName
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public String getMapping(String indexName) throws StarRocksConnectorException {
        String path = indexName + "/_mapping";
        String indexMapping = execute(path);
        if (indexMapping == null) {
            throw new StarRocksConnectorException("index[" + indexName + "] not found");
        }
        return indexMapping;
    }

    /**
     * Get Shard location
     *
     * @param indexName
     * @return
     * @throws StarRocksConnectorException
     */
    public EsShardPartitions searchShards(String indexName) throws StarRocksConnectorException {
        String path = indexName + "/_search_shards";
        String searchShards = execute(path);
        if (searchShards == null) {
            throw new StarRocksConnectorException("request index [" + indexName + "] search_shards failure");
        }
        return EsShardPartitions.findShardPartitions(indexName, searchShards);
    }

    /**
     * execute request for specific path, it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    private String execute(String path) throws StarRocksConnectorException {
        int retrySize = nodes.length;
        StarRocksConnectorException scratchExceptionForThrow = null;
        OkHttpClient client;
        if (sslEnabled) {
            client = getOrCreateSSLClient();
        } else {
            client = NETWORK_CLIENT;
        }
        for (int i = 0; i < retrySize; i++) {
            // maybe should add HTTP schema to the address
            // actually, at this time we can only process http protocol
            // NOTE. currentNode may have some spaces.
            // User may set a config like described below:
            // hosts: "http://192.168.0.1:8200, http://192.168.0.2:8200"
            // then currentNode will be "http://192.168.0.1:8200", " http://192.168.0.2:8200"
            currentNode = currentNode.trim();
            if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
                currentNode = "http://" + currentNode;
            }
            Request request = builder.get()
                    .url(currentNode + "/" + path)
                    .build();
            Response response = null;
            if (LOG.isTraceEnabled()) {
                LOG.trace("es rest client request URL: {}", currentNode + "/" + path);
            }
            try {
                response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body().string();
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = new StarRocksConnectorException(e.getMessage());
            } finally {
                if (response != null) {
                    response.close();
                }
            }
            selectNextNode();
        }
        LOG.warn("try all nodes [{}],no other nodes left", nodes);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
        return null;
    }

    public <T> T get(String q, String key) throws StarRocksConnectorException {
        return parseContent(execute(q), key);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(String response, String key) {
        Map<String, Object> map = Collections.emptyMap();
        try {
            JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(response);
            map = mapper.readValue(jsonParser, Map.class);
        } catch (IOException ex) {
            LOG.error("parse es response failure: [{}]", response);
            throw new StarRocksConnectorException(ex.getMessage());
        }
        return (T) (key != null ? map.get(key) : map);
    }

    private synchronized OkHttpClient getOrCreateSSLClient() {
        if (sslNetworkClient == null) {
            sslNetworkClient = new OkHttpClient.Builder()
                    .readTimeout(10, TimeUnit.SECONDS)
                    .sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts())
                    .hostnameVerifier(new TrustAllHostnameVerifier())
                    .build();
        }
        return sslNetworkClient;
    }

    private static class TrustAllCerts implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private static class TrustAllHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    private static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory ssfFactory;
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] {new TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }

    public static class EsIndex {
        private String index;

        public String getIndex() {
            return index;
        }
    }

    /**
     * response
     [
        {
        "index": ".kibana_1"
        },
        {
        "index": ".opendistro_security"
        },
        {
        "index": "kibana_sample_data_ecommerce"
        },
        {
        "index": "kibana_sample_data_ecommerce_2"
        },
        {
        "index": "kibana_sample_data_flights"
        },
        {
        "index": "kibana_sample_data_logs"
        }
     ]
     * indices are same as table
     * @return
     */
    private Set<String> getIndices() {
        String response = execute("_cat/indices?h=index&format=json&s=index:asc");
        if (Objects.isNull(response)) {
            throw new StarRocksConnectorException("es indexes are null, maybe this is error");
        }

        EsIndex[] esIndices = getFromJSONArray(response, EsIndex[].class);
        return Arrays.asList(esIndices).stream()
                .filter(index -> !index.getIndex().startsWith("."))
                .map(index -> index.getIndex())
                .collect(Collectors.toSet());
    }
    /**
     {
        "kibana_sample_data_ecommerce": {
            "aliases": {}
        }
     }
     * Get all alias.
     **/
    private Map<String, List<String>> getAliases() {
        String response = execute("_aliases");
        // key: index, value: aliases
        Map<String, List<String>> aliases = new HashMap<>();
        JsonNode root = readTree(response);
        if (root == null) {
            return aliases;
        }
        Iterator<Map.Entry<String, JsonNode>> indexElements = root.fields();
        while (indexElements.hasNext()) {
            Map.Entry<String, JsonNode> indexAlias = indexElements.next();
            JsonNode aliasesNode = indexAlias.getValue().get("aliases");
            Iterator<String> aliasNames = aliasesNode.fieldNames();
            if (aliasNames.hasNext()) {
                aliases.put(indexAlias.getKey(), ImmutableList.copyOf(aliasNames));
            }
        }
        return aliases;
    }

    /**
     * get tables union between indices and aliases
     * @return
     */
    public List<String> listTables() {
        Set<String> indices = getIndices();
        Map<String, List<String>> aliases = getAliases();
        aliases.entrySet().stream()
                .filter(e -> (!e.getKey().startsWith(".") && !indices.contains(e.getKey())))
                .flatMap(e -> e.getValue().stream())
                .forEach(indices::add);
        return new ArrayList<>(indices);
    }
}
