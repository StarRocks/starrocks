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

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.external.elasticsearch.EsUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.starrocks.catalog.EsTable.TRANSPORT;
import static com.starrocks.catalog.EsTable.TRANSPORT_HTTP;
import static com.starrocks.catalog.EsTable.TRANSPORT_THRIFT;

/**
 * ES resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_es"
 * PROPERTIES
 * (
 * "type" = "es",
 * "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
 * "index" = "test",
 * "es.type" = "doc",
 * "user" = "root",
 * "password" = "root"
 * );
 */
public class EsResource extends Resource {
    public static final String ES_PROPERTIES_PREFIX = "elasticsearch.";

    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String TYPE = "type";
    public static final String VERSION = "version";

    public static final String DOC_VALUE_SCAN = "enable_docvalue_scan";
    public static final String KEYWORD_SNIFF = "enable_keyword_sniff";

    public static final String WAN_ONLY = "es.nodes.wan.only";
    public static final String ES_NET_SSL = "es.net.ssl";
    public static final String INDEX = "index";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public EsResource(String name) {
        super(name, Resource.ResourceType.ES);
        properties = Maps.newHashMap();
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        checkAlterProperties(properties);
        this.properties = properties;
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    public static void checkAlterProperties(Map<String, String> properties) throws DdlException {
        if (StringUtils.isEmpty(properties.get(HOSTS))) {
            throw new DdlException("Hosts of ES table is null. "
                    + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
        }

        if (properties.containsKey(ES_NET_SSL)) {
            boolean httpSslEnabled = EsUtil.tryGetBoolean(properties, ES_NET_SSL);
            // check protocol
            String[] seeds = properties.get(HOSTS).trim().split(",");
            for (String seed : seeds) {
                if (httpSslEnabled && seed.startsWith("http://")) {
                    throw new DdlException("if http_ssl_enabled is true, the https protocol must be used");
                }
                if (!httpSslEnabled && seed.startsWith("https://")) {
                    throw new DdlException("if http_ssl_enabled is false, the http protocol must be used");
                }
            }
        }
        if (!Strings.isNullOrEmpty(properties.get(TRANSPORT))
                && !Strings.isNullOrEmpty(properties.get(TRANSPORT).trim())) {
            String transport = properties.get(TRANSPORT).trim();
            if (!(TRANSPORT_HTTP.equals(transport) || TRANSPORT_THRIFT.equals(transport))) {
                throw new DdlException("transport of ES table must be http(recommend) or thrift(reserved inner usage),"
                        + " but value is " + transport);
            }
        }

        if (properties.containsKey(EsResource.ES_NET_SSL)) {
            EsUtil.tryGetBoolean(properties, EsResource.ES_NET_SSL);
        }
        if (properties.containsKey(EsResource.DOC_VALUE_SCAN)) {
            EsUtil.tryGetBoolean(properties, EsResource.DOC_VALUE_SCAN);
        }
        if (properties.containsKey(EsResource.KEYWORD_SNIFF)) {
            EsUtil.tryGetBoolean(properties, EsResource.KEYWORD_SNIFF);
        }
        if (properties.containsKey(EsResource.WAN_ONLY)) {
            EsUtil.tryGetBoolean(properties, EsResource.WAN_ONLY);
        }
    }
}
