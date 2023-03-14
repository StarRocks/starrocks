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


package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.EsTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ElasticsearchConnector
        implements Connector {
    private static final Logger LOG = LogManager.getLogger(ElasticsearchConnector.class);

    private EsRestClient esRestClient;

    private final String catalogName;

    private String[] nodes;

    private String username = null;

    private String password = null;

    private boolean enableDocValueScan = true;

    private boolean enableKeywordSniff = true;

    private boolean enableSsl = false;

    private boolean enableWanOnly = true;

    private Map<String, String> properties;

    private ConnectorMetadata metadata;

    /**
     * Default constructor for EsExternalCatalog.
     */
    public ElasticsearchConnector(ConnectorContext contex) throws DdlException {
        this.catalogName = contex.getCatalogName();
        this.properties = processDefaultValue(contex.getProperties());
        this.esRestClient = new EsRestClient(this.nodes, this.username, this.password, this.enableSsl);
    }

    /**
     * add some default value
     */
    private Map<String, String> processDefaultValue(Map<String, String> properties) throws DdlException {
        nodes = properties.get(EsTable.HOSTS).trim().split(",");
        if (properties.containsKey("ssl")) {
            properties.put(EsTable.ES_NET_SSL, properties.remove("ssl"));
        }
        if (properties.containsKey(EsTable.ES_NET_SSL)) {
            enableSsl = EsUtil.tryGetBoolean(properties, EsTable.ES_NET_SSL);
        } else {
            properties.put(EsTable.ES_NET_SSL, String.valueOf(enableSsl));
        }

        if (properties.containsKey("username")) {
            properties.put(EsTable.USER, properties.remove("username"));
        }
        if (StringUtils.isNotBlank(properties.get(EsTable.USER))) {
            username = properties.get(EsTable.USER).trim();
        }

        if (StringUtils.isNotBlank(properties.get(EsTable.PASSWORD))) {
            password = properties.get(EsTable.PASSWORD).trim();
        }

        if (properties.containsKey("doc_value_scan")) {
            properties.put(EsTable.DOC_VALUE_SCAN, properties.remove("doc_value_scan"));
        }
        if (properties.containsKey(EsTable.DOC_VALUE_SCAN)) {
            enableDocValueScan = EsUtil.tryGetBoolean(properties, EsTable.DOC_VALUE_SCAN);
        } else {
            properties.put(EsTable.DOC_VALUE_SCAN, String.valueOf(enableDocValueScan));
        }

        if (properties.containsKey("keyword_sniff")) {
            properties.put(EsTable.KEYWORD_SNIFF, properties.remove("keyword_sniff"));
        }
        if (properties.containsKey(EsTable.KEYWORD_SNIFF)) {
            enableKeywordSniff = EsUtil.tryGetBoolean(properties, EsTable.KEYWORD_SNIFF);
        } else {
            properties.put(EsTable.KEYWORD_SNIFF, String.valueOf(enableKeywordSniff));
        }

        if (properties.containsKey(EsTable.WAN_ONLY)) {
            enableWanOnly = EsUtil.tryGetBoolean(properties, EsTable.WAN_ONLY);
        } else {
            properties.put(EsTable.WAN_ONLY, String.valueOf(enableWanOnly));
        }
        return properties;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new ElasticsearchMetadata(esRestClient, catalogName, properties);
            } catch (StarRocksESException e) {
                LOG.error("Failed to create elasticsearch metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

}