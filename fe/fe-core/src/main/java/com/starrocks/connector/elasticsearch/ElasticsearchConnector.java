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

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticsearchConnector
        implements Connector {
    private static final Logger LOG = LogManager.getLogger(ElasticsearchConnector.class);

    private final String catalogName;

    private EsConfig esConfig;
    private EsRestClient esRestClient;
    private ConnectorMetadata metadata;

    /**
     * Default constructor for EsExternalCatalog.
     */
    public ElasticsearchConnector(ConnectorContext contex) {
        this.catalogName = contex.getCatalogName();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new ElasticsearchMetadata(esRestClient, esConfig.getProperties(), catalogName);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create elasticsearch metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        esConfig = (EsConfig) config;
        this.esRestClient = new EsRestClient(esConfig.getNodes(), esConfig.getUserName(),
                esConfig.getPassword(), esConfig.isEnableSsl());
    }

}