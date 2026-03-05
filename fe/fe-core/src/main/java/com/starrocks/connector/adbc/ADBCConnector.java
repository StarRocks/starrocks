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

package com.starrocks.connector.adbc;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ADBCConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(ADBCConnector.class);

    public static final String PROP_DRIVER = "adbc.driver";
    public static final String PROP_URL = "adbc.url";

    private final Map<String, String> properties;
    private final String catalogName;
    private ConnectorMetadata metadata;

    public ADBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        validate(PROP_DRIVER);
        validate(PROP_URL);

        // Try to create ADBC metadata; if failed, it will be created later when getMetadata() is called.
        try {
            metadata = new ADBCMetadata(properties, catalogName);
        } catch (Exception e) {
            metadata = null;
            LOG.error("Failed to create adbc metadata on [catalog : {}]", catalogName, e);
        }
    }

    private void validate(String propertyKey) {
        String value = properties.get(propertyKey);
        if (value == null) {
            throw new StarRocksConnectorException("Missing " + propertyKey + " in properties");
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new ADBCMetadata(properties, catalogName);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create adbc metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    @Override
    public void shutdown() {
        if (metadata != null) {
            ((ADBCMetadata) metadata).shutdown();
        }
    }
}
