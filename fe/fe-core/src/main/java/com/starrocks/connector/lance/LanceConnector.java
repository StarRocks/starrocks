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

package com.starrocks.connector.lance;

import com.google.common.base.Strings;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;

import java.util.Map;

public class LanceConnector implements Connector {
    public static final String LANCE_CATALOG_TYPE = "lance.catalog.type";
    public static final String LANCE_CATALOG_WAREHOUSE = "lance.catalog.warehouse";
    public static final String DIRECTORY_CATALOG = "directory";
    public static final String DEFAULT_DB = "default";

    private final String catalogName;
    private final Map<String, String> properties;
    private final HdfsEnvironment hdfsEnvironment;

    public LanceConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        String catalogType = properties.getOrDefault(LANCE_CATALOG_TYPE, DIRECTORY_CATALOG);
        if (!DIRECTORY_CATALOG.equalsIgnoreCase(catalogType)) {
            throw new StarRocksConnectorException("Unsupported lance catalog type: %s. Only directory is supported.",
                    catalogType);
        }
        if (Strings.isNullOrEmpty(properties.get(LANCE_CATALOG_WAREHOUSE))) {
            throw new StarRocksConnectorException("The property %s must be set for a lance catalog.",
                    LANCE_CATALOG_WAREHOUSE);
        }
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new LanceMetadata(catalogName, properties, hdfsEnvironment);
    }
}
