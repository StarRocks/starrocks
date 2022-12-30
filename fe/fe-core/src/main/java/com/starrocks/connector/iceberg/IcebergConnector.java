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


package com.starrocks.connector.iceberg;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_LEGACY;
import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private ConnectorMetadata metadata;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.cloudConfiguration = CloudConfigurationFactory.tryBuildForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(null, cloudConfiguration);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            if (null == properties.get(ICEBERG_CATALOG_TYPE) || properties.get(ICEBERG_CATALOG_TYPE).length() == 0) {
                properties.put(ICEBERG_CATALOG_TYPE, properties.get(ICEBERG_CATALOG_LEGACY));
            }
            try {
                metadata = new IcebergMetadata(catalogName, properties, hdfsEnvironment);
            } catch (Exception e) {
                LOG.error("Failed to create iceberg metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
