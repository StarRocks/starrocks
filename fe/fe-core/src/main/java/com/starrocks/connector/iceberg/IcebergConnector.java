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

import com.google.common.base.Strings;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_LEGACY;
import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final IcebergCatalog icebergNativeCatalog;

    private String metastoreURI;
    private String catalogImpl;
    private String catalogType;
    private Map<String, String> customProperties;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.cloudConfiguration = CloudConfigurationFactory.tryBuildForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(null, cloudConfiguration);
        this.icebergNativeCatalog = buildIcebergNativeCatalog();
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = getNativeCatalogType();
        Configuration conf = hdfsEnvironment.getConfiguration();
        String icebergNativeCatalogName = "native-" + nativeCatalogType.name() + "-" + catalogName;
        CatalogLoader catalogLoader;
        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                String metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
                Util.validateMetastoreUris(metastoreURI);
                catalogLoader = CatalogLoader.hive(icebergNativeCatalogName, conf, properties);
                this.metastoreURI = metastoreURI;
                break;
            case GLUE_CATALOG:
                catalogLoader = CatalogLoader.glue(icebergNativeCatalogName, conf, properties);
                break;
            case REST_CATALOG:
                catalogLoader = CatalogLoader.rest(icebergNativeCatalogName, conf, properties);
                break;
            case CUSTOM_CATALOG:
                String nativeCatalogImpl = properties.get(ICEBERG_IMPL);
                catalogLoader = CatalogLoader.custom(icebergNativeCatalogName, conf, properties, nativeCatalogImpl);
                this.catalogImpl = nativeCatalogImpl;
                this.customProperties = properties;
                break;
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.", ICEBERG_CATALOG_TYPE);
        }
        return  (IcebergCatalog) catalogLoader.loadCatalog();
    }

    private IcebergCatalogType getNativeCatalogType() {
        String nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_TYPE);
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_LEGACY);
        }
        this.catalogType = nativeCatalogTypeStr;
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            throw new StarRocksConnectorException("Can't find iceberg native catalog type");
        }
        return IcebergCatalogType.fromString(nativeCatalogTypeStr);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, icebergNativeCatalog, metastoreURI, catalogImpl, catalogType, customProperties);
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
