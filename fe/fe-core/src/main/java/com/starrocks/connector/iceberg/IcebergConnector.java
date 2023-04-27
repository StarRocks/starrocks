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
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.Util;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.IcebergGlueCatalog;
import org.apache.iceberg.hive.IcebergHiveCatalog;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class IcebergConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    @Deprecated
    public static final String ICEBERG_CATALOG_LEGACY = "starrocks.catalog-type";
    @Deprecated
    public static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String ICEBERG_IMPL = "iceberg.catalog-impl";

    private final Map<String, String> properties;
    private final CloudConfiguration cloudConfiguration;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private IcebergCatalog icebergNativeCatalog;

    public IcebergConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
    }

    private IcebergCatalog buildIcebergNativeCatalog() {
        IcebergCatalogType nativeCatalogType = getNativeCatalogType();
        Configuration conf = hdfsEnvironment.getConfiguration();
        String icebergNativeCatalogName = "native-" + nativeCatalogType.name() + "-" + catalogName;

        String catalogImpl;
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        switch (nativeCatalogType) {
            case HIVE_CATALOG:
                String metastoreURI = properties.get(HIVE_METASTORE_URIS);
                if (metastoreURI == null) {
                    metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
                }
                Util.validateMetastoreUris(metastoreURI);
                copiedProperties.put(CatalogProperties.URI, metastoreURI);
                catalogImpl = IcebergHiveCatalog.class.getName();
                break;
            case GLUE_CATALOG:
                catalogImpl = IcebergGlueCatalog.class.getName();
                break;
            case REST_CATALOG:
                catalogImpl = IcebergRESTCatalog.class.getName();
                break;
            case CUSTOM_CATALOG:
                catalogImpl = properties.get(ICEBERG_IMPL);
                break;
            default:
                throw new StarRocksConnectorException("Property %s is missing or not supported now.", ICEBERG_CATALOG_TYPE);
        }
        copiedProperties.put(CatalogProperties.CATALOG_IMPL, catalogImpl);
        copiedProperties.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);

        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(Config.iceberg_worker_num_threads));
        }

        return (IcebergCatalog) CatalogUtil.buildIcebergCatalog(icebergNativeCatalogName, copiedProperties, conf);
    }

    private IcebergCatalogType getNativeCatalogType() {
        String nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_TYPE);
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_LEGACY);
        }
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            throw new StarRocksConnectorException("Can't find iceberg native catalog type");
        }
        return IcebergCatalogType.fromString(nativeCatalogTypeStr);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new IcebergMetadata(catalogName, getNativeCatalog());
    }

    // In order to be compatible with the catalog created with the wrong configuration,
    // icebergNativeCatalog is lazy, mainly to prevent fe restart failure.
    public IcebergCatalog getNativeCatalog() {
        if (icebergNativeCatalog == null) {
            this.icebergNativeCatalog = buildIcebergNativeCatalog();
        }
        return icebergNativeCatalog;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
