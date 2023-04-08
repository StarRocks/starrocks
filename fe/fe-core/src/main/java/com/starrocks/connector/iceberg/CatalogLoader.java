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

import com.google.common.collect.Maps;
import com.starrocks.common.util.Util;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_IMPL;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_METASTORE_URIS;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;

public interface CatalogLoader {

    static Catalog loadCatalog(String name, IcebergCatalogType nativeCatalogType,
                                      Configuration hadoopConf, Map<String, String> properties) {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        String catalogImpl;
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
        copiedProperties.remove(ICEBERG_CATALOG_TYPE);

        return CatalogUtil.buildIcebergCatalog(name, copiedProperties, hadoopConf);
    }
}
