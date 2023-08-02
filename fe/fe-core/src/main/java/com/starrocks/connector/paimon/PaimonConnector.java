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

package com.starrocks.connector.paimon;

import com.google.common.base.Strings;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.starrocks.connector.CatalogConnectorMetadata.wrapInfoSchema;
import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(PaimonConnector.class);
    private static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    private static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final CloudConfiguration cloudConfiguration;
    private Catalog paimonNativeCatalog;
    private final String catalogType;
    private final String metastoreUris;
    private final String warehousePath;
    private final String catalogName;
    private final Options paimonOptions;
    private final InfoSchemaDb infoSchemaDb;

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.catalogType = properties.get(PAIMON_CATALOG_TYPE);
        this.metastoreUris = properties.get(HIVE_METASTORE_URIS);
        this.warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

        this.paimonOptions = new Options();
        if (Strings.isNullOrEmpty(catalogType)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_TYPE);
        }
        paimonOptions.setString(METASTORE.key(), catalogType);
        if (catalogType.equals("hive")) {
            if (!Strings.isNullOrEmpty(metastoreUris)) {
                paimonOptions.setString(URI.key(), metastoreUris);
            } else {
                throw new StarRocksConnectorException("The property %s must be set if paimon catalog is hive.",
                        HIVE_METASTORE_URIS);
            }
        }
        if (Strings.isNullOrEmpty(warehousePath)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        this.infoSchemaDb = new InfoSchemaDb(catalogName);

        Configuration storageConfig = new Configuration();
        cloudConfiguration.applyToConfiguration(storageConfig);
        Iterator<Entry<String, String>> propsIterator = storageConfig.iterator();
        while (propsIterator.hasNext()) {
            Entry<String, String> item = propsIterator.next();
            if (item.getKey().startsWith("fs.s3")) {
                paimonOptions.setString(item.getKey(), item.getValue());
            }
        }
    }

    public Catalog getPaimonNativeCatalog() {
        if (paimonNativeCatalog == null) {
            this.paimonNativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(paimonOptions));
        }
        return paimonNativeCatalog;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        ConnectorMetadata metadata =
                new PaimonMetadata(catalogName, getPaimonNativeCatalog(), catalogType, metastoreUris, warehousePath);
        return wrapInfoSchema(metadata, infoSchemaDb);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
