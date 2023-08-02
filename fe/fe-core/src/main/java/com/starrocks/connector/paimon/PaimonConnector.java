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
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(PaimonConnector.class);
    private static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    private static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String AWS_S3_ENABLE_SSL = "aws.s3.enable_ssl";
    private static final String AWS_S3_ENABLE_PATH_STYLE_ACCESS = "aws.s3.enable_path_style_access";
    private static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
    private static final String AWS_S3_ACCESS_KEY = "aws.s3.access_key";
    private static final String AWS_S3_SECRET_KEY = "aws.s3.secret_key";
    private static final String PAIMON_S3A_ENABLE_SSL = "fs.s3a.enable.ssl";
    private static final String PAIMON_S3A_ENABLE_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    private static final String PAIMON_S3A_ENDPOINT = "fs.s3a.endpoint";
    private static final String PAIMON_S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String PAIMON_S3A_SECRET_KEY = "fs.s3a.secret.key";

    private final CloudConfiguration cloudConfiguration;
    private Catalog paimonNativeCatalog;
    private final String catalogType;
    private final String metastoreUris;
    private final String warehousePath;
    private final String catalogName;
    private final String awsS3EnableSsl;
    private final String awsS3EnablePathStyleAccess;
    private final String awsS3Endpoint;
    private final String awsS3AccessKey;
    private final String awsS3SecretKey;


    private final Options paimonOptions;

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.catalogType = properties.get(PAIMON_CATALOG_TYPE);
        this.metastoreUris = properties.get(HIVE_METASTORE_URIS);
        this.warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

        this.awsS3EnableSsl = properties.get(AWS_S3_ENABLE_SSL);
        this.awsS3EnablePathStyleAccess = properties.get(AWS_S3_ENABLE_PATH_STYLE_ACCESS);
        this.awsS3Endpoint = properties.get(AWS_S3_ENDPOINT);
        this.awsS3AccessKey = properties.get(AWS_S3_ACCESS_KEY);
        this.awsS3SecretKey = properties.get(AWS_S3_SECRET_KEY);

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

        if (!Strings.isNullOrEmpty(awsS3AccessKey) && !Strings.isNullOrEmpty(awsS3SecretKey)) {
            paimonOptions.setString(PAIMON_S3A_ENABLE_SSL, awsS3EnableSsl);
            paimonOptions.setString(PAIMON_S3A_ENABLE_PATH_STYLE_ACCESS, awsS3EnablePathStyleAccess);
            paimonOptions.setString(PAIMON_S3A_ENDPOINT, awsS3Endpoint);
            paimonOptions.setString(PAIMON_S3A_ACCESS_KEY, awsS3AccessKey);
            paimonOptions.setString(PAIMON_S3A_SECRET_KEY, awsS3SecretKey);
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
        return new PaimonMetadata(catalogName, getPaimonNativeCatalog(), catalogType, metastoreUris, warehousePath);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }
}
