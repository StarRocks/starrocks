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
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String DLF_CATGALOG_ID = "dlf.catalog.id";
    private static final String JDBC_URI = "uri";
    private static final String JDBC_USER = "jdbc.user";
    private static final String JDBC_PASSWORD = "jdbc.password";
    private static final String CATALOG_KEY = "catalog-key";
    private final HdfsEnvironment hdfsEnvironment;
    private Catalog paimonNativeCatalog;
    private final String catalogName;
    private final Options paimonOptions;
    private final ConnectorProperties connectorProperties;

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.connectorProperties = new ConnectorProperties(ConnectorType.PAIMON, properties);
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        String catalogType = properties.get(PAIMON_CATALOG_TYPE);
        String metastoreUris = properties.get(HIVE_METASTORE_URIS);
        String warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

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
        } else if (catalogType.equalsIgnoreCase("dlf")) {
            String dlfCatalogId = properties.get(DLF_CATGALOG_ID);
            if (null != dlfCatalogId && !dlfCatalogId.isEmpty()) {
                paimonOptions.setString(DLF_CATGALOG_ID, dlfCatalogId);
            }
        } else if (catalogType.equalsIgnoreCase("jdbc")) {
            // Support for Paimon JDBC catalog
            // JDBC URI is required for connecting to the metastore database (MySQL, PostgreSQL, etc.)
            String jdbcUri = properties.get(JDBC_URI);
            if (Strings.isNullOrEmpty(jdbcUri)) {
                throw new StarRocksConnectorException(
                        "The property '%s' must be set if paimon catalog type is jdbc. " +
                        "Example: jdbc:mysql://host:3306/paimon_metastore", JDBC_URI);
            }
            paimonOptions.setString(URI.key(), jdbcUri);

            // JDBC authentication credentials (passed with jdbc. prefix as per Paimon convention)
            String jdbcUser = properties.get(JDBC_USER);
            if (!Strings.isNullOrEmpty(jdbcUser)) {
                paimonOptions.setString(JDBC_USER, jdbcUser);
            }
            String jdbcPassword = properties.get(JDBC_PASSWORD);
            if (!Strings.isNullOrEmpty(jdbcPassword)) {
                paimonOptions.setString(JDBC_PASSWORD, jdbcPassword);
            }

            // catalog-key is optional, used to isolate multiple Paimon catalogs
            // sharing the same JDBC metastore database. Default value in Paimon is "jdbc"
            String catalogKey = properties.get(CATALOG_KEY);
            if (!Strings.isNullOrEmpty(catalogKey)) {
                paimonOptions.setString(CATALOG_KEY, catalogKey);
            }
        }
        if (Strings.isNullOrEmpty(warehousePath)
                && !catalogType.equals("hive")
                && !catalogType.equalsIgnoreCase("dlf")) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        if (!Strings.isNullOrEmpty(warehousePath)) {
            paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        }
        initFsOption(cloudConfiguration);

        // cache expire time, set to 2h
        this.paimonOptions.set("cache.expiration-interval", "7200s");
        // max num of cached partitions of a Paimon catalog
        this.paimonOptions.set("cache.partition.max-num", "1000");
        // max size of cached manifest files, 10m means cache all since files usually no more than 8m
        this.paimonOptions.set("cache.manifest.small-file-threshold", "10m");
        // max size of memory manifest cache uses
        this.paimonOptions.set("cache.manifest.small-file-memory", "1g");

        String keyPrefix = "paimon.option.";
        Set<String> optionKeys = properties.keySet().stream().filter(k -> k.startsWith(keyPrefix)).collect(Collectors.toSet());
        for (String k : optionKeys) {
            String key = k.substring(keyPrefix.length());
            paimonOptions.setString(key, properties.get(k));
        }
    }

    public void initFsOption(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration.getCloudType() == CloudType.AWS) {
            AwsCloudConfiguration awsCloudConfiguration = (AwsCloudConfiguration) cloudConfiguration;
            paimonOptions.set("s3.connection.ssl.enabled", String.valueOf(awsCloudConfiguration.getEnableSSL()));
            paimonOptions.set("s3.path.style.access", String.valueOf(awsCloudConfiguration.getEnablePathStyleAccess()));
            AwsCloudCredential awsCloudCredential = awsCloudConfiguration.getAwsCloudCredential();
            if (!awsCloudCredential.getEndpoint().isEmpty()) {
                paimonOptions.set("s3.endpoint", awsCloudCredential.getEndpoint());
            }
            if (!awsCloudCredential.getAccessKey().isEmpty()) {
                paimonOptions.set("s3.access-key", awsCloudCredential.getAccessKey());
            }
            if (!awsCloudCredential.getSecretKey().isEmpty()) {
                paimonOptions.set("s3.secret-key", awsCloudCredential.getSecretKey());
            }
        }
        if (cloudConfiguration.getCloudType() == CloudType.ALIYUN) {
            AliyunCloudConfiguration aliyunCloudConfiguration = (AliyunCloudConfiguration) cloudConfiguration;
            AliyunCloudCredential aliyunCloudCredential = aliyunCloudConfiguration.getAliyunCloudCredential();
            if (!aliyunCloudCredential.getEndpoint().isEmpty()) {
                paimonOptions.set("fs.oss.endpoint", aliyunCloudCredential.getEndpoint());
            }
            if (!aliyunCloudCredential.getAccessKey().isEmpty()) {
                paimonOptions.set("fs.oss.accessKeyId", aliyunCloudCredential.getAccessKey());
            }
            if (!aliyunCloudCredential.getSecretKey().isEmpty()) {
                paimonOptions.set("fs.oss.accessKeySecret", aliyunCloudCredential.getSecretKey());
            }
        }
    }

    public Options getPaimonOptions() {
        return this.paimonOptions;
    }

    public Catalog getPaimonNativeCatalog() {
        if (paimonNativeCatalog == null) {
            Configuration configuration = new Configuration();
            hdfsEnvironment.getCloudConfiguration().applyToConfiguration(configuration);
            this.paimonNativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(getPaimonOptions(), configuration));
            GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                    .registerPaimonCatalog(catalogName, this.paimonNativeCatalog);
        }
        return paimonNativeCatalog;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new PaimonMetadata(catalogName, hdfsEnvironment, getPaimonNativeCatalog(), connectorProperties);
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterPaimonCatalog(catalogName);
    }
}
