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
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.credential.aws.AWSCloudConfiguration;
import com.starrocks.credential.aws.AWSCloudCredential;
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
    private static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    private static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private final HdfsEnvironment hdfsEnvironment;
    private Catalog paimonNativeCatalog;
    private final String catalogName;
    private final Options paimonOptions;

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
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
        }
        if (Strings.isNullOrEmpty(warehousePath)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        initFsOption(cloudConfiguration);
        String keyPrefix = "paimon.option.";
        Set<String> optionKeys = properties.keySet().stream().filter(k -> k.startsWith(keyPrefix)).collect(Collectors.toSet());
        for (String k : optionKeys) {
            String key = k.substring(keyPrefix.length());
            paimonOptions.setString(key, properties.get(k));
        }
    }

    public void initFsOption(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration.getCloudType() == CloudType.AWS) {
            AWSCloudConfiguration awsCloudConfiguration = (AWSCloudConfiguration) cloudConfiguration;
            paimonOptions.set("s3.connection.ssl.enabled", String.valueOf(awsCloudConfiguration.getEnableSSL()));
            paimonOptions.set("s3.path.style.access", String.valueOf(awsCloudConfiguration.getEnablePathStyleAccess()));
            AWSCloudCredential awsCloudCredential = awsCloudConfiguration.getAWSCloudCredential();
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
            this.paimonNativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(getPaimonOptions()));
        }
        return paimonNativeCatalog;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new PaimonMetadata(catalogName, hdfsEnvironment, getPaimonNativeCatalog());
    }
}
