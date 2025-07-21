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
import com.starrocks.common.util.DlfUtil;
import com.starrocks.common.util.Util;
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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.hadoop.HadoopFileIOLoader;
import org.apache.paimon.options.Options;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_INSTANCE_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_AUTH_USER_NAME;
import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String DLF_CATALOG_ID = "dlf.catalog.id";
    private static final String DLF_USER_AGENT_KEY = "header.User-Agent";
    private static final String OSS_USER_AGENT_KEY = "fs.oss.user.agent.extended";
    private final HdfsEnvironment hdfsEnvironment;
    private final Map<String, Catalog> nativePaimonCatalogs = new ConcurrentHashMap<>();
    private final String catalogName;
    private final String catalogType;
    private final Options paimonOptions;
    private final ConnectorProperties connectorProperties;
    private String ramUser = "";

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.connectorProperties = new ConnectorProperties(ConnectorType.PAIMON, properties);
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        this.catalogType = properties.get(PAIMON_CATALOG_TYPE);
        String metastoreUris = properties.get(HIVE_METASTORE_URIS);
        String warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

        this.paimonOptions = new Options();
        if (Strings.isNullOrEmpty(catalogType)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_TYPE);
        }
        this.paimonOptions.setString(METASTORE.key(), catalogType);
        if (catalogType.equals("hive")) {
            if (!Strings.isNullOrEmpty(metastoreUris)) {
                this.paimonOptions.setString(URI.key(), metastoreUris);
            } else {
                throw new StarRocksConnectorException("The property %s must be set if paimon catalog is hive.",
                        HIVE_METASTORE_URIS);
            }
        } else if (catalogType.equalsIgnoreCase("dlf") || catalogType.equalsIgnoreCase("dlf-hive")) {
            String dlfCatalogId = properties.get(DLF_CATALOG_ID);
            if (null != dlfCatalogId && !dlfCatalogId.isEmpty()) {
                this.paimonOptions.setString(DLF_CATALOG_ID, dlfCatalogId);
            }
            // By default, dlf-sdk-assembly uses hive2 to access dlf 1.0, however StarRocks only include hive3 in its
            // dependency, so we set this config to let dlf-sdk-assembly use hive3 manually.
            this.paimonOptions.setString("hive.dlf.imetastoreclient.class",
                    "com.aliyun.datalake.metastore.hive3.ProxyMetaStoreClient");
        } else if (catalogType.equalsIgnoreCase("dlf-paimon")) {
            if (Strings.isNullOrEmpty(properties.get(CATALOG_ID))) {
                // CATALOG_INSTANCE_ID is deprecated
                if (null != properties.get(CATALOG_INSTANCE_ID)) {
                    properties.put(CATALOG_ID, properties.get(CATALOG_INSTANCE_ID));
                    properties.remove(CATALOG_INSTANCE_ID);
                } else {
                    throw new StarRocksConnectorException("The property %s must be set.", CATALOG_ID);
                }
            }
        } else if (catalogType.equalsIgnoreCase("rest")) {
            // DLF 2.5
            if ("dlf".equalsIgnoreCase(properties.get("token.provider"))) {
                this.paimonOptions.set(URI.key(), properties.get("uri"));
                this.paimonOptions.set("token.provider", "dlf");
            }
        }
        if (Strings.isNullOrEmpty(warehousePath)
                && !catalogType.equals("hive")
                && !catalogType.equalsIgnoreCase("dlf")
                && !catalogType.equalsIgnoreCase("dlf-hive")
                && !catalogType.equalsIgnoreCase("dlf-paimon")) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        // use only for oss-hdfs
        if (!Strings.isNullOrEmpty(warehousePath)
                && !catalogType.equals("rest")
                && warehousePath.charAt(warehousePath.length() - 1) != '/') {
            warehousePath += "/";
        }
        if (!Strings.isNullOrEmpty(warehousePath)) {
            this.paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        }
        initFsOption(cloudConfiguration);
        // default cache expire time
        this.paimonOptions.set("cache.expiration-interval", "7200s");
        this.paimonOptions.set("cache.expire-after-access", "7200s");
        this.paimonOptions.set("cache.expire-after-write", "3600s");
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
            this.paimonOptions.setString(key, properties.get(k));
        }
        properties.keySet().stream()
                .filter(k -> k.startsWith("dlf.") && !k.equals(DLF_AUTH_USER_NAME))
                .forEach(k -> paimonOptions.setString(k, properties.get(k)));
    }

    public void initFsOption(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration.getCloudType() == CloudType.AWS) {
            AwsCloudConfiguration awsCloudConfiguration = (AwsCloudConfiguration) cloudConfiguration;
            this.paimonOptions.set("s3.connection.ssl.enabled", String.valueOf(awsCloudConfiguration.getEnableSSL()));
            this.paimonOptions.set("s3.path.style.access", String.valueOf(awsCloudConfiguration.getEnablePathStyleAccess()));
            AwsCloudCredential awsCloudCredential = awsCloudConfiguration.getAwsCloudCredential();
            if (!awsCloudCredential.getEndpoint().isEmpty()) {
                this.paimonOptions.set("s3.endpoint", awsCloudCredential.getEndpoint());
            }
            if (!awsCloudCredential.getAccessKey().isEmpty()) {
                this.paimonOptions.set("s3.access-key", awsCloudCredential.getAccessKey());
            }
            if (!awsCloudCredential.getSecretKey().isEmpty()) {
                this.paimonOptions.set("s3.secret-key", awsCloudCredential.getSecretKey());
            }
        }
        if (cloudConfiguration.getCloudType() == CloudType.ALIYUN) {
            AliyunCloudConfiguration aliyunCloudConfiguration = (AliyunCloudConfiguration) cloudConfiguration;
            AliyunCloudCredential aliyunCloudCredential = aliyunCloudConfiguration.getAliyunCloudCredential();
            if (!aliyunCloudCredential.getEndpoint().isEmpty()) {
                this.paimonOptions.set("fs.oss.endpoint", aliyunCloudCredential.getEndpoint());
            }
            if (!aliyunCloudCredential.getAccessKey().isEmpty()) {
                this.paimonOptions.set("fs.oss.accessKeyId", aliyunCloudCredential.getAccessKey());
            }
            if (!aliyunCloudCredential.getSecretKey().isEmpty()) {
                this.paimonOptions.set("fs.oss.accessKeySecret", aliyunCloudCredential.getSecretKey());
            }
        }
    }

    public Options getPaimonOptions() {
        return this.paimonOptions;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public Catalog getPaimonNativeCatalog() {
        try {
            String catalogKey = "";
            String ramUser = "";
            // DLF 2.5 or DLF 2.0
            if ((catalogType.equalsIgnoreCase("rest") && this.paimonOptions.get("token.provider").equalsIgnoreCase("dlf"))
                    || catalogType.equalsIgnoreCase("dlf-paimon")) {
                ramUser = DlfUtil.getRamUser();
                boolean noAK = Strings.isNullOrEmpty(this.paimonOptions.get("dlf.access-key-id"))
                        || Strings.isNullOrEmpty(this.paimonOptions.get("dlf.access-key-secret"));
                // Only search for meta token path when users do not config ak/sk themselves
                if ("dlf".equalsIgnoreCase(this.paimonOptions.get("token.provider")) && noAK) {
                    // For DLF 2.5, we should get the exact meta token for user
                    this.paimonOptions.set("dlf.token-path", DlfUtil.getMetaToken(ramUser));
                }
                // Do not need ram user check when using ak/sk
                if (noAK) {
                    if (Strings.isNullOrEmpty(ramUser)) {
                        String qualifiedUser = ConnectContext.get().getQualifiedUser();
                        String user = ConnectContext.get().getCurrentUserIdentity().getUser();
                        throw new StarRocksConnectorException("Failed to find a valid RAM user from {} and {}.",
                                qualifiedUser, user);
                    } else {
                        catalogKey = this.catalogName + "-" + ramUser;
                        this.paimonOptions.set(DLF_AUTH_USER_NAME, ramUser);
                    }
                } else {
                    catalogKey = this.catalogName + "-" + "base";
                }
            } else {
                catalogKey = this.catalogName + "-" + "base";
            }

            if (Util.isRootUser()) {
                this.paimonOptions.set(DLF_USER_AGENT_KEY, "starrocks/internal");
                this.paimonOptions.set(OSS_USER_AGENT_KEY, "starrocks/internal");
            } else {
                this.paimonOptions.set(DLF_USER_AGENT_KEY, "starrocks/user");
                this.paimonOptions.set(OSS_USER_AGENT_KEY, "starrocks/user");
            }

            if (!catalogKey.isEmpty() && this.nativePaimonCatalogs.get(catalogKey) != null) {
                return this.nativePaimonCatalogs.get(catalogKey);
            }
            Configuration configuration = new Configuration();
            hdfsEnvironment.getCloudConfiguration().applyToConfiguration(configuration);
            Catalog paimonNativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(
                    getPaimonOptions(), configuration, null, new HadoopFileIOLoader()));
            this.nativePaimonCatalogs.put(catalogKey, paimonNativeCatalog);
            if (paimonNativeCatalog instanceof CachingCatalog) {
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                        .registerPaimonCatalog(catalogKey, this.nativePaimonCatalogs.get(catalogKey));
            }
            return paimonNativeCatalog;
        } catch (Exception e) {
            if (e instanceof NullPointerException ||
                    (e.getMessage() != null && e.getMessage().contains(DLF_AUTH_USER_NAME))) {
                throw new StarRocksConnectorException("NPE found. Maybe current user is not a ram user. " + e.getMessage(), e);
            }
            throw new StarRocksConnectorException("Error creating a paimon catalog.", e);
        }
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
