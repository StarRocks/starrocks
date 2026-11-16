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

package com.starrocks.credential;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.common.util.CredentialMask;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.connector.iceberg.IcebergCatalogProperties;
import com.starrocks.connector.iceberg.rest.OAuth2SecurityConfig;
import com.starrocks.connector.odps.OdpsProperties;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.azure.AzureStoragePath;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CredentialUtil {
    public static final Logger LOG = LogManager.getLogger(CredentialUtil.class);

    private static final String ICEBERG_PREFIX = IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX;

<<<<<<< HEAD
    public static void maskCredential(Map<String, String> properties) {
        // Mask for aws's credential
        doMask(properties, CloudConfigurationConstants.AWS_S3_ACCESS_KEY);
        doMask(properties, CloudConfigurationConstants.AWS_S3_SECRET_KEY);
        doMask(properties, CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY);
        doMask(properties, CloudConfigurationConstants.AWS_GLUE_SECRET_KEY);

        // Mask for azure's credential
        doMask(properties, CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY);
        doMask(properties, CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN);
        doMask(properties, CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL);
        doMask(properties, CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY);
        doMask(properties, CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN);
        doMask(properties, CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET);

        // Mask for gcs's credential
        doMask(properties, CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY);

        // Mask for aliyun's credential
        doMask(properties, CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY);
        doMask(properties, CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY);

        // Mask for huawei obs credential
        doMask(properties, CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY);
        doMask(properties, CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY);
        doMask(properties, CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY_DOT);
        doMask(properties, CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY_DOT);

        // Mask for tencent's credential
        doMask(properties, CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY);
        doMask(properties, CloudConfigurationConstants.TENCENT_COS_SECRET_KEY);

        // Mask for iceberg rest catalog credential
        doMask(properties, IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                OAuth2SecurityConfig.OAUTH2_CREDENTIAL);
        doMask(properties, IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                OAuth2SecurityConfig.OAUTH2_TOKEN);
        doMask(properties, IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                AwsProperties.REST_ACCESS_KEY_ID);
        doMask(properties, IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                AwsProperties.REST_SECRET_ACCESS_KEY);

        // Mask for iceberg jdbc catalog credential
        doMask(properties, IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                IcebergCatalogProperties.ICEBERG_JDBC_PASSWORD);

        // Mask for odps catalog credential
        doMask(properties, OdpsProperties.ACCESS_ID);
        doMask(properties, OdpsProperties.ACCESS_KEY);
=======
    /**
     * Catalog / storage properties whose values are credentials. This is the single list that
     * SHOW CREATE CATALOG ({@link #maskCredential}), FE logs ({@link #maskedCopy}) and, through
     * {@link com.starrocks.common.util.PrintableMap#SENSITIVE_KEY}, SHOW CREATE / AST rendering and the
     * audit log redaction ({@link com.starrocks.common.util.SqlCredentialRedactor}) all read, so a key
     * added here is masked on every path.
     */
    public static final List<String> CREDENTIAL_KEYS = ImmutableList.of(
            // aws
            CloudConfigurationConstants.AWS_S3_ACCESS_KEY,
            CloudConfigurationConstants.AWS_S3_SECRET_KEY,
            CloudConfigurationConstants.AWS_S3_SESSION_TOKEN,
            CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY,
            CloudConfigurationConstants.AWS_GLUE_SECRET_KEY,
            CloudConfigurationConstants.AWS_GLUE_SESSION_TOKEN,

            // azure
            CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY,
            CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN,
            CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET,
            CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL,
            CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY,
            CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET,

            // gcs
            CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY,

            // aliyun
            CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY,
            CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY,

            // huawei obs
            CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY,
            CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY,
            CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY_DOT,
            CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY_DOT,

            // tencent
            CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY,
            CloudConfigurationConstants.TENCENT_COS_SECRET_KEY,

            // hdfs. The *_KEYTAB keys hold a path rather than the credential itself, so only the inline
            // keytab content and the password are masked. The deprecated bare "password" key needs no
            // entry: maskCredential removes it outright as JDBCResource.PASSWORD.
            CloudConfigurationConstants.HDFS_PASSWORD,
            CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB_CONTENT,
            CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED,

            // iceberg rest catalog
            ICEBERG_PREFIX + OAuth2SecurityConfig.OAUTH2_CREDENTIAL,
            ICEBERG_PREFIX + OAuth2SecurityConfig.OAUTH2_TOKEN,
            ICEBERG_PREFIX + AwsProperties.REST_ACCESS_KEY_ID,
            ICEBERG_PREFIX + AwsProperties.REST_SECRET_ACCESS_KEY,

            // iceberg S3FileIO, passed through the catalog properties
            ICEBERG_PREFIX + S3FileIOProperties.ACCESS_KEY_ID,
            ICEBERG_PREFIX + S3FileIOProperties.SECRET_ACCESS_KEY,
            ICEBERG_PREFIX + S3FileIOProperties.SESSION_TOKEN,

            // iceberg GCSFileIO / ADLSFileIO (GCPProperties / AzureProperties), passed through the catalog properties
            ICEBERG_PREFIX + "gcs.oauth2.token",
            ICEBERG_PREFIX + "gcs.client-lib-token",
            ICEBERG_PREFIX + "gcs.encryption-key",
            ICEBERG_PREFIX + "gcs.decryption-key",
            ICEBERG_PREFIX + "adls.auth.shared-key.account.key",
            ICEBERG_PREFIX + "adls.token",

            // iceberg jdbc catalog
            ICEBERG_PREFIX + IcebergCatalogProperties.ICEBERG_JDBC_PASSWORD,

            // odps catalog
            OdpsProperties.ACCESS_ID,
            OdpsProperties.ACCESS_KEY,

            // databricks
            DatabricksUnityMetastore.DATABRICKS_TOKEN,
            DatabricksUnityMetastore.DATABRICKS_CLIENT_SECRET,

            // StarRocks external catalog
            StarRocksConnectorConfig.PASSWORD);

    /**
     * Keys that carry a credential under a variable suffix, e.g. the per storage account ADLS SAS token
     * {@code iceberg.catalog.adls.sas-token.<account>}.
     */
    public static final List<String> CREDENTIAL_KEY_PREFIXES = ImmutableList.of(
            ICEBERG_PREFIX + "adls.sas-token.");

    /**
     * Returns a copy of {@code properties} with credentials masked, leaving the argument untouched.
     * Use this when the properties are only being rendered, e.g. logged: {@link #maskCredential}
     * mutates in place and would corrupt live configuration.
     */
    public static Map<String, String> maskedCopy(Map<String, String> properties) {
        Map<String, String> masked = new HashMap<>(properties);
        maskCredential(masked);
        return masked;
>>>>>>> e0fce297ee5... [BugFix][RB-1362] Mask catalog credentials in logs, audit and SHOW CREATE CATALOG (#69857)
    }

    public static void maskCredential(Map<String, String> properties) {
        // This key is only auxiliary authentication for Azure and does not need to be exposed.
        properties.remove(CloudConfigurationConstants.AZURE_PATH_KEY);

        // Remove for jdbc catalog's password
        properties.remove(JDBCResource.PASSWORD);

        // PrintableMap.SENSITIVE_KEY contains CREDENTIAL_KEYS, so this masks the keys listed above, the
        // prefixed keys, and the keys only PrintableMap lists (e.g. fs.s3a.secret.key).
        properties.replaceAll((key, value) -> value != null && PrintableMap.isSensitiveKey(key) ? mask(value) : value);
    }

    private static String mask(String value) {
        if (value.length() <= 4) {
            return CredentialMask.LONG;
        } else {
            return new StringBuilder(value).
                    replace(2, value.length() - 2, CredentialMask.LONG).
                    toString();
        }
    }

    /**
     *  Supported URI:
     *  ADLS Gen2:
     *      abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<path>/<file_name>
     *  ADLS Gen1:
     *  ADLS Gen1's path don't need to be parsed, storage account and container is unnecessary for it.
     *      adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>
     *  Blob
     *      wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<path>/<file_name>
     */
    public static AzureStoragePath parseAzureStoragePath(String path) {
        try {
            URI uri = new URI(path);
            String rawAuthority = uri.getRawAuthority();
            if (rawAuthority == null) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }
            String[] parts = uri.getRawAuthority().split("@");
            if (parts.length < 2) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }

            if (!path.contains(".blob.core.windows.net") && !path.contains(".dfs.core.windows.net")) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }

            String container = parts[0];
            if (container.isEmpty()) {
                throw new URISyntaxException(path, "Empty container name in azure storage path");
            }

            String[] leftParts = parts[1].split("\\.");
            String storageAccount = leftParts[0];
            if (storageAccount.isEmpty()) {
                throw new URISyntaxException(path, "Empty storage account in azure storage path");
            }
            return new AzureStoragePath(storageAccount, container);
        } catch (URISyntaxException exception) {
            LOG.debug(exception.getMessage());
        }
        // Return empty AzureStoragePath
        return new AzureStoragePath("", "");
    }
}
