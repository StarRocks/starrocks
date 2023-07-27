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

import com.starrocks.catalog.JDBCResource;
import com.starrocks.credential.azure.AzureCloudConfigurationFactory;
import com.starrocks.credential.azure.AzureStoragePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class CloudCredentialUtil {
    public static final Logger LOG = LogManager.getLogger(CloudCredentialUtil.class);

    private static final String MASK_CLOUD_CREDENTIAL_WORDS = "******";

    public static void maskCloudCredential(Map<String, String> properties) {
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
        doMask(properties, CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET);

        // Mask for gcs's credential
        doMask(properties, CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY);
    }

    private static void doMask(Map<String, String> properties, String configKey) {
        // This key is only auxiliary authentication for Azure and does not need to be exposed.
        properties.remove(AzureCloudConfigurationFactory.AZURE_PATH_KEY);
        // Remove password of jdbc catalog
        properties.remove(JDBCResource.PASSWORD);

        // do mask
        properties.computeIfPresent(configKey, (key, value) -> {
            if (value.length() <= 4) {
                return MASK_CLOUD_CREDENTIAL_WORDS;
            } else {
                return new StringBuilder(value).
                        replace(2, value.length() - 2, MASK_CLOUD_CREDENTIAL_WORDS).
                        toString();
            }
        });
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
