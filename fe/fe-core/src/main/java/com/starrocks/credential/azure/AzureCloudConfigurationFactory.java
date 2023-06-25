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

package com.starrocks.credential.azure;

import com.google.common.base.Preconditions;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudCredentialUtil;

import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CLIENT_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS1_USE_MANAGED_SERVICE_IDENTITY;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_ADLS2_STORAGE_ACCOUNT;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_CONTAINER;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_STORAGE_ACCOUNT;

public class AzureCloudConfigurationFactory extends CloudConfigurationFactory {

    private final Map<String, String> properties;

    // Used to retrieve azure load path from configuration map
    public static final String AZURE_PATH_KEY = "azure_path_key";

    public AzureCloudConfigurationFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        Preconditions.checkNotNull(properties);

        AzureStoragePath azureStoragePath = tryGetAzureStoragePath(properties);
        String storageAccount = azureStoragePath.getStorageAccount();
        String container = azureStoragePath.getContainer();

        // Try to build azure blob storage
        AzureBlobCloudCredential blob = new AzureBlobCloudCredential(
                properties.getOrDefault(AZURE_BLOB_ENDPOINT, ""),
                properties.getOrDefault(AZURE_BLOB_STORAGE_ACCOUNT, storageAccount),
                properties.getOrDefault(AZURE_BLOB_SHARED_KEY, ""),
                properties.getOrDefault(AZURE_BLOB_CONTAINER, container),
                properties.getOrDefault(AZURE_BLOB_SAS_TOKEN, "")
        );
        if (blob.validate()) {
            return new AzureCloudConfiguration(blob);
        }

        // Try to build azure data lake gen1
        AzureADLS1CloudCredential adls1 = new AzureADLS1CloudCredential(
                Boolean.parseBoolean(
                        properties.getOrDefault(AZURE_ADLS1_USE_MANAGED_SERVICE_IDENTITY, "false")),
                properties.getOrDefault(AZURE_ADLS1_OAUTH2_CLIENT_ID, ""),
                properties.getOrDefault(AZURE_ADLS1_OAUTH2_CREDENTIAL, ""),
                properties.getOrDefault(AZURE_ADLS1_OAUTH2_ENDPOINT, "")
        );
        if (adls1.validate()) {
            return new AzureCloudConfiguration(adls1);
        }

        // Try to build azure data lake gen2
        AzureADLS2CloudCredential adls2 = new AzureADLS2CloudCredential(
                Boolean.parseBoolean(properties.getOrDefault(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "false")),
                properties.getOrDefault(AZURE_ADLS2_OAUTH2_TENANT_ID, ""),
                properties.getOrDefault(AZURE_ADLS2_OAUTH2_CLIENT_ID, ""),
                properties.getOrDefault(AZURE_ADLS2_STORAGE_ACCOUNT, storageAccount),
                properties.getOrDefault(AZURE_ADLS2_SHARED_KEY, ""),
                properties.getOrDefault(AZURE_ADLS2_OAUTH2_CLIENT_SECRET, ""),
                properties.getOrDefault(AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT, "")
        );
        if (adls2.validate()) {
            return new AzureCloudConfiguration(adls2);
        }
        return null;
    }

    // Consider for FileTable, broker load, we can deduce storage account and container name from path,
    // so we don't need user to specific storage account & container name specifically.
    private AzureStoragePath tryGetAzureStoragePath(Map<String, String> properties) {
        String path = properties.getOrDefault(AZURE_PATH_KEY, "");
        if (path == null) {
            return new AzureStoragePath("", "");
        }
        return CloudCredentialUtil.parseAzureStoragePath(path);
    }
}
