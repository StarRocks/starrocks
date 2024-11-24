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
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.AdlConfKeys;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

// For Azure Blob Storage (wasb:// & wasbs://)
// We support Shared Key & SAS Token
// For Azure Data Lake Gen1 (adl://)
// We support Managed Service Identity & Service Principal
// For Azure Data Lake Gen2 (abfs:// & abfss://)
// We support Managed Identity & Shared Key & Service Principal
abstract class AzureStorageCloudCredential implements CloudCredential {

    public static final Logger LOG = LogManager.getLogger(AzureStorageCloudCredential.class);

    protected Map<String, String> generatedConfigurationMap = new HashMap<>();

    @Override
    public void applyToConfiguration(Configuration configuration) {
        for (Map.Entry<String, String> entry : generatedConfigurationMap.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean validate() {
        return !generatedConfigurationMap.isEmpty();
    }

    @Override
    public void toThrift(Map<String, String> properties) {
        properties.putAll(generatedConfigurationMap);
    }

    abstract void tryGenerateConfigurationMap();
}

class AzureBlobCloudCredential extends AzureStorageCloudCredential {
    private final String endpoint;
    private final String storageAccount;
    private final String sharedKey;
    private final String container;
    private final String sasToken;

    AzureBlobCloudCredential(String endpoint, String storageAccount, String sharedKey, String container, String sasToken) {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(storageAccount);
        Preconditions.checkNotNull(sharedKey);
        Preconditions.checkNotNull(container);
        Preconditions.checkNotNull(sasToken);
        this.endpoint = endpoint;
        this.storageAccount = storageAccount;
        this.sharedKey = sharedKey;
        this.container = container;
        this.sasToken = sasToken;
        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (!endpoint.isEmpty()) {
            // If user specific endpoint, they don't need to specific storage account anymore
            // Like if user is using Azurite, they need to specific endpoint
            if (!sharedKey.isEmpty()) {
                String key = String.format("fs.azure.account.key.%s", endpoint);
                generatedConfigurationMap.put(key, sharedKey);
            } else if (!container.isEmpty() && !sasToken.isEmpty()) {
                String key = String.format("fs.azure.sas.%s.%s", container, endpoint);
                generatedConfigurationMap.put(key, sasToken);
            }
        } else {
            if (!storageAccount.isEmpty() && !sharedKey.isEmpty()) {
                String key = String.format("fs.azure.account.key.%s.blob.core.windows.net", storageAccount);
                generatedConfigurationMap.put(key, sharedKey);
            } else if (!storageAccount.isEmpty() && !container.isEmpty() && !sasToken.isEmpty()) {
                String key =
                        String.format("fs.azure.sas.%s.%s.blob.core.windows.net", container, storageAccount);
                generatedConfigurationMap.put(key, sasToken);
            }
        }
    }

    @Override
    public String toCredString() {
        return "AzureBlobCloudCredential{" +
                "endpoint='" + endpoint + '\'' +
                ", storageAccount='" + storageAccount + '\'' +
                ", sharedKey='" + sharedKey + '\'' +
                ", container='" + container + '\'' +
                ", sasToken='" + sasToken + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.AZBLOB);
        AzBlobFileStoreInfo.Builder azBlobFileStoreInfo = AzBlobFileStoreInfo.newBuilder();
        azBlobFileStoreInfo.setEndpoint(endpoint);
        AzBlobCredentialInfo.Builder azBlobCredentialInfo = AzBlobCredentialInfo.newBuilder();
        azBlobCredentialInfo.setSharedKey(sharedKey);
        azBlobCredentialInfo.setSasToken(sasToken);
        azBlobFileStoreInfo.setCredential(azBlobCredentialInfo.build());
        fileStore.setAzblobFsInfo(azBlobFileStoreInfo.build());
        return fileStore.build();
    }
}

class AzureADLS1CloudCredential extends AzureStorageCloudCredential {
    private final boolean useManagedServiceIdentity;
    private final String oauth2ClientId;
    private final String oauth2Credential;
    private final String oauth2Endpoint;

    public AzureADLS1CloudCredential(boolean useManagedServiceIdentity, String oauth2ClientId, String oauth2Credential,
                      String oauth2Endpoint) {
        Preconditions.checkNotNull(oauth2ClientId);
        Preconditions.checkNotNull(oauth2Credential);
        Preconditions.checkNotNull(oauth2Endpoint);
        this.useManagedServiceIdentity = useManagedServiceIdentity;
        this.oauth2ClientId = oauth2ClientId;
        this.oauth2Credential = oauth2Credential;
        this.oauth2Endpoint = oauth2Endpoint;

        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (useManagedServiceIdentity) {
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, "Msi");
        } else if (!oauth2ClientId.isEmpty() && !oauth2Credential.isEmpty() &&
                !oauth2Endpoint.isEmpty()) {
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, "ClientCredential");
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_CLIENT_ID_KEY, oauth2ClientId);
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_CLIENT_SECRET_KEY, oauth2Credential);
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_REFRESH_URL_KEY, oauth2Endpoint);
        }
    }

    @Override
    public String toCredString() {
        return "AzureADLS1CloudCredential{" +
                "useManagedServiceIdentity=" + useManagedServiceIdentity +
                ", oauth2ClientId='" + oauth2ClientId + '\'' +
                ", oauth2Credential='" + oauth2Credential + '\'' +
                ", oauth2Endpoint='" + oauth2Endpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return null;
    }
}

class AzureADLS2CloudCredential extends AzureStorageCloudCredential {
    private final boolean oauth2ManagedIdentity;
    private final String oauth2TenantId;
    private final String oauth2ClientId;
    private final String storageAccount;
    private final String sharedKey;
    private final String oauth2ClientSecret;
    private final String oauth2ClientEndpoint;

    public AzureADLS2CloudCredential(boolean oauth2ManagedIdentity, String oauth2TenantId, String oauth2ClientId,
                                     String storageAccount, String sharedKey, String oauth2ClientSecret,
                                     String oauth2ClientEndpoint) {
        Preconditions.checkNotNull(oauth2TenantId);
        Preconditions.checkNotNull(oauth2ClientId);
        Preconditions.checkNotNull(storageAccount);
        Preconditions.checkNotNull(sharedKey);
        Preconditions.checkNotNull(oauth2ClientSecret);
        Preconditions.checkNotNull(oauth2ClientEndpoint);

        this.oauth2ManagedIdentity = oauth2ManagedIdentity;
        this.oauth2TenantId = oauth2TenantId;
        this.oauth2ClientId = oauth2ClientId;
        this.storageAccount = storageAccount;
        this.sharedKey = sharedKey;
        this.oauth2ClientSecret = oauth2ClientSecret;
        this.oauth2ClientEndpoint = oauth2ClientEndpoint;

        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (oauth2ManagedIdentity && !oauth2TenantId.isEmpty() && !oauth2ClientId.isEmpty()) {
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME),
                    "OAuth");
            generatedConfigurationMap.put(
                    createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME),
                    MsiTokenProvider.class.getName());
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT),
                    oauth2TenantId);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
                    oauth2ClientId);
        } else if (!storageAccount.isEmpty() && !sharedKey.isEmpty()) {
            // Shared Key is always used by specific storage account, so we don't need to invoke createConfigKey()
            generatedConfigurationMap.put(
                    String.format("fs.azure.account.auth.type.%s.dfs.core.windows.net", storageAccount),
                    "SharedKey");
            generatedConfigurationMap.put(
                    String.format("fs.azure.account.key.%s.dfs.core.windows.net", storageAccount),
                    sharedKey);
        } else if (!oauth2ClientId.isEmpty() && !oauth2ClientSecret.isEmpty() &&
                !oauth2ClientEndpoint.isEmpty()) {
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME),
                    "OAuth");
            generatedConfigurationMap.put(
                    createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME),
                    ClientCredsTokenProvider.class.getName());
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
                    oauth2ClientId);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET),
                    oauth2ClientSecret);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT),
                    oauth2ClientEndpoint);
        }
    }

    @Override
    public String toCredString() {
        return "AzureADLS2CloudCredential{" +
                "oauth2ManagedIdentity=" + oauth2ManagedIdentity +
                ", oauth2TenantId='" + oauth2TenantId + '\'' +
                ", oauth2ClientId='" + oauth2ClientId + '\'' +
                ", storageAccount='" + storageAccount + '\'' +
                ", sharedKey='" + sharedKey + '\'' +
                ", oauth2ClientSecret='" + oauth2ClientSecret + '\'' +
                ", oauth2ClientEndpoint='" + oauth2ClientEndpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        // TODO: Support azure credential
        return null;
    }

    // Create Hadoop configuration key for specific storage account, if storage account is not set, means this property
    // is shared by all storage account.
    // This grammar only supported by ABFS
    // https://hadoop.apache.org/docs/r3.3.4/hadoop-azure/abfs.html#Configuring_ABFS
    private String createConfigKey(String key) {
        if (storageAccount.isEmpty()) {
            return key;
        } else {
            return String.format("%s.%s.dfs.core.windows.net", key, storageAccount);
        }
    }
}