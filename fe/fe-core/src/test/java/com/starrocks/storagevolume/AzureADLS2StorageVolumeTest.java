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

package com.starrocks.storagevolume;

import com.staros.proto.ADLS2CredentialInfo;
import com.staros.proto.ADLS2CredentialType;
import com.staros.proto.FileStoreInfo;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.SharedDataStorageVolumeMgr;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TOKEN_FILE;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AzureADLS2StorageVolumeTest {
    private Map<String, String> workloadParams() {
        return new HashMap<>(Map.of(
                AZURE_ADLS2_ENDPOINT, "https://account.dfs.core.windows.net/",
                AZURE_ADLS2_OAUTH2_TENANT_ID, "tenant",
                AZURE_ADLS2_OAUTH2_CLIENT_ID, "client",
                AZURE_ADLS2_OAUTH2_TOKEN_FILE, "/var/run/secrets/azure/tokens/azure-identity-token"));
    }

    private StorageVolume volume(Map<String, String> params) throws Exception {
        return new StorageVolume("1", "adls", "ADLS2", List.of("adls2://container/path"), params, true, "");
    }

    private StorageVolume reload(StorageVolume volume) throws Exception {
        return StorageVolume.fromFileStoreInfo(FileStoreInfo.parseFrom(volume.toFileStoreInfo().toByteArray()));
    }

    private void assertWorkloadIdentity(StorageVolume volume) {
        FileStoreInfo info = volume.toFileStoreInfo();
        ADLS2CredentialInfo credential = info.getAdls2FsInfo().getCredential();
        assertEquals(ADLS2CredentialType.ADLS2_CREDENTIAL_WORKLOAD_IDENTITY, credential.getCredentialType());
        assertEquals(workloadParams().get(AZURE_ADLS2_OAUTH2_TOKEN_FILE), credential.getOauth2TokenFile());
        assertEquals("tenant", credential.getTenantId());
        assertEquals("client", credential.getClientId());
        Map<String, String> restored = StorageVolume.getParamsFromFileStoreInfo(info);
        assertEquals(workloadParams().get(AZURE_ADLS2_OAUTH2_TOKEN_FILE), restored.get(AZURE_ADLS2_OAUTH2_TOKEN_FILE));
        assertFalse(Boolean.parseBoolean(restored.get(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY)));
        Configuration conf = new Configuration(false);
        volume.getCloudConfiguration().applyToConfiguration(conf);
        assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
                conf.get("fs.azure.account.oauth.provider.type"));
        assertEquals(credential.getOauth2TokenFile(), conf.get("fs.azure.account.oauth2.token.file"));
    }

    @Test
    public void testWorkloadIdentitySurvivesRepeatedPersistence() throws Exception {
        StorageVolume volume = volume(workloadParams());
        assertWorkloadIdentity(volume);
        for (int i = 0; i < 3; i++) {
            volume = reload(volume);
            assertWorkloadIdentity(volume);
        }
    }

    @Test
    public void testAlterSharedKeyToWorkloadIdentityAndBack() throws Exception {
        Map<String, String> params = workloadParams();
        params.remove(AZURE_ADLS2_OAUTH2_TOKEN_FILE);
        params.put(AZURE_ADLS2_SHARED_KEY, "key");
        StorageVolume volume = reload(volume(params));
        // ALTER merges properties. A retained key must not become a managed identity after persistence.
        volume.setCloudConfiguration(workloadParams());
        volume = reload(volume);
        assertEquals(ADLS2CredentialType.ADLS2_CREDENTIAL_SHARED_KEY,
                volume.toFileStoreInfo().getAdls2FsInfo().getCredential().getCredentialType());
        volume.setCloudConfiguration(Map.of(AZURE_ADLS2_SHARED_KEY, ""));
        volume = reload(volume);
        assertWorkloadIdentity(volume);
        volume.setCloudConfiguration(Map.of(AZURE_ADLS2_SHARED_KEY, "replacement-key"));
        volume = reload(volume);
        assertEquals(ADLS2CredentialType.ADLS2_CREDENTIAL_SHARED_KEY,
                volume.toFileStoreInfo().getAdls2FsInfo().getCredential().getCredentialType());
        Configuration conf = new Configuration(false);
        volume.getCloudConfiguration().applyToConfiguration(conf);
        assertEquals("replacement-key", conf.get("fs.azure.account.key.account.dfs.core.windows.net"));
    }

    @Test
    public void testManagedIdentityRemainsExplicitWithOtherCredentials() throws Exception {
        Map<String, String> params = workloadParams();
        params.put(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "true");
        params.put(AZURE_ADLS2_SHARED_KEY, "key");
        StorageVolume volume = reload(volume(params));
        assertEquals(ADLS2CredentialType.ADLS2_CREDENTIAL_MANAGED_IDENTITY,
                volume.toFileStoreInfo().getAdls2FsInfo().getCredential().getCredentialType());
        Configuration conf = new Configuration(false);
        volume.getCloudConfiguration().applyToConfiguration(conf);
        assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
                conf.get("fs.azure.account.oauth.provider.type"));
    }

    @Test
    public void testLegacyCredentialInferenceDoesNotShadowOtherCredentials() throws Exception {
        Map<String, ADLS2CredentialType> credentialTypes = Map.of(
                AZURE_ADLS2_SHARED_KEY, ADLS2CredentialType.ADLS2_CREDENTIAL_SHARED_KEY,
                AZURE_ADLS2_SAS_TOKEN, ADLS2CredentialType.ADLS2_CREDENTIAL_SAS_TOKEN,
                AZURE_ADLS2_OAUTH2_CLIENT_SECRET, ADLS2CredentialType.ADLS2_CREDENTIAL_CLIENT_SECRET,
                AZURE_ADLS2_OAUTH2_TOKEN_FILE, ADLS2CredentialType.ADLS2_CREDENTIAL_WORKLOAD_IDENTITY,
                AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, ADLS2CredentialType.ADLS2_CREDENTIAL_MANAGED_IDENTITY);
        for (Map.Entry<String, ADLS2CredentialType> entry : credentialTypes.entrySet()) {
            String key = entry.getKey();
            Map<String, String> params = workloadParams();
            params.remove(AZURE_ADLS2_OAUTH2_TOKEN_FILE);
            params.put(AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT, "https://login.microsoftonline.com/tenant/oauth2/token");
            params.put(key, key.equals(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY) ? "true" : "value");
            FileStoreInfo info = volume(params).toFileStoreInfo();
            ADLS2CredentialType expectedType = entry.getValue();
            assertEquals(expectedType, info.getAdls2FsInfo().getCredential().getCredentialType(), key);
            // Simulate data written without the new credential-type field.
            info = info.toBuilder().setAdls2FsInfo(info.getAdls2FsInfo().toBuilder()
                    .setCredential(info.getAdls2FsInfo().getCredential().toBuilder().clearCredentialType())).build();
            StorageVolume restored = StorageVolume.fromFileStoreInfo(FileStoreInfo.parseFrom(info.toByteArray()));
            assertEquals(expectedType, restored.toFileStoreInfo().getAdls2FsInfo().getCredential().getCredentialType(), key);
            assertEquals(key.equals(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY), Boolean.parseBoolean(
                    StorageVolume.getParamsFromFileStoreInfo(info).get(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY)), key);
        }
    }

    @Test
    public void testBuiltinVolumeReadsTokenFileFromConfig() throws Exception {
        String oldStorageType = Config.cloud_native_storage_type;
        String oldTokenFile = Config.azure_adls2_oauth2_token_file;
        try {
            Config.cloud_native_storage_type = "adls2";
            Config.azure_adls2_oauth2_token_file = workloadParams().get(AZURE_ADLS2_OAUTH2_TOKEN_FILE);
            Map<String, String> params = Deencapsulation.invoke(new SharedDataStorageVolumeMgr(), "parseParamsFromConfig");
            assertEquals(Config.azure_adls2_oauth2_token_file, params.get(AZURE_ADLS2_OAUTH2_TOKEN_FILE));
            params.putAll(workloadParams());
            params.put(AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "false");
            params.put(AZURE_ADLS2_SHARED_KEY, "");
            params.put(AZURE_ADLS2_SAS_TOKEN, "");
            params.put(AZURE_ADLS2_OAUTH2_CLIENT_SECRET, "");
            assertWorkloadIdentity(reload(volume(params)));
        } finally {
            Config.cloud_native_storage_type = oldStorageType;
            Config.azure_adls2_oauth2_token_file = oldTokenFile;
        }
    }
}
