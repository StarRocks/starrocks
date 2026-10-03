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

package com.starrocks.connector.delta;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.WorkloadIdentityCredential;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.starrocks.connector.hive.IHiveMetastore;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AzureDeltaCredentialsTest {
    private static final String HOST = "account.dfs.core.windows.net";
    private static final String PATH = "abfss://container@" + HOST + "/table";
    private static final String PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.";

    @Test
    public void testAccountScopedSharedKeyOverridesGlobalCredential() {
        Configuration conf = new Configuration(false);
        conf.set("fs.azure.account.key", "Z2xvYmFs");
        conf.set("fs.azure.account.key." + HOST, "c2NvcGVk");
        BlobContainerClientBuilder builder = mock(BlobContainerClientBuilder.class);
        AzureDeltaCredentials.configure(builder, URI.create(PATH), conf);
        ArgumentCaptor<StorageSharedKeyCredential> credential = ArgumentCaptor.forClass(StorageSharedKeyCredential.class);
        verify(builder).credential(credential.capture());
        assertEquals("account", credential.getValue().getAccountName());
        assertEquals(new StorageSharedKeyCredential("account", "c2NvcGVk").computeHmac256("test"),
                credential.getValue().computeHmac256("test"));
    }

    @Test
    public void testFixedSasDoesNotRequireAccountKey() {
        Configuration conf = new Configuration(false);
        conf.set("fs.azure.account.auth.type." + HOST, "SAS");
        conf.set("fs.azure.sas.fixed.token." + HOST, "?sig=scoped");
        BlobContainerClientBuilder builder = mock(BlobContainerClientBuilder.class);
        AzureDeltaCredentials.configure(builder, URI.create(PATH), conf);
        verify(builder).sasToken("sig=scoped");
    }

    @Test
    public void testClientSecretManagedIdentityAndWorkloadIdentity() {
        Configuration conf = new Configuration(false);
        conf.set("fs.azure.account.auth.type", "OAuth");
        conf.set("fs.azure.account.oauth2.client.id", "client-id");
        conf.set("fs.azure.account.oauth2.client.secret", "secret");
        conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/tenant/oauth2/token");
        conf.set("fs.azure.account.oauth2.msi.tenant", "tenant");
        conf.set("fs.azure.account.oauth2.token.file", "/var/run/secrets/test-token");
        String[] providers = {"ClientCredsTokenProvider", "MsiTokenProvider", "WorkloadIdentityTokenProvider"};
        Class<?>[] types = {ClientSecretCredential.class, ManagedIdentityCredential.class, WorkloadIdentityCredential.class};
        for (int i = 0; i < providers.length; i++) {
            conf.set("fs.azure.account.oauth.provider.type", PROVIDER + providers[i]);
            BlobContainerClientBuilder builder = mock(BlobContainerClientBuilder.class);
            AzureDeltaCredentials.configure(builder, URI.create(PATH), conf);
            ArgumentCaptor<TokenCredential> credential = ArgumentCaptor.forClass(TokenCredential.class);
            verify(builder).credential(credential.capture());
            assertInstanceOf(types[i], credential.getValue());
        }
    }

    @Test
    public void testUnsupportedCredentialsFailWithoutAmbientFallback() {
        Configuration conf = new Configuration(false);
        BlobContainerClientBuilder builder = mock(BlobContainerClientBuilder.class);
        assertThrows(IllegalArgumentException.class, () -> AzureDeltaCredentials.configure(builder, URI.create(PATH), conf));
        conf.set("fs.azure.account.auth.type", "Custom");
        assertThrows(IllegalArgumentException.class, () -> AzureDeltaCredentials.configure(builder, URI.create(PATH), conf));
        conf.set("fs.azure.account.auth.type", "OAuth");
        conf.set("fs.azure.account.oauth2.client.id", "client");
        conf.set("fs.azure.account.oauth.provider.type", "custom.Provider");
        assertThrows(IllegalArgumentException.class, () -> AzureDeltaCredentials.configure(builder, URI.create(PATH), conf));
        conf.set("fs.azure.account.auth.type", "SAS");
        conf.set("fs.azure.sas.token.provider.type", "custom.Provider");
        assertThrows(IllegalArgumentException.class, () -> AzureDeltaCredentials.configure(builder, URI.create(PATH), conf));
    }

    @Test
    public void testOptInSelectionAndConfigurationIsolation() {
        Configuration conf = new Configuration(false);
        conf.set("fs.azure.account.key." + HOST, "c2NvcGVk");
        DeltaLakeMetastore disabled = new HMSBackedDeltaMetastore("test", mock(IHiveMetastore.class), conf,
                new DeltaLakeCatalogProperties(Map.of()));
        assertInstanceOf(HadoopFileIO.class, disabled.createFileIO(PATH, conf));
        DeltaLakeMetastore enabled = new HMSBackedDeltaMetastore("test", mock(IHiveMetastore.class), conf,
                new DeltaLakeCatalogProperties(Map.of(DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_NATIVE_ADLS, "true")));
        assertInstanceOf(HadoopFileIO.class, enabled.createFileIO("s3://bucket/table", conf));
        AzureDeltaFileIO nativeIO = assertInstanceOf(AzureDeltaFileIO.class, enabled.createFileIO(PATH, conf));
        conf.set("fs.azure.account.key." + HOST, "changed");
        assertEquals("c2NvcGVk", nativeIO.getConf("fs.azure.account.key." + HOST).orElseThrow());
    }
}
