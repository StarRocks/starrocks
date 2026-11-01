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

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureNativeAdlsConfigurationTest {
    @Test
    public void testNativeAdlsIsOptInAndPreservesAccountScopedCredentials() {
        Map<String, String> properties = new HashMap<>(Map.of(
                "azure.adls2.storage_account", "account", "azure.adls2.shared_key", "key"));
        AzureCloudConfigurationProvider provider = new AzureCloudConfigurationProvider();
        TCloudConfiguration thrift = new TCloudConfiguration();
        provider.build(properties).toThrift(thrift);
        assertFalse(thrift.isSetAzure_use_native_sdk());
        properties.put(AzureCloudConfigurationProvider.AZURE_ADLS2_USE_NATIVE_SDK, "true");
        CloudConfiguration cloud = provider.build(properties);
        thrift = new TCloudConfiguration();
        cloud.toThrift(thrift);
        assertTrue(thrift.isAzure_use_native_sdk());
        assertEquals("key", thrift.getCloud_properties().get("fs.azure.account.key.account.dfs.core.windows.net"));
        Configuration hadoop = new Configuration(false);
        cloud.applyToConfiguration(hadoop);
        assertEquals("key", hadoop.get("fs.azure.account.key.account.dfs.core.windows.net"));
    }

    @Test
    public void testWorkloadIdentitySettingsReachBackend() {
        CloudConfiguration cloud = new AzureCloudConfigurationProvider().build(Map.of(
                "azure.adls2.oauth2_token_file", "/var/run/token",
                "azure.adls2.oauth2_client_id", "client",
                "azure.adls2.oauth2_tenant_id", "tenant",
                AzureCloudConfigurationProvider.AZURE_ADLS2_USE_NATIVE_SDK, "true"));
        TCloudConfiguration thrift = new TCloudConfiguration();
        cloud.toThrift(thrift);
        assertTrue(thrift.isAzure_use_native_sdk());
        assertEquals("/var/run/token", thrift.getCloud_properties().get("fs.azure.account.oauth2.token.file"));
        assertEquals("tenant", thrift.getCloud_properties().get("fs.azure.account.oauth2.msi.tenant"));
    }
}
