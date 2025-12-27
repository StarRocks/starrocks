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
import com.starrocks.connector.iceberg.IcebergCatalogProperties;
import com.starrocks.connector.iceberg.rest.OAuth2SecurityConfig;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.credential.azure.AzureStoragePath;
import org.apache.iceberg.aws.AwsProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CredentialUtilTest {
    @Test
    public void testMaskCredential() {
        Map<String, String> properties = new HashMap<>();
        String key = CloudConfigurationConstants.AWS_S3_SECRET_KEY;

        properties.put(key, "");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("******", properties.get(key));

        properties.put(key, "he");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("******", properties.get(key));

        properties.put(key, "hehe");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("******", properties.get(key));

        properties.put(key, "heheh");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("he******eh", properties.get(key));

        properties.put(AzureCloudConfigurationProvider.AZURE_PATH_KEY, "path");
        CredentialUtil.maskCredential(properties);
        Assertions.assertFalse(properties.containsKey(AzureCloudConfigurationProvider.AZURE_PATH_KEY));
    }

    @Test
    public void testMaskIcebergRestCatalogCredential() {
        Map<String, String> properties = new HashMap<>();
        String key = IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                OAuth2SecurityConfig.OAUTH2_CREDENTIAL;
        properties.put(key, "7758258");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("77******58", properties.get(key));

        key = IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                OAuth2SecurityConfig.OAUTH2_TOKEN;
        properties.put(key, "1223344565");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("12******65", properties.get(key));

        key = IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                AwsProperties.REST_ACCESS_KEY_ID;
        properties.put(key, "7758258");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("77******58", properties.get(key));

        key = IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX +
                AwsProperties.REST_SECRET_ACCESS_KEY;
        properties.put(key, "7758258");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("77******58", properties.get(key));
    }

    @Test
    public void testMaskJDBCCatalogPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JDBCResource.PASSWORD, "7758258");
        CredentialUtil.maskCredential(properties);
        Assertions.assertFalse(properties.containsKey(JDBCResource.PASSWORD));
    }

    @Test
    public void testMaskHuaweiOBSCredential() {
        Map<String, String> properties = new HashMap<>();
        
        // Test underscore format
        properties.put(CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        properties.put(CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("AK******LE", properties.get(CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY));
        Assertions.assertEquals("wJ******EY", properties.get(CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY));
        
        // Test dot format
        properties.clear();
        properties.put(CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY_DOT, "AKIAIOSFODNN7EXAMPLE");
        properties.put(CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY_DOT, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        CredentialUtil.maskCredential(properties);
        Assertions.assertEquals("AK******LE", properties.get(CloudConfigurationConstants.HUAWEI_OBS_ACCESS_KEY_DOT));
        Assertions.assertEquals("wJ******EY", properties.get(CloudConfigurationConstants.HUAWEI_OBS_SECRET_KEY_DOT));
    }

    @Test
    public void testAzurePathParseWithABFS() {
        String uri = "abfs://bottle@smith.dfs.core.windows.net/path/1/2";
        AzureStoragePath path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals(path.getContainer(), "bottle");
        Assertions.assertEquals(path.getStorageAccount(), "smith");

        uri = "abfss://bottle@smith.dfs.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("bottle", path.getContainer());
        Assertions.assertEquals("smith", path.getStorageAccount());

        uri = "abfs://a@.dfs.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("", path.getContainer());
        Assertions.assertEquals("", path.getStorageAccount());

        uri = "abfs://a@p/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("", path.getContainer());
        Assertions.assertEquals("", path.getStorageAccount());

        uri = "";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("", path.getContainer());
        Assertions.assertEquals("", path.getStorageAccount());
    }

    @Test
    public void testAzurePathParseWithWASB() {
        String uri = "wasb://bottle@smith.blob.core.windows.net/path/1/2";
        AzureStoragePath path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals(path.getContainer(), "bottle");
        Assertions.assertEquals(path.getStorageAccount(), "smith");

        uri = "wasbs://bottle@smith.blob.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("bottle", path.getContainer());
        Assertions.assertEquals("smith", path.getStorageAccount());

        uri = "wasb://a@.blob.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("", path.getContainer());
        Assertions.assertEquals("", path.getStorageAccount());

        uri = "wasb://a@p/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assertions.assertEquals("", path.getContainer());
        Assertions.assertEquals("", path.getStorageAccount());
    }
}
