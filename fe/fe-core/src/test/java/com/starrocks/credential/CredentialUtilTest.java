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
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.credential.azure.AzureStoragePath;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CredentialUtilTest {
    @Test
    public void testMaskCredential() {
        Map<String, String> properties = new HashMap<>();
        String key = CloudConfigurationConstants.AWS_S3_SECRET_KEY;

        properties.put(key, "");
        CredentialUtil.maskCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "he");
        CredentialUtil.maskCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "hehe");
        CredentialUtil.maskCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "heheh");
        CredentialUtil.maskCredential(properties);
        Assert.assertEquals("he******eh", properties.get(key));

        properties.put(AzureCloudConfigurationProvider.AZURE_PATH_KEY, "path");
        CredentialUtil.maskCredential(properties);
        Assert.assertFalse(properties.containsKey(AzureCloudConfigurationProvider.AZURE_PATH_KEY));
    }

    @Test
    public void testMaskIcebergRestCatalogCredential() {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergRESTCatalog.KEY_CREDENTIAL_WITH_PREFIX, "7758258");
        CredentialUtil.maskCredential(properties);
        Assert.assertEquals("77******58", properties.get(IcebergRESTCatalog.KEY_CREDENTIAL_WITH_PREFIX));
    }

    @Test
    public void testMaskJDBCCatalogPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JDBCResource.PASSWORD, "7758258");
        CredentialUtil.maskCredential(properties);
        Assert.assertFalse(properties.containsKey(JDBCResource.PASSWORD));
    }

    @Test
    public void testAzurePathParseWithABFS() {
        String uri = "abfs://bottle@smith.dfs.core.windows.net/path/1/2";
        AzureStoragePath path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "abfss://bottle@smith.dfs.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "abfs://a@.dfs.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "abfs://a@p/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }

    @Test
    public void testAzurePathParseWithWASB() {
        String uri = "wasb://bottle@smith.blob.core.windows.net/path/1/2";
        AzureStoragePath path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "wasbs://bottle@smith.blob.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "wasb://a@.blob.core.windows.net/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "wasb://a@p/path/1/2";
        path = CredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }
}
