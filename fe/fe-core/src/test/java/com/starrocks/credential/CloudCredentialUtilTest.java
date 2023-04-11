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

import com.starrocks.credential.azure.AzureCloudConfigurationFactory;
import com.starrocks.credential.azure.AzureStoragePath;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CloudCredentialUtilTest {
    @Test
    public void testMaskCloudCredential() {
        Map<String, String> properties = new HashMap<>();
        String key = CloudConfigurationConstants.AWS_S3_SECRET_KEY;

        properties.put(key, "");
        CloudCredentialUtil.maskCloudCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "he");
        CloudCredentialUtil.maskCloudCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "hehe");
        CloudCredentialUtil.maskCloudCredential(properties);
        Assert.assertEquals("******", properties.get(key));

        properties.put(key, "heheh");
        CloudCredentialUtil.maskCloudCredential(properties);
        Assert.assertEquals("he******eh", properties.get(key));

        properties.put(AzureCloudConfigurationFactory.AZURE_PATH_KEY, "path");
        CloudCredentialUtil.maskCloudCredential(properties);
        Assert.assertFalse(properties.containsKey(AzureCloudConfigurationFactory.AZURE_PATH_KEY));
    }

    @Test
    public void testAzurePathParseWithABFS() {
        String uri = "abfs://bottle@smith.dfs.core.windows.net/path/1/2";
        AzureStoragePath path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "abfss://bottle@smith.dfs.core.windows.net/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "abfs://a@.dfs.core.windows.net/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "abfs://a@p/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }

    @Test
    public void testAzurePathParseWithWASB() {
        String uri = "wasb://bottle@smith.blob.core.windows.net/path/1/2";
        AzureStoragePath path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "wasbs://bottle@smith.blob.core.windows.net/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "wasb://a@.blob.core.windows.net/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "wasb://a@p/path/1/2";
        path = CloudCredentialUtil.parseAzureStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }
}
