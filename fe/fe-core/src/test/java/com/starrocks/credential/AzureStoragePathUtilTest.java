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

import com.starrocks.credential.azure.AzureStoragePath;
import com.starrocks.credential.azure.AzureStoragePathUtil;
import org.junit.Assert;
import org.junit.Test;

public class AzureStoragePathUtilTest {

    @Test
    public void testWithABFS() {
        String uri = "abfs://bottle@smith.dfs.core.windows.net/path/1/2";
        AzureStoragePath path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "abfss://bottle@smith.dfs.core.windows.net/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "abfs://a@.dfs.core.windows.net/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "abfs://a@p/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }

    @Test
    public void testWithWASB() {
        String uri = "wasb://bottle@smith.blob.core.windows.net/path/1/2";
        AzureStoragePath path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals(path.getContainer(), "bottle");
        Assert.assertEquals(path.getStorageAccount(), "smith");

        uri = "wasbs://bottle@smith.blob.core.windows.net/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("bottle", path.getContainer());
        Assert.assertEquals("smith", path.getStorageAccount());

        uri = "wasb://a@.blob.core.windows.net/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());

        uri = "wasb://a@p/path/1/2";
        path = AzureStoragePathUtil.parseStoragePath(uri);
        Assert.assertEquals("", path.getContainer());
        Assert.assertEquals("", path.getStorageAccount());
    }
}
