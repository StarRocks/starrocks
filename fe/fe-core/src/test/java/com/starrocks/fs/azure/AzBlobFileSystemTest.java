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

package com.starrocks.fs.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AzBlobFileSystemTest {

    private Map<String, String> properties = Maps.newHashMap();
    private AzBlobFileSystem fs;
    private String path;

    @Before
    public void setUp() {
        properties.clear();
        fs = null;
        path = null;
    }

    // wasbs://testcontainer@testaccount.blob.core.windows.net/data/a0
    @Test
    public void testGlobListOneBlob() throws StarRocksException {
        // Mock
        properties.put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, "shared_key_xxx");
        fs = Mockito.spy(new AzBlobFileSystem(properties));

        path = "wasbs://testcontainer@testaccount.blob.core.windows.net/data/a0";
        AzBlobURI uri = AzBlobURI.parse(path);

        BlobContainerClient mockClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(fs.createBlobContainerClient(uri)).thenReturn(mockClient);

        BlobItem mockBlob = new BlobItem().setName("data/a0").setIsPrefix(false);
        BlobItemProperties props = new BlobItemProperties().setContentLength(123L).setLastModified(OffsetDateTime.now());
        mockBlob.setProperties(props);

        List<BlobItem> blobItems = Lists.newArrayList(mockBlob);
        PagedResponse<BlobItem> page = Mockito.mock(PagedResponse.class);
        Mockito.when(page.getValue()).thenReturn(blobItems);

        PagedIterable<BlobItem> pagedIterable = Mockito.mock(PagedIterable.class);
        Mockito.when(pagedIterable.iterableByPage()).thenReturn(Collections.singletonList(page));

        Mockito.when(mockClient.listBlobsByHierarchy(Mockito.eq("/"), Mockito.any(ListBlobsOptions.class), Mockito.any()))
                .thenReturn(pagedIterable);
        Mockito.doReturn(mockClient).when(fs).createBlobContainerClient(Mockito.any());

        // Act
        List<FileStatus> result = fs.globList(path, false);

        // Assert
        Assert.assertEquals(1, result.size());
        FileStatus status = result.get(0);
        Assert.assertEquals(path, status.getPath().toString());
        Assert.assertFalse(status.isDirectory());
        Assert.assertEquals(123L, status.getLen());
    }

    // wasbs://testcontainer@testaccount.blob.core.windows.net/data/*
    @Test
    public void testGlobListWithWildcard() throws StarRocksException {
        // Mock
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET, "client_secret_xxx");
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID, "11111111-2222-3333-4444-555555555555");
        fs = Mockito.spy(new AzBlobFileSystem(properties));

        String path = "wasbs://testcontainer@testaccount.blob.core.windows.net/data";
        AzBlobURI uri = AzBlobURI.parse(path);

        BlobContainerClient mockClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(fs.createBlobContainerClient(uri)).thenReturn(mockClient);

        BlobItem mockBlob0 = new BlobItem().setName("data/a0").setIsPrefix(false);
        BlobItemProperties props0 = new BlobItemProperties().setContentLength(123L).setLastModified(OffsetDateTime.now());
        mockBlob0.setProperties(props0);
        BlobItem mockBlob1 = new BlobItem().setName("data/b0").setIsPrefix(false);
        BlobItemProperties props1 = new BlobItemProperties().setContentLength(456L).setLastModified(OffsetDateTime.now());
        mockBlob1.setProperties(props1);

        List<BlobItem> blobItems = Lists.newArrayList(mockBlob0, mockBlob1);
        PagedResponse<BlobItem> page = Mockito.mock(PagedResponse.class);
        Mockito.when(page.getValue()).thenReturn(blobItems);

        PagedIterable<BlobItem> pagedIterable = Mockito.mock(PagedIterable.class);
        Mockito.when(pagedIterable.iterableByPage()).thenReturn(Collections.singletonList(page));

        Mockito.when(mockClient.listBlobsByHierarchy(Mockito.eq("/"), Mockito.any(ListBlobsOptions.class), Mockito.any()))
                .thenReturn(pagedIterable);
        Mockito.doReturn(mockClient).when(fs).createBlobContainerClient(Mockito.any());

        // Act
        List<FileStatus> result = fs.globList(path + "/*", false);

        // Assert
        Assert.assertEquals(2, result.size());
        {
            FileStatus status = result.get(0);
            Assert.assertEquals(path + "/a0", status.getPath().toString());
            Assert.assertFalse(status.isDirectory());
            Assert.assertEquals(123L, status.getLen());
        }
        {
            FileStatus status = result.get(1);
            Assert.assertEquals(path + "/b0", status.getPath().toString());
            Assert.assertFalse(status.isDirectory());
            Assert.assertEquals(456L, status.getLen());
        }
    }

    // wasbs://testcontainer@testaccount.blob.core.windows.net/data/b*
    @Test
    public void testGlobListWithFilter() throws StarRocksException {
        // Mock
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_USE_MANAGED_IDENTITY, "true");
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");
        fs = Mockito.spy(new AzBlobFileSystem(properties));

        String path = "wasbs://testcontainer@testaccount.blob.core.windows.net/data";
        AzBlobURI uri = AzBlobURI.parse(path);

        BlobContainerClient mockClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(fs.createBlobContainerClient(uri)).thenReturn(mockClient);

        BlobItem mockBlob0 = new BlobItem().setName("data/a0").setIsPrefix(false);
        BlobItemProperties props0 = new BlobItemProperties().setContentLength(123L).setLastModified(OffsetDateTime.now());
        mockBlob0.setProperties(props0);
        BlobItem mockBlob1 = new BlobItem().setName("data/b0").setIsPrefix(false);
        BlobItemProperties props1 = new BlobItemProperties().setContentLength(456L).setLastModified(OffsetDateTime.now());
        mockBlob1.setProperties(props1);

        List<BlobItem> blobItems = Lists.newArrayList(mockBlob0, mockBlob1);
        PagedResponse<BlobItem> page = Mockito.mock(PagedResponse.class);
        Mockito.when(page.getValue()).thenReturn(blobItems);

        PagedIterable<BlobItem> pagedIterable = Mockito.mock(PagedIterable.class);
        Mockito.when(pagedIterable.iterableByPage()).thenReturn(Collections.singletonList(page));

        Mockito.when(mockClient.listBlobsByHierarchy(Mockito.eq("/"), Mockito.any(ListBlobsOptions.class), Mockito.any()))
                .thenReturn(pagedIterable);
        Mockito.doReturn(mockClient).when(fs).createBlobContainerClient(Mockito.any());

        // Act
        List<FileStatus> result = fs.globList(path + "/b*", false);

        // Assert
        Assert.assertEquals(1, result.size());
        FileStatus status = result.get(0);
        Assert.assertEquals(path + "/b0", status.getPath().toString());
        Assert.assertFalse(status.isDirectory());
        Assert.assertEquals(456L, status.getLen());
    }

    @Test
    public void testGetCloudConfiguration() throws StarRocksException {
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_USE_MANAGED_IDENTITY, "true");
        properties.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");
        fs = new AzBlobFileSystem(properties);
        String path = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";

        // Check TCloudConfiguration
        THdfsProperties hdfsProperties = fs.getHdfsProperties(path);
        TCloudConfiguration cc = hdfsProperties.getCloud_configuration();
        Assert.assertEquals(TCloudType.AZURE, cc.getCloud_type());
        Assert.assertTrue(cc.isAzure_use_native_sdk());
        Map<String, String> cloudProperties = cc.getCloud_properties();
        Assert.assertEquals("client_id_xxx", cloudProperties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID));
    }
}
