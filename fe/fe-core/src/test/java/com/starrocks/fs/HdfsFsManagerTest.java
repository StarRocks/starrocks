// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.fs;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HdfsFsManagerTest {

    private final String testHdfsHost = "hdfs://localhost:9000";

    private HdfsFsManager fileSystemManager;

    @BeforeEach
    public void setUp() throws Exception {
        fileSystemManager = new HdfsFsManager();
    }

    @Test
    public void testGetFileSystemSuccess() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        try {
            HdfsFs fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties, null);
            assertNotNull(fs);
            fs.getDFSFileSystem().close();
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetFileSystemForS3aScheme() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3.test.com");
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, null);
            assertNotNull(fs);
            fs.getDFSFileSystem().close();
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testS3GetRegionFromEndPoint1() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com");
        THdfsProperties property = new THdfsProperties();
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, property);
            assertNotNull(fs);
            Assertions.assertEquals(property.region, "ap-southeast-1");
            fs.getDFSFileSystem().close();
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testS3GetRegionFromEndPoint2() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3-ap-southeast-1.amazonaws.com");
        THdfsProperties property = new THdfsProperties();
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, property);
            assertNotNull(fs);
            Assertions.assertEquals(property.region, "ap-southeast-1");
            fs.getDFSFileSystem().close();
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testList() throws StarRocksException, IOException {
        HdfsFsManager hdfsFsManager = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.when(fs.globStatus(new Path("not_found"))).thenThrow(new FileNotFoundException("not found"));
        Mockito.when(fs.globStatus(new Path("error"))).thenThrow(new RuntimeException("error"));
        FileStatus[] files = new FileStatus[] {
                new FileStatus(1, false, 1, 1, 1, new Path("file1"))
        };
        Mockito.when(fs.globStatus(new Path("s3a://dir/"))).thenReturn(files);

        // listFileMeta
        assertThrows(StarRocksException.class,
                () -> hdfsFsManager.listFileMeta("not_found", Maps.newHashMap()));
        assertThrows(StarRocksException.class,
                () -> hdfsFsManager.listFileMeta("error", Maps.newHashMap()));
        Assertions.assertFalse(hdfsFsManager.listFileMeta("s3a://dir/", Maps.newHashMap()).isEmpty());

        // listPath
        Assertions.assertEquals(1,
                hdfsFsManager.listPath("s3a://dir/", true, Maps.newHashMap()).size());
        Assertions.assertEquals(1,
                hdfsFsManager.listPath("s3a://dir/", false, Maps.newHashMap()).size());
    }

    @Test
    public void testAzblobPathMappingToWasbs() throws StarRocksException {
        HdfsFsManager hdfsFsManager = Mockito.spy(fileSystemManager);
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT, "https://account.blob.core.windows.net");

        Mockito.doAnswer(invocation -> {
            String path = invocation.getArgument(0);
            Map<String, String> loadProps = invocation.getArgument(1);
            Assertions.assertEquals(
                    "wasbs://container@account.blob.core.windows.net/path/file",
                    path);
            Assertions.assertEquals("https://account.blob.core.windows.net",
                    loadProps.get(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT));
            return Mockito.mock(HdfsFs.class);
        }).when(hdfsFsManager).getAzureFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        hdfsFsManager.getFileSystem("azblob://container/path/file", properties, null);
        Assertions.assertEquals("https://account.blob.core.windows.net",
                properties.get(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT));
    }

    @Test
    public void testAdls2PathMappingToAbfs() throws StarRocksException {
        HdfsFsManager hdfsFsManager = Mockito.spy(fileSystemManager);
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT, "http://account.dfs.core.windows.net");

        Mockito.doAnswer(invocation -> {
            String path = invocation.getArgument(0);
            Map<String, String> loadProps = invocation.getArgument(1);
            Assertions.assertEquals(
                    "abfs://container@account.dfs.core.windows.net/dir",
                    path);
            Assertions.assertEquals("http://account.dfs.core.windows.net",
                    loadProps.get(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT));
            return Mockito.mock(HdfsFs.class);
        }).when(hdfsFsManager).getAzureFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        hdfsFsManager.getFileSystem("adls2://container/dir", properties, null);
        Assertions.assertEquals("http://account.dfs.core.windows.net",
                properties.get(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT));
    }

    @Test
    public void testAzblobMissingEndpointThrows() {
        Map<String, String> properties = new HashMap<>();
        assertThrows(StarRocksException.class,
                () -> fileSystemManager.getFileSystem("azblob://container/dir", properties, null));
    }

    @Test
    public void testAdls2InvalidEndpointThrows() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT, "account.dfs.core.windows.net");
        assertThrows(StarRocksException.class,
                () -> fileSystemManager.getFileSystem("adls2://container/dir", properties, null));
    }

    @Test
    public void testS3FileSystemCache() throws StarRocksException, IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "accessKey");
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "secretKey");
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "s3.test.com");
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, "us-east-1");

        String path1 = "s3a://fs-cache-bucket1/path1/file1.parquet";
        String path2 = "s3a://fs-cache-bucket1/path2/file2.parquet";
        String path3 = "s3a://fs-cache-bucket2/path1/file1.parquet";
        String path4 = "s3a://fs-cache-bucket3/path1/file1.parquet";

        testFileSystemCache(properties, Pair.create(path1, path2), Pair.create(path3, path4));
    }

    @Test
    public void testAzureFileSystemCache() throws StarRocksException, IOException {
        // Test ADLS Gen2 schemes (abfs/abfss) with ADLS2 credentials
        Map<String, String> adlsProperties = new HashMap<>();
        adlsProperties.put(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY, "c2hhcmVkS2V5");  // base64 encoded

        // Test abfss:// scheme (Azure Data Lake Storage Gen2 with SSL)
        String abfssPath1 = "abfss://container1@account.dfs.core.windows.net/path1/file.parquet";
        String abfssPath2 = "abfss://container1@account.dfs.core.windows.net/path2/file.parquet";
        String abfssPath3 = "abfss://container2@account.dfs.core.windows.net/path1/file.parquet";
        String abfssPath4 = "abfss://container3@account.dfs.core.windows.net/path1/file.parquet";
        testFileSystemCache(adlsProperties, Pair.create(abfssPath1, abfssPath2), Pair.create(abfssPath3, abfssPath4));

        // Test abfs:// scheme (Azure Data Lake Storage Gen2 without SSL)
        String abfsPath1 = "abfs://container1@account.dfs.core.windows.net/path1/file.parquet";
        String abfsPath2 = "abfs://container1@account.dfs.core.windows.net/path2/file.parquet";
        String abfsPath3 = "abfs://container2@account.dfs.core.windows.net/path1/file.parquet";
        String abfsPath4 = "abfs://container3@account.dfs.core.windows.net/path1/file.parquet";
        testFileSystemCache(adlsProperties, Pair.create(abfsPath1, abfsPath2), Pair.create(abfsPath3, abfsPath4));

        // Test Azure Blob Storage schemes (wasb/wasbs) with Blob credentials
        Map<String, String> blobProperties = new HashMap<>();
        blobProperties.put(CloudConfigurationConstants.AZURE_BLOB_STORAGE_ACCOUNT, "account");
        blobProperties.put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, "c2hhcmVkS2V5");  // base64 encoded

        // Test wasb:// scheme (Azure Blob Storage without SSL)
        String wasbPath1 = "wasb://container1@account.blob.core.windows.net/path1/file.parquet";
        String wasbPath2 = "wasb://container1@account.blob.core.windows.net/path2/file.parquet";
        String wasbPath3 = "wasb://container2@account.blob.core.windows.net/path1/file.parquet";
        String wasbPath4 = "wasb://container3@account.blob.core.windows.net/path1/file.parquet";
        testFileSystemCache(blobProperties, Pair.create(wasbPath1, wasbPath2), Pair.create(wasbPath3, wasbPath4));

        // Test wasbs:// scheme (Azure Blob Storage with SSL)
        String wasbsPath1 = "wasbs://container1@account.blob.core.windows.net/path1/file.parquet";
        String wasbsPath2 = "wasbs://container1@account.blob.core.windows.net/path2/file.parquet";
        String wasbsPath3 = "wasbs://container2@account.blob.core.windows.net/path1/file.parquet";
        String wasbsPath4 = "wasbs://container3@account.blob.core.windows.net/path1/file.parquet";
        testFileSystemCache(blobProperties, Pair.create(wasbsPath1, wasbsPath2), Pair.create(wasbsPath3, wasbsPath4));
    }

    private void testFileSystemCache(Map<String, String> properties, Pair<String, String> sameFsPaths,
                                     Pair<String, String> differentFsPaths) throws StarRocksException, IOException {
        HdfsFs fs1 = fileSystemManager.getFileSystem(sameFsPaths.first, properties, null);
        HdfsFs fs2 = fileSystemManager.getFileSystem(sameFsPaths.second, properties, null);
        try {
            Assertions.assertSame(fs1, fs2);
        } finally {
            fs1.getDFSFileSystem().close();
        }

        HdfsFs fs3 = fileSystemManager.getFileSystem(differentFsPaths.first, properties, null);
        HdfsFs fs4 = fileSystemManager.getFileSystem(differentFsPaths.second, properties, null);
        try {
            Assertions.assertNotSame(fs3, fs4);
        } finally {
            fs3.getDFSFileSystem().close();
            fs4.getDFSFileSystem().close();
        }
    }
}
