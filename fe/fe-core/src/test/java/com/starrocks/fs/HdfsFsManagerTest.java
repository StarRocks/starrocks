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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.fs.hdfs.HdfsFsIdentity;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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

    @Test
    public void testFileSystemExpirationChecker() throws Exception {
        HdfsFsManager hdfsFsManager = new HdfsFsManager();
        // Shut down the auto-scheduled background checker so it cannot race with our manual run
        ScheduledExecutorService mgmtPool = Deencapsulation.getField(hdfsFsManager, "handleManagementPool");
        mgmtPool.shutdownNow();

        ReentrantLock lock = new ReentrantLock();
        CountDownLatch closeLatch = new CountDownLatch(1);

        // Mock an expired HdfsFs
        HdfsFs expiredFs = Mockito.mock(HdfsFs.class);
        Mockito.when(expiredFs.isExpired(Mockito.anyLong())).thenReturn(true);
        HdfsFsIdentity expiredIdentity = new HdfsFsIdentity("hdfs://expired", "user");
        Mockito.when(expiredFs.getIdentity()).thenReturn(expiredIdentity);
        Mockito.when(expiredFs.getLock()).thenReturn(lock);
        Mockito.doAnswer(inv -> {
            closeLatch.countDown();
            return null;
        }).when(expiredFs).closeFileSystem();

        // Mock a non-expired HdfsFs
        HdfsFs normalFs = Mockito.mock(HdfsFs.class);
        Mockito.when(normalFs.isExpired(Mockito.anyLong())).thenReturn(false);
        HdfsFsIdentity normalIdentity = new HdfsFsIdentity("hdfs://normal", "user");
        Mockito.when(normalFs.getIdentity()).thenReturn(normalIdentity);
        Mockito.when(normalFs.getLock()).thenReturn(lock);

        // Add them to cachedFileSystem
        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(hdfsFsManager, "cachedFileSystem");
        cache.put(expiredIdentity, expiredFs);
        cache.put(normalIdentity, normalFs);

        // Run the checker
        Class<?> checkerClass = Class.forName("com.starrocks.fs.hdfs.HdfsFsManager$FileSystemExpirationChecker");
        Runnable checker = (Runnable) Deencapsulation.newInstance(checkerClass, hdfsFsManager);

        checker.run();

        // closeFileSystem() and cache.remove() run sequentially in the same worker thread;
        // once the latch fires, spin-wait briefly for the remove() that immediately follows.
        Assertions.assertTrue(closeLatch.await(5, TimeUnit.SECONDS), "closeFileSystem was not called in time");
        long deadline = System.currentTimeMillis() + 5000;
        while (cache.containsKey(expiredIdentity) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        // Verify expired fs is removed and closed
        Assertions.assertFalse(cache.containsKey(expiredIdentity), "cache.remove() did not complete in time");
        Mockito.verify(expiredFs, Mockito.times(1)).closeFileSystem();

        // Verify normal fs is still there and not closed
        Assertions.assertTrue(cache.containsKey(normalIdentity));
        Mockito.verify(normalFs, Mockito.never()).closeFileSystem();
    }

    @Test
    public void testFileSystemExpirationCheckerBranches() throws Exception {
        HdfsFsManager hdfsFsManager = new HdfsFsManager();
        // Shut down the auto-scheduled background checker so it cannot race with our manual run
        ScheduledExecutorService mgmtPool = Deencapsulation.getField(hdfsFsManager, "handleManagementPool");
        mgmtPool.shutdownNow();
        Deencapsulation.setField(hdfsFsManager, "fileSystemCloseTimeoutSecs", 1L);

        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(hdfsFsManager, "cachedFileSystem");

        // Case 1: isExpired becomes false during double check
        HdfsFs fs1 = Mockito.mock(HdfsFs.class);
        Mockito.when(fs1.isExpired(Mockito.anyLong())).thenReturn(true).thenReturn(false);
        HdfsFsIdentity id1 = new HdfsFsIdentity("hdfs://fs1", "user");
        Mockito.when(fs1.getIdentity()).thenReturn(id1);
        Mockito.when(fs1.getLock()).thenReturn(new ReentrantLock());
        cache.put(id1, fs1);

        // Case 2: no longer in cache
        HdfsFs fs2 = Mockito.mock(HdfsFs.class);
        Mockito.when(fs2.isExpired(Mockito.anyLong())).thenReturn(true);
        HdfsFsIdentity id2 = new HdfsFsIdentity("hdfs://fs2", "user");
        Mockito.when(fs2.getIdentity()).thenReturn(id2);
        Mockito.when(fs2.getLock()).thenReturn(new ReentrantLock() {
            @Override
            public void lock() {
                super.lock();
                cache.remove(id2);
            }
        });
        cache.put(id2, fs2);

        // Case 3: exception thrown during close
        CountDownLatch fs3Latch = new CountDownLatch(1);
        HdfsFs fs3 = Mockito.mock(HdfsFs.class);
        Mockito.when(fs3.isExpired(Mockito.anyLong())).thenReturn(true);
        HdfsFsIdentity id3 = new HdfsFsIdentity("hdfs://fs3", "user");
        Mockito.when(fs3.getIdentity()).thenReturn(id3);
        Mockito.when(fs3.getLock()).thenReturn(new ReentrantLock());
        Mockito.doAnswer(inv -> {
            fs3Latch.countDown();
            throw new RuntimeException("Test Exception");
        }).when(fs3).closeFileSystem();
        cache.put(id3, fs3);

        // Case 4: timeout — blocks until interrupted, then signals via latch
        CountDownLatch fs4InterruptedLatch = new CountDownLatch(1);
        HdfsFs fs4 = Mockito.mock(HdfsFs.class);
        Mockito.when(fs4.isExpired(Mockito.anyLong())).thenReturn(true);
        HdfsFsIdentity id4 = new HdfsFsIdentity("hdfs://fs4", "user");
        Mockito.when(fs4.getIdentity()).thenReturn(id4);
        Mockito.when(fs4.getLock()).thenReturn(new ReentrantLock());
        Mockito.doAnswer(invocation -> {
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                fs4InterruptedLatch.countDown();
                throw new RuntimeException(e);
            }
            return null;
        }).when(fs4).closeFileSystem();
        cache.put(id4, fs4);

        // Run the checker
        Class<?> checkerClass = Class.forName("com.starrocks.fs.hdfs.HdfsFsManager$FileSystemExpirationChecker");
        Runnable checker = (Runnable) Deencapsulation.newInstance(checkerClass, hdfsFsManager);

        checker.run();

        // Wait for case 3 (exception) and case 4 (timeout interrupt) to complete
        Assertions.assertTrue(fs3Latch.await(5, TimeUnit.SECONDS), "fs3 closeFileSystem was not called in time");
        Assertions.assertTrue(fs4InterruptedLatch.await(5, TimeUnit.SECONDS), "fs4 was not interrupted in time");

        // Allow a short window for whenComplete callbacks to update the cache
        long deadline = System.currentTimeMillis() + 2000;
        while (cache.containsKey(id4) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        // Verify case 1
        Mockito.verify(fs1, Mockito.never()).closeFileSystem();
        Assertions.assertTrue(cache.containsKey(id1));

        // Verify case 2
        Mockito.verify(fs2, Mockito.never()).closeFileSystem();
        Assertions.assertFalse(cache.containsKey(id2));

        // Verify case 3
        Mockito.verify(fs3, Mockito.times(1)).closeFileSystem();
        Assertions.assertFalse(cache.containsKey(id3));

        // Verify case 4
        Mockito.verify(fs4, Mockito.times(1)).closeFileSystem();
        Assertions.assertFalse(cache.containsKey(id4));
    }

    @Test
    public void testClosePoolRejection() throws Exception {
        HdfsFsManager hdfsFsManager = new HdfsFsManager();
        ScheduledExecutorService mgmtPool = Deencapsulation.getField(hdfsFsManager, "handleManagementPool");
        mgmtPool.shutdownNow();

        // Replace fileSystemClosePool with a terminated executor to simulate pool saturation
        ExecutorService saturatedPool = Executors.newSingleThreadExecutor();
        saturatedPool.shutdown();
        Deencapsulation.setField(hdfsFsManager, "fileSystemClosePool", saturatedPool);

        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(hdfsFsManager, "cachedFileSystem");

        HdfsFs fs = Mockito.mock(HdfsFs.class);
        Mockito.when(fs.isExpired(Mockito.anyLong())).thenReturn(true);
        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://rejected", "user");
        Mockito.when(fs.getIdentity()).thenReturn(id);
        cache.put(id, fs);

        Class<?> checkerClass = Class.forName("com.starrocks.fs.hdfs.HdfsFsManager$FileSystemExpirationChecker");
        Runnable checker = (Runnable) Deencapsulation.newInstance(checkerClass, hdfsFsManager);

        checker.run();

        // fs should remain in cache — rejected tasks are retried on the next checker run
        Assertions.assertTrue(cache.containsKey(id));
        Mockito.verify(fs, Mockito.never()).closeFileSystem();
    }

}
