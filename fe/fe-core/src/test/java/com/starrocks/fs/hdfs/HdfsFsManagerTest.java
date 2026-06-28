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

package com.starrocks.fs.hdfs;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

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

    /**
     * Regression test for issue #66504: GCS Broker Load incorrectly reuses the
     * bucket from a previous job when bucket names contain underscores.
     *
     * Root cause: java.net.URI.getHost() returns null for authorities containing
     * underscores (DNS hostname rules forbid them). The pre-fix cache key was
     * scheme + "://" + host, which collapsed every such bucket to the literal
     * string "gs://null" and caused Job 2 to receive the cached FileSystem from
     * Job 1. Fixed by PR #68901 which switched the key to use getAuthority().
     */
    @Test
    public void testGcsFileSystemCache() throws StarRocksException, IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "true");

        // Sanity-check the regression trigger: getHost() is null for both,
        // so any cache keyed on host would collide on "gs://null".
        Assertions.assertNull(java.net.URI.create("gs://bucket_us/abc").getHost());
        Assertions.assertNull(java.net.URI.create("gs://bucket_us_west1/xyz").getHost());

        // Same underscore-bearing bucket, different key paths -> must share the cached FS.
        HdfsFs fs1 = fileSystemManager.getFileSystem(
                "gs://bucket_us/abc/file1.parquet", properties, null);
        HdfsFs fs2 = fileSystemManager.getFileSystem(
                "gs://bucket_us/xyz/file2.parquet", properties, null);
        Assertions.assertSame(fs1, fs2);

        // Two distinct underscore-bearing buckets -> must NOT share the cached FS.
        HdfsFs fs3 = fileSystemManager.getFileSystem(
                "gs://bucket_us/abc/file1.parquet", properties, null);
        HdfsFs fs4 = fileSystemManager.getFileSystem(
                "gs://bucket_us_west1/xyz/file2.parquet", properties, null);
        try {
            Assertions.assertNotSame(fs3, fs4);
        } finally {
            fs4.getDFSFileSystem().close();
        }
        try {
            // Pre-fix behavior would hand back the bucket_us FS for bucket_us_west1.
            Assertions.assertSame(fs1, fs3);
        } finally {
            fs3.getDFSFileSystem().close();
        }
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

    // ---- helpers for expiration-checker tests ----

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<HdfsFsIdentity, HdfsFs> getCachedFileSystem(HdfsFsManager mgr) throws Exception {
        Field f = HdfsFsManager.class.getDeclaredField("cachedFileSystem");
        f.setAccessible(true);
        return (ConcurrentHashMap<HdfsFsIdentity, HdfsFs>) f.get(mgr);
    }

    private static Runnable newChecker(HdfsFsManager mgr) throws Exception {
        Class<?> checkerClass = Class.forName("com.starrocks.fs.hdfs.HdfsFsManager$FileSystemExpirationChecker");
        Constructor<?> ctor = checkerClass.getDeclaredConstructor(HdfsFsManager.class);
        ctor.setAccessible(true);
        return (Runnable) ctor.newInstance(mgr);
    }

    private static HdfsFs mockExpiredFs(HdfsFsIdentity identity) {
        HdfsFs fs = Mockito.mock(HdfsFs.class);
        Mockito.when(fs.isExpired(Mockito.anyLong())).thenReturn(true);
        Mockito.when(fs.getIdentity()).thenReturn(identity);
        return fs;
    }

    private static HdfsFs mockNotExpiredFs(HdfsFsIdentity identity) {
        HdfsFs fs = Mockito.mock(HdfsFs.class);
        Mockito.when(fs.isExpired(Mockito.anyLong())).thenReturn(false);
        Mockito.when(fs.getIdentity()).thenReturn(identity);
        return fs;
    }

    /**
     * Expired filesystem: removed from cache, closeFileSystem() called asynchronously.
     */
    @Test
    public void testExpirationCheckerClosesExpiredFs() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host1", "user");
        HdfsFs fs = mockExpiredFs(id);
        cache.put(id, fs);

        newChecker(mgr).run();

        // Cache entry must be gone immediately (before async close finishes)
        Assertions.assertFalse(cache.containsKey(id));

        // closeFileSystem() is called asynchronously; use timeout() instead of Thread.sleep()
        Mockito.verify(fs, Mockito.timeout(2000).times(1)).closeFileSystem();
    }

    /**
     * Non-expired filesystem: stays in cache, never closed.
     */
    @Test
    public void testExpirationCheckerKeepsNonExpiredFs() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host2", "user");
        HdfsFs fs = mockNotExpiredFs(id);
        cache.put(id, fs);

        newChecker(mgr).run();

        Assertions.assertTrue(cache.containsKey(id));
        Mockito.verify(fs, Mockito.never()).closeFileSystem();
    }

    /**
     * Exception during close: cache entry is still removed so new requests are not blocked.
     */
    @Test
    public void testExpirationCheckerRemovesFromCacheEvenIfCloseFails() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host3", "user");
        HdfsFs fs = mockExpiredFs(id);
        Mockito.doThrow(new RuntimeException("close failed")).when(fs).closeFileSystem();
        cache.put(id, fs);

        newChecker(mgr).run();

        // Removed from cache even though close threw
        Assertions.assertFalse(cache.containsKey(id));
        Mockito.verify(fs, Mockito.timeout(2000).times(1)).closeFileSystem();
    }

    /**
     * Hung close: watchdog cancels via interrupt after the configured timeout.
     * Uses a short timeout (1 s) and a close that blocks until interrupted.
     */
    @Test
    public void testExpirationCheckerCancelsHungClose() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        Deencapsulation.setField(mgr, "fileSystemCloseTimeoutSecs", 1L);
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host4", "user");
        HdfsFs fs = mockExpiredFs(id);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeInterrupted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            closeStarted.countDown();
            // Block until interrupted
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                closeInterrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(fs).closeFileSystem();
        cache.put(id, fs);

        newChecker(mgr).run();

        // Cache entry removed immediately
        Assertions.assertFalse(cache.containsKey(id));

        // Close started in the async pool
        Assertions.assertTrue(closeStarted.await(2, TimeUnit.SECONDS));

        // Watchdog must interrupt the hung close within timeout + grace period
        Assertions.assertTrue(closeInterrupted.await(3, TimeUnit.SECONDS));
    }

    /**
     * Atomic remove: if two checker runs overlap, only one close is submitted per instance.
     */
    @Test
    public void testExpirationCheckerAtomicRemovePreventsDoubleClose() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host5", "user");
        HdfsFs fs = mockExpiredFs(id);
        cache.put(id, fs);

        // Run the checker twice concurrently
        Runnable checker = newChecker(mgr);
        Thread t1 = new Thread(checker::run);
        Thread t2 = new Thread(checker::run);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // Exactly one thread wins the atomic remove, so close is called exactly once
        Mockito.verify(fs, Mockito.timeout(2000).times(1)).closeFileSystem();
    }

    /**
     * Concurrent eviction: acquireCachedFileSystem retries when the checker removes
     * an entry between putIfAbsent and the containsKey check, so callers never see null.
     */
    @Test
    public void testAcquireCachedFileSystemRetriesOnConcurrentEviction() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://retry-host", "user");

        // Put an entry, then immediately remove it so the first get() returns null,
        // simulating the checker evicting between putIfAbsent and get.
        // acquireCachedFileSystem should retry and succeed on the second attempt.

        // Access acquireCachedFileSystem via reflection
        java.lang.reflect.Method acquire = HdfsFsManager.class.getDeclaredMethod(
                "acquireCachedFileSystem", HdfsFsIdentity.class);
        acquire.setAccessible(true);

        // Normal case: entry stays in cache, acquire succeeds and returns with lock held
        HdfsFs result = (HdfsFs) acquire.invoke(mgr, id);
        assertNotNull(result, "acquireCachedFileSystem should return non-null");
        Assertions.assertTrue(result.getLock().isHeldByCurrentThread());
        result.getLock().unlock();

        // Eviction case: remove the entry after acquire puts it, forcing a retry.
        // We do this by clearing the cache right before calling acquire — the first
        // putIfAbsent creates an entry, but we have a thread that removes it immediately.
        cache.clear();
        CountDownLatch evictOnce = new CountDownLatch(1);
        Thread evictor = new Thread(() -> {
            // Busy-wait for an entry to appear, then remove it once
            while (evictOnce.getCount() > 0) {
                for (HdfsFsIdentity key : cache.keySet()) {
                    if (cache.remove(key) != null) {
                        evictOnce.countDown();
                        return;
                    }
                }
            }
        });
        evictor.start();

        // acquire should retry after the eviction and succeed
        HdfsFs retryResult = (HdfsFs) acquire.invoke(mgr, id);
        assertNotNull(retryResult, "acquireCachedFileSystem should retry and succeed after eviction");
        Assertions.assertTrue(retryResult.getLock().isHeldByCurrentThread());
        retryResult.getLock().unlock();
        evictor.join(2000);
    }

    /**
     * Pool-full (RejectedExecutionException): filesystem is still closed on a fallback
     * daemon thread so resources are not leaked and the checker thread is not blocked.
     */
    @Test
    public void testExpirationCheckerClosesOnFallbackThreadWhenPoolFull() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        // Replace the close pool with one that always rejects
        ExecutorService alwaysReject = Mockito.mock(ExecutorService.class);
        Mockito.when(alwaysReject.submit(Mockito.any(Runnable.class)))
                .thenThrow(new RejectedExecutionException("pool full"));
        Deencapsulation.setField(mgr, "fileSystemClosePool", alwaysReject);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host6", "user");
        HdfsFs fs = mockExpiredFs(id);
        cache.put(id, fs);

        newChecker(mgr).run();

        // Cache entry removed despite rejection
        Assertions.assertFalse(cache.containsKey(id));
        // Closed on a fallback daemon thread
        Mockito.verify(fs, Mockito.timeout(2000).times(1)).closeFileSystem();
    }

    /**
     * Regression for cluster snapshot upload error reporting: copyToLocal wraps the
     * underlying Hadoop exception in a StarRocksException, and the wrapper message
     * must include the cause's message so callers that surface only
     * StarRocksException#getMessage (e.g. ClusterSnapshotJob.error_message) still
     * see the real reason (AccessDenied, NoSuchBucket, timeout, etc.).
     */
    @Test
    public void testCopyToLocalIncludesCauseMessage() throws Exception {
        HdfsFsManager mgr = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.doReturn(hdfs).when(mgr)
                .getFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        String causeMsg = "AccessDenied: anonymous is not authorized to perform s3:GetObject";
        Mockito.doThrow(new IOException(causeMsg))
                .when(fs).copyToLocalFile(Mockito.anyBoolean(), Mockito.any(Path.class),
                        Mockito.any(Path.class), Mockito.anyBoolean());

        StarRocksException ex = Assertions.assertThrows(StarRocksException.class,
                () -> mgr.copyToLocal("s3a://bucket/key", "/tmp/dest", new HashMap<>()));
        Assertions.assertTrue(ex.getMessage().contains(causeMsg),
                "wrapper message must include cause: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("Failed to copy s3a://bucket/key to local /tmp/dest"),
                "wrapper message must include both paths: " + ex.getMessage());
        Assertions.assertNotNull(ex.getCause(), "cause must still be attached");
    }

    /**
     * Companion to {@link #testCopyToLocalIncludesCauseMessage()} for copyFromLocal.
     * Uses a non-existent local source so FileUtil.copy throws naturally, then checks
     * that the wrapper message preserves the underlying message.
     */
    @Test
    public void testCopyFromLocalIncludesCauseMessage() throws Exception {
        HdfsFsManager mgr = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.doReturn(hdfs).when(mgr)
                .getFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        String nonExistentSrc = "/this/path/should/not/exist/" + System.nanoTime();
        StarRocksException ex = Assertions.assertThrows(StarRocksException.class,
                () -> mgr.copyFromLocal(nonExistentSrc, "s3a://bucket/key", new HashMap<>()));
        Assertions.assertNotNull(ex.getCause(), "cause must still be attached");
        Assertions.assertNotNull(ex.getCause().getMessage(), "cause must carry a message");
        Assertions.assertTrue(ex.getMessage().contains(ex.getCause().getMessage()),
                "wrapper message must include cause's message: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("Failed to copy local " + nonExistentSrc
                        + " to s3a://bucket/key"),
                "wrapper message must include both paths: " + ex.getMessage());
    }

    /**
     * Covers the {@link InterruptedIOException} branch of copyToLocal: the cause's
     * message must still be visible in the wrapper, and the catch clears the
     * thread's interrupt flag so callers don't observe spurious interruption.
     */
    @Test
    public void testCopyToLocalInterruptedIncludesCauseMessage() throws Exception {
        HdfsFsManager mgr = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.doReturn(hdfs).when(mgr)
                .getFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        String causeMsg = "interrupted during S3 GET";
        Mockito.doThrow(new InterruptedIOException(causeMsg))
                .when(fs).copyToLocalFile(Mockito.anyBoolean(), Mockito.any(Path.class),
                        Mockito.any(Path.class), Mockito.anyBoolean());

        try {
            StarRocksException ex = Assertions.assertThrows(StarRocksException.class,
                    () -> mgr.copyToLocal("s3a://bucket/key", "/tmp/dest", new HashMap<>()));
            Assertions.assertTrue(ex.getMessage().contains(causeMsg),
                    "wrapper message must include cause: " + ex.getMessage());
            Assertions.assertTrue(ex.getMessage().contains(
                            "Failed to copy s3a://bucket/key to local /tmp/dest"),
                    "wrapper message must include both paths: " + ex.getMessage());
            Assertions.assertInstanceOf(InterruptedIOException.class, ex.getCause(),
                    "cause must be the InterruptedIOException");
        } finally {
            // Defensive: the catch calls Thread.interrupted() to clear the flag.
            Thread.interrupted();
        }
    }

    /**
     * Covers the {@link InterruptedIOException} branch of copyFromLocal. Uses a
     * real local source file so FileUtil.copy reaches the destination
     * FileSystem.create(...) call, where we inject the interrupt.
     */
    @Test
    public void testCopyFromLocalInterruptedIncludesCauseMessage() throws Exception {
        HdfsFsManager mgr = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.doReturn(hdfs).when(mgr)
                .getFileSystem(Mockito.anyString(), Mockito.anyMap(), Mockito.isNull());

        File srcFile = Files.createTempFile("hdfs-fs-mgr-test-", ".bin").toFile();
        try {
            Files.write(srcFile.toPath(), new byte[] {1, 2, 3});

            String causeMsg = "interrupted during S3 PUT";
            // FileUtil.copy ends up calling FileSystem.create(Path) — stub that overload directly
            // (the (Path, boolean) overload isn't reached because Mockito short-circuits at the mocked
            // create(Path) method without delegating to its real implementation).
            Mockito.doThrow(new InterruptedIOException(causeMsg))
                    .when(fs).create(Mockito.any(Path.class));

            StarRocksException ex = Assertions.assertThrows(StarRocksException.class,
                    () -> mgr.copyFromLocal(srcFile.getAbsolutePath(), "s3a://bucket/key", new HashMap<>()));
            Assertions.assertTrue(ex.getMessage().contains(causeMsg),
                    "wrapper message must include cause: " + ex.getMessage());
            Assertions.assertTrue(ex.getMessage().contains(
                            "Failed to copy local " + srcFile.getAbsolutePath() + " to s3a://bucket/key"),
                    "wrapper message must include both paths: " + ex.getMessage());
            Assertions.assertInstanceOf(InterruptedIOException.class, ex.getCause(),
                    "cause must be the InterruptedIOException");
        } finally {
            srcFile.delete();
            Thread.interrupted();
        }
    }

    /**
     * Fallback thread watchdog: when the close pool is full and the fallback daemon thread
     * hangs, the watchdog interrupts it after the configured timeout so stuck threads
     * don't accumulate.
     */
    @Test
    public void testFallbackThreadInterruptedOnTimeout() throws Exception {
        HdfsFsManager mgr = new HdfsFsManager();
        Deencapsulation.setField(mgr, "fileSystemCloseTimeoutSecs", 1L);
        ConcurrentHashMap<HdfsFsIdentity, HdfsFs> cache = getCachedFileSystem(mgr);

        // Replace the close pool with one that always rejects
        ExecutorService alwaysReject = Mockito.mock(ExecutorService.class);
        Mockito.when(alwaysReject.submit(Mockito.any(Runnable.class)))
                .thenThrow(new RejectedExecutionException("pool full"));
        Deencapsulation.setField(mgr, "fileSystemClosePool", alwaysReject);

        HdfsFsIdentity id = new HdfsFsIdentity("hdfs://host-fallback-timeout", "user");
        HdfsFs fs = mockExpiredFs(id);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeInterrupted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            closeStarted.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                closeInterrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(fs).closeFileSystem();
        cache.put(id, fs);

        newChecker(mgr).run();

        // Cache entry removed immediately
        Assertions.assertFalse(cache.containsKey(id));

        // Close started on fallback daemon thread
        Assertions.assertTrue(closeStarted.await(2, TimeUnit.SECONDS));

        // Watchdog must interrupt the hung fallback thread within timeout + grace period
        Assertions.assertTrue(closeInterrupted.await(3, TimeUnit.SECONDS));
    }
}
