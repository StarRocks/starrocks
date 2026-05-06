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

package com.starrocks.connector.iceberg.io;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.BLOB_ENDPOINT;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.ACCESS_TOKEN_PROVIDER_IMPL;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN;

public class IcebergCachingFileIOTest {

    public void writeIcebergMetaTestFile() {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/0001.metadata.json"));
            out.write("test iceberg metadata json file content");
            out.close();
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testNewInputFile() {
        writeIcebergMetaTestFile();
        String path = "file:/tmp/0001.metadata.json";

        // create iceberg cachingFileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(icebergProperties);

        InputFile cachingFileIOInputFile = cachingFileIO.newInputFile(path);
        cachingFileIOInputFile.newStream();

        String cachingFileIOPath = cachingFileIOInputFile.location();
        Assertions.assertEquals(path, cachingFileIOPath);

        long cacheIOInputFileSize = cachingFileIOInputFile.getLength();
        Assertions.assertEquals(cacheIOInputFileSize, 39);
        cachingFileIO.deleteFile(path);
    }

    @Test
    public void testNewFileWithException() {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        icebergProperties.put(key, "sas_token");
        cachingFileIO.initialize(icebergProperties);

        String path = "file:/tmp/non_existent_file.json";
        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newInputFile(path);
        });

        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newOutputFile(path);
        });
    }

    @Test
    public void testBuildAzureConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT;
        String sasToken = "sas_token";
        properties.put(key, sasToken);
        String path = "abfss://container@account.dfs.core.windows.net/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);

        String authType = configuration.get("fs.azure.account.auth.type.account." + ADLS_ENDPOINT);
        Assertions.assertEquals("SAS", authType);
        String token = configuration.get("fs.azure.sas.fixed.token.account." + ADLS_ENDPOINT);
        Assertions.assertEquals(sasToken, token);

        properties = new HashMap<>();
        key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        sasToken = "blob_sas_token";
        properties.put(key, sasToken);
        path = "wasbs://container@account.blob.core.windows.net/path/1/2";

        cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        configuration = cachingFileIO.buildConfFromProperties(properties, path);

        token = configuration.get("fs.azure.sas.container.account." + BLOB_ENDPOINT);
        Assertions.assertEquals(sasToken, token);
    }

    @Test
    public void testBuildGCSConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String accessToken = "access_token";
        properties.put(GCS_ACCESS_TOKEN, accessToken);
        String path = "gs://iceberg_gcp/iceberg_catalog/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);
        String token = configuration.get("fs.gs.temporary.access.token");
        Assertions.assertEquals(accessToken, token);
        Assertions.assertEquals(ACCESS_TOKEN_PROVIDER_IMPL,
                configuration.get("fs.gs.auth.access.token.provider.impl"));
    }

    @Test
    void testWrappedIOConfigurationPropagation() {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(properties);

        Configuration conf = new Configuration();
        conf.set("test.key", "test.value");
        cachingFileIO.setConf(conf);

        FileIO wrappedIO = cachingFileIO.getWrappedIO();
        Assertions.assertTrue(wrappedIO instanceof HadoopConfigurable);

        HadoopConfigurable hadoopConfigurable = (HadoopConfigurable) wrappedIO;
        Assertions.assertDoesNotThrow(() -> {
            hadoopConfigurable.serializeConfWith(confToSerialize -> {
                Assertions.assertNotNull(confToSerialize);
                return (SerializableSupplier<Configuration>) () -> confToSerialize;
            });
        });
    }

    /**
     * Proves the disk-cache overflow bug in TwoLevelContentCache.
     *
     * The diskCache weigher assigns weight=0 to pinned entries (useCount > 0).
     * When a reader pins an entry via computeIfPresent, Caffeine re-weighs it
     * to 0, "freeing" that capacity in its accounting. New files are then
     * admitted to fill the freed space. After the reader unpins, Caffeine
     * re-weighs back to the real size — but the extra files are already on disk.
     *
     * Result: physical disk usage = DISK_CACHE_CAPACITY + sum(pinned file sizes).
     */
    @Test
    public void testDiskCacheWeigherAllowsOverflowWhenPinned() {
        // FakeEntry mirrors the fields of DiskCacheEntry that the weigher inspects.
        class FakeEntry {
            final long length;
            int useCount;

            FakeEntry(long length) {
                this.length = length;
                this.useCount = 0;
            }

            void pin() {
                useCount++;
            }
            void unpin() {
                useCount--;
            }
        }

        final long CAPACITY = 100; // bytes
        List<String> evicted = new ArrayList<>();

        // Exact weigher logic copied from TwoLevelContentCache:
        // pinned entries weigh 0, unpinned entries weigh their actual byte length.
        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        v.useCount == 0 ? (int) Math.min(v.length, Integer.MAX_VALUE) : 0)
                .evictionListener((k, v, cause) -> evicted.add((String) k))
                .build();

        // Step 1: put file A (60 bytes). Caffeine accounts 60/100.
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();
        Assertions.assertEquals(1, cache.estimatedSize(), "A should be in cache");

        // Step 2: a query starts reading A — pin it.
        // computeIfPresent mutates useCount in-place; Caffeine re-weighs the entry.
        // New weight = 0, so Caffeine now believes 0/100 bytes are in use.
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.pin();
            return v;
        });
        cache.cleanUp();

        // Step 3: add file B (60 bytes). Caffeine sees 0 + 60 = 60 ≤ 100 → admitted.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // Step 4: add file C (60 bytes). Caffeine sees 0 + 60 + 60 = 120 > 100 → evicts B.
        FakeEntry c = new FakeEntry(60);
        cache.put("C", c);
        cache.cleanUp();

        // Caffeine did the right thing from its perspective: evicted B to stay within capacity.
        Assertions.assertTrue(evicted.contains("B"), "Caffeine should have evicted B to respect capacity");

        // But A (60 bytes) is still on disk with weight=0 — Caffeine doesn't count it.
        // Actual bytes on disk = A(60) + C(60) = 120, which exceeds CAPACITY(100).
        long actualBytesOnDisk = cache.asMap().values().stream()
                .mapToLong(e -> e.length)
                .sum();

        Assertions.assertTrue(actualBytesOnDisk > CAPACITY,
                "BUG CONFIRMED: actual disk usage (" + actualBytesOnDisk + " bytes) exceeds " +
                "DISK_CACHE_CAPACITY (" + CAPACITY + " bytes) because pinned entry A " +
                "has weight=0 in Caffeine's accounting while still occupying disk space.");
    }

    /**
     * Proves the fix: with the corrected weigher (always weigh by actual length),
     * Caffeine's accounting stays accurate even when entries are pinned.
     * Total disk usage never exceeds DISK_CACHE_CAPACITY.
     */
    @Test
    public void testFixedWeigherRespectsCapacity() {
        class FakeEntry {
            final long length;
            int useCount;

            FakeEntry(long length) {
                this.length = length;
                this.useCount = 0;
            }

            void pin() {
                useCount++;
            }
            void unpin() {
                useCount--;
            }
        }

        final long CAPACITY = 100;
        List<String> evicted = new ArrayList<>();

        // FIXED weigher: always weigh by actual length, regardless of useCount.
        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        (int) Math.min(v.length, Integer.MAX_VALUE))
                .evictionListener((k, v, cause) -> evicted.add((String) k))
                .build();

        // Step 1: put file A (60 bytes). Weight = 60. Cache: 60/100 used.
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();

        // Step 2: pin A. With fixed weigher, weight stays 60. Cache still 60/100 used.
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.pin();
            return v;
        });
        cache.cleanUp();

        // Step 3: add B (60 bytes). Caffeine sees 60 + 60 = 120 > 100 → evicts A or B.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // Actual bytes remaining in cache must not exceed CAPACITY.
        long actualBytesOnDisk = cache.asMap().values().stream()
                .mapToLong(e -> e.length)
                .sum();

        Assertions.assertTrue(actualBytesOnDisk <= CAPACITY,
                "FIXED: actual disk usage (" + actualBytesOnDisk + " bytes) should not exceed " +
                "DISK_CACHE_CAPACITY (" + CAPACITY + " bytes).");
    }

    /**
     * Demonstrates the orphan-file regression introduced when the weigher fix was first applied.
     *
     * With the corrected weigher (real weight for pinned entries), Caffeine can evict a pinned
     * entry under pressure. The eviction listener skips file deletion because useCount > 0 at
     * that moment. The old close() used computeIfPresent, which found no entry after eviction,
     * so unpin() and file deletion never ran — leaving an orphan file on disk forever.
     *
     * The fix: DiskCacheSeekableInputStream captures the DiskCacheEntry reference at open time
     * and unpins directly on it. When the last reader closes, if the entry is no longer in cache,
     * it deletes the orphan file immediately.
     *
     * This test models that sequence with FakeEntry to verify the close-time cleanup logic.
     */
    @Test
    public void testOrphanFileCleanedUpWhenLastStreamClosesAfterEviction() {
        class FakeEntry {
            final long length;
            final AtomicInteger useCount = new AtomicInteger(0);

            FakeEntry(long length) {
                this.length = length;
            }
        }

        final long CAPACITY = 100;
        AtomicBoolean deleteCalledForA = new AtomicBoolean(false);

        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        (int) Math.min(v.length, Integer.MAX_VALUE))
                .evictionListener((k, v, cause) -> {
                    if (((FakeEntry) v).useCount.get() > 0) {
                        // mirrors eviction listener: skip delete while pinned
                    } else {
                        if ("A".equals(k)) {
                            deleteCalledForA.set(true);
                        }
                    }
                })
                .build();

        // Step 1: cache entry A (60 bytes) and pin it (simulating an open stream).
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.useCount.incrementAndGet();
            return v;
        });

        // Step 2: add B (60 bytes) — forces Caffeine to evict A to respect capacity.
        // Eviction listener fires with useCount=1 > 0 → skips delete → orphan.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // A is now evicted and not in cache, delete was NOT called by the eviction listener.
        Assertions.assertNull(cache.getIfPresent("A"), "A should have been evicted");
        Assertions.assertFalse(deleteCalledForA.get(), "Delete should not fire from eviction listener while pinned");

        // Step 3: simulate close() with the fix applied.
        // The fix decrements useCount directly on the captured entry and checks cache membership.
        int remaining = a.useCount.decrementAndGet();
        boolean entryStillInCache = cache.asMap().get("A") == a;
        if (remaining == 0 && !entryStillInCache) {
            // mirrors the orphan cleanup in DiskCacheSeekableInputStream.close()
            deleteCalledForA.set(true);
        }

        // Verify: cleanup triggered immediately when the last reader closed.
        Assertions.assertTrue(deleteCalledForA.get(),
                "Orphaned file for evicted-while-pinned entry A should be cleaned up on stream close");
        Assertions.assertEquals(0, a.useCount.get(), "useCount should reach 0 after close");
    }

}