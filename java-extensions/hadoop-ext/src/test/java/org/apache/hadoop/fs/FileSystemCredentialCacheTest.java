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

package org.apache.hadoop.fs;

import com.starrocks.connector.hadoop.HadoopExt;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FileSystemCredentialCacheTest {
    private static final String SCHEME = "credtest";
    private static final URI URI_A = URI.create(SCHEME + "://bucket/");
    private static final String FINGERPRINT_KEY = HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "test";

    public static class TrackingFileSystem extends RawLocalFileSystem {
        volatile boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    public static class CatalogFileSystem extends TrackingFileSystem {
        @Override
        public URI getUri() {
            return URI.create("abfss://retained-catalog@account/");
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            if (closed) {
                throw new IOException("Catalog filesystem is closed");
            }
            return new FileStatus(0, true, 1, 4096, 0, path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            getFileStatus(path);
            return new FileStatus[0];
        }
    }

    private long savedTtl;

    @BeforeEach
    public void setUp() {
        savedTtl = FileSystem.Cache.credentialIdleTtlMs;
        FileSystem.Cache.credentialIdleTtlMs = TimeUnit.HOURS.toMillis(1);
    }

    @AfterEach
    public void tearDown() {
        FileSystem.Cache.credentialIdleTtlMs = savedTtl;
    }

    private static Configuration conf(String fingerprint) {
        Configuration conf = new Configuration(false);
        conf.set("fs." + SCHEME + ".impl", TrackingFileSystem.class.getName());
        if (fingerprint != null) {
            conf.set(FINGERPRINT_KEY, fingerprint);
        }
        return conf;
    }

    private static Configuration azureConf(String scheme, String token) {
        Configuration conf = new Configuration(false);
        conf.set("fs." + scheme + ".impl", TrackingFileSystem.class.getName());
        if (token != null) {
            conf.set(HadoopExt.HADOOP_CLOUD_CONFIGURATION_STRING,
                    "AzureCloudConfiguration{cred=AzureADLS2CloudCredential{sasToken='" + token + "'}}");
        }
        return conf;
    }

    private static Configuration expiringAzureConf(String scheme, String token) {
        Configuration conf = azureConf(scheme, token);
        conf.set(HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "delta.azure", "test-digest");
        return conf;
    }

    @Test
    public void testHadoopCatalogRetainsFileSystemWhileCredentialEntriesExpire() throws Exception {
        URI uri = URI.create("abfss://retained-catalog@account/");
        Configuration catalogConf = azureConf("abfss", "same-token");
        catalogConf.set("fs.abfss.impl", CatalogFileSystem.class.getName());
        Configuration deltaConf = new Configuration(catalogConf);
        deltaConf.set(HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "delta.azure", "credential-digest");
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        FileSystem.Cache cache = (FileSystem.Cache) cacheField.get(null);
        Field nextSweepField = FileSystem.Cache.class.getDeclaredField("nextCredentialSweepMs");
        nextSweepField.setAccessible(true);
        URI triggerUri = URI.create(SCHEME + "://retained-catalog-trigger/");
        Configuration triggerConf = conf(null);
        try (FileSystem trigger = FileSystem.get(triggerUri, triggerConf);
                HadoopCatalog catalog = new HadoopCatalog(catalogConf, uri + "warehouse");
                CatalogFileSystem retained = (CatalogFileSystem) FileSystem.get(uri, catalogConf);
                CatalogFileSystem expiring = (CatalogFileSystem) FileSystem.get(uri, deltaConf)) {
            Assertions.assertNotSame(retained, expiring);
            Assertions.assertTrue(catalog.listNamespaces().isEmpty());
            FileSystem.Cache.credentialIdleTtlMs = 0;
            nextSweepField.setLong(cache, 0);
            Assertions.assertSame(trigger, FileSystem.get(triggerUri, triggerConf));
            Assertions.assertTrue(catalog.listNamespaces().isEmpty());
            nextSweepField.setLong(cache, 0);
            Assertions.assertSame(trigger, FileSystem.get(triggerUri, triggerConf));
            Assertions.assertTrue(expiring.closed);
            Assertions.assertTrue(catalog.listNamespaces().isEmpty());
            Assertions.assertFalse(retained.closed);
        }
    }

    @Test
    public void testAzureCloudConfigurationRequiresExplicitFingerprint() throws IOException {
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs", "adl")) {
            URI uri = URI.create(scheme + "://container@account/");
            Configuration conf = azureConf(scheme, "token-a");
            Assertions.assertFalse(new FileSystem.Cache.Key(uri, conf).isEvictable(), scheme);
            Assertions.assertTrue(new FileSystem.Cache.Key(uri, expiringAzureConf(scheme, "token-a")).isEvictable(), scheme);
            Assertions.assertFalse(new FileSystem.Cache.Key(uri, expiringAzureConf(scheme, "token-a"), 1)
                    .isEvictable(), scheme);
            Assertions.assertFalse(new FileSystem.Cache.Key(uri, conf, 1).isEvictable(), scheme);
            Assertions.assertFalse(new FileSystem.Cache.Key(uri, azureConf(scheme, null)).isEvictable(), scheme);
            conf.set(HadoopExt.HADOOP_CLOUD_CONFIGURATION_STRING, "");
            Assertions.assertFalse(new FileSystem.Cache.Key(uri, conf).isEvictable(), scheme);
        }
        for (String scheme : List.of("file", "hdfs", "s3a", "gs", SCHEME)) {
            Assertions.assertFalse(new FileSystem.Cache.Key(URI.create(scheme + "://bucket/"),
                    azureConf(scheme, "token-a")).isEvictable(), scheme);
        }
        Assertions.assertFalse(new FileSystem.Cache.Key(URI.create("ABFSS://container@account/"),
                azureConf("abfss", "token-a")).isEvictable());
    }

    @Test
    public void testRotatingAzureCloudConfigurationsAreEvictedAndClosed() throws IOException {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        URI uri = URI.create("abfss://container@account/");
        try {
            for (int round = 0; round < 3; round++) {
                List<TrackingFileSystem> instances = new ArrayList<>();
                for (int token = 0; token < 32; token++) {
                    Configuration conf = expiringAzureConf("abfss", "token-" + round + "-" + token);
                    TrackingFileSystem fs = (TrackingFileSystem) cache.get(uri, conf);
                    Assertions.assertSame(fs, cache.get(uri, new Configuration(conf)));
                    instances.add(fs);
                }
                Assertions.assertEquals(32, cache.size());
                FileSystem.Cache.credentialIdleTtlMs = 0;
                cache.sweepNowForTesting();
                Assertions.assertEquals(0, cache.size());
                Assertions.assertEquals(32, cache.pendingCloseSize());
                instances.forEach(fs -> Assertions.assertFalse(fs.closed));

                cache.sweepNowForTesting();
                Assertions.assertEquals(0, cache.pendingCloseSize());
                instances.forEach(fs -> Assertions.assertTrue(fs.closed));
                FileSystem.Cache.credentialIdleTtlMs = savedTtl;
            }
        } finally {
            cache.closeAll();
        }
    }

    @Test
    public void testAzureCloudConfigurationPreservesUnmanagedAndUniqueInstances() throws IOException {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        URI uri = URI.create("abfss://container@account/");
        try {
            TrackingFileSystem managed = (TrackingFileSystem) cache.get(uri, expiringAzureConf("abfss", "token-a"));
            TrackingFileSystem plain = (TrackingFileSystem) cache.get(uri, azureConf("abfss", null));
            TrackingFileSystem unique = (TrackingFileSystem) cache.getUnique(uri, expiringAzureConf("abfss", "token-a"));
            FileSystem.Cache.credentialIdleTtlMs = 0;
            cache.sweepNowForTesting();
            Assertions.assertEquals(2, cache.size());
            Assertions.assertEquals(1, cache.pendingCloseSize());
            cache.sweepNowForTesting();
            Assertions.assertTrue(managed.closed);
            Assertions.assertFalse(plain.closed);
            Assertions.assertFalse(unique.closed);
        } finally {
            cache.closeAll();
        }
    }

    @Test
    public void testAzureCloudConfigurationRespectsDisableCache() throws IOException {
        Configuration conf = azureConf("abfss", "uncached-token");
        conf.setBoolean("fs.abfss.impl.disable.cache", true);
        URI uri = URI.create("abfss://container@account/");
        try (FileSystem first = FileSystem.get(uri, conf); FileSystem second = FileSystem.get(uri, conf)) {
            Assertions.assertNotSame(first, second);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAzureCloudConfigurationLookupRefreshesAccessAndTriggersEviction() throws Exception {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        URI uri = URI.create("abfss://container@account/");
        Configuration activeConf = expiringAzureConf("abfss", "active-token");
        try {
            TrackingFileSystem active = (TrackingFileSystem) cache.get(uri, activeConf);
            TrackingFileSystem idle = (TrackingFileSystem) cache.get(uri, expiringAzureConf("abfss", "idle-token"));
            Field accessTimesField = FileSystem.Cache.class.getDeclaredField("credentialLastAccessMs");
            accessTimesField.setAccessible(true);
            Map<FileSystem.Cache.Key, Long> accessTimes =
                    (Map<FileSystem.Cache.Key, Long>) accessTimesField.get(cache);
            long expired = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
            accessTimes.replaceAll((key, time) -> expired);
            Field nextSweepField = FileSystem.Cache.class.getDeclaredField("nextCredentialSweepMs");
            nextSweepField.setAccessible(true);
            nextSweepField.setLong(cache, 0);

            // Exercise the lookup-triggered sweep, including refreshing the requested key before eviction.
            Assertions.assertSame(active, cache.get(uri, activeConf));
            Assertions.assertEquals(1, cache.size());
            Assertions.assertEquals(1, cache.pendingCloseSize());
            Assertions.assertFalse(active.closed);
            Assertions.assertFalse(idle.closed);
            cache.closeAll();
            Assertions.assertTrue(active.closed);
            Assertions.assertTrue(idle.closed);
        } finally {
            cache.closeAll();
        }
    }

    @Test
    public void testActiveReadersSurviveSweepsUntilLastReaderCloses() throws Exception {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        Configuration activeConf = expiringAzureConf("abfss", "active-readers");
        URI uri = URI.create("abfss://container@account/");
        HadoopExt ext = HadoopExt.getInstance();
        try (HadoopExt.FileSystemAccess first = ext.acquireFileSystemAccess(activeConf);
                HadoopExt.FileSystemAccess second = ext.acquireFileSystemAccess(new Configuration(activeConf))) {
            TrackingFileSystem active = (TrackingFileSystem) cache.get(uri, activeConf);
            TrackingFileSystem obsolete = (TrackingFileSystem) cache.get(uri, expiringAzureConf("abfss", "old-token"));
            FileSystem.Cache.credentialIdleTtlMs = 0;
            cache.sweepNowForTesting();
            cache.sweepNowForTesting();
            Assertions.assertTrue(obsolete.closed);
            Assertions.assertFalse(active.closed);
            Assertions.assertSame(active, cache.get(uri, activeConf));
            first.close();
            first.close();
            cache.sweepNowForTesting();
            cache.sweepNowForTesting();
            Assertions.assertFalse(active.closed);
            second.close();
            cache.sweepNowForTesting();
            Assertions.assertFalse(active.closed);
            cache.sweepNowForTesting();
            Assertions.assertTrue(active.closed);
            Assertions.assertEquals(0, cache.size());
            Assertions.assertEquals(0, cache.pendingCloseSize());
            Assertions.assertFalse(ext.hasActiveFileSystemAccess(
                    ext.getCloudConfString(activeConf), ext.getCredentialFingerprint(activeConf)));
        } finally {
            cache.closeAll();
        }
    }

    @Test
    public void testAccessAfterEvictionUsesNewInstance() throws Exception {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        Configuration conf = expiringAzureConf("abfss", "reopen");
        URI uri = URI.create("abfss://container@account/");
        try {
            TrackingFileSystem retired = (TrackingFileSystem) cache.get(uri, conf);
            FileSystem.Cache.credentialIdleTtlMs = 0;
            cache.sweepNowForTesting();
            try (HadoopExt.FileSystemAccess ignored = HadoopExt.getInstance().acquireFileSystemAccess(conf)) {
                TrackingFileSystem reopened = (TrackingFileSystem) cache.get(uri, conf);
                Assertions.assertNotSame(retired, reopened);
                cache.sweepNowForTesting();
                cache.sweepNowForTesting();
                Assertions.assertTrue(retired.closed);
                Assertions.assertFalse(reopened.closed);
            }
            cache.sweepNowForTesting();
            cache.sweepNowForTesting();
            Assertions.assertEquals(0, cache.size());
            Assertions.assertEquals(0, cache.pendingCloseSize());
        } finally {
            cache.closeAll();
        }
    }

    @Test
    public void testKeySeparatesFingerprints() throws IOException {
        FileSystem.Cache.Key none = new FileSystem.Cache.Key(URI_A, conf(null));
        FileSystem.Cache.Key empty = new FileSystem.Cache.Key(URI_A, new Configuration(false));
        FileSystem.Cache.Key a = new FileSystem.Cache.Key(URI_A, conf("a"));
        FileSystem.Cache.Key a2 = new FileSystem.Cache.Key(URI_A, conf("a"));
        FileSystem.Cache.Key b = new FileSystem.Cache.Key(URI_A, conf("b"));

        Assertions.assertEquals(none, empty);
        Assertions.assertEquals(a, a2);
        Assertions.assertEquals(a.hashCode(), a2.hashCode());
        Assertions.assertNotEquals(a, b);
        Assertions.assertNotEquals(a, none);
        Assertions.assertFalse(none.isEvictable());
        Assertions.assertTrue(a.isEvictable());
    }

    @Test
    public void testCacheReusesPerFingerprint() throws IOException {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        FileSystem a = cache.get(URI_A, conf("a"));
        Assertions.assertSame(a, cache.get(URI_A, conf("a")));
        Assertions.assertNotSame(a, cache.get(URI_A, conf("b")));
        Assertions.assertEquals(2, cache.size());
        cache.closeAll();
    }

    @Test
    public void testFingerprintOverridesDisableCache() throws IOException {
        Configuration withFingerprint = conf("disable-test");
        withFingerprint.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
        FileSystem first = FileSystem.get(URI_A, withFingerprint);
        FileSystem second = FileSystem.get(URI_A, withFingerprint);
        Assertions.assertSame(first, second);

        Configuration withoutFingerprint = conf(null);
        withoutFingerprint.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
        FileSystem third = FileSystem.get(URI_A, withoutFingerprint);
        FileSystem fourth = FileSystem.get(URI_A, withoutFingerprint);
        Assertions.assertNotSame(third, fourth);

        first.close();
        third.close();
        fourth.close();
    }

    @Test
    public void testIdleFingerprintEntryEvictedThenClosedAfterAnotherTtl() throws IOException {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        TrackingFileSystem dynamic = (TrackingFileSystem) cache.get(URI_A, conf("a"));
        TrackingFileSystem plain = (TrackingFileSystem) cache.get(URI_A, conf(null));
        TrackingFileSystem unique = (TrackingFileSystem) cache.getUnique(URI_A, conf("a"));
        Assertions.assertEquals(3, cache.size());

        FileSystem.Cache.credentialIdleTtlMs = 0;
        cache.sweepNowForTesting();
        // Evicted from the map, but not closed yet: a reader may still hold it.
        Assertions.assertEquals(2, cache.size());
        Assertions.assertEquals(1, cache.pendingCloseSize());
        Assertions.assertFalse(dynamic.closed);

        // A lookup after eviction creates a fresh instance.
        FileSystem.Cache.credentialIdleTtlMs = TimeUnit.HOURS.toMillis(1);
        TrackingFileSystem fresh = (TrackingFileSystem) cache.get(URI_A, conf("a"));
        Assertions.assertNotSame(dynamic, fresh);

        FileSystem.Cache.credentialIdleTtlMs = 0;
        cache.sweepNowForTesting();
        Assertions.assertTrue(dynamic.closed);
        Assertions.assertFalse(plain.closed);
        Assertions.assertFalse(unique.closed);

        // The fresh instance was evicted in the same sweep; the older one closing did not take it out of
        // the map, and an instance without a fingerprint or a unique one is never evicted.
        Assertions.assertFalse(fresh.closed);
        Assertions.assertEquals(2, cache.size());
        Assertions.assertEquals(1, cache.pendingCloseSize());

        cache.closeAll();
        Assertions.assertTrue(fresh.closed);
        Assertions.assertEquals(0, cache.pendingCloseSize());
    }

    @Test
    public void testRemoveOfEvictedInstanceKeepsNewerInstance() throws IOException {
        FileSystem.Cache cache = new FileSystem.Cache(new Configuration(false));
        FileSystem old = cache.get(URI_A, conf("a"));
        FileSystem.Cache.credentialIdleTtlMs = 0;
        cache.sweepNowForTesting();
        FileSystem.Cache.credentialIdleTtlMs = TimeUnit.HOURS.toMillis(1);
        FileSystem newer = cache.get(URI_A, conf("a"));
        Assertions.assertNotSame(old, newer);

        cache.remove(new FileSystem.Cache.Key(URI_A, conf("a")), old);
        Assertions.assertSame(newer, cache.get(URI_A, conf("a")));
        cache.closeAll();
    }
}
