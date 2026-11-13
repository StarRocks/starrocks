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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
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
