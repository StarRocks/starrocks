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

package com.starrocks.connector.adbc;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCMetaCacheTest {

    private Map<String, String> makeProps(boolean enable, long expireSec) {
        Map<String, String> props = new HashMap<>();
        props.put("adbc_meta_cache_enable", String.valueOf(enable));
        props.put("adbc_meta_cache_expire_sec", String.valueOf(expireSec));
        return props;
    }

    @Test
    public void testCacheDisabled_bypassesCache() {
        Map<String, String> props = makeProps(false, 10);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        AtomicInteger callCount = new AtomicInteger(0);
        String result1 = cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value1";
        });
        String result2 = cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value1";
        });

        assertEquals("value1", result1);
        assertEquals("value1", result2);
        // Function called twice because cache is disabled
        assertEquals(2, callCount.get());
    }

    @Test
    public void testCacheEnabled_returnsCachedValue() {
        Map<String, String> props = makeProps(true, 10);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        AtomicInteger callCount = new AtomicInteger(0);
        String result1 = cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value1";
        });
        String result2 = cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value1_updated";
        });

        assertEquals("value1", result1);
        assertEquals("value1", result2);  // returns cached value
        assertEquals(1, callCount.get());  // function called only once
    }

    @Test
    public void testPermanentCache_alwaysCaches() {
        Map<String, String> props = makeProps(false, 10);  // even with cache disabled
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, true);

        AtomicInteger callCount = new AtomicInteger(0);
        String result1 = cache.getPersistentCache("key1", k -> {
            callCount.incrementAndGet();
            return "value1";
        });
        String result2 = cache.getPersistentCache("key1", k -> {
            callCount.incrementAndGet();
            return "value1_updated";
        });

        assertEquals("value1", result1);
        assertEquals("value1", result2);
        assertEquals(1, callCount.get());
    }

    @Test
    public void testInvalidate_removesKey() {
        Map<String, String> props = makeProps(true, 600);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        cache.get("key1", k -> "value1");
        cache.invalidate("key1");

        AtomicInteger callCount = new AtomicInteger(0);
        String result = cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value2";
        });
        assertEquals("value2", result);
        assertEquals(1, callCount.get());
    }

    @Test
    public void testGetIfPresent_returnsNullWhenCacheDisabled() {
        Map<String, String> props = makeProps(false, 10);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        assertNull(cache.getIfPresent("key1"));
    }

    @Test
    public void testGetIfPresent_returnsValueWhenCached() {
        Map<String, String> props = makeProps(true, 600);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        cache.get("key1", k -> "value1");
        assertEquals("value1", cache.getIfPresent("key1"));
    }

    @Test
    public void testIsEnableCache_returnsFalseWhenDisabled() {
        Map<String, String> props = makeProps(false, 10);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);
        assertFalse(cache.isEnableCache());
    }

    @Test
    public void testIsEnableCache_returnsTrueWhenEnabled() {
        Map<String, String> props = makeProps(true, 10);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);
        assertTrue(cache.isEnableCache());
    }

    @Test
    public void testGetCurrentExpireSec_returnsConfiguredValue() {
        Map<String, String> props = makeProps(true, 42);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);
        assertEquals(42, cache.getCurrentExpireSec());
    }

    @Test
    public void testCacheTTLExpiry() throws InterruptedException {
        Map<String, String> props = makeProps(true, 1);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        AtomicInteger callCount = new AtomicInteger(0);
        cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value1";
        });
        assertEquals(1, callCount.get());

        // Wait for TTL to expire
        Thread.sleep(1100);

        cache.get("key1", k -> {
            callCount.incrementAndGet();
            return "value2";
        });
        // Function should have been called again after TTL expiry
        assertEquals(2, callCount.get());
    }

    @Test
    public void testPut_storesValueWhenCacheEnabled() {
        Map<String, String> props = makeProps(true, 600);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        cache.put("key1", "value1");
        assertEquals("value1", cache.getIfPresent("key1"));
    }

    @Test
    public void testPut_noOpWhenCacheDisabled() {
        Map<String, String> props = makeProps(false, 600);
        ADBCMetaCache<String, String> cache = new ADBCMetaCache<>(props, false);

        cache.put("key1", "value1");
        assertNull(cache.getIfPresent("key1"));
    }
}
