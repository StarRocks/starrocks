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

import com.starrocks.common.jmockit.Deencapsulation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
