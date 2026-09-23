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

package com.starrocks.fs.hdfs;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBrokerFD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The file-system layer's two doors: creating the file system for an identity that has none, and
 * the per-call operations that go on to use it. Both matter because this layer serves broker-less
 * load and {@code TableFunctionTable}, whose schema inference used to run inside the planner's
 * critical section.
 */
public class HdfsFsManagerDoorTest {
    private static final long INTERNAL_DB_ID = 50001L;

    private String savedMode;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_blocking_call_validation_mode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    @AfterEach
    public void tearDown() {
        Config.lock_blocking_call_validation_mode = savedMode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    /**
     * The construction half. An identity with no file system yet means the caller is about to
     * create one, which contacts the namenode or the object store.
     */
    @Test
    public void testCreatingAFileSystemUnderALockIsReported() {
        Config.lock_blocking_call_validation_mode = "warn";
        HdfsFsManager manager = new HdfsFsManager();
        HdfsFsIdentity identity = new HdfsFsIdentity("hdfs://namenode:9000", "user,");

        HdfsFs acquired = underLock(() -> Deencapsulation.invoke(manager, "acquireCachedFileSystem", identity));
        acquired.getLock().unlock();

        // Only the count is asserted: the call goes through reflection, so the frame the report names
        // is jmockit's rather than this test's. Which frame gets named is BlockingCallValidator's own
        // property and is pinned there; what is under test here is that a miss is reported at all.
        Assertions.assertEquals(1, LockInvariantViolations.totalViolations());
    }

    /**
     * The half that keeps the door honest. An identity whose file system is already built answers
     * from the cache, so there is no wait to report -- this is the case that rules out putting the
     * guard at {@code getFileSystem}'s entry, where every cached path would be reported.
     */
    @Test
    public void testReusingACachedFileSystemIsNotReported() throws Exception {
        Config.lock_blocking_call_validation_mode = "error";
        HdfsFsManager manager = new HdfsFsManager();
        HdfsFsIdentity identity = new HdfsFsIdentity("file://", "user,");

        HdfsFs cached = new HdfsFs(identity);
        // a real FileSystem that contacts nothing, so the entry is genuinely "already built"
        cached.setFileSystem(FileSystem.get(new URI("file:///"), new Configuration()));
        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(manager, "cachedFileSystem");
        cache.put(identity, cached);

        HdfsFs acquired = underLock(() -> Deencapsulation.invoke(manager, "acquireCachedFileSystem", identity));
        acquired.getLock().unlock();

        Assertions.assertSame(cached, acquired);
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /**
     * The per-call half, and where it sits: below the local validation, above the I/O, outside the
     * {@code try} whose catch rewrites every failure.
     *
     * <p>Two reports for the first call -- the file system did not exist, so the construction door
     * fires as well -- and one for the second, when only the operation itself is a wait. That
     * difference is the whole point of having both doors: one marks building the client, the other
     * marks using it, and they are reported against different moments.
     *
     * <p>A {@code file://} path reaches a real Hadoop {@code LocalFileSystem} and contacts nothing,
     * which is what lets this exercise the actual path rather than a failure before it.
     */
    @Test
    public void testAnOperationThatReachesTheFileSystemIsReported() throws Exception {
        Config.lock_blocking_call_validation_mode = "warn";
        HdfsFsManager manager = new HdfsFsManager();
        String path = "file://" + System.getProperty("java.io.tmpdir") + "/starrocks-door-test-*";

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            manager.listPath(path, false, new HashMap<>());
            Assertions.assertEquals(2, LockInvariantViolations.totalViolations(),
                    "the first call builds the file system and then uses it, so both doors fire");
            LockInvariantViolations.clearViolations();
            manager.listPath(path, false, new HashMap<>());
            Assertions.assertEquals(1, LockInvariantViolations.totalViolations(),
                    "the second call reuses the file system, so only the per-call door fires");
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * And where it must not sit: a path that never resolves is rejected by local validation, so
     * nothing remote was ever going to happen. Reporting it would inflate the diagnostic counts with
     * calls that did not occur, and in error mode would replace the argument error the caller should
     * get with a lock violation.
     */
    @Test
    public void testAPathThatFailsLocallyIsNotReported() {
        Config.lock_blocking_call_validation_mode = "error";
        HdfsFsManager manager = new HdfsFsManager();

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                    () -> manager.listPath("no-scheme-at-all", false, new HashMap<>()));
            Assertions.assertTrue(e.getMessage().contains("scheme"), e.getMessage());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testNoLockMeansNoReport() throws Exception {
        Config.lock_blocking_call_validation_mode = "error";
        HdfsFsManager manager = new HdfsFsManager();
        manager.listPath("file://" + System.getProperty("java.io.tmpdir") + "/starrocks-door-test-*",
                false, new HashMap<>());
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /**
     * The stream operations carry their own door, on the same rule as the rest: below the handle
     * lookup and the offset validation, above the read or write. A round trip through a real local
     * file exercises all six, which is the only way to reach them -- they need an open stream, not
     * just a path.
     */
    @Test
    public void testTheStreamOperationsAreGuardedToo() throws Exception {
        Config.lock_blocking_call_validation_mode = "warn";
        HdfsFsManager manager = new HdfsFsManager();
        String path = "file://" + System.getProperty("java.io.tmpdir")
                + "/starrocks-door-test-" + UUID.randomUUID();
        Map<String, String> properties = new HashMap<>();
        byte[] payload = "door".getBytes(StandardCharsets.UTF_8);

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            TBrokerFD writer = manager.openWriter(path, properties);
            manager.pwrite(writer, 0, payload);
            manager.closeWriter(writer);

            TBrokerFD reader = manager.openReader(path, 0, properties);
            Assertions.assertArrayEquals(payload, manager.pread(reader, 0, payload.length));
            manager.closeReader(reader);
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
            manager.deletePath(path, properties);
        }

        // Six operations, each a wait, plus the construction of the file system the first of them
        // needed. Asserted as a lower bound because what matters is that none of the six is silent.
        Assertions.assertTrue(LockInvariantViolations.totalViolations() >= 7,
                "every stream operation should have reported, got: " + LockInvariantViolations.totalViolations());
    }

    /**
     * In error mode the construction door refuses, and it does so holding a lock this method took
     * itself. That lock has to go back, or the next caller for the same identity waits on a thread
     * that is long gone.
     */
    @Test
    public void testErrorModeGivesTheEntryLockBackBeforeItThrows() {
        Config.lock_blocking_call_validation_mode = "error";
        HdfsFsManager manager = new HdfsFsManager();
        HdfsFsIdentity identity = new HdfsFsIdentity("hdfs://namenode:9000", "user,");

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertThrows(RuntimeException.class,
                    () -> Deencapsulation.invoke(manager, "acquireCachedFileSystem", identity));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(manager, "cachedFileSystem");
        HdfsFs entry = cache.get(identity);
        Assertions.assertNotNull(entry);
        Assertions.assertFalse(entry.getLock().isLocked(),
                "the refusal left the entry's lock held, so the next caller for this identity would block");
    }

    /** The cache is keyed per identity, so the map the test primes is the one the manager reads. */
    @Test
    public void testTheCacheFieldIsTheOneTheDoorConsults() {
        HdfsFsManager manager = new HdfsFsManager();
        Map<HdfsFsIdentity, HdfsFs> cache = Deencapsulation.getField(manager, "cachedFileSystem");
        Assertions.assertInstanceOf(ConcurrentHashMap.class, cache);
        Assertions.assertTrue(cache.isEmpty());
    }

    private <T> T underLock(java.util.function.Supplier<T> body) {
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            return body.get();
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }
}
