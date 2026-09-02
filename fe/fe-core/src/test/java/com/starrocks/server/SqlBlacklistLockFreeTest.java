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

package com.starrocks.meta;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class SqlBlacklistLockFreeTest {
    private static final long RESPONSIVE_MS = 5000;

    private SqlBlackList sqlBlackList;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void beforeEach() {
        sqlBlackList = GlobalStateMgr.getCurrentState().getSqlBlackList();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
        sqlBlackList.cleanup();
    }

    @Test
    public void testSnapshotReflectsAddAndDelete() {
        Assertions.assertTrue(sqlBlackList.getBlackLists().isEmpty());

        long first = sqlBlackList.put(Pattern.compile("select .* from t1"));
        Assertions.assertEquals(1, sqlBlackList.getBlackLists().size());

        long second = sqlBlackList.put(Pattern.compile("select .* from t2"));
        Assertions.assertEquals(2, sqlBlackList.getBlackLists().size());

        sqlBlackList.delete(first);
        List<BlackListSql> remaining = sqlBlackList.getBlackLists();
        Assertions.assertEquals(1, remaining.size());
        Assertions.assertEquals(second, remaining.get(0).id);
    }

    @Test
    public void testSnapshotReflectsReplay() {
        sqlBlackList.put(7L, Pattern.compile("replayed_rule_b"));
        sqlBlackList.put(3L, Pattern.compile("replayed_rule_a"));

        List<BlackListSql> rules = sqlBlackList.getBlackLists();
        Assertions.assertEquals(2, rules.size());
        Assertions.assertEquals(3L, rules.get(0).id);
        Assertions.assertEquals(7L, rules.get(1).id);

        long next = sqlBlackList.put(Pattern.compile("locally_added_rule"));
        Assertions.assertTrue(next > 7L, "expected an id above the replayed ones, got " + next);
    }

    @Test
    public void testSnapshotIsRebuiltAfterImageRoundTrip() throws Exception {
        SqlBlackList original = new SqlBlackList();
        long first = original.put(Pattern.compile("rule_from_image_a"));
        long second = original.put(Pattern.compile("rule_from_image_b"));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        original.save(image.getImageWriter());

        SqlBlackList recovered = new SqlBlackList();
        Assertions.assertTrue(recovered.getBlackLists().isEmpty());
        recovered.load(image.getMetaBlockReader());

        List<BlackListSql> rules = recovered.getBlackLists();
        Assertions.assertEquals(2, rules.size());
        Assertions.assertEquals(first, rules.get(0).id);
        Assertions.assertEquals(second, rules.get(1).id);
        Assertions.assertEquals("rule_from_image_a", rules.get(0).pattern.pattern());
        Assertions.assertEquals("rule_from_image_b", rules.get(1).pattern.pattern());

        AnalysisException hit = Assertions.assertThrows(AnalysisException.class,
                () -> recovered.verifying("select rule_from_image_b from t"));
        Assertions.assertTrue(hit.getMessage().contains(String.valueOf(second)), hit.getMessage());
        recovered.verifying("select nothing_matches_here from t");

        long third = recovered.put(Pattern.compile("rule_added_after_load"));
        Assertions.assertTrue(third > second, "expected an id above the loaded ones, got " + third);
        Assertions.assertEquals(3, recovered.getBlackLists().size());
    }

    @Test
    public void testLowestMatchingIdIsReported() {
        long low = sqlBlackList.put(Pattern.compile("from orders"));
        long high = sqlBlackList.put(Pattern.compile("select"));
        Assertions.assertTrue(low < high);

        for (int i = 0; i < 20; i++) {
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> sqlBlackList.verifying("select id from orders"));
            Assertions.assertTrue(e.getMessage().contains(String.valueOf(low)),
                    "expected the lowest matching rule id " + low + " in: " + e.getMessage());
        }
    }

    @Test
    public void testVerifyingIsNotBlockedByAnInProgressMutation() throws Exception {
        sqlBlackList.put(Pattern.compile("this_rule_never_matches"));

        assertCompletesWhileUpdateLockIsHeld(() -> {
            try {
                sqlBlackList.verifying("select 1");
            } catch (AnalysisException e) {
                throw new IllegalStateException("unexpected blacklist hit", e);
            }
        });
    }

    @Test
    public void testShowIsNotBlockedByAnInProgressMutation() throws Exception {
        sqlBlackList.put(Pattern.compile("some_rule"));

        assertCompletesWhileUpdateLockIsHeld(() -> Assertions.assertEquals(1, sqlBlackList.getBlackLists().size()));
    }

    @Test
    public void testConcurrentVerifyAndMutateDoNotFail() throws Exception {
        final int readers = 8;
        final int rounds = 200;
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(readers);
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < readers; i++) {
            Thread t = new Thread(() -> {
                started.countDown();
                try {
                    while (!stop.get()) {
                        for (BlackListSql rule : sqlBlackList.getBlackLists()) {
                            Assertions.assertNotNull(rule.pattern);
                        }
                        sqlBlackList.verifying("select id from a_table_no_rule_matches");
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            t.setDaemon(true);
            threads.add(t);
            t.start();
        }
        Assertions.assertTrue(started.await(RESPONSIVE_MS, TimeUnit.MILLISECONDS));

        for (int i = 0; i < rounds; i++) {
            long id = sqlBlackList.put(Pattern.compile("churn_rule_" + i));
            sqlBlackList.delete(id);
        }
        stop.set(true);
        for (Thread t : threads) {
            t.join(RESPONSIVE_MS);
        }
        if (failure.get() != null) {
            throw new AssertionError("a concurrent reader failed", failure.get());
        }
        Assertions.assertTrue(sqlBlackList.getBlackLists().isEmpty());
    }

    private void assertCompletesWhileUpdateLockIsHeld(Runnable queryPathAction) throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            sqlBlackList.getUpdateLock().lock();
            try {
                locked.countDown();
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                sqlBlackList.getUpdateLock().unlock();
            }
        });
        holder.setDaemon(true);
        holder.start();
        try {
            Assertions.assertTrue(locked.await(RESPONSIVE_MS, TimeUnit.MILLISECONDS),
                    "could not acquire the update lock");

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                try {
                    queryPathAction.run();
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
            reader.setDaemon(true);
            reader.start();

            Assertions.assertTrue(done.await(RESPONSIVE_MS, TimeUnit.MILLISECONDS),
                    "the query path blocked while a mutation held the update lock");
            if (failure.get() != null) {
                throw new AssertionError("the query path failed", failure.get());
            }
        } finally {
            release.countDown();
            holder.join(RESPONSIVE_MS);
        }
    }
}
