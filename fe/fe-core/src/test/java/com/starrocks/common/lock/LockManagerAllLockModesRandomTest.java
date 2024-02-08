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

package com.starrocks.common.lock;

import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrent test scenario:
 * <p>
 * - Create 10 TestDBResource instances, each with 5 tables.
 * <p>
 * - Each TestDBResource starts a group of threads (16 in total) for concurrent testing,
 * with the following responsibilities:
 * <p>
 *   - 5 threads perform concurrent write operations. Each thread randomly selects a table and executes
 *   1 million updateOneRandomTable operations on that table. Acquiring the intent write lock on the db and
 *   then the write lock on the table is required before updating the table.
 * <p>
 *   - 5 threads perform concurrent write operations. Each thread executes 1 million updateAllTables
 *   operations on the db. Acquiring the write lock on the db is required before executing this action.
 * <p>
 *   - 3 threads perform concurrent read operations. Each thread acquires a read lock on the db,
 *   then randomly selects a table and verifies whether the two counters of that table are equal.
 *   If not, Assert.assertEquals() fails.
 * <p>
 *   - 3 threads perform concurrent read operations. Each thread acquires an intent read lock on the db and
 *   a read lock on the table. It then verifies whether the two counters of that table are equal.
 *   If not, Assert.assertEquals() fails.
 * <p>
 * - Finally, verify that the sum of counter1 for all tables under each db is 2 million.
 * If not, Assert.assertEquals() fails.
 * <p>
 * - Also, verify that the sum of counter2 for all tables under each db is 2 million.
 * If not, Assert.assertEquals() fails.
 * <p>
 * All read threads continue execution until all write threads have completed, and then the test exits.
 */
public class LockManagerAllLockModesRandomTest {
    private Random randomTableGenerator = new Random();
    private static final long TABLE_ID_START = 10000;
    private static final long DB_ID_START = 20000;


    /**
     * We will acquire intensive lock or non-intensive lock on DB resource.
     * When acquiring intensive lock, we will also acquire specific non-intensive lock on the table to update it.
     */
    private class TestDBResource {
        List<TestTableResource> tables = new ArrayList<>();
        protected static final int NUM_TEST_TABLES = 5;
        private final long id;

        public TestDBResource(long id) {
            this.id = id;
            for (int i = 0; i < NUM_TEST_TABLES; i++) {
                tables.add(new TestTableResource(i + TABLE_ID_START));
            }
        }

        public long getId() {
            return id;
        }

        /**
         * Randomly choose a table and update it. (need to protect it by lock)
         */
        public void updateTableByIndexUnsafe(int tableIndex) {
            tables.get(tableIndex).incrTwoCounters();
        }

        public void updateAllTables() {
            for (TestTableResource table : tables) {
                table.incrTwoCounters();
            }
        }

        public TestTableResource getOneRandomTable() {
            int tableIndex = randomTableGenerator.nextInt(NUM_TEST_TABLES);
            return tables.get(tableIndex);
        }

        public TestTableResource getTableByIndex(int index) {
            return tables.get(index);
        }
    }

    private static class TestTableResource {
        private long counter1 = 0;
        private long counter2 = 0;
        private final long id;

        public TestTableResource(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public void incrTwoCounters() {
            counter1++;
            counter2++;
        }

        /**
         * We always increase the two counters together, so we will assert whether the two counters are equal to test
         * the correctness of the lock manager.
         * @return the two counters in a pair
         */
        public Pair<Long, Long> getTwoCounters() {
            return new Pair<>(counter1, counter2);
        }
    }

    @Test
    public void testAllLockModesConcurrent() throws InterruptedException {
        final int NUM_TEST_DBS = 10;
        final int NUM_TEST_THREADS_PER_DB = 16;
        final int NUM_TEST_OPERATIONS = 10000; // 100k
        final long THREAD_STATE_LOG_INTERVAL_MS = 2000; // 2s
        final AtomicBoolean readerStop = new AtomicBoolean(false);

        final List<TestDBResource> dbs = new ArrayList<>();
        for (int i = 0; i < NUM_TEST_DBS; i++) {
            dbs.add(new TestDBResource(DB_ID_START + i));
        }

        final List<Thread> writeThreadList = new ArrayList<>();
        final List<Thread> readThreadList = new ArrayList<>();
        final List<Thread> allThreadList = new ArrayList<>();
        for (int d = 0; d < NUM_TEST_DBS; d++) {
            final int dbIndex = d;
            for (int i = 0; i < NUM_TEST_THREADS_PER_DB; i++) {
                Thread t = null;
                if (i < 3) {
                    // Init read threads type 1: acquire db read lock.
                    t = new Thread(() -> {
                        TestDBResource db = dbs.get(dbIndex);
                        Locker locker = null;
                        long start = System.currentTimeMillis();
                        long checked = 0;
                        while (!readerStop.get()) {
                            try {
                                locker = new Locker();
                                locker.lock(db.getId(), LockType.READ);
                                Pair<Long, Long> result = db.getOneRandomTable().getTwoCounters();
                                Assert.assertEquals(result.first, result.second);
                            } catch (IllegalLockStateException e) {
                                Assert.fail();
                            } finally {
                                assert locker != null;
                                locker.release(db.getId(), LockType.READ);
                            }
                            checked++;
                            if (System.currentTimeMillis() - start > THREAD_STATE_LOG_INTERVAL_MS) {
                                System.out.println("db --- " + db.getId() +
                                        ", read thread type 1, checked --- " + checked);
                                start = System.currentTimeMillis();
                            }
                        }
                    });
                    readThreadList.add(t);
                } else if (i < 6) {
                    // Init read threads type 2: acquire db intensive read lock and table read lock.
                    t = new Thread(() -> {
                        TestDBResource db = dbs.get(dbIndex);
                        Locker locker = null;
                        Random random = new Random();
                        long start = System.currentTimeMillis();
                        long checked = 0;
                        while (!readerStop.get()) {
                            int tableIndex = random.nextInt(TestDBResource.NUM_TEST_TABLES);
                            try {
                                locker = new Locker();
                                locker.lock(db.getId(), LockType.INTENTION_SHARED);
                                locker.lock(db.getTableByIndex(tableIndex).getId(), LockType.READ);
                                Pair<Long, Long> result = db.getOneRandomTable().getTwoCounters();
                                Assert.assertEquals(result.first, result.second);
                            } catch (IllegalLockStateException e) {
                                Assert.fail();
                            } finally {
                                assert locker != null;
                                locker.release(db.getTableByIndex(tableIndex).getId(), LockType.READ);
                                locker.release(db.getId(), LockType.INTENTION_SHARED);
                            }
                            checked++;
                            if (System.currentTimeMillis() - start > THREAD_STATE_LOG_INTERVAL_MS) {
                                System.out.println("db --- " + db.getId() +
                                        ", read thread type 2, checked --- " + checked);
                                start = System.currentTimeMillis();
                            }
                        }
                    });
                    readThreadList.add(t);
                } else if (i < 11) {
                    // Init write threads type 1: acquire db intensive write lock and table write lock.
                    t = new Thread(() -> {
                        TestDBResource db = dbs.get(dbIndex);
                        Locker locker = null;
                        Random random = new Random();
                        long start = System.currentTimeMillis();
                        long checked = 0;
                        for (int count = 0; count < NUM_TEST_OPERATIONS; count++) {
                            int tableIndex = random.nextInt(TestDBResource.NUM_TEST_TABLES);
                            try {
                                locker = new Locker();
                                locker.lock(db.getId(), LockType.INTENTION_EXCLUSIVE);
                                locker.lock(db.getTableByIndex(tableIndex).getId(), LockType.WRITE);
                                db.updateTableByIndexUnsafe(tableIndex);
                            } catch (IllegalLockStateException e) {
                                Assert.fail();
                            } finally {
                                assert locker != null;
                                locker.release(db.getTableByIndex(tableIndex).getId(), LockType.WRITE);
                                locker.release(db.getId(), LockType.INTENTION_EXCLUSIVE);
                            }
                            checked++;
                            if (System.currentTimeMillis() - start > THREAD_STATE_LOG_INTERVAL_MS) {
                                System.out.println("db --- " + db.getId() +
                                        ", write thread type 1, updated --- " + checked);
                                start = System.currentTimeMillis();
                            }
                        }
                    });
                    writeThreadList.add(t);
                } else {
                    // Init write threads type 2: acquire db write lock.
                    t = new Thread(() -> {
                        TestDBResource db = dbs.get(dbIndex);
                        Locker locker = null;
                        long start = System.currentTimeMillis();
                        long checked = 0;
                        for (int count = 0; count < NUM_TEST_OPERATIONS; count++) {
                            try {
                                locker = new Locker();
                                locker.lock(db.getId(), LockType.WRITE);
                                db.updateAllTables();
                            } catch (IllegalLockStateException e) {
                                Assert.fail();
                            } finally {
                                assert locker != null;
                                locker.release(db.getId(), LockType.WRITE);
                            }
                            checked++;
                            if (System.currentTimeMillis() - start > THREAD_STATE_LOG_INTERVAL_MS) {
                                System.out.println("db --- " + db.getId() +
                                        ", write thread type 2, updated --- " + checked);
                                start = System.currentTimeMillis();
                            }
                        }
                    });
                    writeThreadList.add(t);
                }
                allThreadList.add(t);
                Thread.sleep(10);
            } // end for single db
        } // enf for all dbs

        // Start all threads.
        for (Thread t : allThreadList) {
            t.start();
        }

        // Wait for write threads end.
        for (Thread t : writeThreadList) {
            t.join();
        }
        System.out.println("All write threads end.");

        readerStop.set(true);
        // Wait for read threads end.
        for (Thread t : readThreadList) {
            t.join();
        }
        System.out.println("All read threads end.");


        // Verify the correctness of the lock manager.
        for (TestDBResource db : dbs) {
            long counter1Sum = 0;
            long counter2Sum = 0;
            for (TestTableResource table : db.tables) {
                Pair<Long, Long> result = table.getTwoCounters();
                Assert.assertEquals(result.first, result.second);
                counter1Sum += result.first;
                counter2Sum += result.second;
            }
            Assert.assertEquals(30 * NUM_TEST_OPERATIONS, counter1Sum);
            Assert.assertEquals(30 * NUM_TEST_OPERATIONS, counter2Sum);
        }

    }
}
