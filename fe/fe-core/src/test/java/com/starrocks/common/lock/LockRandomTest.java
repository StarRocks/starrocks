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

import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class LockRandomTest {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        counter = 0;
    }

    static int counter;

    @Test
    public void testOnlyXWithX() throws InterruptedException {
        int threadNums = 32;
        int runTimes = 100;

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threadNums; ++i) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < runTimes; ++j) {
                    Locker locker = new Locker();
                    try {
                        try {
                            locker.lock(1L, LockType.WRITE);
                            counter++;
                        } finally {
                            locker.release(1L, LockType.WRITE);
                        }
                    } catch (IllegalLockStateException ie) {
                        Assert.fail();
                    }
                }
            });

            threadList.add(t);
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).start();
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).join();
        }

        Assert.assertEquals(threadNums * runTimes, counter);
    }

    @Test
    public void testOnlyXWithS() throws InterruptedException {
        int threadNums = 32;
        int runTimes = 100;
        AtomicInteger atomicInteger = new AtomicInteger(0);

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threadNums; ++i) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < runTimes; ++j) {
                    Random random = new Random();
                    Locker locker = new Locker();
                    try {
                        if (random.nextInt(10) % 2 == 0) {
                            try {
                                locker.lock(1L, LockType.WRITE);
                                counter++;
                                atomicInteger.incrementAndGet();
                            } finally {
                                locker.release(1L, LockType.WRITE);
                            }
                        } else {
                            try {
                                locker.lock(1L, LockType.READ);
                                Assert.assertEquals(counter, atomicInteger.get());
                            } finally {
                                locker.release(1L, LockType.READ);
                            }
                        }
                    } catch (IllegalLockStateException ie) {
                        Assert.fail();
                    }
                }
            });

            threadList.add(t);
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).start();
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).join();
        }

        Assert.assertEquals(counter, atomicInteger.get());
    }

    @Test
    public void testOnlyXWithIS() throws InterruptedException {
        int threadNums = 32;
        int runTimes = 100;
        AtomicInteger atomicInteger = new AtomicInteger(0);

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threadNums; ++i) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < runTimes; ++j) {
                    Random random = new Random();
                    Locker locker = new Locker();
                    try {
                        if (random.nextInt(10) % 2 == 0) {
                            try {
                                locker.lock(1L, LockType.WRITE);
                                counter++;
                                atomicInteger.incrementAndGet();
                            } finally {
                                locker.release(1L, LockType.WRITE);
                            }
                        } else {
                            try {
                                locker.lock(1L, LockType.INTENTION_SHARED);
                                Assert.assertEquals(counter, atomicInteger.get());
                            } finally {
                                locker.release(1L, LockType.INTENTION_SHARED);
                            }
                        }
                    } catch (IllegalLockStateException ie) {
                        Assert.fail();
                    }
                }
            });

            threadList.add(t);
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).start();
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).join();
        }

        Assert.assertEquals(counter, atomicInteger.get());
    }

    @Test
    public void testOnlyXWithIX() throws InterruptedException {
        int threadNums = 32;
        int runTimes = 100;
        AtomicInteger atomicInteger = new AtomicInteger(0);

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threadNums; ++i) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < runTimes; ++j) {
                    Random random = new Random();
                    Locker locker = new Locker();
                    try {
                        if (random.nextInt(10) % 2 == 0) {
                            try {
                                locker.lock(1L, LockType.WRITE);
                                counter++;
                                atomicInteger.incrementAndGet();
                            } finally {
                                locker.release(1L, LockType.WRITE);
                            }
                        } else {
                            try {
                                locker.lock(1L, LockType.INTENTION_EXCLUSIVE);
                                Assert.assertEquals(counter, atomicInteger.get());
                            } finally {
                                locker.release(1L, LockType.INTENTION_EXCLUSIVE);
                            }
                        }
                    } catch (IllegalLockStateException ie) {
                        Assert.fail();
                    }
                }
            });

            threadList.add(t);
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).start();
        }

        for (int i = 0; i < threadNums; ++i) {
            threadList.get(i).join();
        }

        Assert.assertEquals(counter, atomicInteger.get());
    }
}
