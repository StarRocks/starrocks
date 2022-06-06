// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.google.common.collect.Lists;
import com.starrocks.transaction.TransactionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LockManagerTest {
    private LockManager lockManager;

    @Before
    public void setUp() {
        lockManager = new LockManager();
    }

    @Test
    public void testDatabase() {
        Long[] pathIds = new Long[1];
        pathIds[0] = 10L;

        LockContext lockContext = new LockContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget1 = new LockTarget(pathIds, lockContext);
        Lock lock = lockManager.tryLock(lockTarget1);
        Assert.assertTrue(lock != null);
        Assert.assertEquals(pathIds, lock.getPathIds());
        Assert.assertEquals(100L, lock.getLockContextId());
        Assert.assertEquals(LockMode.SHARED, lock.getLockMode());

        LockContext lockContextEx = new LockContext(101L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds, lockContextEx);
        Lock lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 == null);

        lockManager.unlock(lock);
        lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 != null);
        Assert.assertEquals(pathIds, lock2.getPathIds());
        Assert.assertEquals(101L, lock2.getLockContextId());
        Assert.assertEquals(LockMode.EXCLUSIVE, lock2.getLockMode());
    }

    @Test
    public void testTable() {
        Long[] pathIds = new Long[2];
        pathIds[0] = 10L;
        pathIds[1] = 11L;

        LockContext lockContext = new LockContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget1 = new LockTarget(pathIds, lockContext);
        Lock lock = lockManager.tryLock(lockTarget1);
        Assert.assertTrue(lock != null);
        Assert.assertEquals(LockMode.SHARED, lock.getLockMode());

        LockContext lockContextEx = new LockContext(101L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds, lockContextEx);
        Lock lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 == null);

        LockContext lockContextShared = new LockContext(102L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget3 = new LockTarget(pathIds, lockContextShared);
        Lock lock3 = lockManager.tryLock(lockTarget3);
        Assert.assertTrue(lock3 != null);
        Assert.assertEquals(LockMode.SHARED, lock3.getLockMode());

        List<Lock> locks = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            LockContext lockContextTmp = new LockContext(1000L + i, LockMode.SHARED,
                    System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
            LockTarget lockTargetTmp = new LockTarget(pathIds, lockContextTmp);
            Lock lockTmp = lockManager.tryLock(lockTargetTmp);
            Assert.assertTrue(lockTmp != null);
            Assert.assertEquals(LockMode.SHARED, lockTmp.getLockMode());
            locks.add(lockTmp);
        }
        lockManager.unlock(locks);

        List<LockTarget> targets = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            LockContext lockContextTmp = new LockContext(1000L + i, LockMode.SHARED,
                    System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
            LockTarget lockTargetTmp = new LockTarget(pathIds, lockContextTmp);
            targets.add(lockTargetTmp);
        }
        List<Lock> batchLocks = lockManager.tryLock(targets);
        Assert.assertTrue(batchLocks != null);
        Assert.assertEquals(10, batchLocks.size());
        lockManager.unlock(batchLocks);

        List<LockTarget> target2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            Long[] pathIdsTmp = new Long[2];
            pathIdsTmp[0] = 10000L;
            pathIdsTmp[1] = Long.valueOf(10 - i);
            if (i < 5) {
                LockContext lockContextTmp = new LockContext(1000L + i, LockMode.SHARED,
                        System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
                LockTarget lockTargetTmp = new LockTarget(pathIdsTmp, lockContextTmp);
                target2.add(lockTargetTmp);
            } else {
                LockContext lockContextTmp = new LockContext(1000L + i, LockMode.EXCLUSIVE,
                        System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
                LockTarget lockTargetTmp = new LockTarget(pathIdsTmp, lockContextTmp);
                target2.add(lockTargetTmp);
            }
        }
        // locks will be sorted
        List<Lock> batchLocks2 = lockManager.tryLock(target2);
        Assert.assertTrue(batchLocks2 != null);
        Assert.assertEquals(10, batchLocks2.size());
        Assert.assertEquals(LockMode.EXCLUSIVE, batchLocks2.get(0).getLockMode());
        Lock tmpLock = batchLocks2.get(0);
        long tableId = tmpLock.getPathIds()[1];
        Assert.assertEquals(1L, tableId);
        Assert.assertEquals(LockMode.SHARED, batchLocks2.get(5).getLockMode());
        lockManager.unlock(batchLocks2);

        lockManager.unlock(lock);
        lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 == null);
        lockManager.unlock(lock3);
        lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 != null);

        lock = lockManager.tryLock(lockTarget1);
        Assert.assertTrue(lock == null);

        lockManager.unlock(lock2);

        lock = lockManager.tryLock(lockTarget1);
        Assert.assertTrue(lock != null);
    }

    @Test
    public void testPartition() {
        Long[] pathIds = new Long[3];
        pathIds[0] = 10L;
        pathIds[1] = 11L;
        pathIds[2] = 12L;

        LockContext lockContext = new LockContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget1 = new LockTarget(pathIds, lockContext);
        Lock lock = lockManager.tryLock(lockTarget1);
        Assert.assertTrue(lock != null);

        Long[] pathIds2 = new Long[2];
        pathIds2[0] = 10L;
        pathIds2[1] = 11L;
        LockContext lockContext2 = new LockContext(101L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds2, lockContext2);
        Lock lock2 = lockManager.tryLock(lockTarget2);
        Assert.assertTrue(lock2 == null);

        LockContext lockContext5 = new LockContext(102L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget5 = new LockTarget(pathIds2, lockContext5);
        Lock lock5 = lockManager.tryLock(lockTarget5);
        Assert.assertTrue(lock5 != null);

        lockManager.unlock(lock);

        LockContext lockContext3 = new LockContext(103L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget3 = new LockTarget(pathIds, lockContext3);
        // parent has locks, the request to child for exclusive lock will fail.
        Lock lock3 = lockManager.tryLock(lockTarget3);
        Assert.assertTrue(lock3 == null);

        lockManager.unlock(lock5);
        lock3 = lockManager.tryLock(lockTarget3);
        Assert.assertTrue(lock3 != null);

        LockContext lockContext4 = new LockContext(104L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_STREAMING);
        LockTarget lockTarget4 = new LockTarget(pathIds2, lockContext4);
        // the child has been exclusive locked.
        Lock lock4 = lockManager.tryLock(lockTarget4);
        Assert.assertTrue(lock4 == null);
        lockManager.unlock(lock3);
        lock4 = lockManager.tryLock(lockTarget4);
        Assert.assertTrue(lock4 != null);
    }
}
