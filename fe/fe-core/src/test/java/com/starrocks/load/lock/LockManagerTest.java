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
        LockTarget lockTarget = new LockTarget();
        Assert.assertEquals(null, lockTarget.getPathIds());
        Assert.assertEquals(null, lockTarget.getTargetContext());

        Long[] pathIds = new Long[1];
        pathIds[0] = 10L;

        LockTarget.TargetContext targetContext = new LockTarget.TargetContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget1 = new LockTarget(pathIds, targetContext);
        Lock lock = lockManager.tryLock(lockTarget1, LockMode.SHARED);
        Assert.assertTrue(lock != null);

        LockTarget.TargetContext targetContextEx = new LockTarget.TargetContext(101L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds, targetContextEx);
        Lock lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 == null);

        lockManager.unlock(lock);
        lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 != null);
    }

    @Test
    public void testTable() {
        LockTarget lockTarget = new LockTarget();
        Assert.assertTrue(lockTarget.getPathIds() == null);
        Assert.assertEquals(null, lockTarget.getTargetContext());

        Long[] pathIds = new Long[2];
        pathIds[0] = 10L;
        pathIds[1] = 11L;

        LockTarget.TargetContext targetContext = new LockTarget.TargetContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget1 = new LockTarget(pathIds, targetContext);
        Lock lock = lockManager.tryLock(lockTarget1, LockMode.SHARED);
        Assert.assertTrue(lock != null);
        Assert.assertEquals(LockMode.SHARED, lock.getLockMode());

        LockTarget.TargetContext targetContextEx = new LockTarget.TargetContext(101L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds, targetContextEx);
        Lock lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 == null);

        LockTarget.TargetContext targetContextShared = new LockTarget.TargetContext(102L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget3 = new LockTarget(pathIds, targetContextShared);
        Lock lock3 = lockManager.tryLock(lockTarget3, LockMode.SHARED);
        Assert.assertTrue(lock3 != null);
        Assert.assertEquals(LockMode.SHARED, lock3.getLockMode());

        List<Lock> locks = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            LockTarget.TargetContext targetContextTmp = new LockTarget.TargetContext(1000L + i, LockMode.SHARED,
                    System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
            LockTarget lockTargetTmp = new LockTarget(pathIds, targetContextTmp);
            Lock lockTmp = lockManager.tryLock(lockTargetTmp, LockMode.SHARED);
            Assert.assertTrue(lockTmp != null);
            Assert.assertEquals(LockMode.SHARED, lockTmp.getLockMode());
            locks.add(lockTmp);
        }
        lockManager.unlock(locks);

        List<LockTargetDesc> targetDescs = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            LockTarget.TargetContext targetContextTmp = new LockTarget.TargetContext(1000L + i, LockMode.SHARED,
                    System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
            LockTarget lockTargetTmp = new LockTarget(pathIds, targetContextTmp);
            LockTargetDesc targetDesc = new LockTargetDesc(lockTargetTmp, LockMode.SHARED);
            targetDescs.add(targetDesc);
        }
        List<Lock> batchLocks = lockManager.tryLock(targetDescs);
        Assert.assertTrue(batchLocks != null);
        Assert.assertEquals(10, batchLocks.size());
        lockManager.unlock(batchLocks);

        List<LockTargetDesc> targetDescs2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            Long[] pathIdsTmp = new Long[2];
            pathIdsTmp[0] = 10000L;
            pathIdsTmp[1] = Long.valueOf(10 - i);
            if (i < 5) {
                LockTarget.TargetContext targetContextTmp = new LockTarget.TargetContext(1000L + i, LockMode.SHARED,
                        System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
                LockTarget lockTargetTmp = new LockTarget(pathIdsTmp, targetContextTmp);
                LockTargetDesc targetDesc = new LockTargetDesc(lockTargetTmp, LockMode.SHARED);
                targetDescs2.add(targetDesc);
            } else {
                LockTarget.TargetContext targetContextTmp = new LockTarget.TargetContext(1000L + i, LockMode.EXCLUSIVE,
                        System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
                LockTarget lockTargetTmp = new LockTarget(pathIdsTmp, targetContextTmp);
                LockTargetDesc targetDesc = new LockTargetDesc(lockTargetTmp, LockMode.EXCLUSIVE);
                targetDescs2.add(targetDesc);
            }
        }
        // locks will be sorted
        List<Lock> batchLocks2 = lockManager.tryLock(targetDescs2);
        Assert.assertTrue(batchLocks2 != null);
        Assert.assertEquals(10, batchLocks2.size());
        Assert.assertEquals(LockMode.EXCLUSIVE, batchLocks2.get(0).getLockMode());
        Lock tmpLock = batchLocks2.get(0);
        long tableId = tmpLock.getLockTarget().getPathIds()[1];
        Assert.assertEquals(1L, tableId);
        Assert.assertEquals(LockMode.SHARED, batchLocks2.get(5).getLockMode());
        lockManager.unlock(batchLocks2);

        lockManager.unlock(lock);
        lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 == null);
        lockManager.unlock(lock3);
        lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 != null);

        lock = lockManager.tryLock(lockTarget1, LockMode.SHARED);
        Assert.assertTrue(lock == null);

        lockManager.unlock(lock2);

        lock = lockManager.tryLock(lockTarget1, LockMode.SHARED);
        Assert.assertTrue(lock != null);
    }

    @Test
    public void testPartition() {
        Long[] pathIds = new Long[3];
        pathIds[0] = 10L;
        pathIds[1] = 11L;
        pathIds[2] = 12L;

        LockTarget.TargetContext targetContext = new LockTarget.TargetContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget1 = new LockTarget(pathIds, targetContext);
        Lock lock = lockManager.tryLock(lockTarget1, LockMode.SHARED);
        Assert.assertTrue(lock != null);

        Long[] pathIds2 = new Long[2];
        pathIds2[0] = 10L;
        pathIds2[1] = 11L;
        LockTarget.TargetContext targetContext2 = new LockTarget.TargetContext(100L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget2 = new LockTarget(pathIds2, targetContext2);
        Lock lock2 = lockManager.tryLock(lockTarget2, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock2 == null);

        LockTarget.TargetContext targetContext5 = new LockTarget.TargetContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget5 = new LockTarget(pathIds2, targetContext5);
        Lock lock5 = lockManager.tryLock(lockTarget5, LockMode.SHARED);
        Assert.assertTrue(lock5 != null);

        lockManager.unlock(lock);

        LockTarget.TargetContext targetContext3 = new LockTarget.TargetContext(100L, LockMode.EXCLUSIVE,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget3 = new LockTarget(pathIds, targetContext3);
        Lock lock3 = lockManager.tryLock(lockTarget3, LockMode.EXCLUSIVE);
        Assert.assertTrue(lock3 != null);


        LockTarget.TargetContext targetContext4 = new LockTarget.TargetContext(100L, LockMode.SHARED,
                System.currentTimeMillis(), TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
        LockTarget lockTarget4 = new LockTarget(pathIds2, targetContext4);
        Lock lock4 = lockManager.tryLock(lockTarget4, LockMode.SHARED);
        Assert.assertTrue(lock4 == null);
        lockManager.unlock(lock3);
        lock4 = lockManager.tryLock(lockTarget4, LockMode.SHARED);
        Assert.assertTrue(lock4 != null);
    }
}
