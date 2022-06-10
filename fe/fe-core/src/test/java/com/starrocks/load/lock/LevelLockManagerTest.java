// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LevelLockManagerTest {
    private LevelLockManager levelLockManager;

    @Before
    public void setUp() {
        levelLockManager = new LevelLockManager();
    }

    @Test
    public void testDatabase() {
        Long[] pathIds = new Long[1];
        pathIds[0] = 10L;

        ReadWriteLevelLock levelLock = new ReadWriteLevelLock(pathIds);
        LevelLock readLock = levelLock.readLock();
        boolean ret = levelLockManager.tryReadLock(readLock);
        Assert.assertTrue(ret);
        Assert.assertEquals(pathIds, levelLock.getPathIds());

        LevelLock writeLock = levelLock.writeLock();
        boolean ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertFalse(ret2);

        levelLockManager.unlockReadLock(readLock);
        ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertTrue(ret2);
    }

    @Test
    public void testTable() {
        Long[] pathIds = new Long[2];
        pathIds[0] = 10L;
        pathIds[1] = 11L;

        ReadWriteLevelLock levelLock = new ReadWriteLevelLock(pathIds);
        LevelLock readLock = levelLock.readLock();
        boolean ret = levelLockManager.tryReadLock(readLock);
        Assert.assertTrue(ret);

        ReadWriteLevelLock levelLock2 = new ReadWriteLevelLock(pathIds);
        LevelLock writeLock = levelLock2.writeLock();
        boolean ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertFalse(ret2);

        ReadWriteLevelLock levelLock3 = new ReadWriteLevelLock(pathIds);
        LevelLock readLock2 = levelLock3.readLock();
        boolean ret3 = levelLockManager.tryReadLock(readLock2);
        Assert.assertTrue(ret3);

        levelLockManager.unlockReadLock(readLock);
        ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertFalse(ret2);
        levelLockManager.unlockReadLock(readLock2);
        ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertTrue(ret2);

        ret = levelLockManager.tryReadLock(readLock);
        Assert.assertFalse(ret);

        levelLockManager.unlockWriteLock(writeLock);

        ret = levelLockManager.tryReadLock(readLock);
        Assert.assertTrue(ret);
    }

    @Test
    public void testPartition() {
        Long[] pathIds = new Long[3];
        pathIds[0] = 10L;
        pathIds[1] = 11L;
        pathIds[2] = 12L;

        ReadWriteLevelLock levelLock = new ReadWriteLevelLock(pathIds);
        LevelLock readLock = levelLock.readLock();
        boolean ret = levelLockManager.tryReadLock(readLock);
        Assert.assertTrue(ret);

        Long[] pathIds2 = new Long[2];
        pathIds2[0] = 10L;
        pathIds2[1] = 11L;
        ReadWriteLevelLock levelLock2 = new ReadWriteLevelLock(pathIds);
        LevelLock writeLock = levelLock2.writeLock();
        boolean ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertFalse(ret2);

        ReadWriteLevelLock levelLock3 = new ReadWriteLevelLock(pathIds);
        LevelLock readLock2 = levelLock3.readLock();
        boolean ret3 = levelLockManager.tryReadLock(readLock2);
        Assert.assertTrue(ret3);

        // unlock one
        levelLockManager.unlockReadLock(readLock);

        ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertFalse(ret2);

        levelLockManager.unlockReadLock(readLock2);

        ret2 = levelLockManager.tryWriteLock(writeLock);
        Assert.assertTrue(ret2);

        ReadWriteLevelLock levelLock4 = new ReadWriteLevelLock(pathIds2);
        LevelLock readLock3 = levelLock4.readLock();
        boolean ret4 = levelLockManager.tryReadLock(readLock3);
        Assert.assertFalse(ret4);

        levelLockManager.unlockWriteLock(writeLock);

        ret4 = levelLockManager.tryReadLock(readLock3);
        Assert.assertTrue(ret4);

        // parent has been read locked
        ReadWriteLevelLock levelLock5 = new ReadWriteLevelLock(pathIds);
        LevelLock writeLock2 = levelLock5.writeLock();
        boolean ret5 = levelLockManager.tryWriteLock(writeLock2);
        Assert.assertFalse(ret5);
    }
}
