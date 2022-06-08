package com.starrocks.load.lock;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class LevelLock implements ReadWriteLock {
    private Long[] pathIds;
    private

    LevelLock(Long[] pathIds) {
        this.pathIds = pathIds;
    }

    @NotNull
    @Override
    public Lock readLock() {
        return null;
    }

    @NotNull
    @Override
    public Lock writeLock() {
        return null;
    }

    /*
    @Override
    public void lock() {
        throw new UnsupportedOperationException("lock is not supported");
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("lock is not supported");
    }

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("lock is not supported");
    }

    @Override
    public void unlock() {

    }

    @NotNull
    @Override
    public Condition newCondition() {
        return null;
    }

     */
}
