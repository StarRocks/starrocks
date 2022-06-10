// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.starrocks.server.GlobalStateMgr;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;

public class ReadWriteLevelLock implements ReadWriteLock {
    private Long[] pathIds;
    private ReadLock readLock;
    private WriteLock writeLock;

    ReadWriteLevelLock(Long[] pathIds) {
        this.pathIds = pathIds;
        this.readLock = new ReadLock(this);
        this.writeLock = new WriteLock(this);
    }

    public Long[] getPathIds() {
        return pathIds;
    }

    @NotNull
    @Override
    public LevelLock readLock() {
        return readLock;
    }

    @NotNull
    @Override
    public LevelLock writeLock() {
        return writeLock;
    }

    public static class ReadLock extends LevelLock {
        public ReadLock(ReadWriteLevelLock levelLock) {
            super(levelLock.pathIds);
        }

        @Override
        public void lock() {
            GlobalStateMgr.getCurrentState().getLevelLockManager().readLock(this);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("lockInterruptibly is not supported");
        }

        @Override
        public boolean tryLock() {
            return GlobalStateMgr.getCurrentState().getLevelLockManager().tryReadLock(this);
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException("tryLock by timeout is not supported");
        }

        @Override
        public void unlock() {
            GlobalStateMgr.getCurrentState().getLevelLockManager().unlockReadLock(this);
        }

        @NotNull
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("newCondition is not supported");
        }
    }

    public static class WriteLock extends LevelLock {

        WriteLock(ReadWriteLevelLock levelLock) {
            super(levelLock.pathIds);
        }

        @Override
        public void lock() {
            GlobalStateMgr.getCurrentState().getLevelLockManager().writeLock(this);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("lockInterruptibly is not supported");
        }

        @Override
        public boolean tryLock() {
            return GlobalStateMgr.getCurrentState().getLevelLockManager().tryWriteLock(this);
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException("tryLock by timeout is not supported");
        }

        @Override
        public void unlock() {
            GlobalStateMgr.getCurrentState().getLevelLockManager().unlockWriteLock(this);
        }

        @NotNull
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("newCondition is not supported");
        }
    }
}
