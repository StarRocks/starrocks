// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

// non-reentrant lock manager
public class PartitionLockManager {
    private static final Logger LOG = LogManager.getLogger(PartitionLockManager.class);
    public static enum LockType {
        LOCK_SHARED,
        LOCK_EXCLUSIVE
    };

    public static class LockState {
        public volatile long refCount;
        public LockType lockType;
        public Map<Long, LockState> childLockStates;

        LockState(LockType lockType) {
            this.lockType = lockType;
            this.refCount = 1;
        }
    }

    private ReentrantLock lock;
    private Map<Long, LockState> tableLockStates;

    public PartitionLockManager() {
        lock = new ReentrantLock();
        tableLockStates = Maps.newHashMap();
    }

    @VisibleForTesting
    public Map<Long, LockState> getTableLockStates() {
        return tableLockStates;
    }

    // how to seperate whole table?
    public boolean tryReadLockTable(long tableId) {
        lock.lock();
        try {
            if (tableLockStates.containsKey(tableId)) {
                LockState tableState = tableLockStates.get(tableId);
                if (tableState.lockType == LockType.LOCK_EXCLUSIVE) {
                    return false;
                }
                tableState.refCount++;
            } else {
                LockState tableState = new LockState(LockType.LOCK_SHARED);
                tableLockStates.put(tableId, tableState);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void readUnlockTable(long tableId) {
        lock.lock();
        try {
            if (!tableLockStates.containsKey(tableId)) {
                LOG.warn("invalid lock state for table:{}", tableId);
                return;
            }
            LockState lockState = tableLockStates.get(tableId);
            if (lockState.lockType != LockType.LOCK_SHARED) {
                LOG.warn("invalid lock state for table:{}, lockType:{}", tableId, lockState.lockType);
                return;
            }
            lockState.refCount--;
            if (lockState.refCount <= 0) {
                tableLockStates.remove(lockState);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryReadLockPartitions(long tableId, List<Long> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return tryReadLockTable(tableId);
        }
        lock.lock();
        try {
            if (tableLockStates.containsKey(tableId)) {
                LockState tableState = tableLockStates.get(tableId);
                // check table state
                if (tableState.lockType == LockType.LOCK_EXCLUSIVE) {
                    return false;
                }
                if (tableState.childLockStates != null) {
                    // check every partition lock state
                    // if any partition is LOCK_EXCLUSIVE, return false
                    boolean isPartitionExclusive = partitionIds.stream().anyMatch(
                            pid -> tableState.childLockStates.get(pid) != null &&
                                    tableState.childLockStates.get(pid).lockType == LockType.LOCK_EXCLUSIVE);
                    if (isPartitionExclusive) {
                        return false;
                    }
                    for (long pid : partitionIds) {
                        if (tableState.childLockStates.containsKey(pid)) {
                            LockState partitionState = tableState.childLockStates.get(pid);
                            partitionState.refCount++;
                        } else {
                            LockState partitionState = new LockState(LockType.LOCK_SHARED);
                            tableState.childLockStates.put(pid, partitionState);
                        }
                        tableState.refCount++;
                    }
                } else {
                    tableState.childLockStates = Maps.newHashMap();
                    for (long pid : partitionIds) {
                        LockState partitionState = new LockState(LockType.LOCK_SHARED);
                        tableState.childLockStates.put(pid, partitionState);
                    }
                    tableState.refCount++;
                }
            } else {
                // add new tableState for table id, lockType is LOCK_SHARED
                // initial refCount is 1
                LockState tableState = new LockState(LockType.LOCK_SHARED);
                for (long pid : partitionIds) {
                    LockState partitionState = new LockState(LockType.LOCK_SHARED);
                    tableState.childLockStates.put(pid, partitionState);
                }
                tableLockStates.put(tableId, tableState);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void readUnlockPartitions(long tableId, List<Long> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            readUnlockTable(tableId);
            return;
        }
        lock.lock();
        try {
            if (!tableLockStates.containsKey(tableId)) {
                LOG.warn("invalid lock state for table:{}", tableId);
                return;
            }
            LockState tableLockState = tableLockStates.get(tableId);
            if (tableLockState.lockType != LockType.LOCK_SHARED) {
                LOG.warn("invalid lock state for table:{} lockType:{}, require LOCK_SHARED", tableId, tableLockState.lockType);
                return;
            }
            if (tableLockState.childLockStates == null) {
                LOG.warn("invalid lock state for table:{}, childLockStates is null", tableId);
                return;
            }
            for (long pid : partitionIds) {
                if (!tableLockState.childLockStates.containsKey(pid)) {
                    LOG.warn("invalid lock state for table:{}, do not contain parition:{}", pid);
                    continue;
                }
                LockState partitionLockState = tableLockState.childLockStates.get(pid);
                partitionLockState.refCount--;
                if (partitionLockState.refCount <= 0) {
                    tableLockState.childLockStates.remove(pid);
                }
            }
            tableLockState.refCount--;
            if (tableLockState.refCount <= 0) {
                tableLockStates.remove(tableId);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryWriteLockTable(long tableId) {
        lock.lock();
        try {
            if (tableLockStates.containsKey(tableId)) {
                // if exist any type lock, just return false
                return false;
            }
            LockState tableState = new LockState(LockType.LOCK_EXCLUSIVE);
            tableLockStates.put(tableId, tableState);
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void writeUnlockTable(long tableId) {
        lock.lock();
        try {
            if (!tableLockStates.containsKey(tableId)) {
                LOG.warn("invalid lock state. do not contain table:{}", tableId);
                return;
            }
            LockState tableLockState = tableLockStates.get(tableId);
            if (tableLockState.lockType != LockType.LOCK_EXCLUSIVE) {
                LOG.warn("invalid lock state for table:{} lockType:{}, require LOCK_EXCLUSIVE",
                        tableId, tableLockState.lockType);
                return;
            }
            tableLockStates.remove(tableId);
        } finally {
            lock.unlock();
        }
    }

    public boolean tryWriteLockPartitions(long tableId, List<Long> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return tryWriteLockTable(tableId);
        }
        lock.lock();
        try {
            if (tableLockStates.containsKey(tableId)) {
                LockState tableLockState = tableLockStates.get(tableId);
                if (tableLockState.lockType != LockType.LOCK_SHARED) {
                    return false;
                }
                if (tableLockState.childLockStates != null) {
                    // check every partition lock state
                    // if any partition has been locked, return false
                    boolean isPartitionLocked = partitionIds.stream().anyMatch(
                            pid -> tableLockState.childLockStates.get(pid) != null);
                    if (isPartitionLocked) {
                        return false;
                    }
                    for (long pid : partitionIds) {
                        LockState paritionLockState = new LockState(LockType.LOCK_EXCLUSIVE);
                        tableLockState.childLockStates.put(pid, paritionLockState);
                    }
                    tableLockState.refCount++;
                } else {
                    for (long pid : partitionIds) {
                        LockState paritionLockState = new LockState(LockType.LOCK_EXCLUSIVE);
                        tableLockState.childLockStates.put(pid, paritionLockState);
                    }
                    tableLockState.refCount++;
                }
            } else {
                LockState tableLockState = new LockState(LockType.LOCK_SHARED);
                for (long pid : partitionIds) {
                    LockState partitionLockState = new LockState(LockType.LOCK_EXCLUSIVE);
                    tableLockState.childLockStates.put(pid, partitionLockState);
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void writeUnlockPartitions(long tableId, List<Long> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            writeUnlockTable(tableId);
            return;
        }
        lock.lock();
        try {
            if (!tableLockStates.containsKey(tableId)) {
                LOG.warn("invalid lock state. do not contain table:{}", tableId);
                return;
            }
            LockState tableLockState = tableLockStates.get(tableId);
            if (tableLockState.lockType != LockType.LOCK_SHARED) {
                LOG.warn("invalid lock state for table:{} lockType:{}, require LOCK_SHARED",
                        tableId, tableLockState.lockType);
                return;
            }
            if (tableLockState.childLockStates == null) {
                LOG.warn("invalid lock state for table:{}, childLockStates is null", tableId);
                return;
            }
            for (long pid : partitionIds) {
                if (!tableLockState.childLockStates.containsKey(pid)) {
                    continue;
                }
                LockState partitionLockState = tableLockState.childLockStates.get(pid);
                if (partitionLockState.lockType != LockType.LOCK_EXCLUSIVE) {
                    LOG.warn("invalid lock state for table:{}, partition id:{}, state is:{} ",
                            tableId, pid, partitionLockState.lockType);
                    continue;
                }
                tableLockState.childLockStates.remove(pid);
            }
            tableLockState.refCount--;
            if (tableLockState.refCount <= 0) {
                tableLockStates.remove(tableId);
            }
        } finally {
            lock.unlock();
        }
    }
}
