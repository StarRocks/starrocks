// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

// non-reentrant lock manager
public class LockManager {
    private static final Logger LOG = LogManager.getLogger(LockManager.class);
    public static enum LockType {
        LOCK_SHARED,
        LOCK_EXCLUSIVE
    };

    public static class PartitionEntity {
        // if partition id is equal to table id, it means the lock is for table
        public long id;
        public volatile long refCount;

        PartitionEntity(long id) {
            this.id = id;
            this.refCount = 1;
        }
    }

    private ReentrantLock lock;
    // tableId -> LockType -> partitionIds
    private Map<Long, Map<LockType, Set<PartitionEntity>>> lockTables;

    public LockManager() {
        lock = new ReentrantLock();
        lockTables = Maps.newHashMap();
    }

    // how to seperate whole table?
    public boolean tryReadLockTable(long tableId) {
        return tryReadLockPartitions(tableId, Lists.newArrayList(tableId));
    }

    public void readUnlockTable(long tableId) {
        readUnlockPartitions(tableId, Lists.newArrayList(tableId));
    }

    public boolean tryReadLockPartitions(long tableId, List<Long> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return tryReadLockTable(tableId);
        }
        lock.lock();
        try {
            if (lockTables.containsKey(tableId)) {
                Map<LockType, Set<PartitionEntity>> typeLocks = lockTables.get(tableId);
                if (typeLocks.containsKey(LockType.LOCK_EXCLUSIVE)) {
                    Set<PartitionEntity> exclusiveSet = typeLocks.get(LockType.LOCK_SHARED);
                    boolean isTableWriteLock = exclusiveSet.stream().anyMatch(partitionEntity -> partitionEntity.id == tableId);
                    if (isTableWriteLock) {
                        return false;
                    }
                    for (long pid : partitionIds) {
                        if (exclusiveSet.stream().anyMatch(partitionEntity -> partitionEntity.id == pid)) {
                            return false;
                        }
                    }
                }
                Set<PartitionEntity> readPartitions = typeLocks.getOrDefault(LockType.LOCK_SHARED, Sets.newHashSet());
                for (long pid : partitionIds) {
                    Optional<PartitionEntity> optional =
                            readPartitions.stream().filter(partitionEntity -> partitionEntity.id == pid).findFirst();
                    if (optional.isPresent()) {
                        PartitionEntity entity = optional.get();
                        entity.refCount++;
                    } else {
                        PartitionEntity entity = new PartitionEntity(tableId);
                        readPartitions.add(entity);
                    }
                }
                typeLocks.put(LockType.LOCK_SHARED, readPartitions);
                lockTables.put(tableId, typeLocks);
            } else {
                Map<LockType, Set<PartitionEntity>> typeLocks = Maps.newHashMap();
                Set<PartitionEntity> typeSet = Sets.newHashSet();
                for (long pid : partitionIds) {
                    typeSet.add(new PartitionEntity(pid));
                }
                typeLocks.put(LockType.LOCK_SHARED, typeSet);
                lockTables.put(tableId, typeLocks);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void readUnlockPartitions(long tableId, List<Long> partitionIds) {
        lock.lock();
        try {
            if (!lockTables.containsKey(tableId)) {
                LOG.warn("invalid lock state. not contain:{}", tableId);
                return;
            }
            Map<LockType, Set<PartitionEntity>> typeLocks = lockTables.get(tableId);
            Set<PartitionEntity> typeSet = typeLocks.get(LockType.LOCK_SHARED);
            if (typeSet == null || typeSet.isEmpty()) {
                LOG.warn("invalid lock state. no LockType.LOCK_SHARED");
                return;
            }
            for (long pid : partitionIds) {
                Optional<PartitionEntity> optional =
                        typeSet.stream().filter(partitionEntity -> partitionEntity.id == pid).findFirst();
                if (!optional.isPresent()) {
                    LOG.warn("invalid lock state. no partitions for table:{}", tableId);
                    continue;
                }
                PartitionEntity partitionEntity = optional.get();
                partitionEntity.refCount--;
                if (partitionEntity.refCount < 0) {
                    LOG.warn("invalid lock state. partition ref count:{}, tableId:{}", partitionEntity.refCount, tableId);
                }
                if (partitionEntity.refCount <= 0) {
                    // the last refrence, remove from the set
                    typeLocks.remove(partitionEntity);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryWriteLockTable(long tableId) {
        lock.lock();
        try {
            if (lockTables.containsKey(tableId)) {
                return false;
            }
            Map<LockType, Set<PartitionEntity>> typeLocks = Maps.newHashMap();
            Set<PartitionEntity> typeSet = Sets.newHashSet(new PartitionEntity(tableId));
            typeLocks.put(LockType.LOCK_EXCLUSIVE, typeSet);
            lockTables.put(tableId, typeLocks);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void writeUnlockTable(long tableId) {
        lock.lock();
        try {
            if (!lockTables.containsKey(tableId)) {
                throw new RuntimeException("invalid lock state. do not contain table:" + tableId);
            }
            Map<LockType, Set<PartitionEntity>> typeLocks = lockTables.get(tableId);
            Preconditions.checkState(typeLocks.containsKey(LockType.LOCK_EXCLUSIVE));
            Set<PartitionEntity> typeSet = typeLocks.get(LockType.LOCK_EXCLUSIVE);
            Preconditions.checkState(typeSet.size() == 1);
            PartitionEntity partitionEntity = typeSet.stream().iterator().next();
            Preconditions.checkState(partitionEntity.id == tableId);
            Preconditions.checkState(partitionEntity.refCount == 1);
            lockTables.remove(tableId);
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
            if (lockTables.containsKey(tableId)) {
                Map<LockType, Set<PartitionEntity>> typeLocks = lockTables.get(tableId);
                if (typeLocks.containsKey(LockType.LOCK_SHARED)) {
                    Set<PartitionEntity> sharedPartitionSet = typeLocks.get(LockType.LOCK_SHARED);
                    boolean isTableReadLock = sharedPartitionSet.stream().anyMatch(partitionEntity -> partitionEntity.id == tableId);
                    if (isTableReadLock) {
                        return false;
                    }
                    boolean contained = sharedPartitionSet.stream().anyMatch(partitionEntity -> partitionIds.contains(partitionEntity.id));
                    if (contained) {
                        return false;
                    }
                }
                if (typeLocks.containsKey(LockType.LOCK_EXCLUSIVE)) {
                    Set<PartitionEntity> exclusivePartitionSet = typeLocks.get(LockType.LOCK_EXCLUSIVE);
                    boolean isTableWriteLock = exclusivePartitionSet.stream().anyMatch(partitionEntity -> partitionEntity.id == tableId);
                    if (isTableWriteLock) {
                        return false;
                    }
                    boolean contained = exclusivePartitionSet.stream().anyMatch(partitionEntity -> partitionIds.contains(partitionEntity.id));
                    if (contained) {
                        return false;
                    }
                }
                Set<PartitionEntity> exclusiveSet = typeLocks.getOrDefault(LockType.LOCK_EXCLUSIVE, Sets.newHashSet());
                for (long pid : partitionIds) {
                    exclusiveSet.add(new PartitionEntity(pid));
                }
                typeLocks.put(LockType.LOCK_EXCLUSIVE, exclusiveSet);
                lockTables.put(tableId, typeLocks);
            } else {
                Map<LockType, Set<PartitionEntity>> typeLocks = Maps.newHashMap();
                Set<PartitionEntity> typeSet = Sets.newHashSet();
                for (long pid : partitionIds) {
                    typeSet.add(new PartitionEntity(pid));
                }
                typeLocks.put(LockType.LOCK_EXCLUSIVE, typeSet);
                lockTables.put(tableId, typeLocks);
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
            if (!lockTables.containsKey(tableId)) {
                throw new RuntimeException("invalid lock state. do not contain table:" + tableId);
            }
            Map<LockType, Set<PartitionEntity>> typeLocks = lockTables.get(tableId);
            Set<PartitionEntity> exclusiveSet = typeLocks.get(LockType.LOCK_EXCLUSIVE);
            for (long pid : partitionIds) {
                Optional<PartitionEntity> optional =
                        exclusiveSet.stream().filter(partitionEntity -> partitionEntity.id == pid).findFirst();
                if (!optional.isPresent()) {
                    throw new RuntimeException("invalid lock state. no partition:" + pid + " for table:" + tableId);
                }
                exclusiveSet.remove(optional.get());
            }
        } finally {
            lock.unlock();
        }
    }
}
