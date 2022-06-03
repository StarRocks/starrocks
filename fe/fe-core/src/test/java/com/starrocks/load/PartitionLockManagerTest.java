package com.starrocks.load;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.load.PartitionLockManager.LockType.LOCK_SHARED;

public class PartitionLockManagerTest {
    @Test
    public void testReadLockTable() {
        PartitionLockManager partitionLockManager = new PartitionLockManager();
        for (int i = 0; i < 10; i++) {
            boolean ret = partitionLockManager.tryReadLockTable(100L);
            Assert.assertEquals(true, ret);
            Map<Long, PartitionLockManager.LockState> tableLockStates = partitionLockManager.getTableLockStates();
            Assert.assertEquals(1, tableLockStates.size());
            Assert.assertTrue(tableLockStates.containsKey(100L));
            PartitionLockManager.LockState tableLockState = tableLockStates.get(100L);
            Assert.assertEquals(LOCK_SHARED, tableLockState.lockType);
            Assert.assertEquals(i + 1, tableLockState.refCount);
            Assert.assertEquals(null, tableLockState.childLockStates);
        }

        for (int i = 0; i < 10; i++) {
            partitionLockManager.readUnlockTable(100L);
            Map<Long, PartitionLockManager.LockState> tableLockStates = partitionLockManager.getTableLockStates();
            if (i < 10) {
                Assert.assertEquals(1, tableLockStates.size());
                Assert.assertTrue(tableLockStates.containsKey(100L));
                PartitionLockManager.LockState tableLockState = tableLockStates.get(100L);
                Assert.assertEquals(LOCK_SHARED, tableLockState.lockType);
                Assert.assertEquals(9 - i, tableLockState.refCount);
                Assert.assertEquals(null, tableLockState.childLockStates);
            } else {
                Assert.assertFalse(tableLockStates.containsKey(100L));
                Assert.assertEquals(0, tableLockStates.size());
            }
        }
    }

    @Test
    public void testReadLockPartition() {
        PartitionLockManager partitionLockManager = new PartitionLockManager();
        List<Long> partitionList = Lists.newArrayList(10L, 20L, 30L);
        for (int i = 0; i < 10; i++) {
            boolean ret = partitionLockManager.tryReadLockPartitions(100L, partitionList);
            Assert.assertEquals(true, ret);
            Map<Long, PartitionLockManager.LockState> tableLockStates = partitionLockManager.getTableLockStates();
            Assert.assertEquals(1, tableLockStates.size());
            Assert.assertTrue(tableLockStates.containsKey(100L));
            PartitionLockManager.LockState tableLockState = tableLockStates.get(100L);
            Assert.assertEquals(LOCK_SHARED, tableLockState.lockType);
            Assert.assertEquals(i + 1, tableLockState.refCount);
            Assert.assertNotEquals(null, tableLockState.childLockStates);

            Map<Long, PartitionLockManager.LockState> partitionLockStates = tableLockState.childLockStates;
            for (long pid : partitionList) {
                Assert.assertTrue(partitionLockStates.containsKey(pid));
                LockState
            }
        }
    }
}
