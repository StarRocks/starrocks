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
package com.starrocks.common.util.concurrent.lock;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LockManager {
    private static final Logger LOG = LogManager.getLogger(LockManager.class);

    private final int lockTablesSize;
    private final Object[] lockTableMutexes;
    private final Map<Long, Lock>[] lockTables;

    public LockManager() {
        lockTablesSize = Config.lock_manager_lock_table_num;
        lockTableMutexes = new Object[lockTablesSize];
        lockTables = new Map[lockTablesSize];
        for (int i = 0; i < lockTablesSize; i++) {
            lockTableMutexes[i] = new Object();
            lockTables[i] = new HashMap<>();
        }
    }

    /**
     * Attempt to acquire a lock of 'lockType' on rid
     *
     * @param rid      The resource id to lock
     * @param locker   The Locker to lock this on behalf of.
     * @param lockType Then lock type requested
     * @param timeout  milliseconds to time out after if lock couldn't be obtained. 0 means block indefinitely.
     * @throws LockTimeoutException when the transaction time limit was exceeded.
     * @throws DeadlockException    when deadlock was detected
     */

    public void lock(long rid, Locker locker, LockType lockType, long timeout)
            throws LockInterruptException, LockTimeoutException, DeadlockException {

        final long startTime;

        synchronized (locker) {
            int lockTableIdx = getLockTableIndex(rid);
            synchronized (lockTableMutexes[lockTableIdx]) {
                Map<Long, Lock> lockTable = lockTables[lockTableIdx];
                Lock lock = lockTable.get(rid);

                if (lock == null) {
                    lock = new LightWeightLock();
                    lockTable.put(rid, lock);
                } else if (lock instanceof LightWeightLock) {
                    List<LockHolder> owners = new ArrayList<>(lock.getOwners());
                    assert !owners.isEmpty();
                    /* Lock is already held by someone else so mutate. */
                    lock = new MultiUserLock(owners.get(0));
                    lockTable.put(rid, lock);
                }

                LockGrantType lockGrantType = lock.lock(locker, lockType);
                if (lockGrantType == LockGrantType.NEW || lockGrantType == LockGrantType.EXISTING) {
                    return;
                }
            }

            startTime = System.currentTimeMillis();
            locker.setWaitingFor(rid, lockType);

            /*
             * Because deadlock detection also requires a significant cost, but at the first moment
             * when a lock cannot be obtained, we cannot determine whether it is because the required
             * lock is being used normally or if a deadlock has occurred.
             * Therefore, based on the configuration parameter `dead_lock_detection_delay_time_ms`
             * is used to control the waiting time before deadlock detection.
             * If a lock is obtained during this period, there is no need to perform deadlock detection.
             * Avoid frequent and unnecessary deadlock detection due to lock contention
             */
            long deadLockDetectionDelayTimeMs = Config.lock_manager_dead_lock_detection_delay_time_ms;
            if (deadLockDetectionDelayTimeMs > 0) {
                if (timeout != 0) {
                    deadLockDetectionDelayTimeMs = Math.min(deadLockDetectionDelayTimeMs, timeRemain(timeout, startTime));
                }

                try {
                    locker.wait(Math.max(1, deadLockDetectionDelayTimeMs));
                } catch (InterruptedException ie) {
                    removeFromWaiterList(rid, locker, lockType);
                    throw new LockInterruptException(ie);
                }

                if (isOwner(rid, locker, lockType)) {
                    locker.clearWaitingFor();
                    return;
                }
            }

            /*
             * If the timeout time is less than dead_lock_detection_delay_time_ms,
             * there is no need to perform subsequent deadlock detection,
             * and it will be processed directly according to the lock timeout.*/
            boolean lockTimeOut = (timeout != 0) && timeRemain(timeout, startTime) <= 0;
            if (lockTimeOut) {
                removeFromWaiterList(rid, locker, lockType);

                /* Failure to acquire lock within the timeout ms*/
                throw new LockTimeoutException("");
            }

            /*
             * After waiting, not acquire lock and entered the waiting period, with deadlock detection enabled
             */
        }

        while (true) {
            Locker victim = null;
            synchronized (locker) {
                while (true) {
                    if (isOwner(rid, locker, lockType)) {
                        break;
                    }

                    victim = checkAndHandleDeadLock(rid, locker, lockType);
                    if (victim != null) {
                        /* deadlock was detected. */
                        break;
                    }

                    try {
                        if (timeout == 0) {
                            locker.wait(0);
                        } else {
                            locker.wait(Math.max(1, timeRemain(timeout, startTime)));
                        }
                    } catch (InterruptedException ie) {
                        removeFromWaiterList(rid, locker, lockType);
                        throw new LockInterruptException(ie);
                    }

                    //locker is wakeup normally and becomes the owner
                    if (isOwner(rid, locker, lockType)) {
                        break;
                    }

                    boolean lockTimeOut = (timeout != 0) && timeRemain(timeout, startTime) <= 0;
                    if (lockTimeOut) {
                        removeFromWaiterList(rid, locker, lockType);

                        /* Failure to acquire lock within the timeout ms*/
                        throw new LockTimeoutException("");
                    }

                    /*
                     * There are two reasons for the loop below.
                     *
                     * 1. When another thread detects a deadlock and notifies this thread,
                     * it will wake up before the timeout interval has expired. We must loop
                     * again to perform deadlock detection. Normally, if the deadlock
                     * detected by the other thread is still present, this locker will be
                     * selected as the victim, and we will throw DeadLockException below.
                     *
                     * 2. spurious wakeup
                     */
                }

                if (victim == null) {
                    assert isOwner(rid, locker, lockType);
                    locker.clearWaitingFor();
                }
            }

            if (victim == null) {
                /* Locker owns the lock and no deadlock was detected. */
                return;
            } else {
                /*
                 * A deadlock is detected and this locker is not the victim.
                 * Notify the victim.
                 */
                boolean currentLockerIsOwner = notifyVictim(victim, locker, rid, lockType, timeout, startTime);
                if (currentLockerIsOwner) {
                    synchronized (locker) {
                        locker.clearWaitingFor();
                    }
                    return;
                }

                /*
                 * After notify the victim, current locker still cannot get the lock and need to wait to be notified again
                 */
            }
        }
    }

    private boolean notifyVictim(Locker targetedVictim, Locker currentLocker, Long rid, LockType lockType,
                                 Long timeout, Long startTime)
            throws LockInterruptException, DeadlockException {
        DeadLockChecker dc = null;
        while (true) {
            boolean lockTimeOut = (timeout != 0) && timeRemain(timeout, startTime) < 0;
            if (lockTimeOut && dc != null) {
                removeFromWaiterList(rid, currentLocker, lockType);
                /* Failure to acquire lock within the timeout ms */
                DeadlockException exception = DeadlockException.makeDeadlockException(dc, currentLocker, false);
                LOG.warn(exception.getMessage());
                throw exception;
            }

            /*
             * Notify the victim and sleep for 1ms to allow the victim to wake up and abort.
             */
            synchronized (targetedVictim) {
                targetedVictim.notify();
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                removeFromWaiterList(rid, currentLocker, lockType);
                throw new LockInterruptException(e);
            }

            /* If currentLocker is the owner, the deadlock was broken. */
            if (isOwner(rid, currentLocker, lockType)) {
                return true;
            } else {
                dc = new DeadLockChecker(currentLocker, rid, lockType);
                if (dc.hasCycle() && dc.chooseTargetedLocker().equals(targetedVictim)) {
                    /*
                     * DeadLock not broker and the victim is the same, Retry
                     */
                    continue;
                }

                /*
                 * DeadLock was broken or victim is different, let the outer caller retry
                 */
                return false;
            }
        }
    }

    public void release(long rid, Locker locker, LockType lockType) {
        Set<Locker> newOwners;

        int lockTableIdx = getLockTableIndex(rid);
        synchronized (lockTableMutexes[lockTableIdx]) {
            Map<Long, Lock> lockTable = lockTables[lockTableIdx];
            Lock lock = lockTable.get(rid);
            if (lock == null) {
                throw new IllegalMonitorStateException("Attempt to unlock lock, not locked by current locker");
            }

            newOwners = lock.release(locker, lockType);

            if (lock.waiterNum() == 0 && lock.ownerNum() == 0) {
                lockTable.remove(rid);
            }
        }

        if (newOwners != null && newOwners.size() > 0) {
            for (Locker notifyLocker : newOwners) {
                synchronized (notifyLocker) {
                    notifyLocker.notify();
                }
            }
        }
    }

    public boolean isOwner(long rid, Locker locker, LockType lockType) {
        int lockTableIndex = getLockTableIndex(rid);
        synchronized (lockTableMutexes[lockTableIndex]) {
            return isOwnerInternal(rid, locker, lockType, lockTableIndex);
        }
    }

    public boolean isOwnerInternal(long rid, Locker locker, LockType lockType, int lockTableIndex) {
        final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
        final Lock lock = lockTable.get(rid);
        return lock != null && lock.isOwner(locker, lockType);
    }

    private int getLockTableIndex(long rid) {
        return (((int) rid) & 0x7fffffff) % lockTablesSize;
    }

    private static long timeRemain(final long timeout, final long startTime) {
        return (timeout - (System.currentTimeMillis() - startTime));
    }

    private Set<LockHolder> cloneOwnersInternal(Long rid, int lockTableIndex) {
        final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(rid);
        if (useLock == null) {
            return null;
        }
        return useLock.cloneOwners();
    }

    private void removeFromWaiterList(long rid, Locker locker, LockType lockType) {
        int lockTableIndex = getLockTableIndex(rid);
        synchronized (lockTableMutexes[lockTableIndex]) {
            Map<Long, Lock> lockTable = lockTables[lockTableIndex];
            Lock lock = lockTable.get(rid);
            lock.removeWaiter(locker, lockType);
        }
    }

    public String dumpLock() {
        StringBuilder sb = new StringBuilder();
        sb.append("LockTable: \n");

        for (int i = 0; i < lockTablesSize; ++i) {
            synchronized (lockTableMutexes[i]) {
                Map<Long, Lock> lockTable = lockTables[i];

                for (Map.Entry<Long, Lock> lockEntry : lockTable.entrySet()) {
                    Long rid = lockEntry.getKey();
                    Lock lock = lockEntry.getValue();

                    sb.append("---- rid: ").append(rid).append(" ----\n");
                    sb.append(lock);
                    sb.append("\n");
                }
            }
        }

        return sb.toString();
    }

    private Locker checkAndHandleDeadLock(Long rid, Locker locker, LockType lockType) throws DeadlockException {
        DeadLockChecker deadLockChecker = new DeadLockChecker(locker, rid, lockType);
        if (deadLockChecker.hasCycle()) {
            if (Config.lock_manager_enable_resolve_deadlock) {
                Locker victim = deadLockChecker.chooseTargetedLocker();
                if (victim != locker) {
                    return victim;
                } else {
                    removeFromWaiterList(rid, locker, lockType);
                    DeadlockException exception =
                            DeadlockException.makeDeadlockException(deadLockChecker, victim, true);
                    LOG.warn(exception.getMessage());
                    throw exception;
                }
            } else {
                String msg = "Deadlock was detected. \n" + deadLockChecker;
                LOG.warn(msg);
                return null;
            }
        }

        return null;
    }

    public class DeadLockChecker {
        private final Locker rootLocker;
        private final Long rid;
        private final LockType rootLockType;

        private final List<CycleNode> cycle = new ArrayList<>();

        public DeadLockChecker(Locker locker, Long rid, LockType lockType) {
            this.rootLocker = locker;
            this.rid = rid;
            this.rootLockType = lockType;
        }

        public Locker chooseTargetedLocker() {
            cycle.sort(CycleNodeComparator.INSTANCE);
            return cycle.get(getTargetedLockerIndex()).getLocker();
        }

        int getTargetedLockerIndex() {
            long sum = 0;
            int nLockers = 0;
            for (final CycleNode cn : cycle) {
                sum += System.identityHashCode(cn.getLock());
                nLockers++;
            }

            return (int) (Math.abs(sum) % nLockers);
        }

        public boolean hasCycle() {
            return hasCycleInternal(rootLocker, rid, rootLockType, null);
        }

        private boolean hasCycleInternal(Locker checkedLocker, Long rid, LockType requestLockType, LockType ownLockType) {
            Lock ridLock;
            Set<LockHolder> ownersForCheckedLock;

            final int lockTableIndex = getLockTableIndex(rid);
            synchronized (lockTableMutexes[lockTableIndex]) {
                if (isOwnerInternal(rid, checkedLocker, requestLockType, lockTableIndex)) {
                    return false;
                }

                final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
                ridLock = lockTable.get(rid);
                ownersForCheckedLock = cloneOwnersInternal(rid, lockTableIndex);
            }

            if (ownersForCheckedLock == null) {
                return false;
            }

            CycleNode node = new CycleNode(checkedLocker, rid, ridLock, requestLockType, ownLockType);
            cycle.add(node);

            for (final LockHolder lockHolder : ownersForCheckedLock) {
                final Locker locker = lockHolder.getLocker();
                final LockType lockHolderOwnLockType = lockHolder.getLockType();
                final Long lockHolderWaitingForRid = locker.getWaitingForRid();
                final LockType lockHolderWaitingForType = locker.getWaitingForType();

                if (locker != checkedLocker) {
                    if (locker.equals(rootLocker)) {
                        cycle.get(0).ownLockType = lockHolderOwnLockType;
                        return true;
                    }
                }

                for (int i = 0; i < cycle.size(); ++i) {
                    if (cycle.get(i).getLocker().equals(locker)) {
                        cycle.subList(0, i).clear();
                        return true;
                    }
                }

                if (lockHolderWaitingForRid != null) {
                    if (hasCycleInternal(locker, lockHolderWaitingForRid, lockHolderWaitingForType, lockHolderOwnLockType)) {
                        return true;
                    }
                }
            }

            cycle.remove(node);
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            CycleNode end = cycle.get(cycle.size() - 1);
            Lock ownLock = end.lock;
            Long lockRID = end.rid;

            for (CycleNode cycleNode : cycle) {
                final Locker locker = cycleNode.getLocker();
                final Lock lock = cycleNode.getLock();
                final Long rid = cycleNode.getRid();
                final LockType requestType = cycleNode.getRequestLockType();
                final LockType ownType = cycleNode.getOwnLockType();

                sb.append("Locker: \"");
                sb.append(locker).append("\" --- owns lock: ");
                sb.append(System.identityHashCode(ownLock)).append("(RID: ").append(lockRID);
                sb.append(", ownedType: ").append(ownType).append("), ");

                sb.append("waits for lock: ");
                sb.append(System.identityHashCode(lock)).append("(RID: ").append(rid);
                sb.append(", requestType: ").append(requestType).append(")");
                sb.append("\n");

                ownLock = lock;
                lockRID = rid;
            }

            return sb.toString();
        }

        private class CycleNode {
            private final Locker locker;
            private final Long rid;
            private final Lock lock;
            private final LockType requestLockType;
            private LockType ownLockType;

            public CycleNode(Locker locker, Long rid, Lock lock, LockType requestLockType, LockType ownLockType) {
                this.locker = locker;
                this.rid = rid;
                this.lock = lock;
                this.requestLockType = requestLockType;
                this.ownLockType = ownLockType;
            }

            private Locker getLocker() {
                return locker;
            }

            private Long getRid() {
                return rid;
            }

            private Lock getLock() {
                return lock;
            }

            private LockType getRequestLockType() {
                return requestLockType;
            }

            private LockType getOwnLockType() {
                return ownLockType;
            }
        }
    }

    static class CycleNodeComparator implements Comparator<DeadLockChecker.CycleNode> {
        static final CycleNodeComparator INSTANCE = new CycleNodeComparator();

        @Override
        public int compare(DeadLockChecker.CycleNode nc1, DeadLockChecker.CycleNode nc2) {

            return (int) (nc1.getLocker().getThreadID() -
                    nc2.getLocker().getThreadID());
        }
    }
}

