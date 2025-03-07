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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MultiUserLock extends Lock {
    private static final Logger LOG = LogManager.getLogger(MultiUserLock.class);
    /*
     * The owner of the current Lock. For efficiency reasons, the first owner is stored separately.
     * If Locker successfully obtains the lock, it will be added to the owner.
     **/
    protected LockHolder firstOwner;
    private Set<LockHolder> otherOwners;

    /*
     * The waiter of the current Lock. For efficiency reasons, the first waiter is stored separately.
     * When a lock conflict occurs and the lock cannot be obtained immediately,
     * the Locker will be added to the waiter to wait
     *
     * Using the fair lock strategy, when release occurs, wake up in order from the waiter list
     **/
    protected LockHolder firstWaiter;
    private List<LockHolder> otherWaiters;

    public MultiUserLock(LockHolder lockHolder) {
        this.firstOwner = lockHolder;
    }

    @Override
    public LockGrantType lock(Locker locker, LockType lockType) throws LockException {
        LockHolder lockHolderRequest = new LockHolder(locker, lockType);
        LockGrantType lockGrantType = tryLock(lockHolderRequest, waiterNum() == 0);
        if (lockGrantType == LockGrantType.NEW) {
            addOwner(lockHolderRequest);
        } else if (lockGrantType == LockGrantType.WAIT) {
            addWaiterToEnd(lockHolderRequest);
        }

        return lockGrantType;
    }

    /**
     * @param noWaiters indicates whether there are other waiters. This will determine whether the lock
     *                  can be directly acquired. If there are other waiters, the current locker cannot jump in
     *                  line to acquire the lock first. A special scenario is to notify waiters in the
     *                  existing wait list during release. At this time, the wait list needs to be ignored and
     *                  as many waiters as possible need to be awakened.
     * @return LockGrantType.NEW means that the lock ownership can be obtained.
     * LockGrantType.EXISTING means that the current lock already exists and needs to be re-entered.
     * LockGrantType.WAIT means that there is a lock conflict with the current owner and it is necessary to wait.
     */
    private LockGrantType tryLock(LockHolder lockHolderRequest, boolean noWaiters) throws LockException {
        if (ownerNum() == 0) {
            return LockGrantType.NEW;
        }

        LockHolder lockOwner = null;
        Iterator<LockHolder> ownerIterator = null;
        if (otherOwners != null) {
            ownerIterator = otherOwners.iterator();
        }

        if (firstOwner != null) {
            lockOwner = firstOwner;
        } else {
            if (ownerIterator != null && ownerIterator.hasNext()) {
                lockOwner = ownerIterator.next();
            }
        }

        boolean hasConflicts = false;
        boolean hasSameLockerWithDifferentLockType = false;
        while (lockOwner != null) {
            /*
             * If there is a Locker of the same Lock Type, directly increase the reference count and return.
             * If the types are different, need to continue traversing to determine
             * whether there are other Lockers with the same LockType.
             */
            if (lockHolderRequest.getLocker().equals(lockOwner.getLocker())) {
                LockType lockOwnerLockType = lockOwner.getLockType();
                LockType lockRequestLockType = lockHolderRequest.getLockType();

                if (lockRequestLockType.equals(lockOwnerLockType)) {
                    lockOwner.increaseRefCount();
                    return LockGrantType.EXISTING;
                } else {
                    /*
                     * This does not conform to the use of hierarchical locks.
                     * The outer layer has already obtained the intention lock,
                     * and the inner layer code should not apply for read-write locks.
                     */

                    if (lockOwnerLockType.isIntentionLock() && !lockRequestLockType.isIntentionLock()) {
                        throw new NotSupportLockException("Can't request Database " + lockRequestLockType + " Lock "
                                + lockHolderRequest.getLocker()
                                + " in the scope of Database " + lockOwnerLockType
                                + " Lock " + lockOwner.getLocker());
                    }

                    /*
                     * The same Locker can upgrade or degrade locks when it requests different types of locks
                     *
                     * If you acquire an exclusive lock first and then request a shared lock,
                     * you can successfully acquire the lock. This scenario is generally called "lock downgrade",
                     * but this lock does not actually reduce the original write lock directly to a read lock.
                     * In fact, it is still two independent read and write locks, and the two locks still need
                     * to be released independently. The actual scenario is that before releasing the write lock,
                     * acquire the read lock first, so that there is no gap time to release the lock.
                     */
                    hasSameLockerWithDifferentLockType = true;
                }
            } else {
                if (lockOwner.isConflict(lockHolderRequest)) {
                    hasConflicts = true;
                }
            }

            if (ownerIterator != null && ownerIterator.hasNext()) {
                lockOwner = ownerIterator.next();
            } else {
                break;
            }
        }

        if (!hasConflicts && (hasSameLockerWithDifferentLockType || noWaiters)) {
            return LockGrantType.NEW;
        } else {
            return LockGrantType.WAIT;
        }
    }

    @Override
    public Set<Locker> release(Locker locker, LockType lockType) throws LockException {
        boolean hasOwner = false;
        boolean reentrantLock = false;
        LockHolder lockHolder = new LockHolder(locker, lockType);

        if (firstOwner != null && firstOwner.equals(lockHolder)) {
            hasOwner = true;
            firstOwner.decreaseRefCount();
            if (firstOwner.getRefCount() > 0) {
                reentrantLock = true;
            }

            if (firstOwner.getRefCount() == 0) {
                firstOwner = null;
            }
        } else if (otherOwners != null) {
            Iterator<LockHolder> iter = otherOwners.iterator();
            while (iter.hasNext()) {
                LockHolder o = iter.next();
                if (o.equals(lockHolder)) {
                    hasOwner = true;
                    o.decreaseRefCount();

                    if (o.getRefCount() > 0) {
                        reentrantLock = true;
                    }

                    if (o.getRefCount() == 0) {
                        iter.remove();
                    }
                }
            }
        }

        if (!hasOwner) {
            throw new IllegalMonitorStateException("Attempt to unlock lock, not locked by current locker");
        }

        if (reentrantLock) {
            return null;
        }

        Set<Locker> lockersToNotify = new HashSet<>();

        if (waiterNum() == 0) {
            return lockersToNotify;
        }

        boolean isFirstWaiter = false;
        LockHolder lockWaiter = null;
        Iterator<LockHolder> lockWaiterIterator = null;

        if (otherWaiters != null) {
            lockWaiterIterator = otherWaiters.iterator();
        }

        if (firstWaiter != null) {
            lockWaiter = firstWaiter;
            isFirstWaiter = true;
        } else {
            if (lockWaiterIterator != null && lockWaiterIterator.hasNext()) {
                lockWaiter = lockWaiterIterator.next();
            }
        }

        while (lockWaiter != null) {
            LockGrantType lockGrantType = tryLock(lockWaiter, true);

            if (lockGrantType == LockGrantType.NEW
                    || lockGrantType == LockGrantType.EXISTING) {
                if (isFirstWaiter) {
                    firstWaiter = null;
                } else {
                    lockWaiterIterator.remove();
                }

                lockersToNotify.add(lockWaiter.getLocker());
                if (lockGrantType == LockGrantType.NEW) {
                    addOwner(lockWaiter);
                }
            } else {
                assert lockGrantType == LockGrantType.WAIT;
                /* Stop on first waiter that cannot be an owner. */
                break;
            }

            if (lockWaiterIterator != null && lockWaiterIterator.hasNext()) {
                lockWaiter = lockWaiterIterator.next();
                isFirstWaiter = false;
            } else {
                break;
            }
        }

        return lockersToNotify;
    }

    @Override
    public boolean isOwner(Locker locker, LockType lockType) {
        LockHolder lockHolder = new LockHolder(locker, lockType);

        if (firstOwner != null && firstOwner.equals(lockHolder)) {
            return firstOwner.getLockType() == lockType;
        }

        if (otherOwners != null) {
            for (LockHolder owner : otherOwners) {
                if (owner.equals(lockHolder)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public int ownerNum() {
        int count = 0;
        if (firstOwner != null) {
            count++;
        }

        if (otherOwners != null) {
            count += otherOwners.size();
        }
        return count;
    }

    private void addOwner(LockHolder lockHolder) {
        if (firstOwner == null) {
            firstOwner = lockHolder;
        } else {
            if (otherOwners == null) {
                otherOwners = new HashSet<>();
            }
            otherOwners.add(lockHolder);
        }

        lockHolder.setLockAcquireTimeMs(System.currentTimeMillis());
    }

    @Override
    public int waiterNum() {
        int count = 0;
        if (firstWaiter != null) {
            count++;
        }

        if (otherWaiters != null) {
            count += otherWaiters.size();
        }

        return count;
    }

    @Override
    public Set<LockHolder> getOwners() {
        Set<LockHolder> owners = new HashSet<>();
        if (firstOwner != null) {
            owners.add(firstOwner);
        }

        if (otherOwners != null) {
            owners.addAll(otherOwners);
        }

        return owners;
    }

    @Override
    public Set<LockHolder> cloneOwners() {
        Set<LockHolder> owners = new HashSet<>();
        if (firstOwner != null) {
            owners.add(firstOwner.clone());
        }

        if (otherOwners != null) {
            for (LockHolder lockHolder : otherOwners) {
                owners.add(lockHolder.clone());
            }
        }

        return owners;
    }

    @Override
    public void removeWaiter(Locker locker, LockType lockType) {
        LockHolder lockHolder = new LockHolder(locker, lockType);

        if (firstWaiter != null && firstWaiter.equals(lockHolder)) {
            firstWaiter = null;
        } else if (otherWaiters != null) {
            Iterator<LockHolder> waiterIter = otherWaiters.iterator();
            while (waiterIter.hasNext()) {
                LockHolder waiter = waiterIter.next();
                if (waiter.equals(lockHolder)) {
                    waiterIter.remove();
                    return;
                }
            }
        }
    }

    public void addWaiterToEnd(LockHolder lockHolder) {
        if (otherWaiters == null) {
            if (firstWaiter == null) {
                firstWaiter = lockHolder;
            } else {
                otherWaiters = new ArrayList<>();
                otherWaiters.add(lockHolder);
            }
        } else {
            otherWaiters.add(lockHolder);
        }
    }

    @Override
    public List<LockHolder> cloneWaiters() {
        List<LockHolder> waiters = new ArrayList<>();
        if (firstWaiter != null) {
            waiters.add(firstWaiter.clone());
        }

        if (otherWaiters != null) {
            for (LockHolder lockHolder : otherWaiters) {
                waiters.add(lockHolder.clone());
            }
        }

        return waiters;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" LockAddr:").append(System.identityHashCode(this));
        sb.append(" Owners: ");
        if (ownerNum() == 0) {
            sb.append("(none)");
        } else {
            if (firstOwner != null) {
                sb.append(firstOwner);
            }

            if (otherOwners != null) {
                for (LockHolder lockHolder : otherOwners) {
                    sb.append(lockHolder);
                }
            }
        }

        sb.append(" Waiters: ");
        if (waiterNum() == 0) {
            sb.append("(none)");
        } else {
            if (firstWaiter != null) {
                sb.append(firstWaiter);
            }

            if (otherWaiters != null) {
                for (LockHolder lockHolder : otherWaiters) {
                    sb.append(lockHolder);
                }
            }
        }
        return sb.toString();
    }
}
