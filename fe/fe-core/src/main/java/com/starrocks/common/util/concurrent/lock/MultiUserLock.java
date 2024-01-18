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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MultiUserLock extends Lock {
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
    public LockGrantType lock(Locker locker, LockType lockType) {
        LockHolder lockHolderRequest = new LockHolder(locker, lockType);
        LockGrantType lockGrantType = tryLock(lockHolderRequest);
        if (lockGrantType == LockGrantType.NEW) {
            addOwner(lockHolderRequest);
        } else if (lockGrantType == LockGrantType.WAIT) {
            addWaiterToEnd(lockHolderRequest);
        }

        return lockGrantType;
    }

    private LockGrantType tryLock(LockHolder lockHolderRequest) {
        if (ownerNum() == 0) {
            return LockGrantType.NEW;
        }

        boolean hasConflicts = false;

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

        LockHolder sameLockHolder = null;
        while (lockOwner != null) {
            if (lockHolderRequest.equals(lockOwner)) {
                sameLockHolder = lockOwner;
            } else {
                boolean isConflict = lockOwner.isConflict(lockHolderRequest);
                if (isConflict) {
                    hasConflicts = true;
                }
            }

            if (ownerIterator != null && ownerIterator.hasNext()) {
                lockOwner = ownerIterator.next();
            } else {
                break;
            }
        }

        if (hasConflicts) {
            return LockGrantType.WAIT;
        } else {
            if (sameLockHolder != null) {
                sameLockHolder.increaseRefCount();
                return LockGrantType.EXISTING;
            } else {
                if (waiterNum() == 0) {
                    return LockGrantType.NEW;
                } else {
                    return LockGrantType.WAIT;
                }
            }
        }
    }

    @Override
    public Set<Locker> release(Locker locker, LockType lockType) {
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
            LockGrantType lockGrantType = tryLock(lockWaiter);

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
