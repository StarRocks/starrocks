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
package com.starrocks.meta.lock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MultiUserLock extends Lock {
    protected LockHolder firstOwner;
    protected LockHolder firstWaiter;
    private List<LockHolder> otherOwners;
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
        } else if (lockGrantType == LockGrantType.WAIT_NEW || lockGrantType == LockGrantType.WAIT_PROMOTION) {
            addWaiterToEnd(lockHolderRequest);
        }

        return lockGrantType;
    }

    private LockGrantType tryLock(LockHolder lockHolderRequest) {
        if (ownerNum() == 0) {
            return LockGrantType.NEW;
        }

        boolean ownerExists = false;
        boolean hasConflicts = false;

        LockHolder lockOwner = null;
        Iterator<LockHolder> ownerIterator = null;
        if (firstOwner != null) {
            lockOwner = firstOwner;
        } else {
            if (otherOwners != null) {
                ownerIterator = otherOwners.iterator();
                if (ownerIterator.hasNext()) {
                    lockOwner = ownerIterator.next();
                }
            }
        }

        LockHolder lockToUpgrade = null;
        while (lockOwner != null) {
            if (lockHolderRequest == lockOwner) {
                boolean isUpgrade = lockOwner.getLockType().upgradeTo(lockHolderRequest.getLockType());
                if (isUpgrade) {
                    lockToUpgrade = lockOwner;
                } else {
                    return LockGrantType.EXISTING;
                }
            } else {
                boolean isConflict = lockOwner.getLockType().isConflict(lockHolderRequest.getLockType());
                if (isConflict) {
                    hasConflicts = true;
                }

                ownerExists = true;
            }

            if (ownerIterator != null && ownerIterator.hasNext()) {
                lockOwner = ownerIterator.next();
            } else {
                lockOwner = null;
            }
        }

        if (lockToUpgrade != null) {
            if (!hasConflicts) {
                lockToUpgrade.setLockType(lockHolderRequest.getLockType());
                return LockGrantType.PROMOTION;
            } else {
                return LockGrantType.WAIT_PROMOTION;
            }
        } else {
            if (!hasConflicts && (!ownerExists || waiterNum() == 0)) {
                return LockGrantType.NEW;
            } else {
                return LockGrantType.WAIT_NEW;
            }
        }
    }

    @Override
    public Set<Locker> release(Locker locker) {
        boolean hasOwner = false;
        if (firstOwner != null && firstOwner.getLocker() == locker) {
            firstOwner = null;
            hasOwner = true;
        } else if (otherOwners != null) {
            Iterator<LockHolder> iter = otherOwners.iterator();
            while (iter.hasNext()) {
                LockHolder o = iter.next();
                if (o.getLocker() == locker) {
                    iter.remove();
                    hasOwner = true;
                }
            }
        }

        if (!hasOwner) {
            return null;
        }

        Set<Locker> lockersToNotify = new HashSet<>();

        if (waiterNum() == 0) {
            return lockersToNotify;
        }

        boolean isFirstWaiter = false;
        LockHolder lockWaiter = null;
        Iterator<LockHolder> lockWaiterIterator = null;
        if (firstWaiter != null) {
            lockWaiter = firstWaiter;
            isFirstWaiter = true;
        } else {
            if (otherWaiters != null) {
                lockWaiterIterator = otherWaiters.iterator();
                if (lockWaiterIterator.hasNext()) {
                    lockWaiter = lockWaiterIterator.next();
                }
            }
        }

        while (lockWaiter != null) {
            LockGrantType lockGrantType = tryLock(lockWaiter);

            if (lockGrantType == LockGrantType.NEW
                    || lockGrantType == LockGrantType.EXISTING
                    || lockGrantType == LockGrantType.PROMOTION) {
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
                assert lockGrantType == LockGrantType.WAIT_NEW ||
                        lockGrantType == LockGrantType.WAIT_PROMOTION;
                /* Stop on first waiter that cannot be an owner. */
                break;
            }

            if (lockWaiterIterator != null && lockWaiterIterator.hasNext()) {
                lockWaiter = lockWaiterIterator.next();
            } else {
                lockWaiter = null;
            }
        }

        return lockersToNotify;
    }

    @Override
    public boolean isOwner(Locker locker, LockType lockType) {
        if (firstOwner != null && firstOwner.getLocker() == locker) {
            return firstOwner.getLockType() == lockType;
        }

        if (otherOwners != null) {
            for (LockHolder lockHolder : otherOwners) {
                if (lockHolder.getLocker() == locker && lockHolder.getLockType() == lockType) {
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
                otherOwners = new ArrayList<>();
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
    public void removeWaiter(Locker locker) {
        if (firstWaiter != null && firstWaiter.getLocker() == locker) {
            firstWaiter = null;
        } else if (otherWaiters != null) {
            Iterator<LockHolder> waiterIter = otherWaiters.iterator();
            while (waiterIter.hasNext()) {
                LockHolder waiter = waiterIter.next();
                if (waiter.getLocker() == locker) {
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
}
