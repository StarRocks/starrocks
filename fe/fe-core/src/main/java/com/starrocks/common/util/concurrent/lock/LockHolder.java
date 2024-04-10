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

import com.google.common.base.Objects;

public class LockHolder implements Cloneable {
    private final Locker locker;
    private final LockType lockType;
    private int refCount;

    public LockHolder(Locker locker, LockType lockType) {
        this.locker = locker;
        this.lockType = lockType;
        this.refCount = 1;
    }

    public Locker getLocker() {
        return locker;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void increaseRefCount() {
        refCount++;
    }

    public void decreaseRefCount() {
        refCount--;
    }

    public int getRefCount() {
        return refCount;
    }

    boolean isConflict(LockHolder lockHolderRequest) {
        if (lockHolderRequest.locker == this.locker
                && this.lockType == LockType.WRITE && lockHolderRequest.lockType == LockType.READ) {
            /*
             * If you acquire an exclusive lock first and then request a shared lock, you can successfully acquire the lock.
             * This scenario is generally called "lock downgrade",
             *  but this lock does not actually reduce the original write lock directly to a read lock.
             * In fact, it is still two independent read and write locks, and the two locks still need
             * to be released independently. The actual scenario is that before releasing the write lock,
             * acquire the read lock first, so that there is no gap time to release the lock.
             */
            return false;
        } else {
            return this.lockType.isConflict(lockHolderRequest.getLockType());
        }
    }

    @Override
    public String toString() {
        return "<locker=\"" + locker + "\" type=\"" + lockType + "\"/>";
    }

    @Override
    public LockHolder clone() {
        try {
            return (LockHolder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockHolder that = (LockHolder) o;
        return Objects.equal(locker, that.locker) && Objects.equal(lockType, that.lockType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(locker, lockType);
    }
}
