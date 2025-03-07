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

    /* The time Locker successfully acquire possession of the lock */
    private long lockAcquireTimeMs;

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

    public long getLockAcquireTimeMs() {
        return lockAcquireTimeMs;
    }

    public void setLockAcquireTimeMs(long lockAcquireTimeMs) {
        this.lockAcquireTimeMs = lockAcquireTimeMs;
    }

    boolean isConflict(LockHolder lockHolderRequest) {
        return this.lockType.isConflict(lockHolderRequest.getLockType());
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
