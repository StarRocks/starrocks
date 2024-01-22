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

import java.util.HashSet;
import java.util.Set;

public class LightWeightLock extends Lock {
    private LockHolder lockHolder;

    public LightWeightLock() {
    }

    @Override
    public LockGrantType lock(Locker locker, LockType requestLockType) {
        assert lockHolder == null;
        this.lockHolder = new LockHolder(locker, requestLockType);
        return LockGrantType.NEW;
    }

    @Override
    public Set<Locker> release(Locker locker, LockType lockType) {
        if (lockHolder.equals(new LockHolder(locker, lockType))) {
            this.lockHolder = null;
            return null;
        } else {
            throw new IllegalMonitorStateException("Attempt to unlock lock, not locked by current locker");
        }
    }

    @Override
    public boolean isOwner(Locker locker, LockType lockType) {
        LockHolder requestLockHolder = new LockHolder(locker, lockType);
        return requestLockHolder.equals(lockHolder);
    }

    @Override
    public int ownerNum() {
        return lockHolder == null ? 0 : 1;
    }

    @Override
    public int waiterNum() {
        return 0;
    }

    @Override
    public Set<LockHolder> getOwners() {
        Set<LockHolder> ret = new HashSet<>();
        if (lockHolder != null) {
            ret.add(lockHolder);
        }
        return ret;
    }

    @Override
    public Set<LockHolder> cloneOwners() {
        Set<LockHolder> ret = new HashSet<>();
        if (lockHolder != null) {
            ret.add(lockHolder.clone());
        }
        return ret;
    }

    @Override
    public void removeWaiter(Locker locker, LockType lockType) {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" LockAddr:").append(System.identityHashCode(this));
        sb.append(" Owner:");
        if (ownerNum() == 0) {
            sb.append(" (none)");
        } else {
            sb.append(lockHolder);
        }

        sb.append(" Waiters: (none)");
        return sb.toString();
    }
}
