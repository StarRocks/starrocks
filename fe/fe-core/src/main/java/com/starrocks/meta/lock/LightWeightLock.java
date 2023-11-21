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

import java.util.Set;

public class LightWeightLock extends Lock {
    private LockHolder lockHolder;

    public LightWeightLock() {
    }

    @Override
    public LockGrantType lock(Locker locker, LockType requestLockType) {
        if (this.lockHolder == null) {
            this.lockHolder = new LockHolder(locker, requestLockType);
            return LockGrantType.NEW;
        } else {
            LockType lockType = lockHolder.getLockType();
            boolean upgrade = lockType.upgradeTo(requestLockType);
            if (upgrade) {
                return LockGrantType.PROMOTION;
            } else {
                return LockGrantType.EXISTING;
            }
        }
    }

    @Override
    public Set<Locker> release(Locker locker) {
        if (locker == lockHolder.getLocker()) {
            this.lockHolder = null;
        }

        return null;
    }

    @Override
    public boolean isOwner(Locker locker, LockType lockType) {
        return lockHolder.getLocker() == locker && lockHolder.getLockType() == lockType;
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
    public void removeWaiter(Locker locker) {

    }

    public Locker getOwner() {
        return lockHolder.getLocker();
    }

    public LockHolder getLockHolder() {
        return lockHolder;
    }
}
