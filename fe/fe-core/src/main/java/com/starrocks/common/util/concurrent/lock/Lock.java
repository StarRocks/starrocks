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

import java.util.Set;

public abstract class Lock {
    public abstract LockGrantType lock(Locker locker, LockType lockType);

    public abstract Set<Locker> release(Locker locker, LockType lockType);

    public abstract boolean isOwner(Locker locker, LockType lockType);

    public abstract int ownerNum();

    public abstract int waiterNum();

    public abstract Set<LockHolder> getOwners();

    public abstract Set<LockHolder> cloneOwners();

    public abstract void removeWaiter(Locker locker, LockType lockType);
}