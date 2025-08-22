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

package com.starrocks.alter.dynamictablet;

import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;

import java.util.List;

public class LockedObject<T> extends AutoCloseableLock {
    private final T object;

    public LockedObject(Long dbId, List<Long> tableList, LockType lockType, T object) {
        super(dbId, tableList, lockType);
        this.object = object;
    }

    public LockedObject(Locker locker, Long dbId, List<Long> tableList, LockType lockType, T object) {
        super(locker, dbId, tableList, lockType);
        this.object = object;
    }

    public T get() {
        return object;
    }
}
