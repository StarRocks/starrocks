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

import com.starrocks.catalog.Database;

import java.util.ArrayList;
import java.util.List;

public class AutoCloseableLock implements AutoCloseable {
    private final Locker locker;
    private final Database database;
    private final List<Long> tableList;
    private final LockType lockType;

    public AutoCloseableLock(Locker locker, Database database, List<Long> tableList, LockType lockType) {
        this.locker = locker;
        this.database = database;
        this.tableList = new ArrayList<>(tableList);
        this.lockType = lockType;

        locker.lockTablesWithIntensiveDbLock(database, tableList, lockType);
    }

    @Override
    public void close() {
        locker.unLockTablesWithIntensiveDbLock(database, tableList, lockType);
    }
}
