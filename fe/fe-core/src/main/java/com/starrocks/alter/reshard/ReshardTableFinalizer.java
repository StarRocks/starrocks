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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockInterruptException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;

import java.util.function.Consumer;

/** Small WAL finalization critical section: cancellation cannot abandon committed metadata. */
final class ReshardTableFinalizer {
    private ReshardTableFinalizer() {
    }

    static void withTableWrite(long dbId, long tableId, Consumer<OlapTable> action) {
        boolean interrupted = Thread.interrupted();
        Locker locker = new Locker();
        try {
            while (true) {
                boolean dbLocked = false;
                boolean tableLocked = false;
                try {
                    locker.lock(dbId, LockType.INTENTION_EXCLUSIVE);
                    dbLocked = true;
                    locker.lock(tableId, LockType.WRITE);
                    tableLocked = true;
                    Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
                    action.accept(table instanceof OlapTable ? (OlapTable) table : null);
                    return;
                } catch (LockInterruptException e) {
                    interrupted = true;
                    Thread.interrupted();
                } catch (LockException e) {
                    throw new TabletReshardException("Cannot finalize reshard job for table " + tableId, e);
                } finally {
                    if (tableLocked) {
                        locker.release(tableId, LockType.WRITE);
                    }
                    if (dbLocked) {
                        locker.release(dbId, LockType.INTENTION_EXCLUSIVE);
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
